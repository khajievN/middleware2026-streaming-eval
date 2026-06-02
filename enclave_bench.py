"""Enclave-side benchmark server for the Middleware 2026 streaming evaluation.

Runs inside the Nitro enclave (vsock) or on a host for smoke tests (tcp). Two
request modes selected by the JSON control header:

  sink      : drain the data stream, report bytes received and server-side
              wall time. Used for the VSock throughput sweep (Graph 2).

  workload  : fork a child that consumes the data stream under one of three
              memory models, report the child's peak RSS, or report `oom` if
              the enclave kernel kills the child (no swap, no spill target).
              Used for the OOM-survival sweep (Graph 1).

Forking per workload request is what lets a single enclave boot sweep every
payload size: an OOM kill targets the memory-heavy child, the listener parent
survives to report the failure and accept the next request.

Workloads:
  baseline_full : accumulate the whole payload, then parse + describe it
                  (the production single-payload model).
  stream_agg    : parse the CSV stream row-by-row, keep only running
                  per-column aggregates (a streamable operator).
  stream_block  : stream the transport but accumulate every row and then run a
                  global sort (a blocking operator with nowhere to spill).
"""
import argparse
import io
import json
import logging
import os
import signal
import sys
import time
from typing import Optional

import pandas as pd

import framing

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s [enclave-bench] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def _drain_sink(sock) -> dict:
    """Consume every data frame, timing from the first byte to the end marker."""
    total = 0
    started: Optional[float] = None
    while True:
        frame = framing.recv_frame(sock)
        if frame is None:
            raise ConnectionError("peer closed before end marker")
        if frame == b"":
            break
        if started is None:
            started = time.monotonic()
        total += len(frame)
    elapsed = (time.monotonic() - started) if started else 0.0
    return {"status": "ok", "bytes": total, "seconds": round(elapsed, 6)}


def _iter_rows(sock):
    """Yield decoded CSV lines from the frame stream without buffering the body.

    Holds at most one partial line plus one inbound chunk, so resident memory is
    bounded by chunk size regardless of total payload.
    """
    carry = b""
    header_skipped = False
    while True:
        frame = framing.recv_frame(sock)
        if frame is None:
            raise ConnectionError("peer closed before end marker")
        if frame == b"":
            if carry:
                yield carry
            break
        carry += frame
        *lines, carry = carry.split(b"\n")
        for line in lines:
            if not header_skipped:
                header_skipped = True
                continue
            if line:
                yield line


def _workload_baseline_full(sock) -> dict:
    """Production single-payload model: buffer everything, then materialize."""
    buf = bytearray()
    while True:
        frame = framing.recv_frame(sock)
        if frame is None:
            raise ConnectionError("peer closed before end marker")
        if frame == b"":
            break
        buf.extend(frame)
    df = pd.read_csv(io.BytesIO(bytes(buf)))
    _ = df.describe(include="all")  # forces full materialization
    return {"rows": int(len(df))}


def _workload_stream_agg(sock) -> dict:
    """Streamable operator: running count/sum/min/max over valuenum."""
    rows = 0
    count = 0
    total = 0.0
    vmin = float("inf")
    vmax = float("-inf")
    for line in _iter_rows(sock):
        rows += 1
        cols = line.split(b",")
        # labevents valuenum is column index 9 (0-based) in the demo schema
        if len(cols) > 9 and cols[9]:
            try:
                v = float(cols[9])
            except ValueError:
                continue
            count += 1
            total += v
            vmin = min(vmin, v)
            vmax = max(vmax, v)
    mean = (total / count) if count else 0.0
    return {"rows": rows, "valuenum_count": count, "valuenum_mean": round(mean, 6)}


def _workload_stream_block(sock) -> dict:
    """Blocking operator: accumulate every row, then a global sort. Streaming the
    transport does not help; with no spill target this OOMs near the baseline."""
    values = []
    for line in _iter_rows(sock):
        cols = line.split(b",")
        if len(cols) > 9 and cols[9]:
            try:
                values.append((float(cols[9]), line))
            except ValueError:
                continue
    values.sort(key=lambda t: t[0])  # global sort: materializes the full set
    return {"rows": len(values)}


_WORKLOADS = {
    "baseline_full": _workload_baseline_full,
    "stream_agg": _workload_stream_agg,
    "stream_block": _workload_stream_block,
}


def _run_workload_child(sock, workload: str, write_fd: int) -> None:
    """Child process: run the workload, write a result JSON to the pipe, exit.

    If the kernel OOM-kills this process, nothing is written and the parent
    detects the signal via waitpid.
    """
    started = time.monotonic()
    try:
        extra = _WORKLOADS[workload](sock)
        result = {
            "status": "ok",
            "workload": workload,
            "peak_rss_mb": round(framing.peak_rss_mb(), 2),
            "seconds": round(time.monotonic() - started, 6),
            **extra,
        }
    except Exception as exc:  # report, don't crash the measurement
        result = {"status": "error", "workload": workload, "error": str(exc)}
    os.write(write_fd, json.dumps(result).encode("utf-8"))
    os.close(write_fd)
    os._exit(0)


def _handle_workload(sock, workload: str) -> dict:
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(read_fd)
        _run_workload_child(sock, workload, write_fd)
        return {}  # unreachable
    os.close(write_fd)
    payload = b""
    while True:
        block = os.read(read_fd, 65536)
        if not block:
            break
        payload += block
    os.close(read_fd)
    _, status = os.waitpid(pid, 0)
    if os.WIFSIGNALED(status):
        sig = os.WTERMSIG(status)
        reason = "oom" if sig in (signal.SIGKILL, signal.SIGABRT) else "killed"
        return {"status": reason, "workload": workload, "signal": sig}
    if payload:
        return json.loads(payload.decode("utf-8"))
    return {"status": "oom", "workload": workload, "note": "child exited without result"}


def handle_connection(sock) -> None:
    control = framing.recv_control(sock)
    if control is None:
        return
    mode = control.get("mode")
    logger.info("request mode=%s detail=%s", mode, control)
    if mode == "sink":
        result = _drain_sink(sock)
    elif mode == "workload":
        result = _handle_workload(sock, control.get("workload", "baseline_full"))
    else:
        result = {"status": "error", "error": f"unknown mode {mode}"}
    try:
        framing.send_control(sock, result)
    except OSError:
        logger.warning("could not send reply (peer gone): %s", result)
    logger.info("reply %s", result)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--transport", choices=["vsock", "tcp"], default="vsock")
    parser.add_argument("--port", type=int, default=framing.VSOCK_BENCH_PORT)
    args = parser.parse_args()

    try:
        listener = framing.open_listener(args.transport, args.port)
        logger.info("listening transport=%s port=%d", args.transport, args.port)
        while True:
            conn, addr = listener.accept()
            logger.info("connection from %s", addr)
            try:
                handle_connection(conn)
            except Exception as exc:
                logger.error("connection error: %s", exc)
            finally:
                try:
                    conn.close()
                except OSError:
                    pass
    except Exception:
        # A crash here would terminate the enclave before `nitro-cli console`
        # can attach; hold the process so the traceback stays observable.
        logger.exception("FATAL: bench server crashed during startup/run")
        sys.stdout.flush()
        time.sleep(60)
        raise


if __name__ == "__main__":
    main()
