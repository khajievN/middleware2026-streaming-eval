"""Parent-side driver for the Middleware 2026 streaming evaluation.

Runs on the EC2 parent instance and drives the enclave bench over vsock (or a
host over tcp for smoke tests). Two subcommands:

  throughput : sweep the send chunk size against a fixed total, recording
               server-authoritative sustained MB/s per chunk size (Graph 2).

  oom        : sweep the ingested payload size for each memory model, recording
               the enclave's peak RSS or an `oom` verdict (Graph 1). Payload
               bytes are streamed from the scaled MIMIC CSV in whole lines, so
               the parent never holds the payload in memory either.

Every number written here comes from a live run. Nothing is synthesized.
"""
import argparse
import datetime
import json
import os
import socket
import statistics
from typing import Iterator, Optional

import framing

ENCLAVE_CID_DEFAULT = 16  # typical Nitro enclave CID; override with --cid


def _const_chunks(total: int, chunk_size: int) -> Iterator[bytes]:
    """Yield `chunk_size` blocks of filler until `total` bytes are produced,
    without allocating `total` bytes on the parent."""
    block = b"\x00" * chunk_size
    sent = 0
    while sent < total:
        remaining = total - sent
        yield block if remaining >= chunk_size else block[:remaining]
        sent += chunk_size


def _csv_chunks(path: str, target_bytes: int, chunk_size: int) -> Iterator[bytes]:
    """Stream whole CSV lines from `path` up to ~target_bytes, batched into
    chunk_size frames. The header line is always emitted first."""
    buf = bytearray()
    sent = 0
    with open(path, "rb") as fh:
        header = fh.readline()
        buf.extend(header)
        for line in fh:
            buf.extend(line)
            if len(buf) >= chunk_size:
                while len(buf) >= chunk_size:
                    yield bytes(buf[:chunk_size])
                    del buf[:chunk_size]
                    sent += chunk_size
            if sent >= target_bytes:
                break
    if buf:
        yield bytes(buf)


def _run_once(transport: str, cid: int, port: int, control: dict,
              chunks: Iterator[bytes], timeout: float) -> dict:
    """Open a connection, send the control header, stream chunks, return the
    enclave's reply. A connection drop mid-stream is reported as an oom."""
    sock = framing.connect(transport, cid, port, timeout)
    try:
        framing.send_control(sock, control)
        for chunk in chunks:
            framing.send_frame(sock, chunk)
        framing.send_frame(sock, b"")  # end marker
        reply = framing.recv_control(sock)
        if reply is None:
            return {"status": "oom", "note": "no reply (connection closed)"}
        return reply
    except (ConnectionError, OSError, socket.timeout) as exc:
        return {"status": "oom", "note": f"connection error: {exc}"}
    finally:
        sock.close()


def _flush(args, result) -> None:
    """Write current results to disk with run metadata. Called after every
    record so an interrupted run (Ctrl-C, OOM, crash) still leaves a valid,
    partial results file rather than nothing."""
    out = dict(result)
    out["meta"] = {
        "instance_type": args.instance_type,
        "enclave_memory_mb": args.enclave_memory_mb,
        "enclave_cpus": args.enclave_cpus,
        "transport": args.transport,
        "utc": datetime.datetime.utcnow().isoformat() + "Z",
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w") as fh:
        json.dump(out, fh, indent=2)


def cmd_throughput(args) -> dict:
    chunk_sizes = [int(c) for c in args.chunk_sizes.split(",")]
    records = []
    for chunk_size in chunk_sizes:
        samples = []
        for trial in range(args.trials):
            reply = _run_once(
                args.transport, args.cid, args.port,
                {"mode": "sink", "chunk_size": chunk_size, "trial": trial},
                _const_chunks(args.total_bytes, chunk_size),
                args.timeout,
            )
            if reply.get("status") == "ok" and reply.get("seconds", 0) > 0:
                mbps = (reply["bytes"] / (1024 * 1024)) / reply["seconds"]
                samples.append(mbps)
        if samples:
            records.append({
                "chunk_size_bytes": chunk_size,
                "trials": len(samples),
                "mbps_median": round(statistics.median(samples), 3),
                "mbps_min": round(min(samples), 3),
                "mbps_max": round(max(samples), 3),
                "mbps_samples": [round(s, 3) for s in samples],
            })
        print(f"chunk {chunk_size:>9}B -> {records[-1] if samples else 'no successful trial'}")
        _flush(args, {"experiment": "throughput", "total_bytes": args.total_bytes, "records": records})
    return {"experiment": "throughput", "total_bytes": args.total_bytes, "records": records}


def cmd_oom(args) -> dict:
    sizes_mb = [int(s) for s in args.sizes_mb.split(",")]
    workloads = args.workloads.split(",")
    records = []
    for workload in workloads:
        for size_mb in sizes_mb:
            target = size_mb * 1024 * 1024
            reply = _run_once(
                args.transport, args.cid, args.port,
                {"mode": "workload", "workload": workload,
                 "chunk_size": args.chunk_size, "target_mb": size_mb},
                _csv_chunks(args.data, target, args.chunk_size),
                args.timeout,
            )
            rec = {"workload": workload, "payload_mb": size_mb,
                   "status": reply.get("status"),
                   "peak_rss_mb": reply.get("peak_rss_mb"),
                   "rows": reply.get("rows"),
                   "seconds": reply.get("seconds"),
                   "note": reply.get("note")}
            records.append(rec)
            print(f"{workload:<14} {size_mb:>6} MB -> {rec['status']} "
                  f"(rss={rec['peak_rss_mb']} MB)")
            _flush(args, {"experiment": "oom", "enclave_memory_mb": args.enclave_memory_mb,
                          "records": records})
            if reply.get("status") in ("oom", "killed") and args.stop_on_oom:
                print(f"  {workload}: OOM at {size_mb} MB; skipping larger sizes")
                break
    return {"experiment": "oom", "enclave_memory_mb": args.enclave_memory_mb,
            "records": records}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--transport", choices=["vsock", "tcp"], default="vsock")
    parser.add_argument("--cid", type=int, default=ENCLAVE_CID_DEFAULT)
    parser.add_argument("--port", type=int, default=framing.VSOCK_BENCH_PORT)
    parser.add_argument("--timeout", type=float, default=900.0)
    parser.add_argument("--instance-type", default="c5a.xlarge")
    parser.add_argument("--enclave-memory-mb", type=int, default=4096)
    parser.add_argument("--enclave-cpus", type=int, default=2)
    parser.add_argument("--out", required=True, help="results JSON path")
    sub = parser.add_subparsers(dest="cmd", required=True)

    tp = sub.add_parser("throughput")
    tp.add_argument("--chunk-sizes", default="4096,16384,65536,262144,1048576,4194304")
    tp.add_argument("--total-bytes", type=int, default=2 * 1024 * 1024 * 1024)
    tp.add_argument("--trials", type=int, default=5)
    tp.set_defaults(func=cmd_throughput)

    oom = sub.add_parser("oom")
    oom.add_argument("--data", required=True, help="scaled MIMIC CSV path")
    oom.add_argument("--sizes-mb", default="50,100,200,247,300,500,1024,2048,4096,8192,10240")
    oom.add_argument("--workloads", default="baseline_full,stream_agg,stream_block")
    oom.add_argument("--chunk-size", type=int, default=1048576)
    oom.add_argument("--stop-on-oom", action="store_true")
    oom.set_defaults(func=cmd_oom)

    args = parser.parse_args()
    result = args.func(args)
    _flush(args, result)  # final authoritative write (records were already flushed incrementally)
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
