"""Shared transport, chunked framing, and resident-memory sampling for the
Middleware 2026 streaming evaluation.

Protocol (deliberately distinct from the production length-prefix protocol,
which caps at 4 GiB via its 4-byte header and so cannot frame a 10 GB payload):

    control frame : [4-byte BE length][UTF-8 JSON control header]
    data frames   : repeated [4-byte BE length][bytes], chunk-sized
    end marker     : [4-byte BE length == 0]
    reply frame   : [4-byte BE length][UTF-8 JSON result]

A length of 0 terminates the data stream, so neither side needs to know the
total payload size in advance. This is what lets the streaming workloads run a
10 GB payload through a 4 GB enclave: bytes are consumed frame-by-frame and
never accumulated unless the chosen workload explicitly buffers them.
"""
import json
import resource
import socket
import struct
import sys
from typing import Optional

HEADER = struct.Struct("!I")
HEADER_SIZE = HEADER.size
VSOCK_BENCH_PORT = 5006  # adjacent to the production enclave's 5005


def open_listener(transport: str, port: int) -> socket.socket:
    """Bind a listening socket on the enclave side (vsock) or a host (tcp)."""
    if transport == "vsock":
        # AF_VSOCK rejects SO_REUSEADDR on the enclave kernel (raises before
        # listen); the production enclave server binds without it, so we match.
        sock = socket.socket(socket.AF_VSOCK, socket.SOCK_STREAM)
        sock.bind((socket.VMADDR_CID_ANY, port))
    else:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", port))
    sock.listen(8)
    return sock


def connect(transport: str, cid: int, port: int, timeout: float) -> socket.socket:
    """Open a client connection from the parent to the enclave (vsock) or host (tcp)."""
    if transport == "vsock":
        sock = socket.socket(socket.AF_VSOCK, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((cid, port))
    else:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect(("127.0.0.1", port))  # cid is unused for tcp smoke tests
    return sock


def recv_exact(sock: socket.socket, n: int) -> Optional[bytes]:
    """Receive exactly n bytes, or None if the peer closes first."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


def send_frame(sock: socket.socket, payload: bytes) -> None:
    """Send one length-prefixed frame."""
    sock.sendall(HEADER.pack(len(payload)) + payload)


def recv_frame(sock: socket.socket) -> Optional[bytes]:
    """Receive one length-prefixed frame. Returns b'' on the zero-length end
    marker and None if the connection drops."""
    head = recv_exact(sock, HEADER_SIZE)
    if head is None:
        return None
    (length,) = HEADER.unpack(head)
    if length == 0:
        return b""
    return recv_exact(sock, length)


def send_control(sock: socket.socket, header: dict) -> None:
    send_frame(sock, json.dumps(header).encode("utf-8"))


def recv_control(sock: socket.socket) -> Optional[dict]:
    raw = recv_frame(sock)
    if not raw:
        return None
    return json.loads(raw.decode("utf-8"))


def peak_rss_mb() -> float:
    """Peak resident set size of the current process, in MB.

    Prefers Linux VmHWM (the kernel high-water mark, exact even when sampled
    once at the end). Falls back to getrusage for non-Linux smoke tests, where
    ru_maxrss is bytes on macOS and kilobytes elsewhere.
    """
    try:
        with open("/proc/self/status", "r") as fh:
            for line in fh:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) / 1024.0
    except OSError:
        pass
    maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return maxrss / (1024.0 * 1024.0) if sys.platform == "darwin" else maxrss / 1024.0
