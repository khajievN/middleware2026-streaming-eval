"""Plot Graph 1 (OOM survival) and Graph 2 (VSock throughput) from live results.

Reads only the JSON written by parent_driver.py. If a results file is missing or
a series is empty, the corresponding figure is skipped with a warning. No data
is interpolated, extrapolated, or synthesized: the curves are exactly what the
enclave reported.
"""
import argparse
import json
import os
from typing import Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

WORKLOAD_LABELS = {
    "baseline_full": "Single-payload (buffer + parse)",
    "stream_agg": "Chunked streaming, streamable op",
    "stream_block": "Chunked streaming, blocking op (sort)",
}
WORKLOAD_STYLE = {
    "baseline_full": {"marker": "o", "linestyle": "-"},
    "stream_agg": {"marker": "s", "linestyle": "-"},
    "stream_block": {"marker": "^", "linestyle": "--"},
}


def _load(path: str) -> Optional[dict]:
    if not os.path.exists(path):
        print(f"[skip] {path} not found")
        return None
    with open(path) as fh:
        return json.load(fh)


def plot_oom(path: str, out: str) -> None:
    data = _load(path)
    if not data or not data.get("records"):
        print(f"[skip] no OOM records in {path}")
        return
    ceiling = data.get("enclave_memory_mb")
    fig, ax = plt.subplots(figsize=(5.2, 3.4))
    by_workload: dict = {}
    for rec in data["records"]:
        by_workload.setdefault(rec["workload"], []).append(rec)

    for workload, recs in by_workload.items():
        recs.sort(key=lambda r: r["payload_mb"])
        xs = [r["payload_mb"] for r in recs if r.get("peak_rss_mb") is not None]
        ys = [r["peak_rss_mb"] for r in recs if r.get("peak_rss_mb") is not None]
        style = WORKLOAD_STYLE.get(workload, {"marker": "o", "linestyle": "-"})
        if xs:
            ax.plot(xs, ys, label=WORKLOAD_LABELS.get(workload, workload),
                    markersize=4, **style)
        oom = [r["payload_mb"] for r in recs if r.get("status") in ("oom", "killed")]
        if oom and ceiling:
            ax.scatter(oom, [ceiling] * len(oom), marker="x", s=80,
                       color="red", zorder=5,
                       label="OOM kill" if workload == "baseline_full" else None)

    if ceiling:
        ax.axhline(ceiling, color="grey", linestyle=":", linewidth=1)
        ax.text(ax.get_xlim()[1], ceiling, f" {ceiling} MB enclave ceiling",
                va="bottom", ha="right", fontsize=7, color="grey")
    ax.set_xscale("log")
    ax.set_xlabel("Ingested payload (MB, log scale)")
    ax.set_ylabel("Enclave peak RSS (MB)")
    ax.legend(fontsize=7, loc="upper left")
    ax.grid(True, which="both", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out, bbox_inches="tight")
    fig.savefig(out.replace(".pdf", ".png"), dpi=150, bbox_inches="tight")
    print(f"wrote {out}")


def plot_throughput(path: str, out: str) -> None:
    data = _load(path)
    if not data or not data.get("records"):
        print(f"[skip] no throughput records in {path}")
        return
    recs = sorted(data["records"], key=lambda r: r["chunk_size_bytes"])
    xs = [r["chunk_size_bytes"] / 1024 for r in recs]
    ys = [r["mbps_median"] for r in recs]
    lo = [r["mbps_median"] - r["mbps_min"] for r in recs]
    hi = [r["mbps_max"] - r["mbps_median"] for r in recs]
    fig, ax = plt.subplots(figsize=(5.2, 3.4))
    ax.errorbar(xs, ys, yerr=[lo, hi], marker="o", markersize=4, capsize=3)
    ax.set_xscale("log")
    ax.set_xlabel("Send chunk size (KB, log scale)")
    ax.set_ylabel("Sustained VSock throughput (MB/s)")
    ax.grid(True, which="both", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out, bbox_inches="tight")
    fig.savefig(out.replace(".pdf", ".png"), dpi=150, bbox_inches="tight")
    print(f"wrote {out}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", default="results")
    parser.add_argument("--oom", default="oom.json")
    parser.add_argument("--throughput", default="throughput.json")
    args = parser.parse_args()
    plot_oom(os.path.join(args.results_dir, args.oom),
             os.path.join(args.results_dir, "graph1_oom_survival.pdf"))
    plot_throughput(os.path.join(args.results_dir, args.throughput),
                    os.path.join(args.results_dir, "graph2_vsock_throughput.pdf"))


if __name__ == "__main__":
    main()
