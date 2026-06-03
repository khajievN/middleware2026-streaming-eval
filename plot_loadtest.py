"""Plot the coordinator load-test results: latency CDF (Task 1) and burst-queue
scatter (Task 2). Reads only the JSON produced by loadtest_cdf.py /
loadtest_burst.py; no data is synthesized.
"""
import argparse
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def _load(path):
    if not os.path.exists(path):
        print(f"[skip] {path} not found")
        return None
    with open(path) as fh:
        return json.load(fh)


def plot_cdf(path: str, out: str) -> None:
    data = _load(path)
    if not data or not data.get("records"):
        return
    lat = sorted(r["wall_latency_s"] for r in data["records"]
                 if r["status"] == "success" and r.get("wall_latency_s") is not None)
    if not lat:
        print("[skip] no successful latency samples for CDF")
        return
    n = len(lat)
    ys = [(i + 1) / n for i in range(n)]
    fig, ax = plt.subplots(figsize=(5.2, 3.4))
    ax.plot(lat, ys, marker=".", markersize=3)
    for q, label in [(0.50, "p50"), (0.95, "p95"), (0.99, "p99")]:
        idx = min(n - 1, int(round(q * n)) - 1)
        v = lat[max(0, idx)]
        ax.axvline(v, color="grey", linestyle=":", linewidth=1)
        ax.text(v, q, f" {label}={v:.1f}s", fontsize=7, va="center")
    ax.set_xlabel("Warm end-to-end latency (s)")
    ax.set_ylabel("Cumulative fraction of jobs")
    ax.set_ylim(0, 1.02)
    ax.grid(True, alpha=0.3)
    fig.tight_layout(); fig.savefig(out, bbox_inches="tight")
    fig.savefig(out.replace(".pdf", ".png"), dpi=150, bbox_inches="tight")
    print(f"wrote {out}  (n={n}, p50={lat[int(0.5*n)-1]:.1f}s, "
          f"p95={lat[min(n-1,int(0.95*n)-1)]:.1f}s, p99={lat[min(n-1,int(0.99*n)-1)]:.1f}s)")


def plot_burst(path: str, out: str) -> None:
    data = _load(path)
    if not data or not data.get("records"):
        return
    recs = [r for r in data["records"] if r.get("time_to_completion_s") is not None]
    if not recs:
        print("[skip] no completed burst jobs")
        return
    xs = [r["submit_offset_s"] for r in recs]
    ys = [r["time_to_completion_s"] for r in recs]
    fig, ax = plt.subplots(figsize=(5.2, 3.4))
    ax.scatter(xs, ys, s=18)
    ax.set_xlabel("Job arrival time (s from burst start)")
    ax.set_ylabel("Time-to-completion (s)")
    ax.grid(True, alpha=0.3)
    fig.tight_layout(); fig.savefig(out, bbox_inches="tight")
    fig.savefig(out.replace(".pdf", ".png"), dpi=150, bbox_inches="tight")
    print(f"wrote {out}  ({len(recs)} jobs)")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--results-dir", default="results")
    p.add_argument("--cdf", default="cdf_warm.json")
    p.add_argument("--burst", default="burst.json")
    args = p.parse_args()
    plot_cdf(os.path.join(args.results_dir, args.cdf),
             os.path.join(args.results_dir, "graph3_latency_cdf.pdf"))
    plot_burst(os.path.join(args.results_dir, args.burst),
               os.path.join(args.results_dir, "graph4_burst_queue.pdf"))


if __name__ == "__main__":
    main()
