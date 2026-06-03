"""Standalone profiler for the coordinator deserialization bottleneck.

The end-to-end breakdown attributes a ${\sim}2$s warm-path ceiling to the
coordinator parsing the decrypted query result, a list-of-dicts of ~100K rows,
in Python. That figure is currently derived by subtraction and uninstrumented;
this script measures it directly and A/B-tests the standard-library `json`
parser against the C-backed `orjson`, with an optional cProfile breakdown.

It needs no AWS infrastructure: it reconstructs a representative 100K-row
MIMIC-IV `labevents` payload (16 columns), serialises it once, then times the
parse path that runs on the coordinator. Every number printed is measured here.

Usage:
  python3 parse_bench.py --rows 100000 --trials 10
  python3 parse_bench.py --rows 100000 --profile      # cProfile the json.loads
"""
import argparse
import cProfile
import io
import json
import pstats
import statistics
import time

# MIMIC-IV v2.2 labevents columns (the cohort query result the coordinator parses)
COLUMNS = [
    "labevent_id", "subject_id", "hadm_id", "specimen_id", "itemid",
    "order_provider_id", "charttime", "storetime", "value", "valuenum",
    "valueuom", "ref_range_lower", "ref_range_upper", "flag", "priority", "comments",
]


def build_payload(rows: int) -> list:
    """A representative list-of-dicts, the in-memory shape the coordinator
    deserialises the decrypted query result into."""
    base = {
        "labevent_id": 1, "subject_id": 10000032, "hadm_id": 22595853,
        "specimen_id": 6543, "itemid": 51221, "order_provider_id": "P49AFC",
        "charttime": "2180-05-06 22:25:00", "storetime": "2180-05-06 23:34:00",
        "value": "12.4", "valuenum": 12.4, "valueuom": "g/dL",
        "ref_range_lower": 12.0, "ref_range_upper": 16.0, "flag": "",
        "priority": "STAT", "comments": "",
    }
    return [dict(base, labevent_id=i, subject_id=10000000 + (i % 100000)) for i in range(rows)]


def time_parser(name: str, loads, raw: bytes, trials: int) -> dict:
    samples = []
    for _ in range(trials):
        start = time.perf_counter()
        obj = loads(raw)
        samples.append(time.perf_counter() - start)
        del obj
    return {
        "parser": name,
        "trials": trials,
        "ms_median": round(statistics.median(samples) * 1000, 2),
        "ms_min": round(min(samples) * 1000, 2),
        "ms_max": round(max(samples) * 1000, 2),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--trials", type=int, default=10)
    parser.add_argument("--profile", action="store_true", help="cProfile the json.loads path")
    args = parser.parse_args()

    payload = build_payload(args.rows)
    raw = json.dumps(payload).encode("utf-8")
    print(f"payload: {args.rows} rows, {len(raw) / (1024 * 1024):.1f} MB serialised JSON")

    results = [time_parser("json (stdlib)", json.loads, raw, args.trials)]
    try:
        import orjson  # optional; reported only if installed
        results.append(time_parser("orjson (C-backed)", orjson.loads, raw, args.trials))
    except ImportError:
        print("orjson not installed (pip install orjson) -- skipping A/B comparison")

    for r in results:
        print(f"  {r['parser']:<18} median {r['ms_median']:>8} ms  "
              f"(min {r['ms_min']}, max {r['ms_max']})")
    if len(results) == 2:
        speedup = results[0]["ms_median"] / results[1]["ms_median"]
        reduction = 100 * (1 - results[1]["ms_median"] / results[0]["ms_median"])
        print(f"  orjson is {speedup:.1f}x faster -> {reduction:.0f}% lower parse overhead")

    if args.profile:
        print("\n=== cProfile of json.loads (top cumulative) ===")
        prof = cProfile.Profile()
        prof.enable()
        json.loads(raw)
        prof.disable()
        s = io.StringIO()
        pstats.Stats(prof, stream=s).sort_stats("cumulative").print_stats(12)
        print(s.getvalue())


if __name__ == "__main__":
    main()
