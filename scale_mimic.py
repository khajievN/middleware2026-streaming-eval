"""Scale the MIMIC-IV demo `labevents` table to a target size for the streaming
stress test.

This is a synthetic volume scaler, not a statistical generator: it replicates
the demo rows in blocks, offsetting the identity columns (so cross-table joins
still group correctly) and jittering `valuenum` (so the numeric aggregates are
not degenerate). It is reproducible (seeded) and streamed to disk, so neither
the parent nor this script holds the full output in memory.

The output is explicitly synthetic and is used only to measure transport and
enclave-memory behaviour, never to make a clinical claim.
"""
import argparse
import csv
import gzip
import os
import random
from typing import List

# 0-based column indices in the MIMIC-IV v2.2 demo labevents schema
COL_LABEVENT_ID = 0
COL_SUBJECT_ID = 1
COL_HADM_ID = 2
COL_VALUENUM = 9

ID_OFFSET = 1_000_000  # per-block id stride, larger than the demo id space


def _load_base(path: str) -> tuple:
    with gzip.open(path, "rt", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        rows = [row for row in reader]
    return header, rows


def _perturb(row: List[str], block: int, rng: random.Random) -> List[str]:
    out = list(row)
    for idx in (COL_LABEVENT_ID, COL_SUBJECT_ID, COL_HADM_ID):
        if idx < len(out) and out[idx].isdigit():
            out[idx] = str(int(out[idx]) + block * ID_OFFSET)
    if COL_VALUENUM < len(out) and out[COL_VALUENUM]:
        try:
            base = float(out[COL_VALUENUM])
            out[COL_VALUENUM] = f"{base * rng.uniform(0.9, 1.1):.4f}"
        except ValueError:
            pass
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, help="labevents.csv.gz path")
    parser.add_argument("--out", required=True, help="output .csv path")
    parser.add_argument("--target-gb", type=float, required=True)
    parser.add_argument("--seed", type=int, default=20260603)
    args = parser.parse_args()

    target_bytes = int(args.target_gb * 1024 * 1024 * 1024)
    rng = random.Random(args.seed)
    header, base_rows = _load_base(args.source)
    print(f"loaded {len(base_rows)} base rows; target {args.target_gb} GB "
          f"({target_bytes} bytes)")

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    written = 0
    block = 0
    rows_out = 0
    with open(args.out, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        written += fh.tell()
        while written < target_bytes:
            for row in base_rows:
                writer.writerow(_perturb(row, block, rng))
                rows_out += 1
            written = fh.tell()
            block += 1
            if block % 50 == 0:
                print(f"  block {block}: {written / (1024**3):.2f} GB, "
                      f"{rows_out:,} rows")

    print(f"done: {written / (1024**3):.2f} GB, {rows_out:,} rows, "
          f"{block} blocks -> {args.out}")


if __name__ == "__main__":
    main()
