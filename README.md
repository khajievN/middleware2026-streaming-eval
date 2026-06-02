# Middleware 2026 — Streaming & OOM Evaluation

Benchmark toolkit for the Middleware 2026 E&D paper's systems lessons. It
produces two figures from **live measurements** on a single AWS Nitro enclave:

- **Graph 1 — OOM survival**: enclave peak RSS vs. ingested payload size, for
  three memory models. Shows the single-payload model OOM-killing at a hard
  threshold, a streamable operator holding flat to 10 GB, and a *blocking*
  operator (global sort) still OOM-ing — because a Nitro enclave has no swap
  **and** its filesystem is RAM-backed, so there is nowhere to spill.
- **Graph 2 — VSock throughput**: sustained MB/s across the host↔enclave VSock
  boundary as a function of send chunk size.

## Scope and honesty notes

- **Measures transport + enclave memory, not the crypto/attestation path.** The
  bench enclave image is deliberately minimal (no KMS/NSM) so the numbers aren't
  confounded by attestation cost. The paper must describe it as an isolated
  ingestion-transport + memory-model microbenchmark, not the full pipeline.
- **Single `c5a.xlarge` only (4 vCPU / 8 GB → 2 vCPU + 4096 MB enclave).** The
  concurrent-attestation contention CDF is **not** in scope on this hardware and
  is **not** synthesized. Reporting it would require an instance that can host
  multiple concurrent enclaves.
- **The "flat to 10 GB" line is true only for streamable operators.** Blocking
  operators (join/sort/high-cardinality groupby) hit the same wall as the
  baseline. The `stream_block` series exists to measure exactly that, so the
  lesson reports the *bifurcation*, not an unconditional "unlimited ingestion".
- The data is the MIMIC-IV demo `labevents` table replicated and jittered by
  `scale_mimic.py`. It is **synthetic volume**, used only for memory/transport
  stress — no clinical claim is attached to it.

## Prerequisites (on the EC2 parent)

- Nitro-enabled instance; `nitro-cli`, `docker`, `jq` installed; the Nitro
  allocator configured for ≥ 4096 MB and 2 vCPUs (`/etc/nitro_enclaves/allocator.yaml`,
  then `sudo systemctl restart nitro-enclaves-allocator`).
- Python 3 with `matplotlib` for `make plot` (plotting can also be done off-box).

## Run order

```bash
# 0. (optional) validate the protocol + workloads with no Nitro, over TCP:
make smoke

# 1. one-time: build the ~10 GB synthetic CSV (set DEMO_LABEVENTS if needed)
make scale TARGET_GB=10

# 2. build the bench enclave image + EIF
make eif

# 3. boot the enclave (debug-mode prints a console you can tail)
make run-enclave

# 4. Graph 2 data — VSock throughput sweep
make throughput

# 5. Graph 1 data — OOM survival sweep (stops each workload at its first OOM)
make oom

# 6. render both figures from results/*.json
make plot
```

Override the enclave CID if `nitro-cli describe-enclaves` reports something
other than 16: `make throughput ENCLAVE_CID=<cid>`.

## What to send back

Two JSON files, both written to `results/`:

- `results/throughput.json` — `records[]` of
  `{chunk_size_bytes, mbps_median, mbps_min, mbps_max, mbps_samples[]}` plus `meta`.
- `results/oom.json` — `records[]` of
  `{workload, payload_mb, status (ok|oom|killed|error), peak_rss_mb, rows, seconds}`
  plus `meta`.

That is all I need to plot the figures and write the lesson numbers. **Send the
raw JSON, not screenshots** — every number in the paper is read directly from
these files.

## Tuning for the 4 GB enclave

- Default OOM sweep: `50,100,200,247,300,500,1024,2048,4096,8192,10240` MB. The
  `--stop-on-oom` flag (on by default via `make oom`) stops a workload once it
  OOM-kills, so `baseline_full` and `stream_block` won't waste time past their
  cliff while `stream_agg` continues to 10 GB.
- If the baseline survives further than expected, the enclave memory is larger
  than assumed — confirm with `nitro-cli describe-enclaves`.
- Throughput default streams 2 GB per chunk-size trial; lower `--total-bytes`
  if a run is slow, but keep it ≫ the socket buffer for a sustained number.
