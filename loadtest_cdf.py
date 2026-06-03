"""Latency-distribution load test for the Epsilon coordinator (Task 1 -> CDF).

Submits N jobs SEQUENTIALLY (insert one, wait for it to reach a terminal
status, record its end-to-end latency, repeat) through the real coordinator
pipeline by inserting into the `job_requests` table the DB-polling fetcher
drains, exactly as scripts/run_30_jobs_e2e.py does. Back-to-back submission
keeps the enclave warm, so this measures the WARM end-to-end distribution.

Latency per job is the authoritative DB delta completed_at - created_at, with a
wall-clock fallback. Output JSON feeds a CDF (p50/p95/p99) plot.

Run on a host that can reach the coordinator Postgres:
    export DATABASE_URL=postgresql://user:pass@host:5432/epsilon_coordinator
    python3 loadtest_cdf.py --jobs 100 --timeout 180 --out results/cdf_warm.json

Every number is from a live run. This drives real enclave provisioning -- 100
warm jobs is ~25-35 min on a single serial executor and incurs EC2 cost.
"""
import argparse
import datetime
import json
import os
import secrets
import time
import uuid

from sqlalchemy import create_engine, text

TERMINAL = ("success", "failed", "rejected")
SAMPLE = {
    "commit_sha": "abc1234567890",
    "commit_message": "loadtest job",
    "commit_author": "loadtest-driver",
}


def insert_job(engine, workspace: str, user: str) -> str:
    job_id = f"JOB-LT-{uuid.uuid4().hex[:10]}"
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO job_requests (
                    job_id, workspace_id, user_id, status,
                    commit_sha, commit_message, commit_author,
                    researcher_nonce, created_at, updated_at
                ) VALUES (
                    :job_id, :ws, :uid, 'pending',
                    :sha, :msg, :author, :nonce, NOW(), NOW()
                )
                """
            ),
            {"job_id": job_id, "ws": workspace, "uid": user,
             "sha": SAMPLE["commit_sha"], "msg": SAMPLE["commit_message"],
             "author": SAMPLE["commit_author"], "nonce": secrets.token_hex(16)},
        )
    return job_id


def poll_terminal(engine, job_id: str, timeout: float, interval: float) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with engine.connect() as conn:
            row = conn.execute(
                text("""SELECT status, created_at, completed_at, updated_at
                        FROM job_requests WHERE job_id = :j"""),
                {"j": job_id},
            ).mappings().first()
        if row and row["status"] in TERMINAL:
            end = row["completed_at"] or row["updated_at"]
            db_latency = (end - row["created_at"]).total_seconds() if (end and row["created_at"]) else None
            return {"status": row["status"], "db_latency_s": db_latency}
        time.sleep(interval)
    return {"status": "timeout", "db_latency_s": None}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    p.add_argument("--jobs", type=int, default=100)
    p.add_argument("--timeout", type=float, default=180.0)
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument("--workspace", default="ws-e2e")
    p.add_argument("--user", default="user-e2e")
    p.add_argument("--out", default="results/cdf_warm.json")
    args = p.parse_args()
    if not args.database_url:
        raise SystemExit("set DATABASE_URL or pass --database-url")

    engine = create_engine(args.database_url)
    records = []
    for i in range(args.jobs):
        t0 = time.monotonic()
        job_id = insert_job(engine, args.workspace, args.user)
        res = poll_terminal(engine, job_id, args.timeout, args.interval)
        wall = round(time.monotonic() - t0, 3)
        rec = {"i": i, "job_id": job_id, "status": res["status"],
               "wall_latency_s": wall, "db_latency_s": res["db_latency_s"]}
        records.append(rec)
        print(f"[{i+1}/{args.jobs}] {job_id} {res['status']:<8} "
              f"wall={wall}s db={res['db_latency_s']}s")
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        with open(args.out, "w") as fh:
            json.dump({"experiment": "latency_cdf", "jobs": args.jobs,
                       "utc": datetime.datetime.utcnow().isoformat() + "Z",
                       "records": records}, fh, indent=2)
    ok = [r for r in records if r["status"] == "success"]
    print(f"\nwrote {args.out} ({len(ok)}/{len(records)} success)")


if __name__ == "__main__":
    main()
