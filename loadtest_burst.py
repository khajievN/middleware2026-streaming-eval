"""Burst-queue load test for the Epsilon coordinator (Task 2 -> scatter plot).

Submits M jobs as fast as possible (a burst), then polls all of them to
completion, recording each job's arrival offset and time-to-completion. With a
single serial executor the burst queues, so a scatter of arrival-time vs
time-to-completion shows the linear queue degradation the paper describes
(\\S eval-burst) as a measured metric rather than prose.

Same insertion path as loadtest_cdf.py / run_30_jobs_e2e.py (insert into
job_requests; the fetcher drains it).

    export DATABASE_URL=postgresql://user:pass@host:5432/epsilon_coordinator
    python3 loadtest_burst.py --jobs 50 --timeout 1800 --out results/burst.json

Drives 50 real jobs through one serial executor: expect ~30-60 min wall-clock.
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


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    p.add_argument("--jobs", type=int, default=50)
    p.add_argument("--timeout", type=float, default=1800.0)
    p.add_argument("--interval", type=float, default=2.0)
    p.add_argument("--workspace", default="ws-e2e")
    p.add_argument("--user", default="user-e2e")
    p.add_argument("--out", default="results/burst.json")
    args = p.parse_args()
    if not args.database_url:
        raise SystemExit("set DATABASE_URL or pass --database-url")

    engine = create_engine(args.database_url)
    t0 = time.monotonic()
    jobs = {}  # job_id -> {submit_offset, complete_offset, status}

    # --- fire the burst as fast as the DB accepts inserts ---
    for _ in range(args.jobs):
        job_id = f"JOB-BURST-{uuid.uuid4().hex[:10]}"
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
                        'abc1234567890', 'burst job', 'loadtest-driver',
                        :nonce, NOW(), NOW()
                    )
                    """
                ),
                {"job_id": job_id, "ws": args.workspace, "uid": args.user,
                 "nonce": secrets.token_hex(16)},
            )
        jobs[job_id] = {"submit_offset_s": round(time.monotonic() - t0, 3),
                        "complete_offset_s": None, "status": "pending"}
    print(f"fired {len(jobs)} jobs in {round(time.monotonic() - t0, 2)}s; draining...")

    # --- drain: poll all until every job is terminal or timeout ---
    deadline = time.monotonic() + args.timeout
    pending = set(jobs)
    while pending and time.monotonic() < deadline:
        with engine.connect() as conn:
            rows = conn.execute(
                text("""SELECT job_id, status FROM job_requests
                        WHERE job_id = ANY(:ids)"""),
                {"ids": list(pending)},
            ).mappings().all()
        for r in rows:
            if r["status"] in TERMINAL:
                jobs[r["job_id"]]["status"] = r["status"]
                jobs[r["job_id"]]["complete_offset_s"] = round(time.monotonic() - t0, 3)
                pending.discard(r["job_id"])
        done = len(jobs) - len(pending)
        print(f"  {done}/{len(jobs)} complete ({round(time.monotonic() - t0)}s)", end="\r")
        if pending:
            time.sleep(args.interval)

    records = []
    for job_id, j in jobs.items():
        ttc = (j["complete_offset_s"] - j["submit_offset_s"]) if j["complete_offset_s"] is not None else None
        records.append({"job_id": job_id, **j, "time_to_completion_s": ttc})
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w") as fh:
        json.dump({"experiment": "burst_queue", "jobs": args.jobs,
                   "utc": datetime.datetime.utcnow().isoformat() + "Z",
                   "records": records}, fh, indent=2)
    done = sum(1 for r in records if r["status"] in TERMINAL)
    print(f"\nwrote {args.out} ({done}/{len(records)} terminal)")


if __name__ == "__main__":
    main()
