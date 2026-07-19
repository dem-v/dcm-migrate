"""One-shot maintenance for a large dcm_migrate state database.

Run while scan/send are STOPPED.  Truncates the WAL, drops text samples that
are no longer needed, makes sure the send-lane indexes exist, and refreshes
the query-planner statistics (the single biggest win on a multi-GB database
that has never been ANALYZEd).

Usage:  python db_maint.py <path\to\state.db>
"""
import sqlite3
import sys
import time

if len(sys.argv) != 2:
    sys.exit(__doc__)

c = sqlite3.connect(sys.argv[1], timeout=600)
c.execute("PRAGMA cache_size=-2097152")          # 2 GB page cache

def step(name, sql):
    t = time.time()
    print(name, "...", flush=True)
    c.execute(sql)
    c.commit()
    print(f"  done in {time.time() - t:.0f}s", flush=True)

step("checkpoint WAL",        "PRAGMA wal_checkpoint(TRUNCATE)")
step("drop unneeded samples", "UPDATE instances SET text_sample=NULL WHERE (facts&1)=0")
step("index: ix_inst_lane",   "CREATE INDEX IF NOT EXISTS ix_inst_lane "
                              "ON instances(source, send_status, study_pk) WHERE canonical=1")
step("index: ix_ledger_inst", "CREATE INDEX IF NOT EXISTS ix_ledger_inst ON send_ledger(instance_id)")
step("planner statistics",    "ANALYZE")
c.close()
print("ALL DONE")
