"""Flat list of every failed-permanent instance with its last recorded error.

Use failed_summary.py first to see the patterns; use this one when you need
the full per-instance list (e.g. to build an exclusion or hand-off CSV).

Usage:  python failed_report.py <path\to\state.db>
"""
import sqlite3
import sys

if len(sys.argv) != 2:
    sys.exit(__doc__)

c = sqlite3.connect(sys.argv[1])
c.row_factory = sqlite3.Row
rows = c.execute("""
    SELECT i.source, i.modality, i.sop_uid, i.attempts, i.last_status, l.error
    FROM instances i
    LEFT JOIN send_ledger l ON l.instance_id = i.instance_id
        AND l.id = (SELECT MAX(id) FROM send_ledger WHERE instance_id = i.instance_id)
    WHERE i.send_status = 3
""").fetchall()
for r in rows:
    print(r["source"], r["modality"], hex(r["last_status"] or 0),
          "attempts:", r["attempts"], "|", (r["error"] or "")[:150], "|", r["sop_uid"])
print(f"-- {len(rows)} failed-permanent instances")
