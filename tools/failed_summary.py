"""Aggregate failed-permanent instances by (normalized error, destination, caller).

Collapses UIDs/hex ids inside error strings so thousands of failures group
into a handful of actionable patterns, each with one example SOP UID you can
hand to the PACS admin.

Usage:  python failed_summary.py <path\to\state.db>
"""
import collections
import re
import sqlite3
import sys

if len(sys.argv) != 2:
    sys.exit(__doc__)

c = sqlite3.connect(sys.argv[1])
c.row_factory = sqlite3.Row
pat = collections.Counter()
sample = {}
for r in c.execute("""SELECT l.error, l.status, a.called_aet, a.calling_aet, i.sop_uid
        FROM instances i
        JOIN send_ledger l ON l.instance_id=i.instance_id
        JOIN associations a ON a.assoc_id=l.assoc_id
        WHERE i.send_status=3 AND i.attempts>0 AND i.canonical=1
        AND l.id=(SELECT MAX(id) FROM send_ledger WHERE instance_id=i.instance_id)"""):
    key = (re.sub(r"[0-9a-fA-F.\-]{8,}", "<id>", r["error"] or f"status {r['status']}"),
           r["called_aet"], r["calling_aet"])
    pat[key] += 1
    sample.setdefault(key, r["sop_uid"])
for (err, called, calling), n in pat.most_common(25):
    print(f"{n:>6,}  {called:<10} <- {calling:<12} {err}")
    print(f"        e.g. {sample[(err, called, calling)]}")
