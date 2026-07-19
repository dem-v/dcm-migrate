# tools/

Small standalone helpers for operating a long migration.  All of them take the
path to the state database as their only argument and only need the standard
library — the state DB is deliberately plain SQLite, so ad-hoc triage is just
SQL away.

| script | what it does |
|---|---|
| `db_maint.py` | WAL checkpoint, sample cleanup, send-lane indexes, `ANALYZE`. Run when the DB has grown to tens of GB and queries feel slow. Stop scan/send first. |
| `failed_summary.py` | Groups failed-permanent instances by normalized error pattern + destination/calling AET, with one example SOP UID each. Start here after a bad night. |
| `failed_report.py` | Flat per-instance list of failed-permanents with their last error. |

## Recipes (ad-hoc SQL against the state DB)

Failed-permanent count per SOP class / modality:

```sql
SELECT sc.uid, i.modality, COUNT(*) n
FROM instances i JOIN sop_classes sc ON sc.id=i.sop_class_id
WHERE i.send_status=3 AND i.canonical=1
GROUP BY sc.uid, i.modality ORDER BY n DESC LIMIT 15;
```

Where did a specific SOP instance actually go:

```sql
SELECT i.sop_uid, a.dest, a.called_aet, l.status, l.ts
FROM instances i
JOIN send_ledger l ON l.instance_id=i.instance_id
JOIN associations a ON a.assoc_id=l.assoc_id
WHERE i.sop_uid='1.2.3...';
```

Requeue a set of instances for re-send (e.g. after fixing a transform bug —
the server treats a re-store of the same SOP UID as a no-op or replace,
depending on its config):

```sql
UPDATE instances SET send_status=0, attempts=0 WHERE instance_id IN (...);
```
