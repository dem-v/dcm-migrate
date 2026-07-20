# Configuration & usage reference

`dcm_migrate` is driven entirely by one TOML file (default `migration.toml`).
`dcm_migrate.py init` writes a fully-commented template; this document is the
exhaustive reference for every key, its type, default, and meaning.

Any edit to a **content-relevant** key disarms the send gate (you must re-run
`analyze` and `approve --arm`). The whole **`[network]`** section is *excluded*
from the gate hash — you can retune bandwidth, hours, retries, and the circuit
breaker mid-campaign without re-approving; the change takes effect the next time
you start `send`.

---

## How to run

```
uv run dcm_migrate.py init                 # write migration.toml template
# edit migration.toml — fill every REPLACE_ME
uv run dcm_migrate.py scan                 # header-only inventory (incremental, resumable)
uv run dcm_migrate.py analyze              # dedupe, classify, assign IDs -> reports/run_N/
uv run dcm_migrate.py report               # (re-emit reports from the last analyze)
uv run dcm_migrate.py exclude ...          # drop unfixable files/studies/problem classes
uv run dcm_migrate.py approve --ack CODE1 CODE2 ...   # ack warning classes (multiple at once)
uv run dcm_migrate.py approve --arm        # arm the gate (needs 0 blockers, all warnings acked)
uv run dcm_migrate.py echo                 # C-ECHO every destination x calling AET
uv run dcm_migrate.py send                 # transform in memory + C-STORE (resumable)
uv run dcm_migrate.py verify               # C-FIND per study: expected vs found counts
uv run dcm_migrate.py status               # one-screen progress summary
uv run dcm_migrate.py maintain             # DB housekeeping (see below)
```

Run with `uv run dcm_migrate.py …` (resolves the pydicom/pynetdicom deps from
the PEP 723 header automatically) or, if the deps are already installed, `py
dcm_migrate.py …`. Global flags: `--config PATH`, `--db PATH` (override
`general.db_path`), `-v`/`-q`, `--log-file PATH`.

Other commands: `probe` (inspect files classified non-DICOM/error), `dup-audit`
(review duplicate groups), `xcheck-orthanc`, `export-mapping` (CSV of ID/UID
maps), `scp` (standalone loopback receiver for rehearsal), `selftest`.

The **GUI** wraps all of this: `py dcm_migrate_gui.py migration.toml`.

### Stopping a send

- **Graceful** (GUI Stop → Graceful, or Ctrl-C / `SIGTERM` on the CLI): the
  engine finishes the stores in flight, releases associations, flushes the
  writer, and commits. No PAUSE file needed. Resume any time by re-running
  `send`.
- **PAUSE file** (`general.pause_file`): create it and senders drain and idle
  *without exiting* — useful to free the link temporarily. Delete it to resume.
- **Force** (GUI Stop → Force kill): terminates immediately. State is still
  recoverable (WAL); in-flight stores are abandoned and retried on resume.

### DB maintenance (`maintain`)

Run when `verify`/`status` feel slow on a large DB. With no flags it does
everything except VACUUM.

```
uv run dcm_migrate.py maintain              # checkpoint + indexes + ANALYZE + PRAGMA optimize
uv run dcm_migrate.py maintain --analyze    # only refresh planner statistics
uv run dcm_migrate.py maintain --indexes    # only create any missing indexes
uv run dcm_migrate.py maintain --checkpoint # only truncate the WAL
uv run dcm_migrate.py maintain --vacuum     # also VACUUM (expensive; needs free space ~= DB size)
```

`ANALYZE` is the single biggest win once the DB has grown: without current
planner statistics SQLite mis-costs the correlated subqueries in `verify` and
the per-source counts in `status`, and falls back to full-table scans. Safe to
run while a `send` is in progress (WAL). It is the fix for a slow `verify`.

---

## `[general]`

| key | type | default | meaning |
|---|---|---|---|
| `db_path` | path | `migration_state.db` | SQLite state DB (WAL). Keep on SSD; grows to ~10–14 GB per 50 M instances. |
| `report_dir` | path | `reports` | analyze/verify reports and CSVs (`run_N/`). |
| `sidecar_dir` | path | `sidecars` | JSONL audit of every modified attribute. |
| `log_file` | path | `dcm_migrate.log` | log destination (overridable with `--log-file`). |
| `pause_file` | path | `PAUSE` | create this file → senders drain and idle; delete → resume. |
| `scan_backend` | `auto`\|`threads`\|`processes` | `auto` | reader-pool model; `auto` picks threads on free-threaded 3.14, else processes. |
| `sidecar_compress` | bool | `true` | zstd-compress sidecars when available (py3.14+), else plain. |
| `uid_root` | str | `2.25` | root for regenerated (invalid/missing) UIDs; `2.25` = UUID-derived, globally safe. |
| `diff_content_policy` | `block`\|`regenerate-uid`\|`keep-priority` | `block` | same SOPInstanceUID, **different pixels** (a real collision): `block` = hard blocker (safest); `regenerate-uid` = keep all, give losers fresh UIDs; `keep-priority` = keep one, discard others (logged loss). |
| `no_identity_policy` | `block`\|`placeholder` | `block` | studies with no PatientID **and** no PatientName: `block`, or synthesize `<Institution>-<YYYYMMDD>-<HHMMSS>-Missing`. |

## `[server]`

| key | type | default | meaning |
|---|---|---|---|
| `host` | str | `REPLACE_ME` | PACS IP/hostname (shared across all archives). |
| `max_pdu` | int | `262144` | max PDU size (bytes). |
| `acse_timeout` | int (s) | `30` | association negotiation timeout. |
| `dimse_timeout` | int (s) | `120` | per-operation DIMSE timeout (raise for very large instances on a throttled link). |
| `network_timeout` | int (s) | `60` | socket timeout. |

### `[server.destinations.<group>]`

One block per routing group (`ct`, `xray`, `xa`, `other`, or your own names).
A study is sent whole to the destination of its routed group.

| key | type | default | meaning |
|---|---|---|---|
| `port` | int | `11112` | archive port. |
| `called_aet` | str | `DCM4CHEE` | archive AE title for this group. |

There must be either a destination for every group your routing can produce, or
an `other` block as fallback.

## `[routing]`

| key | type | default | meaning |
|---|---|---|---|
| `<group> = [modalities]` | table | `ct=["CT"]`, `xray=["CR","DX"]`, `xa=["XA","RF"]` | maps Modality values to a group. A modality not listed anywhere → `other`. |
| `precedence` | list | `["ct","xa","xray","other"]` | when a study has mixed modalities, the highest-precedence group wins (so a CT study with an SR/OT dose report still routes to `ct`, never splits). |
| `companion` | list | `[]` (disabled) | **opt-in.** Companion-only studies (only SR/PR/OT/SEG/… under their own StudyInstanceUID) adopt the destination of a sibling image study (matched by AccessionNumber ≥4 chars, else patient+date) instead of routing to `other`. |

## `[network]` — *excluded from the gate hash; retune freely*

| key | type | default | meaning |
|---|---|---|---|
| `max_associations` | int | `3` | concurrent associations (sender threads); capped at 2× the number of active lanes. |
| `rate_limit_mbit` | float | `350` | token-bucket bandwidth cap; `0` = unlimited (the link is shared — be considerate). |
| `active_hours` | str | `20:00-07:00` | local send window, `HH:MM-HH:MM`; `""` = around the clock. |
| `active_days` | list | mon–sun | days the window applies. |
| `instances_per_association` | int | `500` | recycle the association after N stores. |
| `backoff_initial_s` | float | `5` | pause after a failed/rejected association before requeueing the lane. |
| `backoff_max_s` | float | `600` | reserved ceiling for backoff escalation. |
| `max_instance_retries` | int | `5` | per-instance retry budget for retryable failures before `failed-permanent`. `1` = fail on first error (no retries). |
| `circuit_breaker_failures` | int | `10` | consecutive failures on a lane → open the breaker (600 s cooldown, hardcoded). **`0` is NOT "disabled"** — it clamps to 1 (trips on the first failure). To effectively disable, set a very large number. |
| `memory_budget_mb` | int | `8192` | cap on concurrently loaded datasets. |
| `skip_existing_studies` | bool | `false` | C-FIND each study first and skip if already fully present on the server. |

## `[[source]]` — one block per source archive

| key | type | default | meaning |
|---|---|---|---|
| `name` | str | — | unique source name (used in reports, ledger, calling AET selection). |
| `adapter` | `filetree`\|`orthanc` | `filetree` | `orthanc` understands the 2-hex fan-out blob store and its JSON/zlib attachments. |
| `roots` | list[path] | `[]` | directories to scan. |
| `calling_aet` | str | `MIGRATE` | how this source identifies itself (per-source AETs keep provenance; `send --single-aet X` overrides all). |
| `priority` | int | `100` | duplicate resolution: **lower wins** when the same SOPInstanceUID exists in several sources. |
| `charset_policy` | `utf8`\|`keep-gb`\|`keep` | `utf8` | `utf8` = decode true encoding → `ISO_IR 192`; `keep-gb` = leave GB bytes, fix only the `(0008,0005)` declaration; `keep` = no charset changes. |
| `charset_source` | `auto` or codec | `auto` | preferred source encoding (e.g. `cp1251`, `gb18030`). A **preference, not a blindfold**: implausible forced decodes fall back to `charset_detect_order`. |
| `charset_detect_order` | list | `["utf-8","cp1251","latin-1"]` | codecs tried, in order, when detecting/repairing. Add `gb18030` here to disable the keep-gb plausibility fallback. |
| `caret_repair` | bool | `false` | repair `"Family Given"` → `Family^Given` (conservative; skips if digits / >4 tokens). |
| `translit_cyrillic` | bool | `false` | optional Cyrillic→Latin transliteration (original preserved in audit). |
| `is_fix_tags` | list[tag] | `[]` | IS-VR tags carrying decimals (e.g. `["00181150","00181151","00181152"]`) → rounded to conformant integers. |
| `max_readers` | int | `4` | reader-pool size for this source's volume; 6–8 for RAID1/SSD (NCQ/mirror-friendly). |
| `exclude_dirs` | list | `[]` | directory names pruned during scan (e.g. `.Database`, `RecycleBin`). |
| `exclude_file_globs` | list | `[]` | filename globs skipped (e.g. `index*`, `*.db`). |
| `file_globs` | list | `[]` (all) | if set, only files matching these globs are scanned. |
| `pid_rules_mode` | `off`\|`optional`\|`required` | `off` | `required` = every study must get an ID (unmatched → hard blocker); `optional` = rules normalize where they match. |
| `index_db` | path | `""` | orthanc adapter: optional read-only path to Orthanc's SQLite `index` for cross-check. |

### `[[source.patient_id_rule]]` — regex ID rewrite (repeatable, first match wins)

| key | type | meaning |
|---|---|---|
| `id` | str | rule name (for reports). |
| `match` | regex | matched against PatientID; named groups usable in the template. |
| `template` | str | new ID, e.g. `"SITEB-{num:0>8}"` (`{num}` = a named group). |
| `station` | regex | optional gate on StationName. |
| `institution` | regex | optional gate on InstitutionName. |

### `[source.fuji]` — built-in study-level ID generator

For CR archives whose PatientIDs are unusable. New ID = `{site}{date}{###}`.

| key | type | default | meaning |
|---|---|---|---|
| `enabled` | bool | `false` | turn the generator on for this source. |
| `sites` | list | `["8K","9K"]` | site codes looked for as substrings of InstitutionName. |
| `folder_site_map` | table | `{}` | `path-fragment = "SITE"` (case-insensitive on the file's absolute path) for files without a site flag in InstitutionName. |
| `fallback_site` | str | `""` | site for studies matching neither InstitutionName nor folder map; `""` makes them `H-PID-RULE-MISS` blockers instead. |
| `date_from_mtime` | bool | `true` | no/invalid StudyDate → derive the ID date from file mtime (`W-FUJI-MTIME-DATE`) instead of blocking (`H-FUJI-NO-DATE`). |
| `original_id` | `drop`\|`suffix` | `drop` | `drop` = replace the ID entirely; `suffix` = append the sanitized/transliterated original (`8K319001-<original>`). |
| `id_date_format` | `myy`\|`mmyy`\|`yymm` | `myy` | date part: `myy` is historical and ambiguous past 999 studies/month; `mmyy`/`yymm` are fixed-width and collision-free (`yymm` also sorts chronologically). |

---

## The gate, in one paragraph

`analyze` classifies every problem as a hard blocker (`H-*`) or a warning
(`W-*`). You cannot `approve --arm` until **every** `H-*` is resolved (fixed by
config + re-analyze, or `exclude`d) and **every** distinct `W-*` class is
acknowledged (`approve --ack`, multiple codes in one call). Acks are keyed to
the analyze run **and** the content-config hash, so any content-config edit or
re-analyze automatically disarms and you re-ack. This is the structural "100%
readiness before the first packet" guarantee.
