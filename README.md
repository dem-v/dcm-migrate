# dcm_migrate

Single-file, production-grade migration of heterogeneous legacy DICOM archives
into a PACS you do **not** control — as if the images had been sent by the
original modalities.

Battle-tested on a real hospital migration: **~45 million instances / ~13 TB**
scanned from five incompatible legacy systems (Sante/SyngoVia CT, an Orthanc
blob store, VXvue, Nanjing Perlove DR, Fuji Prima T2 CR) and delivered into a
multi-archive DCM4CHEE over a shared 1 Gbit link, surviving source drift, a
server outage, and every flavor of broken character encoding a decade of
mixed-vendor operation can produce.

```
uv run dcm_migrate.py init      # writes a fully commented migration.toml
# edit migration.toml
uv run dcm_migrate.py scan      # header-only inventory (incremental, resumable)
uv run dcm_migrate.py analyze   # offline analysis -> reports/run_N/summary.html + CSVs
uv run dcm_migrate.py exclude / approve --ack ... / approve --arm
uv run dcm_migrate.py echo      # pre-flight every destination
uv run dcm_migrate.py send      # throttled, scheduled, resumable C-STORE
uv run dcm_migrate.py verify    # C-FIND count reconciliation per study
```

One file, two dependencies (`pydicom`, `pynetdicom` — both pure Python),
SQLite + TOML from the standard library. Python ≥ 3.11; 3.14 recommended
(zstd-compressed audit sidecars, thread-based scanning on free-threaded
builds).

## Why this exists

Migrating a legacy archive into a hospital's official PACS is usually a
one-way door: you can't delete studies, can't merge patients afterwards, and
can't install coercion rules on the server. That inverts the usual priorities —
**everything must be right before the first packet leaves**, and every change
made in transit must be provable later.

## The safety model

1. **Hard readiness gate.** Nothing is transmitted until a full offline
   inventory + analysis has run, every hard blocker (`H-*`) is resolved or
   explicitly excluded, every warning class (`W-*`) is explicitly
   acknowledged, and the gate is armed (`approve --arm`). Any config edit or
   re-analyze **automatically disarms** the gate — acks are keyed to the
   analyze run and the content-relevant config hash. The `[network]` section
   (bandwidth, hours, association count) is excluded from that hash, so
   runtime tuning never forces a re-approval cycle.
2. **Sources are never modified.** All tag repair happens client-side, in
   memory, at send time. No staging copies of a multi-TB archive either.
3. **Pixels are never touched.** Transfer syntax is preserved; only text/ID
   attributes are coerced. If the server won't accept a compressed transfer
   syntax, the instance fails visibly — it is never silently transcoded.
4. **Every coercion is auditable twice**: inside the transmitted object
   (Original Attributes Sequence, `0400,0561`, with the pre-modification
   values) and in JSONL sidecar files on disk.
5. **Deterministic and resumable.** State lives in a WAL-mode SQLite DB;
   sends resume at instance granularity; re-running `analyze` on unchanged
   input reproduces the same decisions.

## What the analyzer can fix (all config-driven, all previewed before send)

- **Character-set repair** for archives that lie about their encoding:
  mojibake detection and repair to UTF-8 (`ISO_IR 192`), including
  double-encoded text, cp1251-as-latin1, cp1251/UTF-8 stored under GB
  declarations, text hidden under ISO 2022 escape declarations, and
  wrong-declaration passthroughs. A `keep-gb` policy preserves GB18030 byte
  streams and fixes only the `(0008,0005)` declaration, for servers/viewers
  that expect GB. Forced source encodings are a *preference, not a
  blindfold*: implausible decodes fall back to detection.
- **Patient-ID engines**: regex rules with templates, and a built-in
  site/ordinal generator (`{site}{date}{###}`) for CR archives whose IDs are
  unusable — with collision guards, configurable date format, optional
  preservation of the original ID as a suffix, and an old→new preview CSV
  that is the approval artifact.
- **Person-name repair**: missing-caret `Family Given` → `Family^Given`
  (conservative), optional transliteration.
- **IS-VR decimals** (`"6.300000"` in an Integer String tag) rounded to
  conformant integers.
- **UID hygiene**: invalid/missing UIDs regenerated deterministically
  (uuid5), consistent across referencing tags; missing StudyInstanceUID
  synthesized so siblings reunite.
- **Deduplication**: byte-hash tier plus transfer-syntax-aware pixel-hash
  tier for same-UID-different-bytes collisions; policies
  `block | regenerate-uid | keep-priority`.
- **Study-level routing** to multi-archive servers (same host, different
  port/AET per modality group) — a study is routed as a whole so it never
  splits across archives; companion-only studies (dose SRs, presentation
  states) can opt in to adopting their sibling image study's destination.

## Sending

Multiple concurrent associations with per-source calling AETs, token-bucket
bandwidth cap, active-hours window, pause-file kill switch, exponential
backoff, and a circuit breaker fed by both association and store-level
failures (a dying server pauses the source instead of poisoning the queue).
DIMSE status taxonomy distinguishes retryable, class-stop and permanent
failures. `verify` reconciles per-study instance counts over C-FIND.

## Trying it without a PACS

```
uv run dcm_migrate.py selftest        # synthetic end-to-end against an in-process SCP
uv run dcm_migrate.py scp --dir X:\rehearsal   # standalone loopback receiver
uv run dcm_migrate.py send --dry-run           # full dress rehearsal, locally
```

`selftest` generates dozens of pathological files (mis-declared GB18030,
cp1251 mojibake, caret-less names, decimal IS values, preamble-less files,
Orthanc blob decoys, duplicate/invalid UIDs, truncated files, multi-site CR
ordinals) and asserts the whole pipeline end to end.

## Caveats (read before pointing it at patient data)

- `regenerate-uid` dedupe can orphan references *into* the regenerated
  instance from PR/SR objects stored elsewhere.
- Charset heuristics were tuned on Cyrillic/GB mixed archives; the
  expected-script assumptions are documented in the config template. Run
  `analyze` and read `charset_suspects.csv` before trusting them on other
  scripts.
- Companion-study routing adoption trusts AccessionNumber (≥4 chars) first.
- The tool coerces identity attributes by design. The preview CSVs exist so
  a human signs off on every rule before anything is sent. Use them.

## Control panel (Windows, no browser)

```
py dcm_migrate_gui.py [migration.toml]
```

`dcm_migrate_gui.py` is a native tkinter cockpit (stdlib only — no extra
dependencies, no web server). It deliberately contains **zero migration
logic**: status comes from read-only queries against the state database,
every action runs the CLI as a child process with live colorized output in
the console pane, and pause/resume is the engine's own PAUSE-file mechanism.

- gate state banner (armed/disarmed), per-source instance-state table,
  problem classes with ack status, verify summary
- one-click scan / analyze / echo / send / verify (send and analyze ask for
  confirmation), warning-ack dialog, ARM with a point-of-no-return prompt
- live `progress:` ticker parsed from a running send, PAUSE/RESUME toggle,
  Stop button, latest `summary.html` opener

## Operational helpers

See [`tools/`](tools/) for state-DB maintenance and failure-triage scripts.
The state database is intentionally plain SQLite — when something odd
happens at 2 a.m., the answer is one query away.

## License & credits

MIT — free to use, modify and redistribute as long as the copyright /
attribution notice is preserved. © 2026 Dmytro Valantsevych.

Co-authored with Claude (Fable 5, Anthropic) — the tool was designed, built
and hardened in an AI-pair-programming workflow over the course of a live
production migration.
