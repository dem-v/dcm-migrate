# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "pydicom>=3.0,<4",
#   "pynetdicom>=3.0,<4",
# ]
# ///
"""
dcm_migrate.py — single-file, production-grade migration of heterogeneous DICOM
archives into a DCM4CHEE (or any DIMSE) PACS that you do NOT control.

Built for the real-world case of ~8 TB of legacy data from five very different
sources (Sante/SyngoVia CT, an Orthanc storage-area "blob pack", VXvue,
Nanjing Perlove DR with GB-encoded text and decimal values in IS tags, and
Fuji Prima T2 CR with unreliable per-site patient IDs), but every
source-specific behavior is driven by the TOML config, so it generalizes.

Design pillars
==============
* Two-phase, gated workflow:  nothing is transmitted until a full offline
  inventory + analysis has produced human-readable reports, every hard blocker
  is resolved, every warning class explicitly acknowledged, and the gate is
  armed (`approve --arm`).  Any config edit or re-scan disarms the gate.
* All tag repair happens CLIENT-SIDE, in memory, at send time.  Source files
  are never modified; no staging copies are made.  Pixel data is NEVER
  transcoded; the original transfer syntax is preserved.
* Every modification is recorded in the transmitted object itself
  (Original Attributes Sequence, 0400,0561) and in JSONL sidecar files.
* Resumable at instance granularity via a SQLite state database (WAL mode).
* Network-considerate: token-bucket bandwidth throttle, active-hours window,
  bounded association count, exponential backoff, per-source circuit breaker,
  and a pause-file kill switch.
* Disk-considerate AND efficient: header-only reads during scan, a tunable
  reader pool per volume (NCQ/RAID1-aware), and stat-only incremental rescans
  (unchanged files are never reopened).

Quick start
===========
    uv run dcm_migrate.py init                # writes migration.toml template
    <edit migration.toml>
    uv run dcm_migrate.py scan
    uv run dcm_migrate.py analyze             # emits reports/run_N/summary.html
    uv run dcm_migrate.py exclude ... / approve --ack ... / approve --arm
    uv run dcm_migrate.py echo                # pre-flight the server
    uv run dcm_migrate.py send
    uv run dcm_migrate.py verify

Runs anywhere Python >= 3.11 exists; Python 3.14 recommended (zstd-compressed
sidecars, thread-based scanning on free-threaded builds).

License: MIT.  Home: shipped as a single file on purpose — copy it, read it.
"""
from __future__ import annotations

import argparse
import csv
import dataclasses
import datetime as _dt
import fnmatch
import hashlib
import html as _html
import io
import json
import logging
import multiprocessing as mp
import os
import queue
import re
import shutil
import socket
import sqlite3
import sys
import threading
import time
import tomllib
import uuid
import warnings
import zlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Optional

# Third-party (declared in the PEP 723 header above)
import pydicom
import pydicom.config
from pydicom import dcmread
from pydicom.dataset import Dataset, FileMetaDataset
from pydicom.dataelem import DataElement, RawDataElement
from pydicom.tag import Tag
from pydicom.uid import UID, ImplicitVRLittleEndian, ExplicitVRLittleEndian, ExplicitVRBigEndian

from pynetdicom import AE, build_context, evt
from pynetdicom.presentation import PresentationContext
from pynetdicom.sop_class import Verification, StudyRootQueryRetrieveInformationModelFind

VERSION = "1.0.0"
PROG = "dcm_migrate"

# Be lenient when reading legacy garbage; we do our own validation.
pydicom.config.settings.reading_validation_mode = pydicom.config.WARN
try:  # writing side: we sanitize ourselves; don't let pydicom hard-fail on odd values
    pydicom.config.settings.writing_validation_mode = pydicom.config.WARN
except Exception:  # pragma: no cover - older pydicom
    pass

# --------------------------------------------------------------------------
# SECTION 2 — CONSTANTS
# --------------------------------------------------------------------------

TEXT_VRS = {"SH", "LO", "ST", "LT", "UT", "UC", "PN"}
DEFAULT_IS_FIX_TAGS = ["00181150", "00181151", "00181152"]  # ExposureTime, XRayTubeCurrent, Exposure

# files.kind
K_UNKNOWN, K_PART10, K_PREAMBLELESS, K_ORTHANC_JSON, K_NONDICOM, K_TRUNCATED, \
    K_ZLIB_PART10, K_ZLIB_PREAMBLELESS, K_HEADER_ONLY, K_ORTHANC_ATTACH, \
    K_DICOMDIR = range(11)
KIND_NAMES = {
    K_UNKNOWN: "unknown", K_PART10: "part10", K_PREAMBLELESS: "preambleless",
    K_ORTHANC_JSON: "orthanc-json", K_NONDICOM: "non-dicom", K_TRUNCATED: "truncated",
    K_ZLIB_PART10: "zlib+part10", K_ZLIB_PREAMBLELESS: "zlib+preambleless",
    K_HEADER_ONLY: "header-only", K_ORTHANC_ATTACH: "orthanc-attach",
    K_DICOMDIR: "dicomdir",
}
DICOMDIR_SOP = "1.2.840.10008.1.3.10"  # Media Storage Directory Storage (index file)
# "2;<md5hex>;" + gzip(DICOM-as-JSON): metadata attachment written by Orthanc
# setups that index loose DICOM files in place (observed in the field)
ORTHANC_ATTACH_RE = re.compile(rb"^\d{1,2};[0-9a-fA-F]{32};")
DICOM_KINDS = {K_PART10, K_PREAMBLELESS, K_ZLIB_PART10, K_ZLIB_PREAMBLELESS}

# files.scan_status
FS_PENDING, FS_OK, FS_ERROR, FS_EXCLUDED, FS_NONDICOM = range(5)

# instances.send_status
S_PENDING, S_SENT, S_SENT_WARN, S_FAILED_PERM, S_FAILED_RETRY, \
    S_SKIPPED_DUP, S_EXCLUDED, S_SKIPPED_EXISTS = range(8)
SEND_STATUS_NAMES = {
    S_PENDING: "pending", S_SENT: "sent", S_SENT_WARN: "sent-warning",
    S_FAILED_PERM: "failed-permanent", S_FAILED_RETRY: "failed-retryable",
    S_SKIPPED_DUP: "skipped-duplicate", S_EXCLUDED: "excluded",
    S_SKIPPED_EXISTS: "skipped-already-on-server",
}
SENDABLE = (S_PENDING, S_FAILED_RETRY)

# instances.facts — raw observations recorded at scan time
F_NONASCII_TEXT = 1 << 0     # some text element contains non-ASCII bytes
F_PN_NONASCII = 1 << 1       # a PN element contains non-ASCII bytes
F_PN_CARETLESS = 1 << 2      # PatientName has spaces but no ^ separator
F_BAD_SOP_UID = 1 << 3
F_BAD_STUDY_UID = 1 << 4
F_BAD_SERIES_UID = 1 << 5
F_NO_STUDY_UID = 1 << 6
F_IS_DECIMAL = 1 << 7        # configured IS tag carries a decimal string
F_PREAMBLELESS = 1 << 8
F_ZLIB = 1 << 9
F_NO_PIXELS = 1 << 10        # image SOP class without PixelData
F_CHARSET_SUSPECT = 1 << 11  # declared charset does not decode the sampled bytes

# instances.needs — transform passes required (computed by `analyze`)
N_CHARSET = 1 << 0           # decode with true codec, re-encode UTF-8
N_KEEPGB = 1 << 1            # only fix (0008,0005) declaration to GB18030
N_CARET = 1 << 2
N_TRANSLIT = 1 << 3
N_PID = 1 << 4
N_ISFIX = 1 << 5
N_UIDFIX = 1 << 6
N_STUDYUID = 1 << 7
N_FILEMETA = 1 << 8          # synthesize file meta (preamble-less / zlib)

# studies.verify_status
V_NONE, V_MATCH, V_MISMATCH, V_UNAVAILABLE = range(4)

# problems.severity
SEV_INFO, SEV_WARN, SEV_HARD = 0, 1, 2

# Problem codes.  H-* block arming until resolved (exclude/fix+re-analyze);
# W-* require an explicit `approve --ack CODE`; I-* are informational.
H_UNREADABLE = "H-UNREADABLE"
H_TRUNCATED = "H-TRUNCATED"
H_DUP_CONFLICT = "H-DUP-CONFLICT"
H_PID_RULE_MISS = "H-PID-RULE-MISS"
H_PID_COLLISION = "H-PID-COLLISION"   # two studies assigned the same study-level ID
H_PIXELLESS_ONLY = "H-PIXELLESS-ONLY"
H_NO_IDENTITY = "H-NO-PATIENT-IDENTITY"
H_FUJI_NO_DATE = "H-FUJI-NO-DATE"
W_CHARSET_GUESSED = "W-CHARSET-GUESSED"
W_CHARSET_LOSSY = "W-CHARSET-LOSSY"
W_PN_CARET = "W-PN-CARET-REPAIRED"
W_PN_UNPARSEABLE = "W-PN-UNPARSEABLE"
W_UID_REGEN = "W-UID-REGENERATED"
W_STUDYUID_SYNTH = "W-STUDYUID-SYNTHESIZED"
W_IS_ROUNDED = "W-IS-ROUNDED"
W_DUP_CROSS = "W-DUP-CROSS-SOURCE"
W_DUP_REGEN = "W-DUP-COLLISION-REGEN"      # same-UID/diff-pixels kept via new UIDs
W_DUP_DROPPED = "W-DUP-COLLISION-DROPPED"  # same-UID/diff-pixels, losers discarded
W_PID_REWRITE = "W-PID-REWRITE"
W_IDENTITY_PLACEHOLDER = "W-IDENTITY-PLACEHOLDER"
W_FUJI_MTIME_DATE = "W-FUJI-MTIME-DATE"   # ID date derived from file mtime
W_KEEPGB = "W-GB-DECLARATION-FIXED"
I_NONDICOM = "I-NON-DICOM"

CHARSET_POLICIES = ("utf8", "keep-gb", "keep")
ADAPTERS = ("filetree", "orthanc")

# DICOM defined term -> python codec (single-byte + the multibyte ones we accept)
DICOM_TERM_TO_CODEC = {
    "": "latin_1",  # unset: bytes pass through losslessly for round-tripping
    "ISO_IR 6": "latin_1",
    "ISO_IR 100": "latin_1",
    "ISO_IR 101": "iso8859_2",
    "ISO_IR 109": "iso8859_3",
    "ISO_IR 110": "iso8859_4",
    "ISO_IR 144": "iso8859_5",
    "ISO_IR 127": "iso8859_6",
    "ISO_IR 126": "iso8859_7",
    "ISO_IR 138": "iso8859_8",
    "ISO_IR 148": "iso8859_9",
    "ISO_IR 166": "cp874",
    "ISO_IR 192": "utf_8",
    "GB18030": "gb18030",
    "GBK": "gbk",
}
CODEC_TO_DICOM_TERM = {
    "utf_8": "ISO_IR 192", "utf-8": "ISO_IR 192", "utf8": "ISO_IR 192",
    "gb18030": "GB18030", "gbk": "GB18030",  # GB18030 supersets GBK
    "latin_1": "ISO_IR 100", "latin-1": "ISO_IR 100", "latin1": "ISO_IR 100",
    "iso8859_5": "ISO_IR 144", "iso8859-5": "ISO_IR 144",
    "cp1251": "ISO_IR 192",  # cp1251 has no DICOM term -> must transcode to UTF-8
}

CYRILLIC_RE = re.compile(r"[Ѐ-ӿ]")
CJK_RE = re.compile(r"[一-鿿㐀-䶿]")
UID_RE = re.compile(r"^(0|[1-9][0-9]*)(\.(0|[1-9][0-9]*))*$")

# Optional zstd (Python 3.14+) for sidecar compression
try:
    from compression import zstd as _zstd  # type: ignore
except Exception:  # pragma: no cover
    _zstd = None

GIL_ENABLED: bool = getattr(sys, "_is_gil_enabled", lambda: True)()

log = logging.getLogger(PROG)


# --------------------------------------------------------------------------
# SECTION 3 — CONFIG
# --------------------------------------------------------------------------

@dataclass
class Destination:
    port: int = 11112
    called_aet: str = "DCM4CHEE"


@dataclass
class ServerCfg:
    host: str = "REPLACE_ME"
    destinations: dict[str, Destination] = field(default_factory=dict)
    max_pdu: int = 262144
    acse_timeout: int = 30
    dimse_timeout: int = 120
    network_timeout: int = 60


@dataclass
class RoutingCfg:
    # group name -> list of Modality values; instance modality not listed -> "other"
    groups: dict[str, list[str]] = field(default_factory=lambda: {
        "ct": ["CT"], "xray": ["CR", "DX"], "xa": ["XA", "RF"]})
    precedence: list[str] = field(default_factory=lambda: ["ct", "xa", "xray", "other"])


@dataclass
class NetworkCfg:
    max_associations: int = 3
    rate_limit_mbit: float = 350.0        # 0 = unlimited
    active_hours: str = "20:00-07:00"     # "" = always
    active_days: list[str] = field(default_factory=lambda: ["mon", "tue", "wed", "thu", "fri", "sat", "sun"])
    instances_per_association: int = 500
    backoff_initial_s: float = 5.0
    backoff_max_s: float = 600.0
    max_instance_retries: int = 5
    circuit_breaker_failures: int = 10
    memory_budget_mb: int = 8192
    skip_existing_studies: bool = False   # C-FIND pre-check per study before sending


@dataclass
class PidRule:
    id: str = ""
    match: str = ""            # regex on PatientID (named groups usable in template)
    template: str = ""         # e.g. "SITEB-{num:0>8}"
    station: str = ""          # optional regex on StationName
    institution: str = ""      # optional regex on InstitutionName


@dataclass
class FujiCfg:
    enabled: bool = False
    sites: list[str] = field(default_factory=lambda: ["8K", "9K"])
    # folder path fragment (matched case-insensitively against the file's absolute path) -> site code
    folder_site_map: dict[str, str] = field(default_factory=dict)
    # site code for studies that match neither InstitutionName flag nor folder map
    # (mixed/legacy sources).  "" => such studies become H-PID-RULE-MISS blockers.
    fallback_site: str = ""
    # studies with no/invalid StudyDate: derive the ID date from file mtime
    # (W-FUJI-MTIME-DATE) instead of blocking with H-FUJI-NO-DATE
    date_from_mtime: bool = True


@dataclass
class SourceCfg:
    name: str = ""
    adapter: str = "filetree"
    roots: list[str] = field(default_factory=list)
    calling_aet: str = "MIGRATE"
    priority: int = 100                    # lower wins duplicate resolution
    charset_policy: str = "utf8"           # utf8 | keep-gb | keep
    charset_source: str = "auto"           # "auto" or a python codec name (cp1251, gb18030, ...)
    charset_detect_order: list[str] = field(default_factory=lambda: ["utf-8", "cp1251", "latin-1"])
    caret_repair: bool = False
    translit_cyrillic: bool = False
    is_fix_tags: list[str] = field(default_factory=list)
    max_readers: int = 4
    exclude_dirs: list[str] = field(default_factory=list)
    exclude_file_globs: list[str] = field(default_factory=list)
    file_globs: list[str] = field(default_factory=list)   # empty = all files
    pid_rules_mode: str = "off"            # off | optional | required
    patient_id_rules: list[PidRule] = field(default_factory=list)
    fuji: FujiCfg = field(default_factory=FujiCfg)
    index_db: str = ""                     # orthanc adapter: optional path to Orthanc SQLite index


@dataclass
class GeneralCfg:
    db_path: str = "migration_state.db"
    report_dir: str = "reports"
    sidecar_dir: str = "sidecars"
    log_file: str = "dcm_migrate.log"
    pause_file: str = "PAUSE"
    scan_backend: str = "auto"             # auto | threads | processes
    sidecar_compress: bool = True          # use zstd when available (py3.14+)
    uid_root: str = "2.25"                 # root for regenerated UIDs (UUID-derived)
    # same-SOPInstanceUID + DIFFERENT pixels (real collision): how to resolve
    #   block          -> hard blocker, requires manual review (default, safest)
    #   regenerate-uid -> keep ALL images, give losers fresh SOPInstanceUIDs (no loss)
    #   keep-priority  -> keep one, DISCARD the others (data loss, logged loudly)
    diff_content_policy: str = "block"
    # studies with no PatientID AND no PatientName:
    #   block       -> hard blocker (default)
    #   placeholder -> synthesize "<Institution>-<YYYYMMDD>-<HHMMSS>-Missing"
    no_identity_policy: str = "block"


@dataclass
class Config:
    general: GeneralCfg = field(default_factory=GeneralCfg)
    server: ServerCfg = field(default_factory=ServerCfg)
    routing: RoutingCfg = field(default_factory=RoutingCfg)
    network: NetworkCfg = field(default_factory=NetworkCfg)
    sources: list[SourceCfg] = field(default_factory=list)
    config_path: str = ""
    config_hash: str = ""

    def source(self, name: str) -> SourceCfg:
        for s in self.sources:
            if s.name == name:
                return s
        raise KeyError(f"unknown source: {name}")

    def dest_for_group(self, group: str) -> Destination:
        if group in self.server.destinations:
            return self.server.destinations[group]
        if "other" in self.server.destinations:
            return self.server.destinations["other"]
        raise KeyError(f"no [server.destinations.{group}] and no ...other] fallback in config")

    def group_for_modalities(self, modalities: Iterable[str]) -> str:
        mods = {m.strip().upper() for m in modalities if m and m.strip()}
        mod2group: dict[str, str] = {}
        for g, lst in self.routing.groups.items():
            for m in lst:
                mod2group[m.upper()] = g
        present = {mod2group.get(m, "other") for m in mods} or {"other"}
        for g in self.routing.precedence:
            if g in present:
                return g
        return "other"


def _dc_from_dict(cls, d: dict, ctx: str, errors: list[str]):
    """Populate dataclass `cls` from dict `d`, collecting unknown-key errors."""
    kwargs = {}
    names = {f.name: f for f in dataclasses.fields(cls)}
    for k, v in d.items():
        key = k.replace("-", "_")
        if key not in names:
            errors.append(f"{ctx}: unknown key {k!r}")
            continue
        kwargs[key] = v
    try:
        return cls(**kwargs)
    except TypeError as e:
        errors.append(f"{ctx}: {e}")
        return cls()


def load_config(path: str) -> Config:
    p = Path(path)
    if not p.is_file():
        raise SystemExit(f"config not found: {path}  (run `{PROG} init` to create a template)")
    raw_bytes = p.read_bytes()
    try:
        data = tomllib.loads(raw_bytes.decode("utf-8"))
    except tomllib.TOMLDecodeError as e:
        raise SystemExit(f"config parse error in {path}: {e}")

    errors: list[str] = []
    cfg = Config()
    cfg.config_path = str(p.resolve())
    cfg.config_hash = hashlib.sha256(raw_bytes).hexdigest()

    cfg.general = _dc_from_dict(GeneralCfg, data.get("general", {}), "[general]", errors)

    sv = dict(data.get("server", {}))
    dests = sv.pop("destinations", {})
    cfg.server = _dc_from_dict(ServerCfg, sv, "[server]", errors)
    for gname, dd in dests.items():
        cfg.server.destinations[gname] = _dc_from_dict(Destination, dd, f"[server.destinations.{gname}]", errors)

    rt = dict(data.get("routing", {}))
    precedence = rt.pop("precedence", None)
    groups = {k: v for k, v in rt.items() if isinstance(v, list)}
    cfg.routing = RoutingCfg()
    if groups:
        cfg.routing.groups = {k: [str(x) for x in v] for k, v in groups.items()}
    if precedence:
        cfg.routing.precedence = [str(x) for x in precedence]
    if "other" not in cfg.routing.precedence:
        cfg.routing.precedence.append("other")

    cfg.network = _dc_from_dict(NetworkCfg, data.get("network", {}), "[network]", errors)

    for i, sd in enumerate(data.get("source", [])):
        sd = dict(sd)
        rules = sd.pop("patient_id_rule", [])
        fuji = sd.pop("fuji", {})
        src = _dc_from_dict(SourceCfg, sd, f"[[source]] #{i}", errors)
        src.patient_id_rules = [_dc_from_dict(PidRule, r, f"source {src.name} rule #{j}", errors)
                                for j, r in enumerate(rules)]
        src.fuji = _dc_from_dict(FujiCfg, fuji, f"source {src.name} [source.fuji]", errors)
        cfg.sources.append(src)

    # ---- validation ----
    if not cfg.sources:
        errors.append("no [[source]] sections defined")
    seen = set()
    for s in cfg.sources:
        c = f"source {s.name!r}"
        if not s.name or not re.fullmatch(r"[A-Za-z0-9_\-]+", s.name):
            errors.append(f"{c}: name must be alphanumeric/underscore")
        if s.name in seen:
            errors.append(f"{c}: duplicate source name")
        seen.add(s.name)
        if s.adapter not in ADAPTERS:
            errors.append(f"{c}: adapter must be one of {ADAPTERS}")
        if not s.roots:
            errors.append(f"{c}: roots is empty")
        if s.charset_policy not in CHARSET_POLICIES:
            errors.append(f"{c}: charset_policy must be one of {CHARSET_POLICIES}")
        if s.pid_rules_mode not in ("off", "optional", "required"):
            errors.append(f"{c}: pid_rules_mode must be off|optional|required")
        if not re.fullmatch(r"[A-Za-z0-9._\- ]{1,16}", s.calling_aet or ""):
            errors.append(f"{c}: calling_aet must be a valid AE title (1-16 chars)")
        for t in s.is_fix_tags:
            if not re.fullmatch(r"[0-9a-fA-F]{8}", t):
                errors.append(f"{c}: is_fix_tags entry {t!r} must be 8 hex digits")
        for r in s.patient_id_rules:
            try:
                re.compile(r.match)
                if r.station:
                    re.compile(r.station)
                if r.institution:
                    re.compile(r.institution)
            except re.error as e:
                errors.append(f"{c} rule {r.id!r}: bad regex: {e}")
        if s.fuji.enabled and s.pid_rules_mode == "off":
            s.pid_rules_mode = "required"
    if cfg.general.diff_content_policy not in ("block", "regenerate-uid", "keep-priority"):
        errors.append("[general].diff_content_policy must be block|regenerate-uid|keep-priority")
    if cfg.general.no_identity_policy not in ("block", "placeholder"):
        errors.append("[general].no_identity_policy must be block|placeholder")
    if not cfg.server.destinations:
        errors.append("no [server.destinations.*] defined (need at least 'other')")
    if "other" not in cfg.server.destinations and set(cfg.routing.precedence) - set(cfg.server.destinations):
        errors.append("routing precedence references groups without a [server.destinations.other] fallback")
    for g in cfg.routing.groups:
        if g not in cfg.routing.precedence:
            errors.append(f"routing group {g!r} missing from precedence list")
    if errors:
        raise SystemExit("config errors:\n  - " + "\n  - ".join(errors))
    return cfg


CONFIG_TEMPLATE = r'''# ============================================================================
# dcm_migrate.py configuration
# Fill every REPLACE_ME.  Any edit to this file automatically disarms the
# send gate (approve --arm must be re-run after re-analyze).
# ============================================================================

[general]
db_path     = "D:/migration/state.db"        # SQLite state (WAL); keep on SSD if possible
report_dir  = "D:/migration/reports"
sidecar_dir = "D:/migration/sidecars"        # JSONL audit of every modified attribute
log_file    = "D:/migration/dcm_migrate.log"
pause_file  = "D:/migration/PAUSE"           # create this file => senders drain and idle
scan_backend = "auto"                        # auto | threads | processes
sidecar_compress = true                      # zstd when available (Python 3.14+), else plain
uid_root    = "2.25"                         # root for regenerated (invalid) UIDs
# same SOPInstanceUID but DIFFERENT pixels (a real collision — the server would
# silently keep only one): block (default) | regenerate-uid (keep all, fresh UIDs) |
# keep-priority (keep one, discard others — logs the loss).  Run `dup-audit` first.
diff_content_policy = "block"
# studies with no PatientID AND no PatientName: block (default) |
# placeholder (synthesize "<Institution>-<YYYYMMDD>-<HHMMSS>-Missing")
no_identity_policy = "block"

[server]
host = "REPLACE_ME"                          # the DCM4CHEE IP / hostname
max_pdu = 262144
acse_timeout = 30
dimse_timeout = 120
network_timeout = 60

# The new PACS is split per modality: same IP, different port + called AET.
# Ask the PACS admin for the real values; `dcm_migrate.py echo` validates all.
[server.destinations.ct]
port = 11112
called_aet = "REPLACE_CT_AET"
[server.destinations.xray]
port = 11113
called_aet = "REPLACE_XRAY_AET"
[server.destinations.xa]
port = 11114
called_aet = "REPLACE_XA_AET"
[server.destinations.other]
port = 11115
called_aet = "REPLACE_OTHER_AET"

[routing]
# Modality values -> destination group.  A study is routed as a whole:
# highest-precedence group among its member modalities wins.
ct   = ["CT"]
xray = ["CR", "DX"]
xa   = ["XA", "RF"]
precedence = ["ct", "xa", "xray", "other"]

[network]
max_associations = 3            # concurrent associations (sender threads)
rate_limit_mbit = 350           # bandwidth cap; 0 = unlimited (link is shared!)
active_hours = "20:00-07:00"    # local time window; "" = around the clock
active_days = ["mon","tue","wed","thu","fri","sat","sun"]
instances_per_association = 500 # recycle the association after N stores
backoff_initial_s = 5
backoff_max_s = 600
max_instance_retries = 5
circuit_breaker_failures = 10   # consecutive assoc failures -> pause that source
memory_budget_mb = 8192         # cap on concurrently loaded datasets
skip_existing_studies = false   # true => C-FIND each study first, skip if fully present

# ============================================================================
# SOURCES.  priority: lower number wins when the same SOPInstanceUID exists
# in several sources.  calling_aet: how this source identifies itself.
# ============================================================================

[[source]]
name = "sante_ct"
adapter = "filetree"
roots = ['REPLACE_ME']
calling_aet = "MIG_SANTE"
priority = 10
charset_policy = "utf8"          # decode true encoding -> ISO_IR 192 (UTF-8)
charset_source = "auto"
charset_detect_order = ["utf-8", "cp1251", "latin-1"]
max_readers = 4                  # 6-8 for RAID1/SSD volumes
exclude_dirs = [".Database", "ActiveStorage", "RecycleBin", "WebViewerCache"]
exclude_file_globs = ["index*", "*.db", "*.log", "*.txt"]

[[source]]
name = "orthanc_blob"
adapter = "orthanc"
roots = ['REPLACE_ME:\OrthancStorage']
index_db = ""                    # optional: path to Orthanc's SQLite 'index' file
calling_aet = "MIG_ORTHANC"
priority = 20
charset_policy = "utf8"
charset_detect_order = ["utf-8", "cp1251", "latin-1"]
max_readers = 4

[[source]]
name = "vxvue"
adapter = "filetree"
roots = ['REPLACE_ME:\archive\vxvue']
calling_aet = "MIG_VXVUE"
priority = 30
charset_policy = "utf8"
charset_detect_order = ["utf-8", "cp1251", "latin-1"]
caret_repair = true
max_readers = 4
pid_rules_mode = "optional"      # IDs are close-to-correct; rules normalize them
# Author the real VXvue normalization rules here (first match wins):
#[[source.patient_id_rule]]
#id = "vxvue-strip-prefix"
#match = '^VX[-_ ]?(?P<num>\d+)$'
#template = "{num}"

[[source]]
name = "perlove"
adapter = "filetree"
roots = ['REPLACE_ME:\archive\perlove']
calling_aet = "MIG_PERLOVE"
priority = 40
charset_policy = "keep-gb"       # newer Perlove records arrive in GB -> keep GB,
                                 # only fix the (0008,0005) declaration
charset_source = "gb18030"
is_fix_tags = ["00181150", "00181151", "00181152"]   # IS tags carrying decimals -> rounded
max_readers = 4

[[source]]
name = "fuji_cr"
adapter = "filetree"
roots = ['REPLACE_ME:\archive\fuji']
calling_aet = "MIG_FUJI"
priority = 50
charset_policy = "utf8"
charset_detect_order = ["utf-8", "cp1251", "latin-1"]
caret_repair = true
max_readers = 4
pid_rules_mode = "required"      # every study MUST get a generated ID
[source.fuji]
enabled = true                   # study-level ID: {site}{M}{YY}{###}
sites = ["8K", "9K"]             # looked for as substrings of InstitutionName
fallback_site = "XK"             # mixed/legacy studies with no clear site -> XK marker
                                 # ("" would make them H-PID-RULE-MISS blockers instead).
                                 # fallback IDs use a 2-digit month (XK0122...) — the
                                 # pooled fallback exceeds 999 studies/month and the
                                 # no-leading-zero form collides (Dec-21#1 == Jan-22#1001)
date_from_mtime = true           # no/invalid StudyDate: use the file's mtime for the ID
                                 # date (W-FUJI-MTIME-DATE) instead of H-FUJI-NO-DATE
# Files without a site flag in InstitutionName get their site from the folder
# they came from (case-insensitive substring match on the absolute path):
[source.fuji.folder_site_map]
#'\8ksante' = "8K"
#'\8kdisk2' = "8K"
#'\8k'      = "8K"
#'\9k'      = "9K"
'''

# --------------------------------------------------------------------------
# SECTION 4 — UTIL
# --------------------------------------------------------------------------

def setup_logging(log_file: str | None, verbosity: int = 0) -> None:
    level = logging.DEBUG if verbosity > 0 else logging.INFO
    if verbosity < 0:
        level = logging.WARNING
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s [%(threadName)s] %(message)s", "%H:%M:%S")
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    try:  # Windows consoles often default to cp1252; logs contain Cyrillic/CJK
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    sh = logging.StreamHandler(sys.stderr)
    sh.setLevel(level)
    sh.setFormatter(fmt)
    root.addHandler(sh)
    if log_file:
        try:
            Path(log_file).parent.mkdir(parents=True, exist_ok=True)
            fh = logging.FileHandler(log_file, encoding="utf-8")
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s [%(threadName)s] %(message)s"))
            root.addHandler(fh)
        except OSError as e:
            log.warning("cannot open log file %s: %s", log_file, e)
    # pynetdicom is chatty at INFO
    logging.getLogger("pynetdicom").setLevel(logging.WARNING)
    # legacy archives trip these on purpose — we detect and repair them ourselves;
    # at tens of millions of files they would otherwise drown the log
    logging.getLogger("pydicom").setLevel(logging.ERROR)
    warnings.filterwarnings("ignore", message=r"Invalid value for VR .*")
    warnings.filterwarnings("ignore", message=r'Value ".*" is not valid for elements .*')


def winpath(path: str | Path) -> str:
    r"""Return an absolute path safe for >260-char Windows paths (\\?\ prefix)."""
    p = os.path.abspath(str(path))
    if os.name != "nt" or p.startswith("\\\\?\\"):
        return p
    if p.startswith("\\\\"):  # UNC
        return "\\\\?\\UNC\\" + p[2:]
    return "\\\\?\\" + p


def now_ms() -> int:
    return int(time.time() * 1000)


def human_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024 or unit == "TB":
            return f"{n:,.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024
    return f"{n:,.1f} TB"


def fmt_dur(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}h{m:02d}m" if h else (f"{m}m{s:02d}s" if m else f"{s}s")


def sha256_file(path: str, bufsize: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(winpath(path), "rb") as f:
        while chunk := f.read(bufsize):
            h.update(chunk)
    return h.hexdigest()


_UID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "https://github.com/dcm-migrate")


def make_uid(uid_root: str, *parts: str) -> str:
    """Deterministic UID from arbitrary parts (same input -> same UID, always)."""
    u = uuid.uuid5(_UID_NAMESPACE, "\x1f".join(parts))
    if uid_root == "2.25":
        return f"2.25.{u.int}"
    tail = str(u.int)
    return f"{uid_root}.{tail}"[:64].rstrip(".")


def valid_uid(uid: str) -> bool:
    return bool(uid) and len(uid) <= 64 and UID_RE.fullmatch(uid) is not None


class TokenBucket:
    """Byte-rate limiter shared by all sender threads.  acquire() blocks."""

    def __init__(self, rate_bytes_per_s: float):
        self.rate = float(rate_bytes_per_s)
        self.capacity = max(self.rate * 2.0, 1 << 20)
        self._tokens = self.capacity
        self._ts = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, n: int, stop: threading.Event | None = None) -> None:
        if self.rate <= 0:
            return
        n = min(float(n), self.capacity)  # oversized instances drain a full bucket
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(self.capacity, self._tokens + (now - self._ts) * self.rate)
                self._ts = now
                if self._tokens >= n:
                    self._tokens -= n
                    return
                wait = (n - self._tokens) / self.rate
            if stop is not None and stop.wait(min(wait, 0.5)):
                return
            elif stop is None:
                time.sleep(min(wait, 0.5))


_DAY_KEYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


class ActiveWindow:
    """Local-time active window, e.g. '20:00-07:00' (may span midnight)."""

    def __init__(self, hours: str, days: list[str]):
        self.always = not hours.strip()
        self.days = {d.strip().lower()[:3] for d in days} or set(_DAY_KEYS)
        self.start = self.end = (0, 0)
        if not self.always:
            m = re.fullmatch(r"(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})", hours.strip())
            if not m:
                raise SystemExit(f"bad active_hours: {hours!r} (expected HH:MM-HH:MM)")
            self.start = (int(m.group(1)), int(m.group(2)))
            self.end = (int(m.group(3)), int(m.group(4)))

    def is_active(self, dt: _dt.datetime | None = None) -> bool:
        dt = dt or _dt.datetime.now()
        if _DAY_KEYS[dt.weekday()] not in self.days:
            # day filter applies to the *start* day of overnight windows;
            # keep it simple: day must be enabled for "now"
            return False
        if self.always:
            return True
        cur = (dt.hour, dt.minute)
        if self.start <= self.end:
            return self.start <= cur < self.end
        return cur >= self.start or cur < self.end  # spans midnight


class Pause:
    def __init__(self, path: str):
        self.path = path

    def is_set(self) -> bool:
        return bool(self.path) and os.path.exists(self.path)


class SidecarWriter:
    """Append-only JSONL audit stream, one file per run, zstd if available."""

    def __init__(self, sidecar_dir: str, run_tag: str, compress: bool):
        Path(sidecar_dir).mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        use_zstd = compress and _zstd is not None
        name = f"changes_{run_tag}.jsonl" + (".zst" if use_zstd else "")
        self.path = os.path.join(sidecar_dir, name)
        raw = open(self.path, "ab")
        if use_zstd:
            self._fh = _zstd.ZstdFile(raw, "wb")
        else:
            self._fh = raw

    def write(self, record: dict) -> None:
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
        with self._lock:
            self._fh.write(line.encode("utf-8"))

    def close(self) -> None:
        with self._lock:
            try:
                self._fh.close()
            except Exception:
                pass


# --------------------------------------------------------------------------
# SECTION 5 — DB
# --------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS roots(
  root_id INTEGER PRIMARY KEY, source TEXT NOT NULL, path TEXT NOT NULL,
  UNIQUE(source, path));
CREATE TABLE IF NOT EXISTS sop_classes(id INTEGER PRIMARY KEY, uid TEXT UNIQUE NOT NULL);
CREATE TABLE IF NOT EXISTS xfer(id INTEGER PRIMARY KEY, uid TEXT UNIQUE NOT NULL);
CREATE TABLE IF NOT EXISTS files(
  file_id INTEGER PRIMARY KEY,
  root_id INTEGER NOT NULL,
  rel_path TEXT NOT NULL,
  size INTEGER NOT NULL DEFAULT 0,
  mtime_ns INTEGER NOT NULL DEFAULT 0,
  kind INTEGER NOT NULL DEFAULT 0,
  scan_status INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  content_hash TEXT,
  pixel_hash TEXT,
  UNIQUE(root_id, rel_path));
CREATE TABLE IF NOT EXISTS patients(
  patient_pk INTEGER PRIMARY KEY,
  source TEXT NOT NULL,
  pid_raw TEXT NOT NULL DEFAULT '',
  name_raw TEXT NOT NULL DEFAULT '',
  birth_date TEXT NOT NULL DEFAULT '',
  sex TEXT NOT NULL DEFAULT '',
  pid_final TEXT,
  rule_id TEXT,
  UNIQUE(source, pid_raw, name_raw, birth_date));
CREATE TABLE IF NOT EXISTS studies(
  study_pk INTEGER PRIMARY KEY,
  study_uid TEXT UNIQUE NOT NULL,
  source TEXT NOT NULL DEFAULT '',
  patient_pk INTEGER,
  study_date TEXT NOT NULL DEFAULT '',
  study_time TEXT NOT NULL DEFAULT '',
  accession TEXT NOT NULL DEFAULT '',
  study_desc TEXT NOT NULL DEFAULT '',
  institution TEXT NOT NULL DEFAULT '',
  station TEXT NOT NULL DEFAULT '',
  modalities TEXT NOT NULL DEFAULT '',
  dest_group TEXT,
  n_instances INTEGER NOT NULL DEFAULT 0,
  n_sent INTEGER NOT NULL DEFAULT 0,
  claimed_by TEXT,
  claimed_at INTEGER,
  verify_status INTEGER NOT NULL DEFAULT 0,
  pid_new TEXT,
  pid_rule TEXT);
CREATE TABLE IF NOT EXISTS instances(
  instance_id INTEGER PRIMARY KEY,
  file_id INTEGER NOT NULL UNIQUE,
  source TEXT NOT NULL,
  sop_uid TEXT NOT NULL,
  sop_class_id INTEGER,
  ts_id INTEGER,
  study_pk INTEGER,
  series_uid TEXT NOT NULL DEFAULT '',
  modality TEXT NOT NULL DEFAULT '',
  charset TEXT NOT NULL DEFAULT '',
  detected_codec TEXT NOT NULL DEFAULT '',
  facts INTEGER NOT NULL DEFAULT 0,
  is_decimal_tags TEXT NOT NULL DEFAULT '',
  text_sample BLOB,
  needs INTEGER NOT NULL DEFAULT 0,
  dup_group INTEGER,
  canonical INTEGER NOT NULL DEFAULT 1,
  new_sop_uid TEXT,
  send_status INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_status INTEGER,
  sent_at INTEGER);
CREATE TABLE IF NOT EXISTS uid_remap(
  old_uid TEXT NOT NULL, uid_type TEXT NOT NULL, new_uid TEXT NOT NULL, reason TEXT,
  PRIMARY KEY(old_uid, uid_type));
CREATE TABLE IF NOT EXISTS problems(
  problem_id INTEGER PRIMARY KEY,
  run_id INTEGER NOT NULL DEFAULT 0,
  severity INTEGER NOT NULL,
  code TEXT NOT NULL,
  scope TEXT NOT NULL,
  ref INTEGER,
  detail TEXT,
  resolved INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS acks(
  ack_id INTEGER PRIMARY KEY, code TEXT NOT NULL, note TEXT,
  acked_at INTEGER, config_hash TEXT, analyze_run INTEGER);
CREATE TABLE IF NOT EXISTS send_ledger(
  id INTEGER PRIMARY KEY, instance_id INTEGER, assoc_id INTEGER,
  ts INTEGER, status INTEGER, bytes INTEGER, dur_ms INTEGER, error TEXT);
CREATE TABLE IF NOT EXISTS associations(
  assoc_id INTEGER PRIMARY KEY, started INTEGER, ended INTEGER,
  source TEXT, dest TEXT, calling_aet TEXT, called_aet TEXT,
  n_ok INTEGER NOT NULL DEFAULT 0, n_warn INTEGER NOT NULL DEFAULT 0,
  n_fail INTEGER NOT NULL DEFAULT 0, end_reason TEXT);
CREATE TABLE IF NOT EXISTS verifications(
  id INTEGER PRIMARY KEY, study_pk INTEGER, ts INTEGER,
  expected INTEGER, found INTEGER, status INTEGER);
"""

INDEX_DDL = """
CREATE INDEX IF NOT EXISTS ix_inst_sop ON instances(sop_uid);
CREATE INDEX IF NOT EXISTS ix_inst_study ON instances(study_pk);
CREATE INDEX IF NOT EXISTS ix_inst_pending ON instances(study_pk, send_status)
  WHERE send_status IN (0, 4);
CREATE INDEX IF NOT EXISTS ix_files_root ON files(root_id);
CREATE INDEX IF NOT EXISTS ix_problems ON problems(code, resolved);
CREATE INDEX IF NOT EXISTS ix_studies_claim ON studies(dest_group, source, claimed_by);
"""


def db_connect(path: str, *, readonly: bool = False) -> sqlite3.Connection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    if readonly:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=60)
    else:
        conn = sqlite3.connect(path, timeout=60, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-262144")       # 256 MB page cache
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA mmap_size=4294967296")
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.row_factory = sqlite3.Row
    return conn


def db_init(path: str) -> None:
    conn = db_connect(path)
    with conn:
        conn.executescript(SCHEMA)
        conn.execute("INSERT OR IGNORE INTO meta(key,value) VALUES('schema_version','1')")
    # additive upgrades for databases created by older versions
    for ddl in ("ALTER TABLE files ADD COLUMN content_hash TEXT",
                "ALTER TABLE files ADD COLUMN pixel_hash TEXT",
                "ALTER TABLE instances ADD COLUMN new_sop_uid TEXT"):
        try:
            with conn:
                conn.execute(ddl)
        except sqlite3.OperationalError:
            pass  # column already present
    conn.close()


def ensure_indexes(conn: sqlite3.Connection) -> None:
    conn.executescript(INDEX_DDL)
    conn.commit()


def meta_get(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def meta_set(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute("INSERT INTO meta(key,value) VALUES(?,?) "
                 "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))


def disarm_gate(conn: sqlite3.Connection, why: str) -> None:
    if meta_get(conn, "gate", "") .startswith("armed"):
        log.warning("send gate DISARMED (%s)", why)
    meta_set(conn, "gate", f"disarmed:{why}:{now_ms()}")


class Interner:
    """Cache-backed id lookup for small high-repetition tables."""

    def __init__(self, table: str):
        self.table = table
        self.cache: dict[str, int] = {}

    def get(self, conn: sqlite3.Connection, uid: str) -> int:
        v = self.cache.get(uid)
        if v is not None:
            return v
        conn.execute(f"INSERT OR IGNORE INTO {self.table}(uid) VALUES(?)", (uid,))
        v = conn.execute(f"SELECT id FROM {self.table} WHERE uid=?", (uid,)).fetchone()[0]
        self.cache[uid] = v
        return v

    def reverse(self, conn: sqlite3.Connection) -> dict[int, str]:
        return {r["id"]: r["uid"] for r in conn.execute(f"SELECT id, uid FROM {self.table}")}


class DbWriter(threading.Thread):
    """The single write connection.  All mutations funnel through its queue.

    Queue items:
      ("exec", sql, params)      one statement
      ("many", sql, seq)         executemany
      ("func", callable)         callable(conn) — runs inside the writer thread
      ("flush", threading.Event) commit and signal
    """

    BATCH_COMMIT_ROWS = 5000
    BATCH_COMMIT_SECS = 2.0

    def __init__(self, db_path: str):
        super().__init__(name="db-writer", daemon=True)
        self.db_path = db_path
        self.q: queue.Queue = queue.Queue(maxsize=10000)
        # NB: not `_stop` — that shadows a private threading.Thread method on py3.11
        self._stopping = threading.Event()
        self.error: BaseException | None = None

    def run(self) -> None:
        conn = db_connect(self.db_path)
        pending = 0
        last_commit = time.monotonic()
        try:
            while True:
                try:
                    item = self.q.get(timeout=0.25)
                except queue.Empty:
                    item = None
                if item is None:
                    if pending and time.monotonic() - last_commit > self.BATCH_COMMIT_SECS:
                        conn.commit()
                        pending, last_commit = 0, time.monotonic()
                    if self._stopping.is_set() and self.q.empty():
                        break
                    continue
                kind = item[0]
                try:
                    if kind == "exec":
                        conn.execute(item[1], item[2])
                        pending += 1
                    elif kind == "many":
                        conn.executemany(item[1], item[2])
                        pending += len(item[2])
                    elif kind == "func":
                        item[1](conn)
                        pending += 1
                    elif kind == "flush":
                        conn.commit()
                        pending, last_commit = 0, time.monotonic()
                        item[1].set()
                except BaseException as e:  # surface, don't die silently
                    self.error = e
                    log.exception("db-writer error on %s", kind)
                    if kind == "flush":
                        item[1].set()
                if pending >= self.BATCH_COMMIT_ROWS:
                    conn.commit()
                    pending, last_commit = 0, time.monotonic()
        finally:
            try:
                conn.commit()
            finally:
                conn.close()

    # -- producer API ------------------------------------------------------
    def exec(self, sql: str, params: tuple = ()) -> None:
        self.q.put(("exec", sql, params))

    def many(self, sql: str, seq: list[tuple]) -> None:
        if seq:
            self.q.put(("many", sql, seq))

    def func(self, fn: Callable[[sqlite3.Connection], None]) -> None:
        self.q.put(("func", fn))

    def flush(self) -> None:
        ev = threading.Event()
        self.q.put(("flush", ev))
        ev.wait()
        if self.error:
            raise RuntimeError(f"db-writer failed: {self.error!r}")

    def stop(self) -> None:
        self._stopping.set()
        self.join()
        if self.error:
            raise RuntimeError(f"db-writer failed: {self.error!r}")


def add_problem(w: DbWriter, run_id: int, severity: int, code: str, scope: str,
                ref: int | None, detail: dict | str) -> None:
    d = detail if isinstance(detail, str) else json.dumps(detail, ensure_ascii=False)
    w.exec("INSERT INTO problems(run_id,severity,code,scope,ref,detail) VALUES(?,?,?,?,?,?)",
           (run_id, severity, code, scope, ref, d))

# --------------------------------------------------------------------------
# SECTION 6 — DICOM IO (sniffing + safe reading)
# --------------------------------------------------------------------------

_PLAUSIBLE_GROUPS = {0x0002, 0x0008, 0x0010, 0x0018, 0x0020, 0x0028, 0x0032, 0x0040, 0x7FE0}


class NotDicomPayload(ValueError):
    """A container/wrapper opened fine but its payload is not a DICOM dataset."""


def sniff_kind(head: bytes, size: int) -> int:
    """Classify a file from its first bytes.  Never raises."""
    if size < 8 or len(head) < 8:
        return K_TRUNCATED
    if len(head) >= 132 and head[128:132] == b"DICM":
        return K_PART10
    stripped = head.lstrip()
    if stripped[:1] in (b"{", b"["):
        return K_ORTHANC_JSON
    if ORTHANC_ATTACH_RE.match(head):
        return K_ORTHANC_ATTACH
    # Orthanc "zlib with size": uint64-LE uncompressed size + zlib stream
    if len(head) >= 10 and head[8] == 0x78 and head[9] in (0x01, 0x5E, 0x9C, 0xDA):
        usize = int.from_bytes(head[0:8], "little")
        if 0 < usize < (1 << 42):
            return K_ZLIB_PART10  # provisional; refined after inflation
    group = int.from_bytes(head[0:2], "little")
    elem = int.from_bytes(head[2:4], "little")
    if group in _PLAUSIBLE_GROUPS and elem < 0x4000:
        return K_PREAMBLELESS
    return K_NONDICOM


class ReadResult:
    __slots__ = ("ds", "kind", "payload_size", "header_end", "orig_implicit", "orig_little")

    def __init__(self, ds, kind, payload_size, header_end, orig_implicit, orig_little):
        self.ds = ds
        self.kind = kind
        self.payload_size = payload_size      # size of the (inflated) DICOM byte stream
        self.header_end = header_end          # stream offset where header read stopped
        self.orig_implicit = orig_implicit
        self.orig_little = orig_little


def read_dicom(path: str, kind: int, *, headers_only: bool,
               fp: io.IOBase | None = None) -> ReadResult:
    """Read a DICOM file of any supported kind.  Raises on parse failure.

    `fp`: an already-open binary handle to reuse (the scanner sniffs and parses
    from ONE open — on HDDs the second open/seek per file is real money).
    Caller keeps ownership of a passed-in fp.  Header-only reads use a modest
    buffer: a 1 MB buffer would drag the whole file off disk for typical CT
    slices when we only need the few-KB header."""
    apath = winpath(path)
    if kind in (K_ZLIB_PART10, K_ZLIB_PREAMBLELESS):
        if fp is not None:
            fp.seek(0)
            blob = fp.read()
        else:
            with open(apath, "rb") as f:
                blob = f.read()
        usize = int.from_bytes(blob[0:8], "little")
        data = zlib.decompress(blob[8:])
        if len(data) != usize:
            raise ValueError(f"zlib size mismatch: header {usize}, got {len(data)}")
        inner = sniff_kind(data[:512], len(data))
        if inner == K_PART10:
            kind = K_ZLIB_PART10
        elif inner == K_PREAMBLELESS:
            kind = K_ZLIB_PREAMBLELESS
        else:
            # e.g. Orthanc's compressed DICOM-as-JSON summaries: metadata, not damage
            raise NotDicomPayload(f"zlib payload is not DICOM ({KIND_NAMES.get(inner)})")
        fp = io.BytesIO(data)
        close_after = False
        payload_size = len(data)
    elif fp is not None:
        fp.seek(0)
        close_after = False
        payload_size = os.fstat(fp.fileno()).st_size
    else:
        fp = open(apath, "rb", buffering=(1 << 18) if headers_only else (1 << 20))
        close_after = True
        payload_size = os.fstat(fp.fileno()).st_size

    try:
        force = kind not in (K_PART10, K_ZLIB_PART10)
        # defer only for header scans; full reads must be self-contained (BytesIO!)
        ds = dcmread(fp, stop_before_pixels=headers_only, force=force,
                     defer_size=4096 if headers_only else None)
        header_end = fp.tell()
    finally:
        if close_after:
            fp.close()

    if force and len(ds) == 0:
        raise ValueError("force-read produced an empty dataset (not DICOM)")
    orig_implicit, orig_little = None, None
    enc = getattr(ds, "original_encoding", None)
    if enc is not None:
        # pydicom 3: named tuple/tuple (implicit_vr, little_endian)
        orig_implicit = getattr(enc, "implicit_vr", None)
        orig_little = getattr(enc, "little_endian", None)
        if orig_implicit is None and isinstance(enc, tuple) and len(enc) == 2:
            orig_implicit, orig_little = enc
    return ReadResult(ds, kind, payload_size, header_end, orig_implicit, orig_little)


def guess_transfer_syntax(rr: ReadResult) -> str:
    """Transfer syntax for a dataset (from meta, or sniffed encoding)."""
    ts = ""
    fm = getattr(rr.ds, "file_meta", None)
    if fm is not None:
        ts = str(fm.get("TransferSyntaxUID", "") or "")
    if ts:
        return ts
    if rr.orig_implicit:
        return str(ImplicitVRLittleEndian)
    if rr.orig_little is False:
        return str(ExplicitVRBigEndian)
    return str(ExplicitVRLittleEndian)


def raw_bytes_of(ds: Dataset, tag: Tag) -> bytes | None:
    """Raw on-disk bytes of an element, without charset conversion (top level)."""
    try:
        item = ds.get_item(tag)
    except Exception:
        return None
    if item is None:
        return None
    v = item.value
    if isinstance(v, bytes):
        return v
    if isinstance(v, str):
        return v.encode("latin-1", "replace")
    if v is None:
        return b""
    return str(v).encode("latin-1", "replace")


def safe_str(ds: Dataset, keyword: str, maxlen: int = 256) -> str:
    """ASCII-ish scalar read that never raises (for indexing fields)."""
    try:
        v = ds.get(keyword, "")
    except Exception:
        try:
            b = raw_bytes_of(ds, Tag(pydicom.datadict.tag_for_keyword(keyword)))
            v = (b or b"").decode("latin-1", "replace")
        except Exception:
            return ""
    if v is None:
        return ""
    try:
        if isinstance(v, (list, pydicom.multival.MultiValue)):
            v = "\\".join(str(x) for x in v)
        s = str(v)
    except Exception:
        return ""
    return s.strip()[:maxlen]


_IMAGE_SOP_PREFIX = "1.2.840.10008.5.1.4.1.1."

# SOP-class families whose instances DO carry PixelData: for these, a file with no
# pixels is a genuinely damaged image (hard blocker).  Everything else under the
# 1.2.840.10008.5.1.4.1.1.* tree that lacks pixels — Presentation States (.11),
# Structured Reports/Key Objects (.88), Registrations/RWV/Fiducials (.66/.67),
# Encapsulated docs (.104), RT (.481), waveforms (.9), spectroscopy (.4.2) — is a
# valid non-image object that MUST be migrated, not flagged as broken.
_PIXEL_IMAGE_SOP = (
    "1.2.840.10008.5.1.4.1.1.1",     # CR / DX / MG / IO (+ .1.x variants)
    "1.2.840.10008.5.1.4.1.1.2",     # CT (+ enhanced .2.1/.2.2)
    "1.2.840.10008.5.1.4.1.1.3",     # US multi-frame
    "1.2.840.10008.5.1.4.1.1.4",     # MR (+ enhanced); .4.2 excluded below
    "1.2.840.10008.5.1.4.1.1.6",     # US
    "1.2.840.10008.5.1.4.1.1.7",     # Secondary Capture (+ multiframe .7.x)
    "1.2.840.10008.5.1.4.1.1.12",    # XA / XRF (+ enhanced)
    "1.2.840.10008.5.1.4.1.1.13",    # X-Ray 3D
    "1.2.840.10008.5.1.4.1.1.14",    # Intravascular OCT
    "1.2.840.10008.5.1.4.1.1.20",    # NM
    "1.2.840.10008.5.1.4.1.1.30",    # Parametric Map
    "1.2.840.10008.5.1.4.1.1.77",    # VL / ophthalmic / endoscopy
    "1.2.840.10008.5.1.4.1.1.128",   # PET
    "1.2.840.10008.5.1.4.1.1.130",   # Enhanced PET
    "1.2.840.10008.5.1.4.1.1.481.1",  # RT Image
)
_NONPIXEL_EXCEPTIONS = {
    "1.2.840.10008.5.1.4.1.1.4.2",   # MR Spectroscopy (no PixelData)
}


def sop_needs_pixels(uid: str) -> bool:
    """True only for SOP classes that carry PixelData — i.e. where a missing-pixel
    file is a defect.  Non-image IODs (PR/SR/SEG-less/REG/RWV/...) return False."""
    if not uid or uid in _NONPIXEL_EXCEPTIONS:
        return False
    return any(uid == p or uid.startswith(p + ".") for p in _PIXEL_IMAGE_SOP)


def image_sop_class_ids(conn: sqlite3.Connection) -> set[int]:
    return {cid for cid, uid in conn.execute("SELECT id, uid FROM sop_classes")
            if sop_needs_pixels(uid)}


def is_dicomdir(ds: Dataset) -> bool:
    """A media directory (DICOMDIR): valid Part-10 but no SOPInstanceUID by design
    — an index/catalog, never a migratable instance."""
    fm = getattr(ds, "file_meta", None)
    if fm is not None:
        try:
            if str(fm.get("MediaStorageSOPClassUID", "")) == DICOMDIR_SOP:
                return True
        except Exception:
            pass
    return Tag(0x0004, 0x1220) in ds or Tag(0x0004, 0x1130) in ds


def extract_header(rr: ReadResult, src: SourceCfg, file_size: int) -> dict:
    """Pull the scan-time facts out of a header-only dataset.  Never raises."""
    ds = rr.ds
    facts = 0
    rec: dict[str, Any] = {}

    sop_uid = safe_str(ds, "SOPInstanceUID", 128)
    sop_class = safe_str(ds, "SOPClassUID", 128)
    study_uid = safe_str(ds, "StudyInstanceUID", 128)
    series_uid = safe_str(ds, "SeriesInstanceUID", 128)
    if not valid_uid(sop_uid):
        facts |= F_BAD_SOP_UID
    if not study_uid:
        facts |= F_NO_STUDY_UID
    elif not valid_uid(study_uid):
        facts |= F_BAD_STUDY_UID
    if series_uid and not valid_uid(series_uid):
        facts |= F_BAD_SERIES_UID

    charset = safe_str(ds, "SpecificCharacterSet", 64)

    # --- raw text probes (no charset conversion; multi-byte-safe delimiters) ---
    sample = bytearray()
    pn_raw = raw_bytes_of(ds, Tag(0x0010, 0x0010)) or b""
    for t in ((0x0010, 0x0010), (0x0010, 0x0020), (0x0008, 0x0080), (0x0008, 0x1030)):
        b = raw_bytes_of(ds, Tag(*t))
        if b:
            sample += b[:160] + b"\x00"
    sample = bytes(sample[:512])
    if any(c >= 0x80 for c in sample):
        facts |= F_NONASCII_TEXT
    if any(c >= 0x80 for c in pn_raw):
        facts |= F_PN_NONASCII
    alpha_group = pn_raw.split(b"=")[0].strip()
    if alpha_group and b"^" not in alpha_group and b" " in alpha_group:
        facts |= F_PN_CARETLESS

    # --- IS tags carrying decimals ---
    dec_tags = []
    for hx in (src.is_fix_tags or []):
        b = raw_bytes_of(ds, Tag(int(hx, 16)))
        if b and re.search(rb"^\s*-?\d+[.,]\d", b.split(b"\\")[0]):
            dec_tags.append(hx)
    if dec_tags:
        facts |= F_IS_DECIMAL

    if rr.kind in (K_PREAMBLELESS, K_ZLIB_PREAMBLELESS):
        facts |= F_PREAMBLELESS
    if rr.kind in (K_ZLIB_PART10, K_ZLIB_PREAMBLELESS):
        facts |= F_ZLIB

    # header-only attachment heuristic: image SOP class but ~nothing after header
    if sop_class.startswith(_IMAGE_SOP_PREFIX):
        remaining = rr.payload_size - rr.header_end
        if remaining < 64 and "PixelData" not in ds:
            facts |= F_NO_PIXELS

    rec.update(
        sop_uid=sop_uid, sop_class=sop_class, ts_uid=guess_transfer_syntax(rr),
        study_uid=study_uid, series_uid=series_uid,
        modality=safe_str(ds, "Modality", 16).upper(),
        pid=(raw_bytes_of(ds, Tag(0x0010, 0x0020)) or b"").decode("latin-1", "replace").strip(),
        pname_raw=pn_raw.decode("latin-1", "replace").strip(),
        birth=safe_str(ds, "PatientBirthDate", 16),
        sex=safe_str(ds, "PatientSex", 8),
        study_date=safe_str(ds, "StudyDate", 16),
        study_time=safe_str(ds, "StudyTime", 24),
        accession=safe_str(ds, "AccessionNumber", 32),
        study_desc=safe_str(ds, "StudyDescription", 128),
        institution=(raw_bytes_of(ds, Tag(0x0008, 0x0080)) or b"").decode("latin-1", "replace").strip(),
        station=safe_str(ds, "StationName", 32),
        charset=charset, facts=facts,
        is_decimal_tags=",".join(dec_tags),
        # the sample only feeds charset detection — pure-ASCII files (most CT)
        # don't need one, and at tens of millions of rows it dominates DB size
        text_sample=sample if facts & F_NONASCII_TEXT else None,
    )
    return rec


# --------------------------------------------------------------------------
# SECTION 7 — SOURCE ADAPTERS (enumeration)
# --------------------------------------------------------------------------

def _match_any(name: str, globs: list[str]) -> bool:
    low = name.lower()
    return any(fnmatch.fnmatch(low, g.lower()) for g in globs)


def iter_filetree(root: str, src: SourceCfg) -> Iterator[tuple[str, int, int]]:
    """Yield (abs_path, size, mtime_ns) under root, honoring source excludes."""
    excl_dirs = {d.lower() for d in src.exclude_dirs}
    stack = [root]
    while stack:
        d = stack.pop()
        try:
            with os.scandir(winpath(d)) as it:
                for entry in it:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            if entry.name.lower() in excl_dirs:
                                continue
                            stack.append(os.path.join(d, entry.name))
                        elif entry.is_file(follow_symlinks=False):
                            if src.file_globs and not _match_any(entry.name, src.file_globs):
                                continue
                            if src.exclude_file_globs and _match_any(entry.name, src.exclude_file_globs):
                                continue
                            st = entry.stat()
                            yield os.path.join(d, entry.name), st.st_size, st.st_mtime_ns
                    except OSError as e:
                        log.warning("scan: cannot stat %s: %s", entry.path, e)
        except OSError as e:
            log.warning("scan: cannot list %s: %s", d, e)


_HEX2 = re.compile(r"^[0-9a-fA-F]{2}$")


def iter_orthanc(root: str, src: SourceCfg) -> Iterator[tuple[str, int, int]]:
    """Orthanc storage area: root/xy/zw/<uuid>.  Non-conforming dirs skipped."""
    try:
        with os.scandir(winpath(root)) as l1:
            level1 = [e.name for e in l1 if e.is_dir(follow_symlinks=False) and _HEX2.match(e.name)]
    except OSError as e:
        log.error("orthanc scan: cannot list %s: %s", root, e)
        return
    for d1 in sorted(level1):
        p1 = os.path.join(root, d1)
        try:
            with os.scandir(winpath(p1)) as l2:
                level2 = [e.name for e in l2 if e.is_dir(follow_symlinks=False) and _HEX2.match(e.name)]
        except OSError as e:
            log.warning("orthanc scan: cannot list %s: %s", p1, e)
            continue
        for d2 in sorted(level2):
            p2 = os.path.join(p1, d2)
            try:
                with os.scandir(winpath(p2)) as files:
                    for entry in files:
                        try:
                            if entry.is_file(follow_symlinks=False):
                                st = entry.stat()
                                yield os.path.join(p2, entry.name), st.st_size, st.st_mtime_ns
                        except OSError as e:
                            log.warning("orthanc scan: cannot stat %s: %s", entry.path, e)
            except OSError as e:
                log.warning("orthanc scan: cannot list %s: %s", p2, e)


def orthanc_index_uuids(index_db: str) -> set[str] | None:
    """UUIDs of fileType=1 (DICOM) attachments from Orthanc's SQLite index.

    Returns None when the index is unavailable — callers then classify blindly.
    """
    if not index_db or not os.path.isfile(index_db):
        return None
    try:
        conn = sqlite3.connect(f"file:{index_db}?mode=ro&immutable=1", uri=True)
        try:
            rows = conn.execute("SELECT uuid FROM AttachedFiles WHERE fileType=1").fetchall()
            return {r[0] for r in rows}
        finally:
            conn.close()
    except sqlite3.Error as e:
        log.warning("orthanc index %s unreadable (%s) — falling back to blind classification",
                    index_db, e)
        return None


def iter_source_files(src: SourceCfg) -> Iterator[tuple[str, str, int, int]]:
    """Yield (root, abs_path, size, mtime_ns) for every candidate file of a source."""
    it = iter_orthanc if src.adapter == "orthanc" else iter_filetree
    for root in src.roots:
        if not os.path.isdir(winpath(root)):
            log.error("source %s: root does not exist: %s", src.name, root)
            continue
        for abs_path, size, mtime_ns in it(root, src):
            yield root, abs_path, size, mtime_ns

# --------------------------------------------------------------------------
# SECTION 8 — CHARSET (detection, reinterpretation, transliteration)
# --------------------------------------------------------------------------

import codecs as _codecs


def canon_codec(name: str) -> str:
    try:
        return _codecs.lookup(name).name
    except LookupError:
        return ""


def declared_to_codec(charset_value: str) -> str | None:
    """Python codec for a declared (0008,0005).  None => code-extension (ISO 2022)
    or unknown — reinterpretation is skipped and pydicom's own decode is kept."""
    terms = [t.strip() for t in charset_value.split("\\")] if charset_value else [""]
    if len(terms) > 1 or terms[0].startswith("ISO 2022"):
        return None
    return canon_codec(DICOM_TERM_TO_CODEC.get(terms[0], "")) or None


def _score_decoded(s: str) -> float:
    """Plausibility of a decoded text: expected scripts / non-ASCII chars."""
    non_ascii = [c for c in s if ord(c) > 127]
    if not non_ascii:
        return 1.0
    good = sum(1 for c in non_ascii
               if CYRILLIC_RE.match(c) or CJK_RE.match(c) or c in "«»–—’“”№°µ")
    bad = sum(1 for c in non_ascii if ord(c) < 0xA0 or c == "�")  # C1 ctrls, U+FFFD
    return (good - 2 * bad) / len(non_ascii)


def detect_codec(sample: bytes, detect_order: list[str]) -> tuple[str, float]:
    """Best-scoring codec from the configured candidate order.

    Returns (codec_name, score).  ('', 0) when nothing decodes acceptably.
    """
    if not sample or all(b < 0x80 for b in sample):
        return "ascii", 1.0
    best, best_score = "", -9.0
    for i, cand in enumerate(detect_order):
        codec = canon_codec(cand)
        if not codec:
            continue
        try:
            decoded = sample.decode(codec)
        except (UnicodeDecodeError, ValueError):
            continue
        score = _score_decoded(decoded) - i * 0.01  # slight priority to earlier candidates
        if score > best_score:
            best, best_score = codec, score
    return best, best_score


def effective_codec(src: SourceCfg, declared: str, sample: bytes) -> tuple[str, bool]:
    """(true_codec, guessed?) for a file's text.  Config override wins; else the
    declared codec is trusted when it decodes the sample cleanly and plausibly;
    else detection kicks in."""
    if src.charset_source and src.charset_source != "auto":
        return canon_codec(src.charset_source) or "latin_1", False
    dec = declared_to_codec(declared)
    if dec is not None and dec != "latin_1":  # a real single/multibyte declaration
        try:
            if _score_decoded(sample.decode(dec)) >= 0.9:
                return dec, False
        except (UnicodeDecodeError, ValueError):
            pass
    if not sample or all(b < 0x80 for b in sample):
        return "ascii", False
    codec, score = detect_codec(sample, src.charset_detect_order)
    if codec and score >= 0.5:
        return codec, True
    return (dec or "latin_1"), True  # nothing convincing: keep declared, flag guessed


# --- Cyrillic -> Latin transliteration (ported from patient_name_coercion.xsl) ---

_TRANSLIT_MULTI = {
    "ж": "zh", "Ж": "Zh", "ч": "ch", "Ч": "Ch", "ш": "sh", "Ш": "Sh",
    "щ": "shch", "Щ": "Shch", "ю": "iu", "Ю": "Iu", "я": "ia", "Я": "Ia",
    "ё": "yo", "Ё": "Yo", "є": "ie", "Є": "Ie", "ї": "ii", "Ї": "Ii",
}
_TRANSLIT_SINGLE = dict(zip(
    "абвгґдезиыйіклмнопрстуфхцэАБВГҐДЕЗИЫЙІКЛМНОПРСТУФХЦЭ",
    "abvggdezyyjiklmnoprstufhceABVGGDEZYYJIKLMNOPRSTUFHCE"))
_TRANSLIT_DROP = set("ъьЪЬ`'’")


def translit_cyrillic(s: str) -> str:
    out = []
    for ch in s:
        if ch in _TRANSLIT_MULTI:
            out.append(_TRANSLIT_MULTI[ch])
        elif ch in _TRANSLIT_SINGLE:
            out.append(_TRANSLIT_SINGLE[ch])
        elif ch in _TRANSLIT_DROP:
            continue
        else:
            out.append(ch)
    return "".join(out)


def repair_caret(pn: str) -> tuple[str, bool]:
    """'Family Given [Middle]' -> 'Family^Given[^Middle]' on the alphabetic group.
    Conservative: skips when digits present or token count not in 2..4."""
    groups = pn.split("=")
    g0 = groups[0]
    if "^" in g0 or " " not in g0.strip():
        return pn, False
    tokens = g0.split()
    if not (2 <= len(tokens) <= 4) or any(any(c.isdigit() for c in t) for t in tokens):
        return pn, False
    groups[0] = "^".join(tokens)
    return "=".join(groups), True


# --------------------------------------------------------------------------
# SECTION 9 — TRANSFORM (in-memory fix-up passes + rules engines)
# --------------------------------------------------------------------------

def apply_pid_rules(src: SourceCfg, pid: str, station: str, institution: str
                    ) -> tuple[str | None, str | None]:
    """First-match-wins regex/template rules.  Returns (new_pid, rule_id)."""
    for rule in src.patient_id_rules:
        if rule.station and not re.search(rule.station, station or ""):
            continue
        if rule.institution and not re.search(rule.institution, institution or "", re.IGNORECASE):
            continue
        m = re.fullmatch(rule.match, pid or "")
        if not m:
            continue
        try:
            gd = {k: (v if v is not None else "") for k, v in m.groupdict().items()}
            new = rule.template.format(pid=pid, **gd)
        except (KeyError, IndexError, ValueError) as e:
            log.warning("pid rule %s: template error for %r: %s", rule.id, pid, e)
            return None, None
        return new.strip(), (rule.id or rule.match)
    return None, None


def fuji_site_for(src: SourceCfg, institution: str, abs_path: str) -> str | None:
    inst = (institution or "").upper()
    for site in src.fuji.sites:
        if site.upper() in inst:
            return site
    low = abs_path.lower().replace("/", "\\")
    for frag, site in src.fuji.folder_site_map.items():
        if frag.lower().replace("/", "\\") in low:
            return site
    return None


def fuji_pid(site: str, study_date: str, ordinal: int, two_digit_month: bool = False) -> str:
    """{site}{M}{YY}{###} — M is 1..12 without leading zero, YY 2-digit year,
    ### per-month ordinal (grows past 999 naturally).

    two_digit_month zero-pads M (used for the fallback site): the historical
    no-leading-zero format is ambiguous once a month exceeds 999 studies —
    Dec-2021 #1 (12+21+001) == Jan-2022 #1001 (1+22+1001) == "XK1221001"."""
    yy = study_date[2:4]
    month = int(study_date[4:6])
    m = f"{month:02d}" if two_digit_month else str(month)
    return f"{site}{m}{yy}{ordinal:03d}"


def placeholder_pid(institution: str, study_date: str, study_time: str) -> str:
    """Synthetic PatientID for studies with no identity: <Inst>-<YYYYMMDD>-<HHMMSS>-Missing."""
    inst = re.sub(r"[^A-Za-z0-9]+", "_", (institution or "").strip()).strip("_")[:24] or "UNKNOWN"
    d = (re.sub(r"\D", "", study_date or "")[:8] or "00000000").ljust(8, "0")
    t = (re.sub(r"\D", "", study_time or "")[:6]).ljust(6, "0")
    return f"{inst}-{d}-{t}-Missing"[:64]


def synth_study_uid(uid_root: str, source: str, pid: str, study_date: str,
                    accession: str, fallback: str) -> str:
    """Deterministic StudyInstanceUID for files that lack one.  Groups siblings
    by (source, patient, date, accession); falls back to the file identity."""
    if pid or study_date or accession:
        return make_uid(uid_root, "studyuid", source, pid, study_date, accession)
    return make_uid(uid_root, "studyuid-file", source, fallback)


TAG_CHARSET = Tag(0x0008, 0x0005)
_OAS_INTEREST = {
    Tag(0x0008, 0x0005), Tag(0x0010, 0x0010), Tag(0x0010, 0x0020),
    Tag(0x0020, 0x000D), Tag(0x0020, 0x000E), Tag(0x0008, 0x0018),
}


class Transformer:
    """Applies the fix-up passes to one dataset.  Stateless between calls except
    for config; UID remaps and final PIDs are injected (computed by `analyze`)."""

    def __init__(self, cfg: Config, src: SourceCfg,
                 uid_lookup: Callable[[str, str], str | None]):
        self.cfg = cfg
        self.src = src
        self.uid_lookup = uid_lookup  # (old_uid, uid_type) -> new_uid | None

    # -- helpers ------------------------------------------------------------
    @staticmethod
    def _tagstr(tag: Tag) -> str:
        return f"{tag.group:04X}{tag.element:04X}"

    def _record(self, changes: list, tag: Tag, vr: str, old, new, pass_name: str) -> None:
        changes.append({"tag": self._tagstr(tag), "vr": vr,
                        "old": old if isinstance(old, str) else repr(old),
                        "new": new if isinstance(new, str) else repr(new),
                        "pass": pass_name})

    # -- passes ---------------------------------------------------------------
    def _pass_is_fix(self, ds: Dataset, changes: list) -> None:
        for hx in self.src.is_fix_tags:
            tag = Tag(int(hx, 16))
            raw = raw_bytes_of(ds, tag)
            if raw is None:
                continue
            parts = raw.decode("latin-1", "replace").split("\\")
            fixed, touched = [], False
            for p in parts:
                p = p.strip().strip("\x00")
                if re.fullmatch(r"-?\d+[.,]\d+([eE][+-]?\d+)?", p):
                    fixed.append(str(round(float(p.replace(",", ".")))))
                    touched = True
                else:
                    fixed.append(p)
            if touched:
                newval = fixed[0] if len(fixed) == 1 else fixed
                ds[tag] = DataElement(tag, "IS", newval)
                self._record(changes, tag, "IS", raw.decode("latin-1", "replace").strip(),
                             "\\".join(fixed), "is-round")

    def _pass_uids(self, ds: Dataset, rec_facts: int, inst: sqlite3.Row,
                   changes: list) -> None:
        for kw, ftag, utype in (("SOPInstanceUID", F_BAD_SOP_UID, "sop"),
                                ("SeriesInstanceUID", F_BAD_SERIES_UID, "series"),
                                ("StudyInstanceUID", F_BAD_STUDY_UID, "study")):
            if not rec_facts & ftag:
                continue
            old = safe_str(ds, kw, 256)
            new = self.uid_lookup(old, utype)
            if not new:  # deterministic regeneration is always available
                new = make_uid(self.cfg.general.uid_root, "uid", utype, old or f"file{inst['file_id']}")
            tag = Tag(pydicom.datadict.tag_for_keyword(kw))
            ds[tag] = DataElement(tag, "UI", new)
            self._record(changes, tag, "UI", old, new, "uid-regen")
        if rec_facts & F_NO_STUDY_UID:
            new = synth_study_uid(self.cfg.general.uid_root, self.src.name,
                                  inst["pid_raw"] if "pid_raw" in inst.keys() else "",
                                  inst["study_date"] if "study_date" in inst.keys() else "",
                                  inst["accession"] if "accession" in inst.keys() else "",
                                  str(inst["file_id"]))
            tag = Tag(0x0020, 0x000D)
            ds[tag] = DataElement(tag, "UI", new)
            self._record(changes, tag, "UI", "", new, "studyuid-synth")

    def _iter_text_elements(self, dset: Dataset) -> Iterator[tuple[Dataset, DataElement]]:
        """Yield (containing_dataset, element) for text VRs, recursing sequences.
        Elements whose lazy conversion explodes are skipped (left raw/verbatim)."""
        for tag in list(dset.keys()):
            try:
                elem = dset[tag]  # triggers conversion of raw elements
            except Exception:
                continue
            if elem.VR == "SQ":
                try:
                    for item in elem.value or []:
                        yield from self._iter_text_elements(item)
                except Exception:
                    continue
            elif elem.VR in TEXT_VRS:
                yield dset, elem

    def _pass_charset(self, ds: Dataset, inst: sqlite3.Row, changes: list) -> None:
        """Normalize text encoding.  NOTE: pydicom 3 re-encodes raw text elements
        at write time whenever (0008,0005) differs from the charset the file was
        read with — so a declaration change is only safe if every non-ASCII text
        element is first materialized as a correctly-decoded str.  Both policies
        therefore share the same walk; only the target charset differs."""
        policy = self.src.charset_policy
        declared = safe_str(ds, "SpecificCharacterSet", 64)
        if policy == "keep" or not inst["facts"] & F_NONASCII_TEXT:
            return
        declared_codec = declared_to_codec(declared)
        if policy == "keep-gb":
            target_term = "GB18030"
            true_codec = canon_codec(inst["detected_codec"] or "") or "gb18030"
            if true_codec == "ascii":
                true_codec = "gb18030"
            pass_name = "gb-declare"
        else:  # utf8
            target_term = "ISO_IR 192"
            true_codec = canon_codec(inst["detected_codec"] or "") or declared_codec or "latin_1"
            if true_codec == "ascii":
                return
            pass_name = "charset"
        if declared == target_term:
            return
        reinterpret = (declared_codec is not None and declared_codec != true_codec)
        for dset, elem in self._iter_text_elements(ds):
            try:
                vals = elem.value if isinstance(elem.value, (list, pydicom.multival.MultiValue)) \
                    else [elem.value]
                out, touched = [], False
                for v in vals:
                    s = str(v) if v is not None else ""
                    if reinterpret and any(ord(c) > 127 for c in s):
                        try:
                            s2 = s.encode(declared_codec, "strict").decode(true_codec, "strict")
                        except (UnicodeError, ValueError):
                            try:
                                s2 = s.encode(declared_codec, "replace").decode(true_codec, "replace")
                            except (UnicodeError, ValueError):
                                s2 = s
                        if s2 != s:
                            touched = True
                        s = s2
                    out.append(s)
                new_val = out[0] if len(out) == 1 else out
                if touched and dset is ds and elem.tag in _OAS_INTEREST:
                    self._record(changes, elem.tag, elem.VR,
                                 "\\".join(str(v) for v in vals),
                                 "\\".join(out), pass_name)
                elem.value = new_val  # re-materialized as str -> encoded per final charset
            except Exception as e:
                log.debug("charset: skipping %s: %s", elem.tag, e)
        ds[TAG_CHARSET] = DataElement(TAG_CHARSET, "CS", target_term)
        self._record(changes, TAG_CHARSET, "CS", declared, target_term, pass_name)

    def _pass_pn(self, ds: Dataset, changes: list) -> None:
        tag = Tag(0x0010, 0x0010)
        if tag not in ds:
            return
        try:
            pn = str(ds[tag].value) if ds[tag].value else ""
        except Exception:
            return
        new = pn
        if self.src.caret_repair:
            new, repaired = repair_caret(new)
        if self.src.translit_cyrillic and CYRILLIC_RE.search(new):
            groups = new.split("=")
            latin = translit_cyrillic(groups[0])
            new = f"{latin}={groups[0]}" if latin != groups[0] else new
        if new != pn:
            ds[tag] = DataElement(tag, "PN", new)
            self._record(changes, tag, "PN", pn, new, "pn-fix")

    def _pass_pid(self, ds: Dataset, pid_new: str | None, changes: list) -> None:
        if not pid_new:
            return
        tag = Tag(0x0010, 0x0020)
        old = safe_str(ds, "PatientID", 128)
        if old == pid_new:
            return
        ds[tag] = DataElement(tag, "LO", pid_new)
        self._record(changes, tag, "LO", old, pid_new, "pid-rewrite")

    def _pass_oas(self, ds: Dataset, changes: list) -> None:
        if not changes:
            return
        mod = Dataset()
        seen: set[Tag] = set()
        for ch in changes:
            tag = Tag(int(ch["tag"], 16))
            if tag in seen or len(seen) >= 24:
                continue
            seen.add(tag)
            try:
                mod[tag] = DataElement(tag, ch["vr"], ch["old"])
            except Exception:
                mod[tag] = DataElement(tag, "LO", str(ch["old"])[:64])
        item = Dataset()
        item.ModifiedAttributesSequence = [mod]
        now = _dt.datetime.now(_dt.timezone.utc)
        item.AttributeModificationDateTime = now.strftime("%Y%m%d%H%M%S.%f")[:21] + "+0000"
        item.ModifyingSystem = f"DCM_MIGRATE/{VERSION}"
        item.SourceOfPreviousValues = self.src.name[:64]
        item.ReasonForTheAttributeModification = "COERCE"
        existing = list(ds.get("OriginalAttributesSequence", []) or [])
        existing.append(item)
        ds.OriginalAttributesSequence = existing

    def _pass_filemeta(self, ds: Dataset, ts_uid: str) -> None:
        fm = getattr(ds, "file_meta", None)
        if fm is None or "TransferSyntaxUID" not in fm:
            fm = FileMetaDataset()
            fm.TransferSyntaxUID = UID(ts_uid)
            ds.file_meta = fm
        ds.file_meta.MediaStorageSOPClassUID = UID(safe_str(ds, "SOPClassUID", 128) or "1.2.840.10008.5.1.4.1.1.7")
        ds.file_meta.MediaStorageSOPInstanceUID = UID(safe_str(ds, "SOPInstanceUID", 128))
        ds.file_meta.ImplementationClassUID = pydicom.uid.PYDICOM_IMPLEMENTATION_UID
        ds.file_meta.ImplementationVersionName = f"DCM_MIGRATE_{VERSION}"[:16]

    # -- entry point ---------------------------------------------------------
    def transform(self, rr: ReadResult, inst: sqlite3.Row, pid_new: str | None
                  ) -> tuple[Dataset, list[dict]]:
        ds = rr.ds
        changes: list[dict] = []
        needs = inst["needs"]
        if needs & N_ISFIX:
            self._pass_is_fix(ds, changes)
        if needs & (N_UIDFIX | N_STUDYUID):
            self._pass_uids(ds, inst["facts"], inst, changes)
        if needs & (N_CHARSET | N_KEEPGB):
            self._pass_charset(ds, inst, changes)
        if needs & (N_CARET | N_TRANSLIT):
            self._pass_pn(ds, changes)
        if needs & N_PID:
            self._pass_pid(ds, pid_new, changes)
        new_sop = inst["new_sop_uid"] if "new_sop_uid" in inst.keys() else None
        if new_sop:  # duplicate-collision resolution: distinct image gets a fresh UID
            tag = Tag(0x0008, 0x0018)
            old = safe_str(ds, "SOPInstanceUID", 128)
            ds[tag] = DataElement(tag, "UI", new_sop)
            self._record(changes, tag, "UI", old, new_sop, "dup-collision-uid")
        self._pass_oas(ds, changes)
        self._pass_filemeta(ds, guess_transfer_syntax(rr))
        return ds, changes

# --------------------------------------------------------------------------
# SECTION 10 — SCAN
# --------------------------------------------------------------------------

def src_to_dict(src: SourceCfg) -> dict:
    return dataclasses.asdict(src)


def src_from_dict(d: dict) -> SourceCfg:
    d = dict(d)
    rules = [PidRule(**r) for r in d.pop("patient_id_rules", [])]
    fuji = FujiCfg(**d.pop("fuji", {}))
    src = SourceCfg(**d)
    src.patient_id_rules = rules
    src.fuji = fuji
    return src


def _scan_one(src: SourceCfg, abs_path: str, base: dict) -> dict:
    """Classify + header-read one file.  Returns a result dict; never raises.
    One open() per file: sniff and parse share the handle (HDD seeks are the
    scan's dominant cost)."""
    try:
        f = open(winpath(abs_path), "rb", buffering=1 << 18)
    except OSError as e:
        return {**base, "status": "error", "kind": K_UNKNOWN, "error": f"open: {e}"}
    try:
        try:
            head = f.read(512)
        except OSError as e:
            return {**base, "status": "error", "kind": K_UNKNOWN, "error": f"read: {e}"}
        kind = sniff_kind(head, base["size"])
        if kind in (K_ORTHANC_JSON, K_NONDICOM, K_ORTHANC_ATTACH):
            return {**base, "status": "nondicom", "kind": kind}
        if kind == K_TRUNCATED:
            return {**base, "status": "error", "kind": kind, "error": "zero-length or truncated"}
        try:
            rr = read_dicom(abs_path, kind, headers_only=True, fp=f)
            rec = extract_header(rr, src, base["size"])
        except NotDicomPayload:
            return {**base, "status": "nondicom", "kind": K_NONDICOM}
        except Exception as e:
            err = str(e)[:300]
            k = K_TRUNCATED if isinstance(e, EOFError) or "unexpected end" in err.lower() else kind
            return {**base, "status": "error", "kind": k, "error": f"parse: {err}"}
    finally:
        f.close()
    if not rec["sop_uid"]:
        # DICOMDIR / media directory: no SOPInstanceUID by design, no pixel data —
        # a catalog file, not a migratable instance.  Skip, don't error.
        if is_dicomdir(rr.ds):
            return {**base, "status": "nondicom", "kind": K_DICOMDIR}
        # otherwise a real DICM marker means it IS DICOM — an empty/UID-less parse is
        # damage, not a decoy, and must surface as a hard problem rather than drop
        if kind in (K_PART10, K_ZLIB_PART10) or rec["sop_class"] or rec["study_uid"]:
            return {**base, "status": "error", "kind": rr.kind,
                    "error": "missing SOPInstanceUID (empty or truncated dataset)"}
        return {**base, "status": "nondicom", "kind": K_NONDICOM}
    return {**base, "status": "ok", "kind": rr.kind, "rec": rec}


def _scan_worker(src_dict: dict, in_q, out_q) -> None:
    """Reader-pool worker (thread or process).  Sentinel None terminates."""
    src = src_from_dict(src_dict)
    while True:
        item = in_q.get()
        if item is None:
            break
        abs_path, base = item
        try:
            out_q.put(_scan_one(src, abs_path, base))
        except BaseException as e:  # keep the pool alive no matter what
            out_q.put({**base, "status": "error", "kind": K_UNKNOWN,
                       "error": f"worker: {e!r}"})


class ScanIngestor:
    """Turns scan results into rows.  All methods run inside the db-writer thread."""

    def __init__(self):
        self.sop = Interner("sop_classes")
        self.ts = Interner("xfer")
        self.root_ids: dict[tuple[str, str], int] = {}
        self.study_cache: dict[str, tuple[int, set[str]]] = {}
        self.patient_cache: dict[tuple, int] = {}

    def root_id(self, conn, source: str, root: str) -> int:
        key = (source, root)
        v = self.root_ids.get(key)
        if v is None:
            conn.execute("INSERT OR IGNORE INTO roots(source, path) VALUES(?,?)", key)
            v = conn.execute("SELECT root_id FROM roots WHERE source=? AND path=?", key).fetchone()[0]
            self.root_ids[key] = v
        return v

    def _upsert_file(self, conn, item: dict, scan_status: int, error: str | None) -> int:
        rid = self.root_id(conn, item["source"], item["root"])
        row = conn.execute(
            "INSERT INTO files(root_id, rel_path, size, mtime_ns, kind, scan_status, error) "
            "VALUES(?,?,?,?,?,?,?) "
            "ON CONFLICT(root_id, rel_path) DO UPDATE SET size=excluded.size, "
            "mtime_ns=excluded.mtime_ns, kind=excluded.kind, scan_status=excluded.scan_status, "
            "error=excluded.error, content_hash=NULL RETURNING file_id",
            (rid, item["rel"], item["size"], item["mtime_ns"], item["kind"], scan_status, error),
        ).fetchone()
        return row[0]

    def _patient_pk(self, conn, source: str, rec: dict) -> int:
        key = (source, rec["pid"], rec["pname_raw"], rec["birth"])
        v = self.patient_cache.get(key)
        if v is not None:
            return v
        conn.execute("INSERT OR IGNORE INTO patients(source, pid_raw, name_raw, birth_date, sex) "
                     "VALUES(?,?,?,?,?)", key + (rec["sex"],))
        v = conn.execute("SELECT patient_pk FROM patients WHERE source=? AND pid_raw=? "
                         "AND name_raw=? AND birth_date=?", key).fetchone()[0]
        self.patient_cache[key] = v
        return v

    _FILL_FIELDS = ("study_date", "study_time", "accession", "institution", "station")

    def _study_pk(self, conn, source: str, rec: dict, patient_pk: int) -> int:
        uid = rec["study_uid"] or f"?missing?{source}?{rec['pid']}?{rec['study_date']}?{rec['accession']}"
        cached = self.study_cache.get(uid)
        if cached is None:
            row = conn.execute("SELECT study_pk, modalities, study_date, study_time,"
                               " accession, institution, station FROM studies WHERE study_uid=?",
                               (uid,)).fetchone()
            if row is None:
                cur = conn.execute(
                    "INSERT INTO studies(study_uid, source, patient_pk, study_date, study_time,"
                    " accession, study_desc, institution, station, modalities) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (uid, source, patient_pk, rec["study_date"], rec["study_time"],
                     rec["accession"], rec["study_desc"], rec["institution"], rec["station"],
                     rec["modality"]))
                pk = cur.lastrowid
                mods = {rec["modality"]} if rec["modality"] else set()
                self.study_cache[uid] = (pk, mods, {f for f in self._FILL_FIELDS if not rec[f]})
                return pk
            pk = row["study_pk"]
            mods = set(filter(None, row["modalities"].split(",")))
            cached = (pk, mods, {f for f in self._FILL_FIELDS if not row[f]})
            self.study_cache[uid] = cached
        pk, mods, missing = cached
        if rec["modality"] and rec["modality"] not in mods:
            mods.add(rec["modality"])
            conn.execute("UPDATE studies SET modalities=? WHERE study_pk=?",
                         (",".join(sorted(mods)), pk))
        # fill still-empty descriptive fields from later instances — but only when
        # this instance actually brings something new (a CT study with a forever-
        # empty StationName must not cost one UPDATE per slice)
        fill = {f for f in missing if rec[f]}
        if fill:
            conn.execute(
                "UPDATE studies SET "
                " study_date=CASE WHEN study_date='' THEN ? ELSE study_date END,"
                " study_time=CASE WHEN study_time='' THEN ? ELSE study_time END,"
                " accession=CASE WHEN accession='' THEN ? ELSE accession END,"
                " institution=CASE WHEN institution='' THEN ? ELSE institution END,"
                " station=CASE WHEN station='' THEN ? ELSE station END "
                "WHERE study_pk=?",
                (rec["study_date"], rec["study_time"], rec["accession"], rec["institution"],
                 rec["station"], pk))
            missing -= fill
        return pk

    def ingest_batch(self, conn, items: list[dict]) -> None:
        for item in items:
            st = item["status"]
            if st == "nondicom":
                self._upsert_file(conn, item, FS_NONDICOM, None)
                continue
            if st == "error":
                fid = self._upsert_file(conn, item, FS_ERROR, item["error"])
                conn.execute("DELETE FROM instances WHERE file_id=?", (fid,))
                continue
            rec = item["rec"]
            fid = self._upsert_file(conn, item, FS_OK, None)
            ppk = self._patient_pk(conn, item["source"], rec)
            spk = self._study_pk(conn, item["source"], rec, ppk)
            conn.execute(
                "INSERT INTO instances(file_id, source, sop_uid, sop_class_id, ts_id, study_pk,"
                " series_uid, modality, charset, facts, is_decimal_tags, text_sample)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)"
                " ON CONFLICT(file_id) DO UPDATE SET source=excluded.source,"
                " sop_uid=excluded.sop_uid, sop_class_id=excluded.sop_class_id,"
                " ts_id=excluded.ts_id, study_pk=excluded.study_pk,"
                " series_uid=excluded.series_uid, modality=excluded.modality,"
                " charset=excluded.charset, facts=excluded.facts,"
                " is_decimal_tags=excluded.is_decimal_tags, text_sample=excluded.text_sample,"
                " needs=0, dup_group=NULL, canonical=1",
                (fid, item["source"], rec["sop_uid"], self.sop.get(conn, rec["sop_class"]),
                 self.ts.get(conn, rec["ts_uid"]), spk, rec["series_uid"], rec["modality"],
                 rec["charset"], rec["facts"], rec["is_decimal_tags"], rec["text_sample"]))


def _load_known(conn: sqlite3.Connection, root_id: int) -> dict[bytes, tuple[int, int, int]]:
    known: dict[bytes, tuple[int, int, int]] = {}
    for rel, size, mt, st in conn.execute(
            "SELECT rel_path, size, mtime_ns, scan_status FROM files WHERE root_id=?", (root_id,)):
        h = hashlib.blake2b(rel.encode("utf-8", "surrogateescape"), digest_size=16).digest()
        known[h] = (size, mt, st)
    return known


class SourceScanJob(threading.Thread):
    """Scans one source: enumerate -> reader pool -> db-writer.  One per source,
    so different volumes proceed in parallel."""

    def __init__(self, cfg: Config, src: SourceCfg, writer: DbWriter,
                 ingestor: ScanIngestor, rescan: str, limit: int,
                 stop: threading.Event, use_threads: bool):
        super().__init__(name=f"scan-{src.name}", daemon=True)
        self.cfg, self.src, self.writer, self.ingestor = cfg, src, writer, ingestor
        self.rescan, self.limit, self.stop = rescan, limit, stop
        self.use_threads = use_threads
        self.stats = {"seen": 0, "skipped": 0, "queued": 0, "ok": 0,
                      "nondicom": 0, "errors": 0}

    def run(self) -> None:
        try:
            self._run()
        except Exception:
            log.exception("scan job %s failed", self.src.name)

    def _run(self) -> None:
        src = self.src
        if self.use_threads:
            in_q: Any = queue.Queue(maxsize=4096)
            out_q: Any = queue.Queue(maxsize=4096)
            workers = [threading.Thread(target=_scan_worker, name=f"rd-{src.name}-{i}",
                                        args=(src_to_dict(src), in_q, out_q), daemon=True)
                       for i in range(src.max_readers)]
        else:
            ctx = mp.get_context("spawn")
            in_q = ctx.Queue(maxsize=4096)
            out_q = ctx.Queue(maxsize=4096)
            workers = [ctx.Process(target=_scan_worker, name=f"rd-{src.name}-{i}",
                                   args=(src_to_dict(src), in_q, out_q), daemon=True)
                       for i in range(src.max_readers)]
        for w in workers:
            w.start()

        drain_done = threading.Event()
        drainer = threading.Thread(target=self._drain, name=f"drain-{src.name}",
                                   args=(out_q, drain_done), daemon=True)
        drainer.start()

        # register roots + load known-file maps (stat-only incremental rescan)
        conn = db_connect(self.cfg.general.db_path, readonly=False)
        try:
            known_by_root: dict[str, dict[bytes, tuple[int, int, int]]] = {}
            for root in src.roots:
                self.writer.flush()
                rid_row = conn.execute("SELECT root_id FROM roots WHERE source=? AND path=?",
                                       (src.name, root)).fetchone()
                if rid_row is None:
                    conn.execute("INSERT OR IGNORE INTO roots(source, path) VALUES(?,?)",
                                 (src.name, root))
                    conn.commit()
                    rid_row = conn.execute("SELECT root_id FROM roots WHERE source=? AND path=?",
                                           (src.name, root)).fetchone()
                known_by_root[root] = _load_known(conn, rid_row[0])
        finally:
            conn.close()

        dicom_uuids = None
        if src.adapter == "orthanc" and src.index_db:
            dicom_uuids = orthanc_index_uuids(src.index_db)
            if dicom_uuids is not None:
                log.info("%s: Orthanc index lists %d DICOM attachments", src.name, len(dicom_uuids))

        try:
            for root, abs_path, size, mtime_ns in iter_source_files(src):
                if self.stop.is_set():
                    break
                self.stats["seen"] += 1
                rel = os.path.relpath(abs_path, root)
                h = hashlib.blake2b(rel.encode("utf-8", "surrogateescape"), digest_size=16).digest()
                prev = known_by_root.get(root, {}).get(h)
                if prev is not None and self.rescan != "full":
                    psize, pmt, pstatus = prev
                    unchanged = (psize == size and pmt == mtime_ns)
                    retry = ((self.rescan == "errors" and pstatus == FS_ERROR)
                             or (self.rescan == "reclassify"
                                 and pstatus in (FS_ERROR, FS_NONDICOM)))
                    if unchanged and not retry:
                        self.stats["skipped"] += 1
                        continue
                base = {"source": src.name, "root": root, "rel": rel,
                        "size": size, "mtime_ns": mtime_ns}
                if dicom_uuids is not None and os.path.basename(abs_path) not in dicom_uuids:
                    base["kind"] = K_NONDICOM
                    base["status"] = "nondicom"
                    out_q.put(base)
                    self.stats["queued"] += 1
                    continue
                in_q.put((abs_path, base))
                self.stats["queued"] += 1
                if self.limit and self.stats["queued"] >= self.limit:
                    log.info("%s: --limit %d reached", src.name, self.limit)
                    break
        finally:
            for _ in workers:
                in_q.put(None)
            for w in workers:
                w.join()
            out_q.put(None)  # drain sentinel
            drain_done.wait()

    def _drain(self, out_q, done: threading.Event) -> None:
        batch: list[dict] = []
        ingestor = self.ingestor

        def flush():
            nonlocal batch
            if batch:
                items = batch
                batch = []
                self.writer.func(lambda conn, items=items: ingestor.ingest_batch(conn, items))

        while True:
            item = out_q.get()
            if item is None:
                break
            st = item.get("status")
            self.stats["ok" if st == "ok" else ("nondicom" if st == "nondicom" else "errors")] += 1
            batch.append(item)
            if len(batch) >= 200:
                flush()
            n = self.stats["ok"] + self.stats["nondicom"] + self.stats["errors"]
            if n % 20000 == 0:
                log.info("%s: %d processed (%d dicom, %d non-dicom, %d errors, %d skipped)",
                         self.src.name, n, self.stats["ok"], self.stats["nondicom"],
                         self.stats["errors"], self.stats["skipped"])
        flush()
        done.set()


def cmd_scan(cfg: Config, args) -> int:
    db_init(cfg.general.db_path)
    sources = [s for s in cfg.sources if not args.source or s.name in args.source]
    if not sources:
        raise SystemExit("no matching sources")
    use_threads = (cfg.general.scan_backend == "threads"
                   or (cfg.general.scan_backend == "auto" and not GIL_ENABLED)
                   or bool(getattr(args, "force_threads", False)))
    log.info("scan starting: sources=%s backend=%s rescan=%s",
             [s.name for s in sources], "threads" if use_threads else "processes", args.rescan)

    writer = DbWriter(cfg.general.db_path)
    writer.start()
    writer.func(lambda conn: disarm_gate(conn, "scan"))
    ingestor = ScanIngestor()
    stop = threading.Event()
    jobs = [SourceScanJob(cfg, s, writer, ingestor, args.rescan, args.limit, stop, use_threads)
            for s in sources]
    t0 = time.monotonic()
    for j in jobs:
        j.start()
    try:
        while any(j.is_alive() for j in jobs):
            time.sleep(0.5)
    except KeyboardInterrupt:
        log.warning("interrupt: stopping scan (state is preserved; rerun to resume)")
        stop.set()
    for j in jobs:
        j.join()
    writer.func(lambda conn: meta_set(conn, "scan_last", str(now_ms())))
    writer.flush()
    writer.stop()

    conn = db_connect(cfg.general.db_path)
    ensure_indexes(conn)
    conn.close()
    dt = time.monotonic() - t0
    total = {k: sum(j.stats[k] for j in jobs) for k in jobs[0].stats}
    for j in jobs:
        log.info("%s: seen=%d skipped=%d dicom=%d non-dicom=%d errors=%d",
                 j.src.name, j.stats["seen"], j.stats["skipped"], j.stats["ok"],
                 j.stats["nondicom"], j.stats["errors"])
    log.info("scan finished in %s (%d files seen, %d skipped as unchanged)",
             fmt_dur(dt), total["seen"], total["skipped"])
    return 0

# --------------------------------------------------------------------------
# SECTION 11 — ANALYZE
# --------------------------------------------------------------------------

def _abs_path(conn: sqlite3.Connection, file_id: int) -> str:
    row = conn.execute(
        "SELECT r.path AS root, f.rel_path AS rel FROM files f JOIN roots r USING(root_id) "
        "WHERE f.file_id=?", (file_id,)).fetchone()
    return os.path.join(row["root"], row["rel"]) if row else ""


def _an_file_problems(conn, cfg: Config, run: int) -> None:
    for row in conn.execute(
            "SELECT f.file_id, f.kind, f.error, r.path AS root, f.rel_path AS rel, r.source "
            "FROM files f JOIN roots r USING(root_id) WHERE f.scan_status=?", (FS_ERROR,)):
        code = H_TRUNCATED if row["kind"] == K_TRUNCATED else H_UNREADABLE
        conn.execute("INSERT INTO problems(run_id,severity,code,scope,ref,detail) VALUES(?,?,?,?,?,?)",
                     (run, SEV_HARD, code, "file", row["file_id"],
                      json.dumps({"source": row["source"],
                                  "path": os.path.join(row["root"], row["rel"]),
                                  "error": row["error"]}, ensure_ascii=False)))
    for src, n in conn.execute(
            "SELECT r.source, COUNT(*) FROM files f JOIN roots r USING(root_id) "
            "WHERE f.scan_status=? GROUP BY r.source", (FS_NONDICOM,)):
        conn.execute("INSERT INTO problems(run_id,severity,code,scope,ref,detail) VALUES(?,?,?,?,?,?)",
                     (run, SEV_INFO, I_NONDICOM, "aggregate", None,
                      json.dumps({"source": src, "count": n})))


def _an_detect_codecs(conn, cfg: Config, run: int) -> None:
    for src in cfg.sources:
        guessed_n, samples, updates = 0, [], []
        cur = conn.execute(
            "SELECT instance_id, charset, text_sample FROM instances "
            "WHERE source=? AND (facts & ?) != 0", (src.name, F_NONASCII_TEXT))
        for row in cur.fetchall():
            sample = row["text_sample"] or b""
            codec, guessed = effective_codec(src, row["charset"], sample)
            updates.append((codec, row["instance_id"]))
            if guessed and src.charset_policy == "utf8":
                guessed_n += 1
                if len(samples) < 10:
                    try:
                        preview = sample.split(b"\x00")[0].decode(codec, "replace")
                    except Exception:
                        preview = repr(sample[:40])
                    samples.append({"declared": row["charset"], "detected": codec,
                                    "preview": preview[:60]})
        conn.executemany("UPDATE instances SET detected_codec=? WHERE instance_id=?", updates)
        if guessed_n:
            conn.execute("INSERT INTO problems(run_id,severity,code,scope,ref,detail) VALUES(?,?,?,?,?,?)",
                         (run, SEV_WARN, W_CHARSET_GUESSED, "aggregate", None,
                          json.dumps({"source": src.name, "count": guessed_n,
                                      "samples": samples}, ensure_ascii=False)))


def _an_compute_needs(conn, cfg: Config) -> None:
    for src in cfg.sources:
        p = (src.name,)
        if src.charset_policy == "utf8":
            conn.execute("UPDATE instances SET needs=needs|? WHERE source=? AND (facts & ?)!=0 "
                         "AND detected_codec NOT IN ('', 'ascii', 'utf-8')",
                         (N_CHARSET, src.name, F_NONASCII_TEXT))
            # already-UTF-8 text still needs the ISO_IR 192 declaration when absent/wrong
            conn.execute("UPDATE instances SET needs=needs|? WHERE source=? AND (facts & ?)!=0 "
                         "AND detected_codec='utf-8' AND charset!='ISO_IR 192'",
                         (N_CHARSET, src.name, F_NONASCII_TEXT))
        elif src.charset_policy == "keep-gb":
            conn.execute("UPDATE instances SET needs=needs|? WHERE source=? AND (facts & ?)!=0 "
                         "AND charset NOT IN ('GB18030')",
                         (N_KEEPGB, src.name, F_NONASCII_TEXT))
        if src.caret_repair:
            conn.execute("UPDATE instances SET needs=needs|? WHERE source=? AND (facts & ?)!=0",
                         (N_CARET, src.name, F_PN_CARETLESS))
        if src.translit_cyrillic:
            conn.execute("UPDATE instances SET needs=needs|? WHERE source=? AND (facts & ?)!=0",
                         (N_TRANSLIT, src.name, F_PN_NONASCII))
        conn.execute("UPDATE instances SET needs=needs|? WHERE source=? AND is_decimal_tags!=''",
                     (N_ISFIX, src.name))
        conn.execute("UPDATE instances SET needs=needs|? WHERE source=? AND (facts & ?)!=0",
                     (N_UIDFIX, src.name, F_BAD_SOP_UID | F_BAD_STUDY_UID | F_BAD_SERIES_UID))
        conn.execute("UPDATE instances SET needs=needs|? WHERE source=? AND (facts & ?)!=0",
                     (N_STUDYUID, src.name, F_NO_STUDY_UID))
        conn.execute("UPDATE instances SET needs=needs|? WHERE source=? AND (facts & ?)!=0",
                     (N_FILEMETA, src.name, F_PREAMBLELESS | F_ZLIB))


def _an_uid_remaps(conn, cfg: Config) -> None:
    root = cfg.general.uid_root
    for utype, fbit, col in (("sop", F_BAD_SOP_UID, "sop_uid"),
                             ("series", F_BAD_SERIES_UID, "series_uid")):
        for (old,) in conn.execute(
                f"SELECT DISTINCT {col} FROM instances WHERE (facts & ?)!=0", (fbit,)):
            conn.execute("INSERT OR IGNORE INTO uid_remap(old_uid, uid_type, new_uid, reason) "
                         "VALUES(?,?,?,?)",
                         (old, utype, make_uid(root, "uid", utype, old), "invalid-uid"))
    for row in conn.execute(
            "SELECT DISTINCT s.study_uid FROM studies s JOIN instances i ON i.study_pk=s.study_pk "
            "WHERE (i.facts & ?)!=0", (F_BAD_STUDY_UID,)):
        old = row["study_uid"]
        conn.execute("INSERT OR IGNORE INTO uid_remap(old_uid, uid_type, new_uid, reason) VALUES(?,?,?,?)",
                     (old, "study", make_uid(root, "uid", "study", old), "invalid-uid"))
    # synthesized StudyInstanceUIDs for the '?missing?...' placeholder studies
    for row in conn.execute(
            "SELECT s.study_pk, s.study_uid, s.source, s.study_date, s.accession, "
            "  COALESCE(p.pid_raw,'') AS pid "
            "FROM studies s LEFT JOIN patients p USING(patient_pk) "
            "WHERE s.study_uid LIKE '?missing?%'"):
        m = re.match(r"\?missing\?file\?(\d+)$", row["study_uid"])
        fallback = m.group(1) if m else ""
        synth = synth_study_uid(root, row["source"], row["pid"], row["study_date"],
                                row["accession"], fallback)
        conn.execute("INSERT OR IGNORE INTO uid_remap(old_uid, uid_type, new_uid, reason) VALUES(?,?,?,?)",
                     (row["study_uid"], "study", synth, "missing-study-uid"))


_HEX64 = re.compile(r"[0-9a-f]{64}")


def _an_dedupe(conn, cfg: Config, run: int) -> None:
    prio = {s.name: s.priority for s in cfg.sources}
    image_ids = image_sop_class_ids(conn)
    ts_rev = {r["id"]: r["uid"] for r in conn.execute("SELECT id, uid FROM xfer")}
    policy = cfg.general.diff_content_policy

    def is_broken_image(r) -> bool:
        # missing pixels only matters for SOP classes that should have them
        return bool(r["facts"] & F_NO_PIXELS) and r["sop_class_id"] in image_ids

    def keep(iid):
        conn.execute("UPDATE instances SET dup_group=? WHERE instance_id=?", (g, iid))

    def drop(iid):
        conn.execute("UPDATE instances SET canonical=0, send_status=?, dup_group=? "
                     "WHERE instance_id=?", (S_SKIPPED_DUP, g, iid))

    dup_uids = [r[0] for r in conn.execute(
        "SELECT sop_uid FROM instances WHERE send_status!=? "
        "GROUP BY sop_uid HAVING COUNT(*)>1", (S_EXCLUDED,))]
    log.info("analyze: %d duplicated SOPInstanceUIDs to resolve (diff-content policy=%s)",
             len(dup_uids), policy)
    n_dedup = n_conflict = n_pixelless_dropped = n_regen = n_dropped = n_benign_px = 0
    for g, sop_uid in enumerate(dup_uids, start=1):
        rows = conn.execute(
            "SELECT i.instance_id, i.source, i.facts, i.sop_class_id, i.ts_id, f.file_id,"
            " f.size, f.mtime_ns, f.content_hash, f.pixel_hash "
            "FROM instances i JOIN files f USING(file_id) "
            "WHERE i.sop_uid=? AND i.send_status!=?", (sop_uid, S_EXCLUDED)).fetchall()
        with_pixels = [r for r in rows if not is_broken_image(r)]
        pixelless = [r for r in rows if is_broken_image(r)]
        if with_pixels and pixelless:  # a full copy beats a broken/header-only one
            for r in pixelless:
                drop(r["instance_id"])
                n_pixelless_dropped += 1
        contenders = with_pixels or pixelless
        if len(contenders) == 1:
            keep(contenders[0]["instance_id"])
            continue
        # winner preference: source priority, then newest file, then lowest id
        contenders.sort(key=lambda r: (prio.get(r["source"], 999),
                                       -(r["mtime_ns"] or 0), r["instance_id"]))

        # tier 1 — byte-identical (cached full-file hash): certainly benign
        byte_same = False
        if len({r["size"] for r in contenders}) == 1:
            hs = set()
            for r in contenders:
                h = r["content_hash"]
                if not h:
                    try:
                        h = sha256_file(_abs_path(conn, r["file_id"]))
                    except OSError:
                        h = f"unreadable:{r['file_id']}"
                    conn.execute("UPDATE files SET content_hash=? WHERE file_id=?",
                                 (h, r["file_id"]))
                hs.add(h)
            byte_same = len(hs) == 1 and _HEX64.fullmatch(next(iter(hs)) or "") is not None
        if byte_same:
            keep(contenders[0]["instance_id"])
            for r in contenders[1:]:
                drop(r["instance_id"])
                n_dedup += 1
            continue

        # tier 2 — compare PIXEL data, transfer-syntax-aware (a raw pixel hash would
        # falsely differ across TS for the same image, so only flag a collision when
        # two copies share a TS yet differ in pixels)
        pinfo = []
        for r in contenders:
            ph = r["pixel_hash"]
            if not ph:
                ph = _pixel_sha(_abs_path(conn, r["file_id"]))
                conn.execute("UPDATE files SET pixel_hash=? WHERE file_id=?", (ph, r["file_id"]))
            pinfo.append((r, ph, ts_rev.get(r["ts_id"], "")))
        good = [(r, ph, ts) for (r, ph, ts) in pinfo if _HEX64.fullmatch(ph or "")]
        by_ts: dict[str, set] = {}
        for r, ph, ts in good:
            by_ts.setdefault(ts, set()).add(ph)
        danger = any(len(hh) > 1 for hh in by_ts.values())
        # pixel-less IODs (PR/SR/…): no PixelData to compare, but tier 1 already
        # proved the bytes differ -> a content collision, resolved by the same policy
        nopix_diff = (not good and bool(pinfo)
                      and all(ph == "no-pixeldata" for (_r, ph, _t) in pinfo))

        if danger or nopix_diff:
            detail = {"sop_uid": sop_uid, "policy": policy,
                      "kind": "pixel-diff" if danger else "no-pixel-iod-diff",
                      "candidates": [
                {"source": r["source"], "path": _abs_path(conn, r["file_id"]),
                 "size": r["size"], "instance_id": r["instance_id"],
                 "ts": ts, "pixel_hash": ph[:12]} for (r, ph, ts) in pinfo]}
            if policy == "block":
                for r in contenders:
                    keep(r["instance_id"])
                conn.execute("INSERT INTO problems(run_id,severity,code,scope,ref,detail) "
                             "VALUES(?,?,?,?,?,?)",
                             (run, SEV_HARD, H_DUP_CONFLICT, "dup", g,
                              json.dumps(detail, ensure_ascii=False)))
                n_conflict += 1
            elif policy == "keep-priority":
                keep(contenders[0]["instance_id"])
                for r in contenders[1:]:
                    drop(r["instance_id"])
                    n_dropped += 1
                conn.execute("INSERT INTO problems(run_id,severity,code,scope,ref,detail) "
                             "VALUES(?,?,?,?,?,?)",
                             (run, SEV_WARN, W_DUP_DROPPED, "dup", g,
                              json.dumps(detail, ensure_ascii=False)))
            else:  # regenerate-uid — keep ALL images, give losers fresh SOPInstanceUIDs
                keep(contenders[0]["instance_id"])
                for r in contenders[1:]:
                    new = make_uid(cfg.general.uid_root, "dupcollide", sop_uid,
                                   str(r["instance_id"]))
                    conn.execute("UPDATE instances SET new_sop_uid=?, needs=needs|?, dup_group=? "
                                 "WHERE instance_id=?", (new, N_UIDFIX, g, r["instance_id"]))
                    n_regen += 1
                conn.execute("INSERT INTO problems(run_id,severity,code,scope,ref,detail) "
                             "VALUES(?,?,?,?,?,?)",
                             (run, SEV_WARN, W_DUP_REGEN, "dup", g,
                              json.dumps(detail, ensure_ascii=False)))
        elif not good:  # nothing readable → cannot verify → conservative block
            for r in contenders:
                keep(r["instance_id"])
            conn.execute("INSERT INTO problems(run_id,severity,code,scope,ref,detail) "
                         "VALUES(?,?,?,?,?,?)",
                         (run, SEV_HARD, H_DUP_CONFLICT, "dup", g,
                          json.dumps({"sop_uid": sop_uid, "reason": "pixels unreadable",
                                      "candidates": [{"source": r["source"],
                                                      "path": _abs_path(conn, r["file_id"]),
                                                      "instance_id": r["instance_id"]}
                                                     for r in contenders]}, ensure_ascii=False)))
            n_conflict += 1
        else:  # same pixels (metadata-only diff) or re-compressed copy → benign
            winner = good[0][0]
            keep(winner["instance_id"])
            for r in contenders:
                if r["instance_id"] != winner["instance_id"]:
                    drop(r["instance_id"])
                    n_dedup += 1
            n_benign_px += 1
        if g % 5000 == 0:
            log.info("analyze: dedupe %d/%d groups", g, len(dup_uids))
            conn.commit()
    # genuinely broken images: an IMAGE-class instance with no pixels and no full
    # copy elsewhere.  Non-image IODs (PR/SR/REG/RWV/…) with no pixels are valid and
    # deliberately NOT flagged here — they must migrate.
    if image_ids:
        placeholders = ",".join("?" * len(image_ids))
        for row in conn.execute(
                f"SELECT instance_id, sop_uid, file_id, source FROM instances "
                f"WHERE canonical=1 AND send_status!=? AND (facts & ?)!=0 "
                f"AND sop_class_id IN ({placeholders})",
                (S_EXCLUDED, F_NO_PIXELS, *image_ids)):
            conn.execute("INSERT INTO problems(run_id,severity,code,scope,ref,detail) VALUES(?,?,?,?,?,?)",
                         (run, SEV_HARD, H_PIXELLESS_ONLY, "instance", row["instance_id"],
                          json.dumps({"source": row["source"], "sop_uid": row["sop_uid"],
                                      "path": _abs_path(conn, row["file_id"])}, ensure_ascii=False)))
    if n_dedup or n_pixelless_dropped or n_benign_px:
        conn.execute("INSERT INTO problems(run_id,severity,code,scope,ref,detail) VALUES(?,?,?,?,?,?)",
                     (run, SEV_WARN, W_DUP_CROSS, "aggregate", None,
                      json.dumps({"byte_identical_deduped": n_dedup,
                                  "same_pixels_deduped": n_benign_px,
                                  "headeronly_dropped": n_pixelless_dropped})))
    if n_regen:
        conn.execute("INSERT INTO problems(run_id,severity,code,scope,ref,detail) VALUES(?,?,?,?,?,?)",
                     (run, SEV_WARN, W_DUP_REGEN, "aggregate", None,
                      json.dumps({"collisions_preserved_via_new_uid": n_regen})))
    if n_dropped:
        conn.execute("INSERT INTO problems(run_id,severity,code,scope,ref,detail) VALUES(?,?,?,?,?,?)",
                     (run, SEV_WARN, W_DUP_DROPPED, "aggregate", None,
                      json.dumps({"collision_images_DISCARDED": n_dropped})))
    log.info("analyze: dedupe done (byte-dup=%d, pixel-dup=%d, header-only=%d, "
             "collisions: block=%d regen=%d dropped=%d)",
             n_dedup, n_benign_px, n_pixelless_dropped, n_conflict, n_regen, n_dropped)


def _study_source_map(conn) -> dict[int, str]:
    """study_pk -> source owning its canonical instances (min priority source name wins;
    in practice studies rarely span sources after dedupe)."""
    out: dict[int, str] = {}
    for row in conn.execute(
            "SELECT study_pk, source, COUNT(*) AS n FROM instances "
            "WHERE canonical=1 AND send_status NOT IN (?,?) GROUP BY study_pk, source",
            (S_EXCLUDED, S_SKIPPED_DUP)):
        pk = row["study_pk"]
        if pk not in out or row["n"] > 0 and row["source"] < out[pk]:
            out.setdefault(pk, row["source"])
    return out


def _an_pid_assign(conn, cfg: Config, run: int) -> None:
    study_src = _study_source_map(conn)
    for src in cfg.sources:
        if src.pid_rules_mode == "off" and not src.fuji.enabled:
            continue
        study_pks = [pk for pk, s in study_src.items() if s == src.name]
        if not study_pks:
            continue
        rows = {}
        for pk in study_pks:
            rows[pk] = conn.execute(
                "SELECT s.study_pk, s.study_uid, s.study_date, s.study_time, s.institution,"
                " s.station, COALESCE(p.pid_raw,'') AS pid, COALESCE(p.name_raw,'') AS name "
                "FROM studies s LEFT JOIN patients p USING(patient_pk) WHERE s.study_pk=?",
                (pk,)).fetchone()
        n_rewritten = 0
        if src.fuji.enabled:
            groups: dict[tuple[str, str], list] = {}
            mtime_dated: list[tuple[str, str]] = []
            for pk, row in rows.items():
                inst = conn.execute(
                    "SELECT i.file_id, f.mtime_ns FROM instances i JOIN files f USING(file_id) "
                    "WHERE i.study_pk=? AND i.canonical=1 "
                    "ORDER BY i.instance_id LIMIT 1", (pk,)).fetchone()
                path = _abs_path(conn, inst["file_id"]) if inst else ""
                site = fuji_site_for(src, row["institution"], path)
                if site is None:
                    site = src.fuji.fallback_site or None
                if site is None:
                    conn.execute("INSERT INTO problems(run_id,severity,code,scope,ref,detail) "
                                 "VALUES(?,?,?,?,?,?)",
                                 (run, SEV_HARD, H_PID_RULE_MISS, "study", pk,
                                  json.dumps({"source": src.name, "study_uid": row["study_uid"],
                                              "institution": row["institution"],
                                              "why": "no site flag in InstitutionName and no folder match",
                                              "path": path}, ensure_ascii=False)))
                    continue
                date = row["study_date"]
                if not re.fullmatch(r"\d{8}", date or ""):
                    if src.fuji.date_from_mtime and inst and inst["mtime_ns"]:
                        date = time.strftime("%Y%m%d", time.localtime(inst["mtime_ns"] / 1e9))
                        mtime_dated.append((row["study_uid"], date))
                    else:
                        conn.execute("INSERT INTO problems(run_id,severity,code,scope,ref,detail) "
                                     "VALUES(?,?,?,?,?,?)",
                                     (run, SEV_HARD, H_FUJI_NO_DATE, "study", pk,
                                      json.dumps({"source": src.name, "study_uid": row["study_uid"],
                                                  "study_date": row["study_date"]},
                                                 ensure_ascii=False)))
                        continue
                groups.setdefault((site, date[:6]), []).append(
                    (date, row["study_time"], row["study_uid"], pk))
            for (site, _ym), lst in groups.items():
                lst.sort()
                # fallback-site pool spans many institutions -> months exceed 999
                # studies; only the fixed-width (zero-padded month) form is
                # collision-free there.  8K/9K keep the historical format.
                two_dig = bool(src.fuji.fallback_site) and site == src.fuji.fallback_site
                for ordinal, (date, _t, _uid, pk) in enumerate(lst, start=1):
                    conn.execute("UPDATE studies SET pid_new=?, pid_rule=? WHERE study_pk=?",
                                 (fuji_pid(site, date, ordinal, two_dig), f"fuji:{site}", pk))
                    n_rewritten += 1
            # study-level IDs MUST be unique: any duplicate blocks the gate
            for pid_new, n_dup, uids in conn.execute(
                    "SELECT pid_new, COUNT(*), GROUP_CONCAT(study_uid) FROM studies "
                    "WHERE source=? AND pid_new IS NOT NULL AND pid_rule LIKE 'fuji:%' "
                    "GROUP BY pid_new HAVING COUNT(*)>1", (src.name,)):
                conn.execute("INSERT INTO problems(run_id,severity,code,scope,ref,detail) "
                             "VALUES(?,?,?,?,?,?)",
                             (run, SEV_HARD, H_PID_COLLISION, "aggregate", None,
                              json.dumps({"source": src.name, "pid_new": pid_new,
                                          "studies": n_dup,
                                          "study_uids": uids.split(",")[:10]},
                                         ensure_ascii=False)))
            if mtime_dated:
                conn.execute("INSERT INTO problems(run_id,severity,code,scope,ref,detail) "
                             "VALUES(?,?,?,?,?,?)",
                             (run, SEV_WARN, W_FUJI_MTIME_DATE, "aggregate", None,
                              json.dumps({"source": src.name, "studies": len(mtime_dated),
                                          "sample": mtime_dated[:20]}, ensure_ascii=False)))
        else:
            for pk, row in rows.items():
                new, rid = apply_pid_rules(src, row["pid"], row["station"], row["institution"])
                if new is not None and new != row["pid"]:
                    conn.execute("UPDATE studies SET pid_new=?, pid_rule=? WHERE study_pk=?",
                                 (new, rid, pk))
                    n_rewritten += 1
                elif new is None and src.pid_rules_mode == "required":
                    conn.execute("INSERT INTO problems(run_id,severity,code,scope,ref,detail) "
                                 "VALUES(?,?,?,?,?,?)",
                                 (run, SEV_HARD, H_PID_RULE_MISS, "study", pk,
                                  json.dumps({"source": src.name, "study_uid": row["study_uid"],
                                              "pid": row["pid"]}, ensure_ascii=False)))
        conn.execute(
            "UPDATE instances SET needs=needs|? WHERE source=? AND study_pk IN "
            "(SELECT study_pk FROM studies WHERE pid_new IS NOT NULL)", (N_PID, src.name))
        if n_rewritten:
            conn.execute("INSERT INTO problems(run_id,severity,code,scope,ref,detail) VALUES(?,?,?,?,?,?)",
                         (run, SEV_WARN, W_PID_REWRITE, "aggregate", None,
                          json.dumps({"source": src.name, "studies": n_rewritten})))
    # studies with no identity at all: block, or synthesize a placeholder ID
    placeholder = cfg.general.no_identity_policy == "placeholder"
    n_placeholder = 0
    for row in conn.execute(
            "SELECT s.study_pk, s.study_uid, s.source, s.institution, s.study_date, s.study_time "
            "FROM studies s LEFT JOIN patients p USING(patient_pk) "
            "WHERE COALESCE(p.pid_raw,'')='' AND COALESCE(p.name_raw,'')='' "
            "AND s.pid_new IS NULL AND EXISTS (SELECT 1 FROM instances i WHERE i.study_pk=s.study_pk "
            " AND i.canonical=1 AND i.send_status NOT IN (?,?))", (S_EXCLUDED, S_SKIPPED_DUP)).fetchall():
        if placeholder:
            pid = placeholder_pid(row["institution"], row["study_date"], row["study_time"])
            conn.execute("UPDATE studies SET pid_new=?, pid_rule=? WHERE study_pk=?",
                         (pid, "no-identity-placeholder", row["study_pk"]))
            conn.execute("UPDATE instances SET needs=needs|? WHERE study_pk=?",
                         (N_PID, row["study_pk"]))
            n_placeholder += 1
        else:
            conn.execute("INSERT INTO problems(run_id,severity,code,scope,ref,detail) VALUES(?,?,?,?,?,?)",
                         (run, SEV_HARD, H_NO_IDENTITY, "study", row["study_pk"],
                          json.dumps({"source": row["source"], "study_uid": row["study_uid"]})))
    if n_placeholder:
        conn.execute("INSERT INTO problems(run_id,severity,code,scope,ref,detail) VALUES(?,?,?,?,?,?)",
                     (run, SEV_WARN, W_IDENTITY_PLACEHOLDER, "aggregate", None,
                      json.dumps({"studies": n_placeholder})))


def _an_routing(conn, cfg: Config) -> None:
    updates = []
    for row in conn.execute("SELECT study_pk, modalities FROM studies"):
        group = cfg.group_for_modalities(row["modalities"].split(","))
        updates.append((group, row["study_pk"]))
    conn.executemany("UPDATE studies SET dest_group=? WHERE study_pk=?", updates)
    conn.execute(
        "UPDATE studies SET n_instances = (SELECT COUNT(*) FROM instances i "
        "WHERE i.study_pk = studies.study_pk AND i.canonical=1 AND i.send_status NOT IN (?,?))",
        (S_EXCLUDED, S_SKIPPED_DUP))


def _an_aggregate_warnings(conn, cfg: Config, run: int) -> None:
    per_bit = ((N_ISFIX, W_IS_ROUNDED), (N_CARET, W_PN_CARET), (N_UIDFIX, W_UID_REGEN),
               (N_STUDYUID, W_STUDYUID_SYNTH), (N_KEEPGB, W_KEEPGB))
    for bit, code in per_bit:
        for src, n in conn.execute(
                "SELECT source, COUNT(*) FROM instances WHERE (needs & ?)!=0 "
                "AND canonical=1 AND send_status NOT IN (?,?) GROUP BY source",
                (bit, S_EXCLUDED, S_SKIPPED_DUP)):
            conn.execute("INSERT INTO problems(run_id,severity,code,scope,ref,detail) VALUES(?,?,?,?,?,?)",
                         (run, SEV_WARN, code, "aggregate", None,
                          json.dumps({"source": src, "count": n})))


def cmd_analyze(cfg: Config, args) -> int:
    db_init(cfg.general.db_path)
    conn = db_connect(cfg.general.db_path)
    ensure_indexes(conn)
    run = int(meta_get(conn, "analyze_run", "0")) + 1
    log.info("analyze: run %d starting", run)
    t0 = time.monotonic()
    with conn:
        disarm_gate(conn, "analyze")
        conn.execute("DELETE FROM problems")
        conn.execute("UPDATE instances SET needs=0, dup_group=NULL, canonical=1, new_sop_uid=NULL, "
                     "send_status=CASE WHEN send_status=? THEN ? ELSE send_status END "
                     "WHERE send_status!=?", (S_SKIPPED_DUP, S_PENDING, S_EXCLUDED))
        conn.execute("UPDATE studies SET pid_new=NULL, pid_rule=NULL")
    _an_file_problems(conn, cfg, run)
    conn.commit()
    _an_detect_codecs(conn, cfg, run)
    conn.commit()
    _an_compute_needs(conn, cfg)
    conn.commit()
    _an_uid_remaps(conn, cfg)
    conn.commit()
    _an_dedupe(conn, cfg, run)
    conn.commit()
    _an_pid_assign(conn, cfg, run)
    conn.commit()
    _an_routing(conn, cfg)
    _an_aggregate_warnings(conn, cfg, run)
    meta_set(conn, "analyze_run", str(run))
    meta_set(conn, "analyze_config_hash", cfg.config_hash)
    meta_set(conn, "analyze_time", str(now_ms()))
    conn.commit()

    hard = conn.execute("SELECT COUNT(*) FROM problems WHERE severity=? AND resolved=0",
                        (SEV_HARD,)).fetchone()[0]
    warn = conn.execute("SELECT COUNT(DISTINCT code) FROM problems WHERE severity=?",
                        (SEV_WARN,)).fetchone()[0]
    log.info("analyze: run %d done in %s — %d unresolved hard blockers, %d warning classes",
             run, fmt_dur(time.monotonic() - t0), hard, warn)
    generate_reports(conn, cfg, run)
    conn.close()
    return 0

# --------------------------------------------------------------------------
# SECTION 12 — REPORTS
# --------------------------------------------------------------------------

def _csv_writer(path: Path, header: list[str]):
    f = open(path, "w", newline="", encoding="utf-8-sig")
    w = csv.writer(f)
    w.writerow(header)
    return f, w


def display_text(raw: str) -> str:
    """Best-effort human rendering of a latin-1-preserved raw DB string, for the
    review CSVs only (the send path uses per-instance codec detection instead)."""
    if not raw or raw.isascii():
        return raw
    try:
        b = raw.encode("latin-1")
    except UnicodeEncodeError:
        return raw  # not a byte-preserving string — already real text
    codec, score = detect_codec(b, ["utf-8", "cp1251", "gb18030"])
    if codec and codec != "ascii" and score >= 0.5:
        try:
            return b.decode(codec)
        except (UnicodeDecodeError, ValueError):
            pass
    return raw


def _report_pid_preview(conn, outdir: Path) -> int:
    f, w = _csv_writer(outdir / "patient_id_preview.csv",
                       ["source", "station", "institution", "old_patient_id", "rule",
                        "new_patient_id", "patient_name", "patient_name_raw",
                        "study_date", "study_uid", "n_instances"])
    n = 0
    with f:
        for row in conn.execute(
                "SELECT s.source, s.station, s.institution, COALESCE(p.pid_raw,'') pid,"
                " s.pid_rule, s.pid_new, COALESCE(p.name_raw,'') name, s.study_date,"
                " s.study_uid, s.n_instances "
                "FROM studies s LEFT JOIN patients p USING(patient_pk) "
                "WHERE s.pid_new IS NOT NULL ORDER BY s.source, s.pid_new"):
            w.writerow([row["source"], row["station"], display_text(row["institution"]),
                        display_text(row["pid"]), row["pid_rule"], row["pid_new"],
                        display_text(row["name"]), row["name"], row["study_date"],
                        row["study_uid"], row["n_instances"]])
            n += 1
    return n


def _report_duplicates(conn, outdir: Path) -> None:
    f, w = _csv_writer(outdir / "duplicates.csv",
                       ["dup_group", "sop_uid", "source", "path", "size", "status", "canonical"])
    with f:
        for row in conn.execute(
                "SELECT i.dup_group, i.sop_uid, i.source, i.file_id, f.size, i.send_status,"
                " i.canonical FROM instances i JOIN files f USING(file_id) "
                "WHERE i.dup_group IS NOT NULL ORDER BY i.dup_group, i.canonical DESC"):
            w.writerow([row["dup_group"], row["sop_uid"], row["source"],
                        _abs_path(conn, row["file_id"]), row["size"],
                        SEND_STATUS_NAMES.get(row["send_status"], row["send_status"]),
                        row["canonical"]])


def _report_charset(conn, cfg: Config, outdir: Path, cap: int = 50000) -> None:
    f, w = _csv_writer(outdir / "charset_suspects.csv",
                       ["source", "declared", "detected", "decoded_preview", "instance_id"])
    with f:
        n = 0
        for row in conn.execute(
                "SELECT instance_id, source, charset, detected_codec, text_sample FROM instances "
                "WHERE (needs & ?)!=0 AND canonical=1", (N_CHARSET | N_KEEPGB,)):
            if n >= cap:
                w.writerow(["...", "...", "...", f"truncated at {cap} rows", ""])
                break
            sample = (row["text_sample"] or b"").split(b"\x00")[0]
            try:
                preview = sample.decode(row["detected_codec"] or "latin-1", "replace")
            except Exception:
                preview = repr(sample[:40])
            w.writerow([row["source"], row["charset"], row["detected_codec"],
                        preview[:80], row["instance_id"]])
            n += 1


def _norm_name_for_cluster(name: str) -> str:
    s = name.replace("^", " ").replace("=", " ")
    s = translit_cyrillic(s.lower())
    return re.sub(r"[^a-z0-9]+", "", s)


def _report_clusters(conn, cfg: Config, outdir: Path) -> None:
    """Informational: candidate same-patient groups for study-level-ID sources."""
    fuji_sources = [s.name for s in cfg.sources if s.fuji.enabled]
    if not fuji_sources:
        return
    f, w = _csv_writer(outdir / "patient_clusters.csv",
                       ["cluster", "birth_date", "name", "old_patient_id",
                        "new_patient_id", "study_date", "study_uid"])
    qmarks = ",".join("?" * len(fuji_sources))
    clusters: dict[tuple[str, str], list] = {}
    for row in conn.execute(
            f"SELECT s.study_uid, s.study_date, s.pid_new, COALESCE(p.pid_raw,'') pid,"
            f" COALESCE(p.name_raw,'') name, COALESCE(p.birth_date,'') birth "
            f"FROM studies s LEFT JOIN patients p USING(patient_pk) "
            f"WHERE s.source IN ({qmarks}) AND s.n_instances>0", fuji_sources):
        key = (row["birth"], _norm_name_for_cluster(display_text(row["name"])))
        if key[1]:
            clusters.setdefault(key, []).append(row)
    with f:
        cid = 0
        for key, rows in sorted(clusters.items()):
            if len(rows) < 2:
                continue
            cid += 1
            for r in rows:
                w.writerow([cid, key[0], display_text(r["name"]), r["pid"], r["pid_new"],
                            r["study_date"], r["study_uid"]])


def _summary_stats(conn, cfg: Config) -> dict:
    stats: dict[str, Any] = {"sources": {}}
    for src in cfg.sources:
        row = conn.execute(
            "SELECT COUNT(*) n, COALESCE(SUM(f.size),0) bytes FROM instances i "
            "JOIN files f USING(file_id) WHERE i.source=? AND i.canonical=1 "
            "AND i.send_status NOT IN (?,?)", (src.name, S_EXCLUDED, S_SKIPPED_DUP)).fetchone()
        mods = {r[0]: r[1] for r in conn.execute(
            "SELECT modality, COUNT(*) FROM instances WHERE source=? AND canonical=1 "
            "GROUP BY modality", (src.name,))}
        studies_n = conn.execute(
            "SELECT COUNT(DISTINCT study_pk) FROM instances WHERE source=? AND canonical=1 "
            "AND send_status NOT IN (?,?)", (src.name, S_EXCLUDED, S_SKIPPED_DUP)).fetchone()[0]
        sent = conn.execute(
            "SELECT COUNT(*) FROM instances WHERE source=? AND send_status IN (?,?)",
            (src.name, S_SENT, S_SENT_WARN)).fetchone()[0]
        errors = conn.execute(
            "SELECT COUNT(*) FROM files f JOIN roots r USING(root_id) "
            "WHERE r.source=? AND f.scan_status=?", (src.name, FS_ERROR)).fetchone()[0]
        needs_rows = {}
        for bit, name in ((N_CHARSET, "utf8-reencode"), (N_KEEPGB, "gb-declare"),
                          (N_CARET, "caret"), (N_TRANSLIT, "translit"), (N_PID, "pid"),
                          (N_ISFIX, "is-round"), (N_UIDFIX, "uid"), (N_STUDYUID, "studyuid"),
                          (N_FILEMETA, "filemeta")):
            c = conn.execute("SELECT COUNT(*) FROM instances WHERE source=? AND (needs & ?)!=0 "
                             "AND canonical=1", (src.name, bit)).fetchone()[0]
            if c:
                needs_rows[name] = c
        stats["sources"][src.name] = {
            "instances": row["n"], "bytes": row["bytes"], "studies": studies_n,
            "modalities": mods, "needs": needs_rows, "sent": sent, "file_errors": errors,
        }
    stats["hard"] = conn.execute(
        "SELECT code, COUNT(*) FROM problems WHERE severity=? AND resolved=0 GROUP BY code",
        (SEV_HARD,)).fetchall()
    stats["warn"] = conn.execute(
        "SELECT code, COUNT(*) FROM problems WHERE severity=? GROUP BY code",
        (SEV_WARN,)).fetchall()
    stats["pending_bytes"] = conn.execute(
        "SELECT COALESCE(SUM(f.size),0) FROM instances i JOIN files f USING(file_id) "
        "WHERE i.canonical=1 AND i.send_status IN (?,?)", (S_PENDING, S_FAILED_RETRY)).fetchone()[0]
    return stats


def generate_reports(conn, cfg: Config, run: int) -> None:
    outdir = Path(cfg.general.report_dir) / f"run_{run}"
    outdir.mkdir(parents=True, exist_ok=True)

    f, w = _csv_writer(outdir / "problems.csv", ["severity", "code", "scope", "ref", "detail", "resolved"])
    with f:
        for row in conn.execute("SELECT severity, code, scope, ref, detail, resolved FROM problems "
                                "ORDER BY severity DESC, code"):
            w.writerow([("info", "warn", "HARD")[row["severity"]], row["code"], row["scope"],
                        row["ref"], row["detail"], row["resolved"]])
    n_pid = _report_pid_preview(conn, outdir)
    _report_duplicates(conn, outdir)
    _report_charset(conn, cfg, outdir)
    _report_clusters(conn, cfg, outdir)
    f, w = _csv_writer(outdir / "uid_remap_preview.csv", ["old_uid", "type", "new_uid", "reason"])
    with f:
        for row in conn.execute("SELECT old_uid, uid_type, new_uid, reason FROM uid_remap"):
            w.writerow([row["old_uid"], row["uid_type"], row["new_uid"], row["reason"]])

    stats = _summary_stats(conn, cfg)
    hard_n = sum(n for _c, n in stats["hard"])
    esc = _html.escape
    rate = cfg.network.rate_limit_mbit or 1000.0
    xfer_h = stats["pending_bytes"] * 8 / (rate * 1e6) / 3600
    parts = [f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>dcm_migrate — analyze run {run}</title><style>
body{{font-family:Segoe UI,Arial,sans-serif;margin:2em;max-width:1100px}}
table{{border-collapse:collapse;margin:1em 0}}td,th{{border:1px solid #ccc;padding:4px 10px;text-align:left}}
th{{background:#f0f0f0}}.ok{{background:#1a7f37;color:#fff;padding:8px 16px;border-radius:6px}}
.bad{{background:#b91c1c;color:#fff;padding:8px 16px;border-radius:6px}}
.warn{{background:#b45309;color:#fff}} code{{background:#f5f5f5;padding:1px 4px}}</style></head><body>
<h1>dcm_migrate — analyze run {run}</h1>
<p>{_dt.datetime.now().strftime("%Y-%m-%d %H:%M")} · config <code>{esc(cfg.config_path)}</code>
 · hash <code>{cfg.config_hash[:12]}</code></p>
<p><span class="{'ok' if hard_n == 0 else 'bad'}">
{'READY (pending warning acks + approve --arm)' if hard_n == 0 else f'{hard_n} HARD BLOCKERS — not ready'}
</span></p>
<h2>Sources</h2><table><tr><th>source</th><th>instances</th><th>studies</th><th>size</th>
<th>modalities</th><th>fixes needed</th><th>sent</th><th>file errors</th></tr>"""]
    for name, s in stats["sources"].items():
        mods = ", ".join(f"{m or '?'}:{n}" for m, n in sorted(s["modalities"].items()))
        needs = "<br>".join(f"{k}: {v:,}" for k, v in s["needs"].items()) or "—"
        parts.append(f"<tr><td>{esc(name)}</td><td>{s['instances']:,}</td><td>{s['studies']:,}</td>"
                     f"<td>{human_bytes(s['bytes'])}</td><td>{esc(mods)}</td><td>{needs}</td>"
                     f"<td>{s['sent']:,}</td><td>{s['file_errors']:,}</td></tr>")
    parts.append("</table><h2>Hard blockers</h2>")
    if stats["hard"]:
        parts.append("<table><tr><th>code</th><th>count</th></tr>")
        parts.extend(f"<tr class='warn'><td>{esc(c)}</td><td>{n:,}</td></tr>" for c, n in stats["hard"])
        parts.append("</table><p>Resolve via <code>exclude</code> (see problems.csv) or fix config and re-analyze.</p>")
    else:
        parts.append("<p>none 🎉</p>")
    parts.append("<h2>Warnings (need <code>approve --ack CODE</code>)</h2>")
    if stats["warn"]:
        parts.append("<table><tr><th>code</th><th>count</th></tr>")
        parts.extend(f"<tr><td>{esc(c)}</td><td>{n:,}</td></tr>" for c, n in stats["warn"])
        parts.append("</table>")
    else:
        parts.append("<p>none</p>")
    parts.append(f"""<h2>Transfer estimate</h2>
<p>{human_bytes(stats['pending_bytes'])} pending ≈ {xfer_h:,.1f} h of pure transfer at
{rate:g} Mbit/s (excl. active-hours windowing).</p>
<h2>Artifacts</h2><ul>
<li>problems.csv — every finding ({hard_n} hard unresolved)</li>
<li>patient_id_preview.csv — {n_pid:,} studies get a rewritten PatientID — REVIEW THIS before arming</li>
<li>duplicates.csv, charset_suspects.csv, uid_remap_preview.csv, patient_clusters.csv</li>
</ul></body></html>""")
    (outdir / "summary.html").write_text("".join(parts), encoding="utf-8")
    log.info("reports written to %s (open summary.html)", outdir)


# --------------------------------------------------------------------------
# SECTION 13 — GATE (exclude / approve / arm)
# --------------------------------------------------------------------------

def _apply_exclusion(conn, scope: str, ref: int | None) -> int:
    if ref is None:
        return 0
    if scope == "file":
        conn.execute("UPDATE files SET scan_status=? WHERE file_id=?", (FS_EXCLUDED, ref))
        return conn.execute("UPDATE instances SET send_status=?, canonical=0 WHERE file_id=?",
                            (S_EXCLUDED, ref)).rowcount
    if scope == "instance":
        return conn.execute("UPDATE instances SET send_status=?, canonical=0 WHERE instance_id=?",
                            (S_EXCLUDED, ref)).rowcount
    if scope == "study":
        return conn.execute("UPDATE instances SET send_status=?, canonical=0 WHERE study_pk=?",
                            (S_EXCLUDED, ref)).rowcount
    return 0


def cmd_exclude(cfg: Config, args) -> int:
    conn = db_connect(cfg.general.db_path)
    n = 0
    with conn:
        disarm_gate(conn, "exclude")
        if args.resolve_dup_priority:
            for row in conn.execute("SELECT problem_id, detail FROM problems "
                                    "WHERE code=? AND resolved=0", (H_DUP_CONFLICT,)).fetchall():
                detail = json.loads(row["detail"])
                cands = detail["candidates"]  # already priority-sorted by analyze
                for c in cands[1:]:
                    n += conn.execute("UPDATE instances SET send_status=?, canonical=0 "
                                      "WHERE instance_id=?", (S_EXCLUDED, c["instance_id"])).rowcount
                conn.execute("UPDATE problems SET resolved=1 WHERE problem_id=?", (row["problem_id"],))
            log.info("dup conflicts resolved by priority: %d loser instances excluded", n)
        if args.problem:
            q = "SELECT problem_id, scope, ref FROM problems WHERE code=? AND resolved=0"
            for row in conn.execute(q, (args.problem,)).fetchall():
                n += _apply_exclusion(conn, row["scope"], row["ref"])
                conn.execute("UPDATE problems SET resolved=1 WHERE problem_id=?", (row["problem_id"],))
        if args.study_uid:
            row = conn.execute("SELECT study_pk FROM studies WHERE study_uid=?",
                               (args.study_uid,)).fetchone()
            if not row:
                raise SystemExit(f"study not found: {args.study_uid}")
            n += _apply_exclusion(conn, "study", row["study_pk"])
            conn.execute("UPDATE problems SET resolved=1 WHERE scope='study' AND ref=?",
                         (row["study_pk"],))
        if args.file_id:
            n += _apply_exclusion(conn, "file", args.file_id)
            conn.execute("UPDATE problems SET resolved=1 WHERE scope='file' AND ref=?",
                         (args.file_id,))
        if args.path_glob:
            pat = args.path_glob.lower()
            hits = [fid for fid, root, rel in conn.execute(
                        "SELECT f.file_id, r.path, f.rel_path FROM files f "
                        "JOIN roots r USING(root_id) WHERE f.scan_status!=?", (FS_EXCLUDED,))
                    if fnmatch.fnmatch(os.path.join(root, rel).lower(), pat)
                    or fnmatch.fnmatch(os.path.basename(rel).lower(), pat)]
            for fid in hits:
                n += _apply_exclusion(conn, "file", fid)
            log.info("path-glob %r matched %d files", args.path_glob, len(hits))
        conn.execute(
            "UPDATE studies SET n_instances = (SELECT COUNT(*) FROM instances i "
            "WHERE i.study_pk = studies.study_pk AND i.canonical=1 AND i.send_status NOT IN (?,?))",
            (S_EXCLUDED, S_SKIPPED_DUP))
    log.info("excluded %d instances", n)
    conn.close()
    return 0


def gate_status(conn, cfg: Config) -> tuple[bool, list[str]]:
    """(armable/armed-valid, issues)."""
    issues = []
    run = int(meta_get(conn, "analyze_run", "0"))
    if run == 0:
        return False, ["no analyze run yet"]
    if meta_get(conn, "analyze_config_hash") != cfg.config_hash:
        issues.append("config changed since last analyze — re-run analyze")
    if int(meta_get(conn, "scan_last", "0")) > int(meta_get(conn, "analyze_time", "0")):
        issues.append("a scan ran after the last analyze — re-run analyze")
    hard = conn.execute("SELECT code, COUNT(*) FROM problems WHERE severity=? AND resolved=0 "
                        "GROUP BY code", (SEV_HARD,)).fetchall()
    for code, cnt in hard:
        issues.append(f"{cnt} unresolved {code}")
    acked = {r[0] for r in conn.execute(
        "SELECT code FROM acks WHERE analyze_run=? AND config_hash=?", (run, cfg.config_hash))}
    warn_codes = {r[0] for r in conn.execute(
        "SELECT DISTINCT code FROM problems WHERE severity=?", (SEV_WARN,))}
    for code in sorted(warn_codes - acked):
        issues.append(f"warning {code} not acknowledged (approve --ack {code})")
    return not issues, issues


def cmd_approve(cfg: Config, args) -> int:
    conn = db_connect(cfg.general.db_path)
    run = int(meta_get(conn, "analyze_run", "0"))
    with conn:
        for code in args.ack or []:
            present = conn.execute("SELECT COUNT(*) FROM problems WHERE code=? AND severity=?",
                                   (code, SEV_WARN)).fetchone()[0]
            if not present:
                log.warning("ack %s: no such warning in current analyze — recorded anyway", code)
            conn.execute("INSERT INTO acks(code, note, acked_at, config_hash, analyze_run) "
                         "VALUES(?,?,?,?,?)", (code, args.note or "", now_ms(),
                                               cfg.config_hash, run))
            log.info("acknowledged %s", code)
    ok, issues = gate_status(conn, cfg)
    if args.arm:
        if not ok:
            log.error("cannot arm the gate:")
            for i in issues:
                log.error("  - %s", i)
            conn.close()
            return 1
        with conn:
            meta_set(conn, "gate", f"armed:{run}:{cfg.config_hash}")
        log.info("GATE ARMED for analyze run %d — `send` is now allowed", run)
    else:
        state = meta_get(conn, "gate", "disarmed")
        log.info("gate: %s", state.split(":")[0])
        for i in issues:
            log.info("  - %s", i)
        if ok and not state.startswith("armed"):
            log.info("all checks pass — run `approve --arm` to arm the gate")
    conn.close()
    return 0


def check_armed(conn, cfg: Config) -> None:
    state = meta_get(conn, "gate", "")
    run = meta_get(conn, "analyze_run", "0")
    expect = f"armed:{run}:{cfg.config_hash}"
    if state != expect:
        ok, issues = gate_status(conn, cfg)
        msg = ["send refused: gate is not armed for the current analyze run + config."]
        msg += [f"  - {i}" for i in issues]
        msg.append("  fix the above, then: approve --arm")
        raise SystemExit("\n".join(msg))

# --------------------------------------------------------------------------
# SECTION 14 — NETWORK (C-STORE senders, throttle, governor)
# --------------------------------------------------------------------------

_UNCOMPRESSED_TS = {str(ImplicitVRLittleEndian), str(ExplicitVRLittleEndian),
                    str(ExplicitVRBigEndian), "1.2.840.10008.1.2.1.99"}


def classify_store_status(status) -> str:
    """'ok' | 'warn' | 'retry' | 'class-stop' | 'perm' | 'assoc'."""
    if status is None or not hasattr(status, "Status") or "Status" not in status:
        return "assoc"
    code = int(status.Status)
    if code == 0x0000:
        return "ok"
    if 0xB000 <= code <= 0xBFFF:
        return "warn"
    if 0xA700 <= code <= 0xA7FF:
        return "retry"
    if code in (0x0122, 0xA800):
        return "class-stop"
    return "perm"


class MemoryBudget:
    def __init__(self, budget_bytes: int):
        self.budget = max(budget_bytes, 256 << 20)
        self.used = 0
        self.cv = threading.Condition()

    def acquire(self, n: int, stop: threading.Event) -> bool:
        n = min(n, self.budget)
        with self.cv:
            while self.used + n > self.budget:
                if stop.is_set():
                    return False
                self.cv.wait(0.5)
            self.used += n
            return True

    def release(self, n: int) -> None:
        with self.cv:
            self.used = max(0, self.used - min(n, self.budget))
            self.cv.notify_all()


class Governor:
    """Active-hours + pause-file + shutdown gatekeeper for senders."""

    def __init__(self, cfg: Config, stop: threading.Event, ignore_window: bool = False):
        self.window = ActiveWindow("" if ignore_window else cfg.network.active_hours,
                                   cfg.network.active_days)
        self.pause = Pause(cfg.general.pause_file)
        self.stop = stop

    def clear_to_send(self) -> bool:
        return not self.stop.is_set() and self.window.is_active() and not self.pause.is_set()

    def wait_clear(self) -> bool:
        """Block until sending is allowed.  False => shutting down."""
        announced = False
        while not self.stop.is_set():
            if self.clear_to_send():
                return True
            if not announced:
                why = "pause file present" if self.pause.is_set() else "outside active hours"
                log.info("sending paused: %s", why)
                announced = True
            self.stop.wait(5)
        return False


@dataclass
class Lane:
    source: str
    dest_group: str


class Breaker:
    def __init__(self, threshold: int, cooldown_s: float = 600.0):
        self.threshold = max(1, threshold)
        self.cooldown = cooldown_s
        self._fail: dict[str, int] = {}
        self._until: dict[str, float] = {}
        self._lock = threading.Lock()

    def ok(self, key: str) -> bool:
        with self._lock:
            return time.monotonic() >= self._until.get(key, 0.0)

    def success(self, key: str) -> None:
        with self._lock:
            self._fail[key] = 0

    def failure(self, key: str) -> None:
        with self._lock:
            n = self._fail.get(key, 0) + 1
            self._fail[key] = n
            if n >= self.threshold:
                self._until[key] = time.monotonic() + self.cooldown
                self._fail[key] = 0
                log.error("circuit breaker OPEN for %s — cooling down %ds", key, int(self.cooldown))


def make_ae(cfg: Config, calling_aet: str) -> AE:
    ae = AE(ae_title=calling_aet)
    ae.acse_timeout = cfg.server.acse_timeout
    ae.dimse_timeout = cfg.server.dimse_timeout
    ae.network_timeout = cfg.server.network_timeout
    ae.maximum_pdu_size = cfg.server.max_pdu
    return ae


def plan_contexts(pairs: set[tuple[str, str]]) -> list[PresentationContext]:
    """One context per (SOPClass, file TS); Implicit LE added as the universal
    fallback for uncompressed files.  Caller caps the batch at <=100 pairs."""
    by_class: dict[str, set[str]] = {}
    for sop, ts in pairs:
        by_class.setdefault(sop, set()).add(ts)
    contexts: list[PresentationContext] = []
    for sop, tss in by_class.items():
        want = set(tss)
        if want & _UNCOMPRESSED_TS:
            want.add(str(ImplicitVRLittleEndian))
            want.add(str(ExplicitVRLittleEndian))
        for ts in sorted(want):
            contexts.append(build_context(sop, [ts]))
    return contexts


def pick_context(assoc, sop_class: str, ts: str) -> tuple[Any, str] | None:
    """Accepted context matching (class, ts); uncompressed may fall back.
    Returns (context, effective_ts) or None."""
    exact, fallback = None, None
    for cx in assoc.accepted_contexts:
        if str(cx.abstract_syntax) != sop_class:
            continue
        cts = str(cx.transfer_syntax[0])
        if cts == ts:
            exact = (cx, ts)
        elif ts in _UNCOMPRESSED_TS and cts in _UNCOMPRESSED_TS and cts != str(ExplicitVRBigEndian):
            fallback = (cx, cts)
    return exact or fallback


class SendCounters:
    def __init__(self):
        self.lock = threading.Lock()
        self.sent = self.warn = self.failed = self.skipped = 0
        self.bytes = 0
        self.t0 = time.monotonic()

    def bump(self, field_: str, nbytes: int = 0) -> None:
        with self.lock:
            setattr(self, field_, getattr(self, field_) + 1)
            self.bytes += nbytes

    def snapshot(self) -> str:
        with self.lock:
            dt = max(time.monotonic() - self.t0, 1e-3)
            return (f"sent={self.sent} warn={self.warn} failed={self.failed} "
                    f"skipped={self.skipped} {human_bytes(self.bytes)} "
                    f"({self.bytes * 8 / dt / 1e6:,.0f} Mbit/s avg)")


class Sender(threading.Thread):
    """One sender thread = one association at a time, study-batch granular."""

    BATCH_STUDIES = 20
    MAX_CTX_PAIRS = 100

    def __init__(self, idx: int, cfg: Config, lanes: "queue.Queue[Lane | None]",
                 writer: DbWriter, bucket: TokenBucket, governor: Governor,
                 breaker: Breaker, membudget: MemoryBudget, counters: SendCounters,
                 sidecar: SidecarWriter, stop: threading.Event,
                 dest_override: Destination | None, single_aet: str | None,
                 uid_remap: dict[tuple[str, str], str], sop_rev: dict[int, str],
                 ts_rev: dict[int, str], skip_existing: bool):
        super().__init__(name=f"sender-{idx}", daemon=True)
        self.cfg, self.lanes, self.writer = cfg, lanes, writer
        self.bucket, self.governor, self.breaker = bucket, governor, breaker
        self.membudget, self.counters, self.sidecar = membudget, counters, sidecar
        self.stop = stop
        self.dest_override, self.single_aet = dest_override, single_aet
        self.uid_remap, self.sop_rev, self.ts_rev = uid_remap, sop_rev, ts_rev
        self.skip_existing = skip_existing
        self.conn = db_connect(cfg.general.db_path)
        self.rejected_classes: set[str] = set()

    # ---- claiming ----------------------------------------------------------
    def claim_studies(self, lane: Lane) -> list[sqlite3.Row]:
        me = self.name
        with self.conn:  # BEGIN..COMMIT
            self.conn.execute("BEGIN IMMEDIATE")
            rows = self.conn.execute(
                "SELECT DISTINCT s.study_pk, s.study_uid, s.dest_group, s.pid_new, s.study_date,"
                " s.accession, s.n_instances "
                "FROM studies s JOIN instances i ON i.study_pk=s.study_pk "
                "WHERE i.source=? AND s.dest_group=? AND s.claimed_by IS NULL "
                "AND i.canonical=1 AND i.send_status IN (?,?) LIMIT ?",
                (lane.source, lane.dest_group, S_PENDING, S_FAILED_RETRY,
                 self.BATCH_STUDIES)).fetchall()
            if rows:
                self.conn.executemany(
                    "UPDATE studies SET claimed_by=?, claimed_at=? WHERE study_pk=? "
                    "AND claimed_by IS NULL",
                    [(me, now_ms(), r["study_pk"]) for r in rows])
        return rows

    def unclaim(self, study_pks: list[int]) -> None:
        if study_pks:
            with self.conn:
                self.conn.executemany("UPDATE studies SET claimed_by=NULL WHERE study_pk=?",
                                      [(pk,) for pk in study_pks])

    def pending_instances(self, study_pk: int, source: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT i.*, f.size AS fsize, f.kind AS fkind, r.path AS root, f.rel_path AS rel,"
            " s.study_uid, s.study_date, s.accession, s.pid_new,"
            " COALESCE(p.pid_raw,'') AS pid_raw "
            "FROM instances i JOIN files f USING(file_id) JOIN roots r USING(root_id) "
            "JOIN studies s ON s.study_pk=i.study_pk "
            "LEFT JOIN patients p ON p.patient_pk=s.patient_pk "
            "WHERE i.study_pk=? AND i.source=? AND i.canonical=1 AND i.send_status IN (?,?) "
            "ORDER BY i.series_uid, i.instance_id",
            (study_pk, source, S_PENDING, S_FAILED_RETRY)).fetchall()

    # ---- helpers -----------------------------------------------------------
    def _dest(self, group: str) -> Destination:
        return self.dest_override or self.cfg.dest_for_group(group)

    def _final_study_uid(self, row: sqlite3.Row) -> str:
        uid = row["study_uid"]
        m = self.uid_remap.get((uid, "study"))
        return m or uid

    def _record_result(self, inst: sqlite3.Row, assoc_id: int, status_int: int | None,
                       verdict: str, nbytes: int, dur_ms: int, error: str = "") -> None:
        st_map = {"ok": S_SENT, "warn": S_SENT_WARN, "retry": S_FAILED_RETRY,
                  "perm": S_FAILED_PERM, "class-stop": S_FAILED_PERM, "assoc": S_FAILED_RETRY}
        new_status = st_map[verdict]
        attempts = inst["attempts"] + 1
        if verdict in ("retry", "assoc") and attempts >= self.cfg.network.max_instance_retries:
            new_status = S_FAILED_PERM
            error = (error + " | max retries exhausted").strip(" |")
        self.writer.exec(
            "UPDATE instances SET send_status=?, attempts=?, last_status=?, sent_at=? "
            "WHERE instance_id=?",
            (new_status, attempts, status_int, now_ms() if new_status in (S_SENT, S_SENT_WARN) else inst["sent_at"],
             inst["instance_id"]))
        self.writer.exec(
            "INSERT INTO send_ledger(instance_id, assoc_id, ts, status, bytes, dur_ms, error) "
            "VALUES(?,?,?,?,?,?,?)",
            (inst["instance_id"], assoc_id, now_ms(), status_int, nbytes, dur_ms, error or None))
        if new_status == S_SENT:
            self.counters.bump("sent", nbytes)
        elif new_status == S_SENT_WARN:
            self.counters.bump("warn", nbytes)
        else:
            self.counters.bump("failed")

    def _study_exists_on_server(self, ae: AE, dest: Destination, study_uid: str,
                                expected: int) -> bool:
        try:
            assoc = ae.associate(self.cfg.server.host, dest.port,
                                 contexts=[build_context(StudyRootQueryRetrieveInformationModelFind)],
                                 ae_title=dest.called_aet)
        except Exception:
            return False
        if not assoc.is_established:
            return False
        try:
            q = Dataset()
            q.QueryRetrieveLevel = "STUDY"
            q.StudyInstanceUID = study_uid
            q.NumberOfStudyRelatedInstances = ""
            found = 0
            for st, ident in assoc.send_c_find(q, StudyRootQueryRetrieveInformationModelFind):
                if st and int(st.Status) in (0xFF00, 0xFF01) and ident is not None:
                    try:
                        found = int(ident.get("NumberOfStudyRelatedInstances", 0) or 0)
                    except (TypeError, ValueError):
                        found = 0
            return found >= expected > 0
        except Exception:
            return False
        finally:
            if assoc.is_established:
                assoc.release()

    # ---- main loop ---------------------------------------------------------
    def run(self) -> None:
        try:
            while not self.stop.is_set():
                try:
                    lane = self.lanes.get(timeout=1.0)
                except queue.Empty:
                    continue
                if lane is None:
                    self.lanes.task_done()
                    break
                key = f"{lane.source}->{lane.dest_group}"
                if not self.breaker.ok(key):
                    time.sleep(1)
                    self.lanes.put(lane)      # re-queue BEFORE task_done: counter stays >0
                    self.lanes.task_done()
                    continue
                had_work = self._process_lane(lane)
                if had_work and not self.stop.is_set():
                    self.lanes.put(lane)      # more batches may remain
                self.lanes.task_done()
        except Exception:
            log.exception("%s crashed", self.name)
        finally:
            self.conn.close()

    def _process_lane(self, lane: Lane) -> bool:
        studies = self.claim_studies(lane)
        if not studies:
            return False
        src = self.cfg.source(lane.source)
        dest = self._dest(lane.dest_group)
        calling = self.single_aet or src.calling_aet
        key = f"{lane.source}->{lane.dest_group}"
        claimed = [r["study_pk"] for r in studies]
        try:
            work: list[tuple[sqlite3.Row, list[sqlite3.Row]]] = []
            pairs: set[tuple[str, str]] = set()
            for srow in studies:
                insts = self.pending_instances(srow["study_pk"], lane.source)
                if insts:
                    work.append((srow, insts))
                    for i in insts:
                        pairs.add((self.sop_rev.get(i["sop_class_id"], ""),
                                   self.ts_rev.get(i["ts_id"], "")))
            if not work:
                return True
            if len(pairs) > self.MAX_CTX_PAIRS:  # split batch: keep first studies within cap
                trimmed, pairs = [], set()
                for srow, insts in work:
                    add = {(self.sop_rev.get(i["sop_class_id"], ""), self.ts_rev.get(i["ts_id"], ""))
                           for i in insts}
                    if pairs and len(pairs | add) > self.MAX_CTX_PAIRS:
                        break
                    pairs |= add
                    trimmed.append((srow, insts))
                keep = {id(x) for x in trimmed}
                self.unclaim([s["study_pk"] for s, _ in work if id((s, _)) not in keep
                              and (s, _) not in trimmed])
                work = trimmed

            if not self.governor.wait_clear():
                return True
            ae = make_ae(self.cfg, calling)
            if self.skip_existing:
                still = []
                for srow, insts in work:
                    uid = self._final_study_uid(srow)
                    if self._study_exists_on_server(ae, dest, uid, srow["n_instances"]):
                        log.info("%s: study %s already complete on server — skipping (%d inst)",
                                 self.name, uid, len(insts))
                        self.writer.many(
                            "UPDATE instances SET send_status=? WHERE instance_id=?",
                            [(S_SKIPPED_EXISTS, i["instance_id"]) for i in insts])
                        for i in insts:
                            self.counters.bump("skipped")
                    else:
                        still.append((srow, insts))
                work = still
                if not work:
                    return True

            try:
                assoc = ae.associate(self.cfg.server.host, dest.port,
                                     contexts=plan_contexts(pairs), ae_title=dest.called_aet)
            except Exception as e:
                log.warning("%s: association to %s:%s failed: %s", self.name,
                            self.cfg.server.host, dest.port, e)
                self.breaker.failure(key)
                time.sleep(self.cfg.network.backoff_initial_s)
                return True
            if not assoc.is_established:
                log.warning("%s: association rejected by %s@%s:%s", self.name,
                            dest.called_aet, self.cfg.server.host, dest.port)
                self.breaker.failure(key)
                time.sleep(self.cfg.network.backoff_initial_s)
                return True
            self.breaker.success(key)
            assoc_id = now_ms() * 10 + int(self.name[-1]) if self.name[-1].isdigit() else now_ms()
            self.writer.exec(
                "INSERT INTO associations(assoc_id, started, source, dest, calling_aet, called_aet)"
                " VALUES(?,?,?,?,?,?)",
                (assoc_id, now_ms(), lane.source, lane.dest_group, calling, dest.called_aet))
            n_stored = 0
            end_reason = "batch-done"
            try:
                for srow, insts in work:
                    for inst in insts:
                        if self.stop.is_set() or not self.governor.clear_to_send():
                            end_reason = "paused/stopped"
                            return True
                        if n_stored >= self.cfg.network.instances_per_association:
                            end_reason = "recycle"
                            self.lanes.put(lane)
                            return True
                        ok = self._send_one(assoc, assoc_id, inst)
                        n_stored += 1
                        if not ok:  # association broke
                            end_reason = "assoc-lost"
                            self.breaker.failure(key)
                            return True
                    self.writer.exec(
                        "UPDATE studies SET n_sent=(SELECT COUNT(*) FROM instances "
                        "WHERE study_pk=? AND send_status IN (?,?)), claimed_by=NULL "
                        "WHERE study_pk=?",
                        (srow["study_pk"], S_SENT, S_SENT_WARN, srow["study_pk"]))
                    claimed.remove(srow["study_pk"])
            finally:
                try:
                    if assoc.is_established:
                        assoc.release()
                except Exception:
                    pass
                self.writer.exec(
                    "UPDATE associations SET ended=?, end_reason=? WHERE assoc_id=?",
                    (now_ms(), end_reason, assoc_id))
            return True
        finally:
            self.unclaim(claimed)

    def _send_one(self, assoc, assoc_id: int, inst: sqlite3.Row) -> bool:
        """Send one instance.  Returns False when the association is unusable."""
        path = os.path.join(inst["root"], inst["rel"])
        size = inst["fsize"]
        sop_class = self.sop_rev.get(inst["sop_class_id"], "")
        ts = self.ts_rev.get(inst["ts_id"], "")
        if sop_class in self.rejected_classes:
            self._record_result(inst, assoc_id, 0x0122, "class-stop", 0, 0,
                                "SOP class rejected earlier on this run")
            return True
        cx = pick_context(assoc, sop_class, ts)
        if cx is None:
            self._record_result(inst, assoc_id, None, "perm", 0, 0,
                                f"no accepted presentation context for {sop_class}/{ts}")
            return assoc.is_established
        self.bucket.acquire(size, self.stop)
        if self.stop.is_set():
            return True
        if not self.membudget.acquire(size, self.stop):
            return True
        t0 = time.monotonic()
        try:
            try:
                src = self.cfg.source(inst["source"])
                rr = read_dicom(path, inst["fkind"], headers_only=False)
            except Exception as e:
                self._record_result(inst, assoc_id, None, "perm", 0, 0, f"read: {e}")
                return True
            try:
                new_sop = inst["new_sop_uid"] if "new_sop_uid" in inst.keys() else None
                if inst["needs"] or new_sop:
                    tr = Transformer(self.cfg, src,
                                     lambda old, t: self.uid_remap.get((old, t)))
                    ds, changes = tr.transform(rr, inst, inst["pid_new"])
                else:
                    ds = rr.ds
                    Transformer(self.cfg, src, lambda o, t: None)._pass_filemeta(
                        ds, guess_transfer_syntax(rr))
                    changes = []
            except Exception as e:
                self._record_result(inst, assoc_id, None, "perm", 0, 0, f"transform: {e}")
                return True
            eff_ts = cx[1]
            if eff_ts != ts:
                ds.file_meta.TransferSyntaxUID = UID(eff_ts)
            try:
                status = assoc.send_c_store(ds)
            except Exception as e:
                self._record_result(inst, assoc_id, None, "assoc", 0,
                                    int((time.monotonic() - t0) * 1000), f"send: {e}")
                return False
            verdict = classify_store_status(status)
            status_int = int(status.Status) if verdict not in ("assoc",) else None
            if verdict == "class-stop":
                self.rejected_classes.add(sop_class)
                log.error("server refuses SOP class %s (status 0x%04X) — skipping the rest of it",
                          sop_class, status_int or 0)
            dur = int((time.monotonic() - t0) * 1000)
            self._record_result(inst, assoc_id, status_int, verdict, size if verdict in ("ok", "warn") else 0,
                                dur, "" if verdict in ("ok", "warn") else f"dimse status {status_int}")
            if changes and verdict in ("ok", "warn"):
                self.sidecar.write({"sop_uid": inst["sop_uid"], "study_uid": inst["study_uid"],
                                    "source": inst["source"], "path": path, "changes": changes})
            if verdict == "assoc":
                return False
            return True
        finally:
            self.membudget.release(size)

def cmd_send(cfg: Config, args) -> int:
    conn = db_connect(cfg.general.db_path)
    ensure_indexes(conn)
    if args.dry_run:
        log.info("DRY RUN: sending to an in-process loopback SCP — gate not required")
    else:
        check_armed(conn, cfg)

    with conn:  # reclaim stale claims from crashed runs
        conn.execute("UPDATE studies SET claimed_by=NULL WHERE claimed_by IS NOT NULL")

    # optional narrowing
    where_src = ""
    params: list[Any] = [S_PENDING, S_FAILED_RETRY]
    if args.source:
        where_src = f" AND i.source IN ({','.join('?' * len(args.source))})"
        params += list(args.source)
    if args.study_uid:
        row = conn.execute("SELECT study_pk FROM studies WHERE study_uid=?",
                           (args.study_uid,)).fetchone()
        if not row:
            raise SystemExit(f"study not found: {args.study_uid}")
        with conn:
            conn.execute("UPDATE studies SET claimed_by='~excluded-by-filter' WHERE study_pk!=?",
                         (row["study_pk"],))

    lanes_rows = conn.execute(
        "SELECT DISTINCT i.source, s.dest_group FROM instances i "
        "JOIN studies s ON s.study_pk=i.study_pk "
        f"WHERE i.canonical=1 AND i.send_status IN (?,?){where_src} "
        "AND s.dest_group IS NOT NULL", params).fetchall()
    if not lanes_rows:
        log.info("nothing to send")
        return 0
    pending_n = conn.execute(
        f"SELECT COUNT(*) FROM instances i WHERE i.canonical=1 AND i.send_status IN (?,?){where_src}",
        params).fetchone()[0]
    log.info("send: %d pending instances across %d lane(s)", pending_n, len(lanes_rows))

    uid_remap = {(r["old_uid"], r["uid_type"]): r["new_uid"]
                 for r in conn.execute("SELECT old_uid, uid_type, new_uid FROM uid_remap")}
    sop_rev = Interner("sop_classes").reverse(conn)
    ts_rev = Interner("xfer").reverse(conn)
    conn.close()

    scp = None
    dest_override = None
    if args.dry_run:
        scp = LoopbackSCP(store_dir=args.dry_run_dir or
                          os.path.join(cfg.general.report_dir, "dry_run_received"))
        scp.start()
        dest_override = Destination(port=scp.port, called_aet="LOOPBACK")
        log.info("dry-run loopback SCP on 127.0.0.1:%d, storing to %s", scp.port, scp.store_dir)

    stop = threading.Event()
    writer = DbWriter(cfg.general.db_path)
    writer.start()
    bucket = TokenBucket(cfg.network.rate_limit_mbit * 1e6 / 8 if cfg.network.rate_limit_mbit else 0)
    governor = Governor(cfg, stop, ignore_window=args.ignore_window or bool(args.dry_run))
    breaker = Breaker(cfg.network.circuit_breaker_failures)
    membudget = MemoryBudget(cfg.network.memory_budget_mb << 20)
    counters = SendCounters()
    sidecar = SidecarWriter(cfg.general.sidecar_dir,
                            _dt.datetime.now().strftime("%Y%m%d_%H%M%S"),
                            cfg.general.sidecar_compress)

    host = cfg.server.host
    if args.dry_run:
        cfg = dataclasses.replace(cfg)  # shallow copy; only host differs
        cfg.server = dataclasses.replace(cfg.server, host="127.0.0.1",
                                         destinations=dict(cfg.server.destinations))

    lanes: "queue.Queue[Lane | None]" = queue.Queue()
    for r in lanes_rows:
        lanes.put(Lane(r["source"], r["dest_group"]))
    n_senders = max(1, min(cfg.network.max_associations, len(lanes_rows) * 2))
    senders = [Sender(i, cfg, lanes, writer, bucket, governor, breaker, membudget,
                      counters, sidecar, stop, dest_override, args.single_aet,
                      uid_remap, sop_rev, ts_rev,
                      cfg.network.skip_existing_studies and not args.dry_run)
               for i in range(n_senders)]
    for s in senders:
        s.start()
    log.info("send: %d sender thread(s) started (host=%s, window=%s, cap=%s Mbit)",
             n_senders, cfg.server.host if not args.dry_run else "127.0.0.1",
             "off" if args.ignore_window or args.dry_run else (cfg.network.active_hours or "always"),
             cfg.network.rate_limit_mbit or "none")
    try:
        last = time.monotonic()
        while any(s.is_alive() for s in senders):
            if lanes.unfinished_tasks == 0:
                break
            if args.limit and (counters.sent + counters.warn + counters.failed
                               + counters.skipped) >= args.limit:
                log.info("--limit %d reached, stopping", args.limit)
                break
            time.sleep(0.5)
            if time.monotonic() - last >= 30:
                log.info("progress: %s", counters.snapshot())
                last = time.monotonic()
    except KeyboardInterrupt:
        log.warning("interrupt: finishing in-flight stores, then stopping (resume any time)")
    stop.set()
    for _ in senders:
        lanes.put(None)
    for s in senders:
        s.join(timeout=max(60, cfg.server.dimse_timeout + 30))
    writer.flush()
    writer.stop()
    sidecar.close()
    if scp:
        scp.stop()

    conn = db_connect(cfg.general.db_path)
    if args.study_uid:
        with conn:
            conn.execute("UPDATE studies SET claimed_by=NULL WHERE claimed_by='~excluded-by-filter'")
    counts = {SEND_STATUS_NAMES.get(r[0], r[0]): r[1] for r in conn.execute(
        "SELECT send_status, COUNT(*) FROM instances WHERE canonical=1 GROUP BY send_status")}
    conn.close()
    log.info("send finished: %s", counters.snapshot())
    log.info("instance states now: %s", json.dumps(counts))
    return 0 if counters.failed == 0 else 1


# --------------------------------------------------------------------------
# SECTION 15 — VERIFY / ECHO / STATUS / EXPORT
# --------------------------------------------------------------------------

def cmd_verify(cfg: Config, args) -> int:
    conn = db_connect(cfg.general.db_path)
    uid_remap = {(r["old_uid"], r["uid_type"]): r["new_uid"]
                 for r in conn.execute("SELECT old_uid, uid_type, new_uid FROM uid_remap")}
    studies = conn.execute(
        "SELECT s.study_pk, s.study_uid, s.dest_group, "
        " (SELECT COUNT(*) FROM instances i WHERE i.study_pk=s.study_pk "
        "  AND i.send_status IN (?,?)) AS sent_n, "
        " (SELECT MIN(i.source) FROM instances i WHERE i.study_pk=s.study_pk "
        "  AND i.send_status IN (?,?)) AS src "
        "FROM studies s WHERE EXISTS (SELECT 1 FROM instances i WHERE i.study_pk=s.study_pk "
        " AND i.send_status IN (?,?))",
        (S_SENT, S_SENT_WARN) * 3).fetchall()
    if not studies:
        log.info("verify: nothing has been sent yet")
        return 0
    log.info("verify: %d studies to reconcile", len(studies))
    if args.fallback_ledger:
        ok = sum(1 for s in studies if s["sent_n"] > 0)
        log.info("ledger-only mode: %d studies have >=1 stored instance (no server query)", ok)
        return 0

    outdir = Path(cfg.general.report_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    f, w = _csv_writer(outdir / f"verify_{_dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                       ["study_uid_final", "dest", "expected(sent)", "found_on_server", "status"])
    n_match = n_mismatch = n_unavail = 0
    host = args.host or cfg.server.host
    by_dest: dict[str, list] = {}
    for s in studies:
        by_dest.setdefault(s["dest_group"] or "other", []).append(s)
    with conn, f:
        for group, rows in by_dest.items():
            dest = (Destination(port=args.port, called_aet=args.called_aet)
                    if args.port else cfg.dest_for_group(group))
            calling = args.calling_aet or cfg.source(rows[0]["src"]).calling_aet
            ae = make_ae(cfg, calling)
            try:
                assoc = ae.associate(host, dest.port,
                                     contexts=[build_context(StudyRootQueryRetrieveInformationModelFind)],
                                     ae_title=dest.called_aet)
            except Exception as e:
                assoc = None
                log.error("verify: cannot associate with %s:%s: %s", host, dest.port, e)
            established = assoc is not None and assoc.is_established
            for s in rows:
                final_uid = uid_remap.get((s["study_uid"], "study"), s["study_uid"])
                found, status = 0, V_UNAVAILABLE
                if established:
                    q = Dataset()
                    q.QueryRetrieveLevel = "STUDY"
                    q.StudyInstanceUID = final_uid
                    q.NumberOfStudyRelatedInstances = ""
                    try:
                        for st, ident in assoc.send_c_find(q, StudyRootQueryRetrieveInformationModelFind):
                            if st and int(st.Status) in (0xFF00, 0xFF01) and ident is not None:
                                try:
                                    found = int(ident.get("NumberOfStudyRelatedInstances", 0) or 0)
                                except (TypeError, ValueError):
                                    found = 0
                        status = V_MATCH if found >= s["sent_n"] else V_MISMATCH
                    except Exception as e:
                        log.warning("verify: C-FIND failed for %s: %s", final_uid, e)
                        established = assoc.is_established
                if status == V_MATCH:
                    n_match += 1
                elif status == V_MISMATCH:
                    n_mismatch += 1
                else:
                    n_unavail += 1
                conn.execute("INSERT INTO verifications(study_pk, ts, expected, found, status) "
                             "VALUES(?,?,?,?,?)", (s["study_pk"], now_ms(), s["sent_n"], found, status))
                conn.execute("UPDATE studies SET verify_status=? WHERE study_pk=?",
                             (status, s["study_pk"]))
                w.writerow([final_uid, f"{group}({dest.called_aet}:{dest.port})",
                            s["sent_n"], found,
                            {V_MATCH: "MATCH", V_MISMATCH: "MISMATCH",
                             V_UNAVAILABLE: "UNAVAILABLE"}[status]])
            if established:
                assoc.release()
    log.info("verify: %d MATCH, %d MISMATCH, %d UNAVAILABLE  (csv in %s)",
             n_match, n_mismatch, n_unavail, outdir)
    conn.close()
    return 0 if n_mismatch == 0 else 1


def cmd_echo(cfg: Config, args) -> int:
    host = args.host or cfg.server.host
    callings = [args.calling_aet] if args.calling_aet else \
        sorted({s.calling_aet for s in cfg.sources})
    failures = 0
    for group, dest in sorted(cfg.server.destinations.items()):
        for calling in callings:
            ae = make_ae(cfg, calling)
            verdict = "?"
            try:
                assoc = ae.associate(host, dest.port, contexts=[build_context(Verification)],
                                     ae_title=dest.called_aet)
                if assoc.is_established:
                    st = assoc.send_c_echo()
                    verdict = "OK" if st and int(st.Status) == 0 else f"status {getattr(st, 'Status', '?')}"
                    assoc.release()
                else:
                    verdict = "association rejected"
            except Exception as e:
                verdict = f"error: {e}"
            ok = verdict == "OK"
            failures += 0 if ok else 1
            log.log(logging.INFO if ok else logging.ERROR,
                    "echo %-18s -> %s@%s:%-5s  %s", calling, dest.called_aet, host, dest.port, verdict)
    return 0 if failures == 0 else 1


def cmd_status(cfg: Config, args) -> int:
    conn = db_connect(cfg.general.db_path, readonly=True)
    print(f"gate: {meta_get(conn, 'gate', 'never armed')}")
    print(f"analyze run: {meta_get(conn, 'analyze_run', '-')}")
    print()
    print(f"{'source':<14}{'pending':>10}{'sent':>10}{'warn':>8}{'failed':>8}"
          f"{'skipped':>9}{'excluded':>9}{'bytes pending':>16}")
    for src in cfg.sources:
        c = {k: 0 for k in range(8)}
        for st, n in conn.execute("SELECT send_status, COUNT(*) FROM instances "
                                  "WHERE source=? AND canonical=1 GROUP BY send_status", (src.name,)):
            c[st] = n
        b = conn.execute("SELECT COALESCE(SUM(f.size),0) FROM instances i JOIN files f USING(file_id) "
                         "WHERE i.source=? AND i.canonical=1 AND i.send_status IN (?,?)",
                         (src.name, S_PENDING, S_FAILED_RETRY)).fetchone()[0]
        print(f"{src.name:<14}{c[S_PENDING] + c[S_FAILED_RETRY]:>10,}{c[S_SENT]:>10,}"
              f"{c[S_SENT_WARN]:>8,}{c[S_FAILED_PERM]:>8,}"
              f"{c[S_SKIPPED_DUP] + c[S_SKIPPED_EXISTS]:>9,}{c[S_EXCLUDED]:>9,}"
              f"{human_bytes(b):>16}")
    v = {r[0]: r[1] for r in conn.execute("SELECT verify_status, COUNT(*) FROM studies "
                                          "WHERE verify_status>0 GROUP BY verify_status")}
    if v:
        print(f"\nverify: {v.get(V_MATCH, 0)} match, {v.get(V_MISMATCH, 0)} mismatch, "
              f"{v.get(V_UNAVAILABLE, 0)} unavailable")
    conn.close()
    return 0


def cmd_export_mapping(cfg: Config, args) -> int:
    conn = db_connect(cfg.general.db_path, readonly=True)
    outdir = Path(cfg.general.report_dir) / "mappings"
    outdir.mkdir(parents=True, exist_ok=True)
    f, w = _csv_writer(outdir / "patient_id_map.csv",
                       ["source", "study_uid", "old_patient_id", "new_patient_id", "rule",
                        "patient_name_raw", "birth_date"])
    with f:
        for r in conn.execute(
                "SELECT s.source, s.study_uid, COALESCE(p.pid_raw,'') pid, s.pid_new, s.pid_rule,"
                " COALESCE(p.name_raw,'') name, COALESCE(p.birth_date,'') birth "
                "FROM studies s LEFT JOIN patients p USING(patient_pk) "
                "WHERE s.pid_new IS NOT NULL"):
            w.writerow([r["source"], r["study_uid"], r["pid"], r["pid_new"], r["pid_rule"],
                        r["name"], r["birth"]])
    f, w = _csv_writer(outdir / "uid_map.csv", ["old_uid", "type", "new_uid", "reason"])
    with f:
        for r in conn.execute("SELECT old_uid, uid_type, new_uid, reason FROM uid_remap"):
            w.writerow([r["old_uid"], r["uid_type"], r["new_uid"], r["reason"]])
    log.info("mappings exported to %s (sidecar JSONL with per-instance changes: %s)",
             outdir, cfg.general.sidecar_dir)
    conn.close()
    return 0

def _probe_file(path: str) -> list[str]:
    """Forensic look at one file: magic bytes, structure guesses, parse attempts."""
    lines = []
    try:
        with open(winpath(path), "rb") as f:
            head = f.read(4096)
            f.seek(0, 2)
            size = f.tell()
    except OSError as e:
        return [f"  OPEN FAILED: {e}"]
    printable = sum(1 for b in head if 32 <= b < 127 or b in (9, 10, 13))
    lines.append(f"  size={size:,}  head16={head[:16].hex(' ')}")
    lines.append(f"  printable={100 * printable // max(1, len(head))}%  "
                 f"distinct_bytes={len(set(head))}  sniff={KIND_NAMES.get(sniff_kind(head, size))}")
    if len(head) >= 132 and head[128:132] == b"DICM":
        lines.append("  DICM marker at offset 128: YES")
    if head[:2] == b"\x1f\x8b":
        lines.append("  gzip magic at 0")
    stripped = head.lstrip()
    if stripped[:1] in (b"{", b"["):
        lines.append(f"  JSON-ish, starts: {stripped[:70]!r}")
    # "N;<md5hex>[;]" ASCII prefix (seen in the wild on Orthanc-layout storages):
    # try every plausible payload interpretation after the prefix
    m = re.match(rb"^(\d+);([0-9a-fA-F]{32})(;?)", head)
    if m:
        off = m.end()
        lines.append(f"  prefixed format: version={m.group(1).decode()} "
                     f"md5={m.group(2).decode()} payload@{off}")
        try:
            with open(winpath(path), "rb") as f:
                blob = f.read(64 << 20)
            payload = blob[off:]
            lines.append(f"    payload_head16={payload[:16].hex(' ')}  payload_len={len(payload):,}")
            inner_direct = sniff_kind(payload[:512], len(payload))
            if inner_direct != K_NONDICOM:
                lines.append(f"    payload as-is sniffs as: {KIND_NAMES.get(inner_direct)}")
            import hashlib as _h
            for what, data_ in (("payload", payload), ("whole-file", blob)):
                if _h.md5(data_).hexdigest().lower() == m.group(2).decode().lower():
                    lines.append(f"    md5 prefix matches {what}")
            for wb, wlabel in ((15, "zlib"), (-15, "raw-deflate"), (31, "gzip")):
                try:
                    data = zlib.decompress(payload, wb)
                except zlib.error:
                    continue
                inner = sniff_kind(data[:512], len(data))
                lines.append(f"    {wlabel}: inflates to {len(data):,} bytes, "
                             f"inner={KIND_NAMES.get(inner)}  inner_head32={data[:32].hex(' ')}")
                if data[:1] in (b"{", b"["):
                    lines.append(f"    inflated JSON starts: {data[:90]!r}")
                if _h.md5(data).hexdigest().lower() == m.group(2).decode().lower():
                    lines.append("    md5 prefix matches INFLATED payload")
                break
            # maybe payload is size-prefixed like Orthanc zlib-with-size
            if len(payload) > 10 and payload[8] == 0x78:
                try:
                    data = zlib.decompress(payload[8:])
                    usize = int.from_bytes(payload[:8], "little")
                    inner = sniff_kind(data[:512], len(data))
                    lines.append(f"    size+zlib@+8: inflates to {len(data):,} "
                                 f"(prefix says {usize:,}), inner={KIND_NAMES.get(inner)}")
                except zlib.error:
                    pass
        except OSError as e:
            lines.append(f"    payload read failed: {e}")
    # zlib stream candidates: bare at offset 0, or Orthanc uint64-size-prefixed at 8
    for off, label in ((0, "bare-zlib@0"), (8, "orthanc-zlib@8")):
        if len(head) > off + 2 and head[off] == 0x78 and head[off + 1] in (0x01, 0x5E, 0x9C, 0xDA):
            if size > (64 << 20):
                lines.append(f"  {label}: candidate, file too big to inflate in probe")
                continue
            try:
                with open(winpath(path), "rb") as f:
                    blob = f.read()
                data = zlib.decompress(blob[off:])
                inner = sniff_kind(data[:512], len(data))
                lines.append(f"  {label}: inflates to {len(data):,} bytes, "
                             f"inner={KIND_NAMES.get(inner)}  inner_head16={data[:16].hex(' ')}")
                if off == 8:
                    usize = int.from_bytes(blob[0:8], "little")
                    lines.append(f"    size-prefix says {usize:,} "
                                 f"({'matches' if usize == len(data) else 'MISMATCH'})")
            except zlib.error as e:
                lines.append(f"  {label}: zlib magic but inflate failed: {e}")
    try:
        ds = dcmread(winpath(path), stop_before_pixels=True, force=True, defer_size=1024)
        if len(ds):
            lines.append(f"  force-dcmread: {len(ds)} elements, "
                         f"SOPClass={safe_str(ds, 'SOPClassUID', 64) or '?'} "
                         f"Modality={safe_str(ds, 'Modality', 8) or '?'} "
                         f"TS={str(getattr(getattr(ds, 'file_meta', None), 'get', lambda *a: '')('TransferSyntaxUID', '') or '?')}")
        else:
            lines.append("  force-dcmread: parses to 0 elements (not DICOM)")
    except Exception as e:
        lines.append(f"  force-dcmread: failed ({str(e)[:90]})")
    return lines


def cmd_probe(cfg: Config, args) -> int:
    if args.path:
        for p in args.path:
            print(f"\n== {p}")
            print("\n".join(_probe_file(p)))
        return 0
    if not args.source:
        raise SystemExit("probe needs --source NAME (or explicit --path files)")
    conn = db_connect(cfg.general.db_path, readonly=True)
    statuses = {"nondicom": (FS_NONDICOM,), "errors": (FS_ERROR,),
                "all": (FS_NONDICOM, FS_ERROR)}[args.status]
    print(f"source {args.source} — scan status so far:")
    for st, n in conn.execute(
            "SELECT f.scan_status, COUNT(*) FROM files f JOIN roots r USING(root_id) "
            "WHERE r.source=? GROUP BY f.scan_status", (args.source,)):
        name = {FS_PENDING: "pending", FS_OK: "dicom-ok", FS_ERROR: "error",
                FS_EXCLUDED: "excluded", FS_NONDICOM: "non-dicom"}.get(st, st)
        print(f"  {name:<10} {n:,}")
    print("size distribution (from DB, no file reads):")
    print(f"  {'status':<10} {'bucket':<10} {'files':>12} {'total':>12}")
    for st, bucket, n, total in conn.execute(
            "SELECT f.scan_status, CASE WHEN f.size < 4096 THEN 'a <4KB' "
            " WHEN f.size < 65536 THEN 'b 4-64KB' WHEN f.size < 1048576 THEN 'c 64KB-1MB' "
            " WHEN f.size < 10485760 THEN 'd 1-10MB' ELSE 'e >10MB' END AS bucket, "
            " COUNT(*), SUM(f.size) FROM files f JOIN roots r USING(root_id) "
            "WHERE r.source=? GROUP BY f.scan_status, bucket ORDER BY f.scan_status, bucket",
            (args.source,)):
        name = {FS_OK: "dicom-ok", FS_ERROR: "error", FS_NONDICOM: "non-dicom",
                FS_EXCLUDED: "excluded", FS_PENDING: "pending"}.get(st, st)
        print(f"  {name:<10} {bucket[2:]:<10} {n:>12,} {human_bytes(total or 0):>12}")
    qmarks = ",".join("?" * len(statuses))
    rows = conn.execute(
        f"SELECT f.file_id, f.scan_status, f.error, r.path AS root, f.rel_path AS rel "
        f"FROM files f JOIN roots r USING(root_id) "
        f"WHERE r.source=? AND f.scan_status IN ({qmarks}) ORDER BY RANDOM() LIMIT ?",
        (args.source, *statuses, args.sample)).fetchall()
    sig_hist: dict[str, int] = {}
    ext_hist: dict[str, int] = {}
    for row in rows:
        p = os.path.join(row["root"], row["rel"])
        status = "error" if row["scan_status"] == FS_ERROR else "non-dicom"
        print(f"\n== [{status}] {p}")
        if row["error"]:
            print(f"  recorded error: {row['error'][:120]}")
        for line in _probe_file(p):
            print(line)
        try:
            with open(winpath(p), "rb") as f:
                sig = f.read(4).hex(" ")
        except OSError:
            sig = "unreadable"
        sig_hist[sig] = sig_hist.get(sig, 0) + 1
        ext = os.path.splitext(row["rel"])[1].lower() or "(none)"
        ext_hist[ext] = ext_hist.get(ext, 0) + 1
    if rows:
        print("\nsampled first-4-bytes histogram:")
        for sig, n in sorted(sig_hist.items(), key=lambda kv: -kv[1]):
            print(f"  {sig:<14} {n}")
        print("sampled extension histogram:")
        for ext, n in sorted(ext_hist.items(), key=lambda kv: -kv[1]):
            print(f"  {ext:<10} {n}")
    errs = conn.execute(
        "SELECT substr(COALESCE(error,''),1,60) e, COUNT(*) FROM files f JOIN roots r USING(root_id) "
        "WHERE r.source=? AND f.scan_status=? GROUP BY e ORDER BY COUNT(*) DESC LIMIT 10",
        (args.source, FS_ERROR)).fetchall()
    if errs:
        print("\ntop recorded scan errors:")
        for e, n in errs:
            print(f"  {n:>8,}  {e}")
    conn.close()
    return 0


def _manifest_tag(d: dict, tag8: str) -> str:
    """Value of a tag from any Orthanc JSON flavor: DICOMweb ('00080018' ->
    {'Value': [...], 'vr': ...}) or legacy ('0008,0018' -> {'Value': '...'}),
    upper- or lowercase hex."""
    v = None
    for k in (tag8, tag8.lower(), f"{tag8[:4]},{tag8[4:]}", f"{tag8[:4]},{tag8[4:]}".lower()):
        v = d.get(k)
        if v is not None:
            break
    if not isinstance(v, dict):
        return ""
    val = v.get("Value")
    if isinstance(val, list):
        val = val[0] if val else ""
    if isinstance(val, dict):  # DICOMweb PN: {"Alphabetic": "..."}
        val = val.get("Alphabetic", "")
    return str(val or "").strip()


def _decode_manifest(path: str) -> list[dict] | None:
    """Decode an Orthanc metadata attachment to a list of tag-dicts, or None."""
    try:
        with open(winpath(path), "rb") as f:
            blob = f.read(32 << 20)
    except OSError:
        return None
    data = None
    m = ORTHANC_ATTACH_RE.match(blob)
    if m:
        payload = blob[m.end():]
        for wb in (31, 15, -15):  # gzip observed in the field; be liberal
            try:
                data = zlib.decompress(payload, wb)
                break
            except zlib.error:
                continue
    elif blob[:1] in (b"{", b"[") or blob.lstrip()[:1] in (b"{", b"["):
        data = blob
    elif len(blob) > 10 and blob[8] == 0x78:  # zlib-with-size attachment
        try:
            data = zlib.decompress(blob[8:])
        except zlib.error:
            return None
    if data is None or data.lstrip()[:1] not in (b"{", b"["):
        return None
    try:
        root = json.loads(data)
    except (ValueError, UnicodeDecodeError):
        return None
    entries = root if isinstance(root, list) else [root]
    return [e for e in entries if isinstance(e, dict)]


def _walk_metadata_dir(root: str, cap_bytes: int = 32 << 20) -> Iterator[str]:
    """Yield candidate metadata files under an arbitrary directory tree."""
    for dirpath, _dirs, files in os.walk(winpath(root)):
        for name in files:
            p = os.path.join(dirpath, name)
            try:
                if 40 <= os.path.getsize(p) <= cap_bytes:
                    yield p
            except OSError:
                continue


def cmd_xcheck(cfg: Config, args) -> int:
    """Completeness proof: every instance referenced by a metadata store (Orthanc
    gzip attachments OR dcm4che DICOMweb-JSON exports) must exist in the scanned
    inventory.  Run AFTER scans finish.  --dir cross-checks a raw directory tree
    (e.g. a metadata-only store not configured as a source)."""
    conn = db_connect(cfg.general.db_path)
    ensure_indexes(conn)
    if args.dir:
        paths = list(_walk_metadata_dir(args.dir))
        log.info("xcheck: scanning %d files under %s", len(paths), args.dir)
    else:
        sources = args.source or [s.name for s in cfg.sources]
        qmarks = ",".join("?" * len(sources))
        rows = conn.execute(
            f"SELECT r.path AS root, f.rel_path AS rel FROM files f JOIN roots r USING(root_id) "
            f"WHERE r.source IN ({qmarks}) AND f.scan_status=? AND f.size BETWEEN 40 AND ?",
            (*sources, FS_NONDICOM, 32 << 20)).fetchall()
        paths = [os.path.join(r["root"], r["rel"]) for r in rows]
    log.info("xcheck: decoding up to %d candidate metadata files...", len(paths))
    manifest: dict[str, dict] = {}
    parsed = 0
    for i, path in enumerate(paths, 1):
        entries = _decode_manifest(path)
        if entries is None:
            continue
        parsed += 1
        for e in entries:
            sop = _manifest_tag(e, "00080018")
            if sop and sop not in manifest:
                manifest[sop] = {
                    "study_uid": _manifest_tag(e, "0020000D"),
                    "pid": _manifest_tag(e, "00100020"),
                    "name": _manifest_tag(e, "00100010"),
                    "date": _manifest_tag(e, "00080020"),
                    "manifest": path,
                }
        if i % 20000 == 0:
            log.info("xcheck: %d/%d files, %d manifests, %d instances referenced",
                     i, len(paths), parsed, len(manifest))
    log.info("xcheck: %d manifests decoded, %d distinct instances referenced",
             parsed, len(manifest))
    if not manifest:
        log.info("xcheck: nothing to check")
        return 0
    conn.execute("CREATE TEMP TABLE xc(sop TEXT PRIMARY KEY)")
    conn.executemany("INSERT OR IGNORE INTO xc(sop) VALUES(?)",
                     [(s,) for s in manifest])
    missing = [r[0] for r in conn.execute(
        "SELECT x.sop FROM xc x LEFT JOIN instances i ON i.sop_uid = x.sop "
        "WHERE i.instance_id IS NULL")]
    found = len(manifest) - len(missing)
    outdir = Path(cfg.general.report_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    if missing:
        f, w = _csv_writer(outdir / f"xcheck_missing_{_dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                           ["sop_uid", "study_uid", "patient_id", "patient_name",
                            "study_date", "manifest_file"])
        with f:
            for sop in missing:
                info = manifest[sop]
                w.writerow([sop, info["study_uid"], info["pid"], info["name"],
                            info["date"], info["manifest"]])
        log.warning("xcheck: %d instances referenced by the metadata store are MISSING "
                    "from the scanned inventory — see the CSV in %s. Their pixel files were "
                    "never scanned (folder not covered by a [[source]] root) or are gone.",
                    len(missing), outdir)
    log.info("xcheck: %d/%d referenced instances present in the scanned inventory (%.2f%%)",
             found, len(manifest), 100.0 * found / len(manifest))
    conn.close()
    return 0 if not missing else 1


def _pixeldata_hash(path: str, max_bytes: int = 300 << 20) -> tuple[str, str, int]:
    """(transfer_syntax, pixeldata_sha256|reason, size).  Read-only, never raises."""
    try:
        size = os.path.getsize(winpath(path))
    except OSError as e:
        return "", f"stat-failed:{e}", 0
    if size > max_bytes:
        return "", "too-large-to-hash", size
    try:
        ds = dcmread(winpath(path), force=True)
        ts = ""
        fm = getattr(ds, "file_meta", None)
        if fm is not None:
            ts = str(fm.get("TransferSyntaxUID", "") or "")
        item = ds.get_item(Tag(0x7FE0, 0x0010))
        if item is None or item.value in (None, b""):
            return ts, "no-pixeldata", size
        val = item.value
        raw = bytes(val) if isinstance(val, (bytes, bytearray, memoryview)) else str(val).encode()
        return ts, hashlib.sha256(raw).hexdigest(), size
    except Exception as e:
        return "", f"read-failed:{str(e)[:60]}", size


def _pixel_sha(path: str, max_bytes: int = 300 << 20) -> str:
    """Just the PixelData sha256 (or a non-hex reason token).  For dedupe."""
    return _pixeldata_hash(path, max_bytes)[1]


def cmd_dup_audit(cfg: Config, args) -> int:
    """Sample H-DUP-CONFLICT groups and characterize them: cross-root vs same-root,
    same transfer syntax, and whether PixelData is identical.  Read-only — informs
    the diff-content resolution policy before any irreversible send."""
    conn = db_connect(cfg.general.db_path, readonly=True)
    root_prefixes = []
    for s in cfg.sources:
        for r in s.roots:
            root_prefixes.append((os.path.abspath(r).lower(), f"{s.name}:{r}"))

    def root_of(path: str) -> str:
        p = os.path.abspath(path).lower()
        for pref, label in root_prefixes:
            if p.startswith(pref):
                return label
        return "?"

    rows = conn.execute(
        "SELECT detail FROM problems WHERE code=? AND resolved=0 ORDER BY RANDOM() LIMIT ?",
        (H_DUP_CONFLICT, args.sample)).fetchall()
    total_conf = conn.execute("SELECT COUNT(*) FROM problems WHERE code=? AND resolved=0",
                              (H_DUP_CONFLICT,)).fetchone()[0]
    conn.close()
    if not rows:
        print("no unresolved H-DUP-CONFLICT groups")
        return 0
    print(f"sampling {len(rows)} of {total_conf:,} dup-conflict groups\n")
    cats: dict[str, int] = {}
    examples: dict[str, str] = {}
    for row in rows:
        detail = json.loads(row["detail"])
        cands = detail.get("candidates", [])
        roots, tss, phs, reasons = set(), set(), set(), []
        for c in cands:
            ts, ph, _sz = _pixeldata_hash(c["path"])
            roots.add(root_of(c["path"]))
            tss.add(ts)
            phs.add(ph)
            if not ph.count("0") or "failed" in ph or ph in ("too-large-to-hash", "no-pixeldata"):
                if not re.fullmatch(r"[0-9a-f]{64}", ph):
                    reasons.append(ph)
        cross = "cross-root" if len(roots) > 1 else "same-root"
        hashes = {h for h in phs if re.fullmatch(r"[0-9a-f]{64}", h or "")}
        if reasons and not hashes:
            why = "/".join(sorted({r.split(":")[0] for r in reasons})[:2])
            cat = f"{cross}/uncheckable({why})"
        elif len(tss) > 1 and len(hashes) > 1:
            cat = f"{cross}/diff-TS(pixels-not-comparable)"
        elif len(hashes) == 1 and len(phs) == 1:
            cat = f"{cross}/BENIGN-same-pixels"
        elif len(hashes) > 1:
            cat = f"{cross}/DANGER-diff-pixels"
        else:
            cat = f"{cross}/unknown"
        cats[cat] = cats.get(cat, 0) + 1
        if cat not in examples:
            examples[cat] = " | ".join(f"{root_of(c['path'])} ts={_pixeldata_hash(c['path'])[0][-8:]}"
                                       for c in cands[:3])
    print("category                                    count")
    for cat, n in sorted(cats.items(), key=lambda kv: -kv[1]):
        print(f"  {cat:<42} {n:>5}  ({100*n/len(rows):.0f}%)")
    print("\nexamples:")
    for cat, ex in examples.items():
        print(f"  [{cat}]\n    {ex}")
    danger = sum(n for c, n in cats.items() if "DANGER" in c)
    print(f"\n=> {danger}/{len(rows)} sampled groups have SAME UID + DIFFERENT PIXELS "
          f"(the real landmine). Extrapolated over {total_conf:,}: ~{danger*total_conf//len(rows):,}.")
    print("   benign (same pixels, or metadata/TS-only differences) can be auto-resolved "
          "by priority; danger groups need UID regeneration or manual review.")
    return 0


# --------------------------------------------------------------------------
# SECTION 16 — SELFTEST (synthetic data, loopback SCP, end-to-end)
# --------------------------------------------------------------------------

from pynetdicom import AllStoragePresentationContexts
from pydicom.uid import AllTransferSyntaxes

CR_SOP = "1.2.840.10008.5.1.4.1.1.1"
CT_SOP = "1.2.840.10008.5.1.4.1.1.2"
PRIVATE_SOP = "1.2.392.200036.9125.1.1.2"  # Fuji private CR Image Storage


class LoopbackSCP:
    """In-process Storage/Find/Echo SCP used by --dry-run, selftest and `scp`."""

    def __init__(self, store_dir: str, port: int = 0,
                 reject_classes: set[str] | None = None,
                 status_by_class: dict[str, int] | None = None,
                 abort_at: int = 0):
        self.store_dir = store_dir
        Path(store_dir).mkdir(parents=True, exist_ok=True)
        self.port = port
        self.reject_classes = reject_classes or set()
        self.status_by_class = status_by_class or {}
        self.abort_at = abort_at
        self._count = 0
        self.received: dict[str, int] = {}
        self.stored_sops: list[str] = []
        self._lock = threading.Lock()
        self._server = None

    def _on_store(self, event):
        with self._lock:
            self._count += 1
            if self.abort_at and self._count == self.abort_at:
                self.abort_at = 0  # only once
                event.assoc.abort()
                return 0xC000
        ds = event.dataset
        ds.file_meta = event.file_meta
        sop_class = str(ds.get("SOPClassUID", ""))
        if sop_class in self.status_by_class:
            return self.status_by_class[sop_class]
        sop = str(ds.get("SOPInstanceUID", "")) or f"no-uid-{self._count}"
        try:
            ds.save_as(os.path.join(self.store_dir, f"{sop}.dcm"), enforce_file_format=True)
        except Exception as e:
            log.error("loopback SCP: cannot store %s: %s", sop, e)
            return 0xA700
        with self._lock:
            self.received[str(ds.get("StudyInstanceUID", ""))] = \
                self.received.get(str(ds.get("StudyInstanceUID", "")), 0) + 1
            self.stored_sops.append(sop)
        return 0x0000

    def _on_find(self, event):
        ident = event.identifier
        uid = str(ident.get("StudyInstanceUID", ""))
        with self._lock:
            n = self.received.get(uid, 0)
        if n:
            rsp = Dataset()
            rsp.QueryRetrieveLevel = "STUDY"
            rsp.StudyInstanceUID = uid
            rsp.NumberOfStudyRelatedInstances = n
            yield 0xFF00, rsp

    def start(self) -> None:
        ae = AE(ae_title="LOOPBACK")
        ae.maximum_pdu_size = 0
        for cx in AllStoragePresentationContexts:
            ae.add_supported_context(cx.abstract_syntax, AllTransferSyntaxes)
        ae.add_supported_context(PRIVATE_SOP, AllTransferSyntaxes)
        ae.add_supported_context(Verification)
        ae.add_supported_context(StudyRootQueryRetrieveInformationModelFind)
        for uid in self.reject_classes:
            ae.remove_supported_context(uid)
        handlers = [(evt.EVT_C_STORE, self._on_store), (evt.EVT_C_FIND, self._on_find)]
        self._server = ae.start_server(("127.0.0.1", self.port), block=False,
                                       evt_handlers=handlers)
        self.port = self._server.socket.getsockname()[1]

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()


def cmd_scp(cfg: Config | None, args) -> int:
    scp = LoopbackSCP(store_dir=args.dir, port=args.port)
    scp.start()
    print(f"loopback SCP listening on 127.0.0.1:{scp.port} (AET LOOPBACK), "
          f"storing to {args.dir} — Ctrl-C to stop")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        scp.stop()
        print(f"stopped; received {len(scp.stored_sops)} instances")
    return 0


# ---- synthetic dataset factory ---------------------------------------------

def _mk_ds(sop_class: str, sop_uid: str, study_uid: str | None, series_uid: str,
           modality: str, pid: str, pname: str, birth: str = "19700101",
           study_date: str = "20200115", study_time: str = "101500",
           institution: str = "", station: str = "", charset: str | None = None,
           accession: str = "ACC1") -> Dataset:
    ds = Dataset()
    if charset is not None:
        ds.SpecificCharacterSet = charset
    ds.SOPClassUID = sop_class
    ds.SOPInstanceUID = sop_uid
    if study_uid is not None:
        ds.StudyInstanceUID = study_uid
    ds.SeriesInstanceUID = series_uid
    ds.Modality = modality
    ds.PatientID = pid
    ds.PatientName = pname
    ds.PatientBirthDate = birth
    ds.PatientSex = "O"
    ds.StudyDate = study_date
    ds.StudyTime = study_time
    ds.AccessionNumber = accession
    ds.StudyDescription = "selftest"
    if institution:
        ds.InstitutionName = institution
    if station:
        ds.StationName = station
    ds.Rows = 8
    ds.Columns = 8
    ds.BitsAllocated = 8
    ds.BitsStored = 8
    ds.HighBit = 7
    ds.PixelRepresentation = 0
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.PixelData = bytes(range(64))
    fm = FileMetaDataset()
    fm.MediaStorageSOPClassUID = UID(sop_class)
    fm.MediaStorageSOPInstanceUID = UID(sop_uid)
    fm.TransferSyntaxUID = ExplicitVRLittleEndian
    ds.file_meta = fm
    return ds


def _write(ds: Dataset, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ds.save_as(str(path), enforce_file_format=True)


def _part10_bytes(ds: Dataset) -> bytes:
    buf = io.BytesIO()
    ds.save_as(buf, enforce_file_format=True)
    return buf.getvalue()


def _strip_to_bare(part10: bytes) -> bytes:
    """Remove preamble/DICM/file-meta -> bare explicit-LE dataset bytes."""
    assert part10[128:132] == b"DICM"
    # (0002,0000) UL, explicit VR: tag(4) 'UL'(2) len(2) value(4)
    assert part10[132:136] == b"\x02\x00\x00\x00"
    glen = int.from_bytes(part10[140:144], "little")
    return part10[144 + glen:]


def _lat(cyr: str, codec: str) -> str:
    """Encode text with codec, reinterpret as latin-1 (crafts mojibake files)."""
    return cyr.encode(codec).decode("latin-1")


def build_selftest_tree(root: Path) -> dict:
    """Create the 5-source pathological tree.  Returns expectations dict."""
    exp: dict[str, Any] = {}
    sante = root / "src_sante"
    orth = root / "src_orthanc"
    vx = root / "src_vxvue"
    per = root / "src_perlove"
    fuji = root / "src_fuji"

    # -- sante_ct: clean CT study (2 instances) + dup-same + dup-diff + caretless
    for i in (1, 2):
        _write(_mk_ds(CT_SOP, f"1.2.3.100.{i}", "1.2.3.100", "1.2.3.100.9", "CT",
                      "CT001", "Latin^Patient"), sante / "ct1" / f"i{i}.dcm")
    dup_same = _mk_ds(CT_SOP, "1.2.3.200.1", "1.2.3.200", "1.2.3.200.9", "CT",
                      "CT002", "Dup^Same")
    _write(dup_same, sante / "ct2" / "a.dcm")
    caret = _mk_ds(CT_SOP, "1.2.3.300.1", "1.2.3.300", "1.2.3.300.9", "CT",
                   "CT003", "Ivanov Ivan")
    _write(caret, sante / "ct3" / "c.dcm")
    exp["caret_sop"] = "1.2.3.300.1"
    # a DICOMDIR index (Part-10, but no SOPInstanceUID by design) must be skipped
    dcmdir = Dataset()
    dcmdir.FileSetID = "FILESET"
    dcmdir.DirectoryRecordSequence = []
    dfm = FileMetaDataset()
    dfm.MediaStorageSOPClassUID = UID(DICOMDIR_SOP)
    dfm.MediaStorageSOPInstanceUID = UID("1.2.3.888.1")
    dfm.TransferSyntaxUID = ExplicitVRLittleEndian
    dcmdir.file_meta = dfm
    _write(dcmdir, sante / "ct1" / "DICOMDIR")
    # a Presentation State: valid DICOM, no PixelData by design — MUST be sent,
    # never flagged pixelless (regression guard for the SOP-class gating)
    pr = Dataset()
    pr.SOPClassUID = "1.2.840.10008.5.1.4.1.1.11.1"  # Grayscale Softcopy PS
    pr.SOPInstanceUID = "1.2.3.950.1"
    pr.StudyInstanceUID = "1.2.3.950"
    pr.SeriesInstanceUID = "1.2.3.950.9"
    pr.Modality = "PR"
    pr.PatientID = "PR001"
    pr.PatientName = "Presentation^State"
    pr.PatientBirthDate = "19800101"
    pr.PatientSex = "O"
    pr.StudyDate = "20200115"
    pr.ContentLabel = "ANNOTATION"
    pfm = FileMetaDataset()
    pfm.MediaStorageSOPClassUID = UID("1.2.840.10008.5.1.4.1.1.11.1")
    pfm.MediaStorageSOPInstanceUID = UID("1.2.3.950.1")
    pfm.TransferSyntaxUID = ExplicitVRLittleEndian
    pr.file_meta = pfm
    _write(pr, sante / "ps" / "pr.dcm")
    exp["pr_sop"] = "1.2.3.950.1"

    # -- orthanc blob: 2-hex fan-out with part10, bare, zlib, json + junk decoys
    p10 = _mk_ds(CR_SOP, "1.2.3.400.1", "1.2.3.400", "1.2.3.400.9", "CR",
                 "XR001", _lat("Сидоренко^Олена", "cp1251"), charset="ISO_IR 100")
    b = _part10_bytes(p10)
    (orth / "aa" / "bb").mkdir(parents=True, exist_ok=True)
    (orth / "aa" / "bb" / "11111111-2222-3333-4444-555555555501").write_bytes(b)
    exp["mojibake_sop"] = "1.2.3.400.1"
    bare = _strip_to_bare(_part10_bytes(_mk_ds(
        CR_SOP, "1.2.3.400.2", "1.2.3.400", "1.2.3.400.9", "CR",
        "XR001", _lat("Сидоренко^Олена", "cp1251"), charset="ISO_IR 100")))
    (orth / "aa" / "bb" / "11111111-2222-3333-4444-555555555502").write_bytes(bare)
    exp["bare_sop"] = "1.2.3.400.2"
    z = _part10_bytes(_mk_ds(CR_SOP, "1.2.3.400.3", "1.2.3.400", "1.2.3.400.9", "CR",
                             "XR001", _lat("Сидоренко^Олена", "cp1251"), charset="ISO_IR 100"))
    (orth / "cc" / "dd").mkdir(parents=True, exist_ok=True)
    (orth / "cc" / "dd" / "11111111-2222-3333-4444-555555555503").write_bytes(
        len(z).to_bytes(8, "little") + zlib.compress(z))
    exp["zlib_sop"] = "1.2.3.400.3"
    (orth / "cc" / "dd" / "11111111-2222-3333-4444-555555555504").write_bytes(
        b'{"ID": "orthanc-json-summary", "Type": "dicom-as-json"}')
    (orth / "cc" / "dd" / "11111111-2222-3333-4444-555555555505").write_bytes(
        b"\xde\xad\xbe\xef" * 64)
    dup_same2 = _mk_ds(CT_SOP, "1.2.3.200.1", "1.2.3.200", "1.2.3.200.9", "CT",
                       "CT002", "Dup^Same")
    (orth / "ee" / "ff").mkdir(parents=True, exist_ok=True)
    (orth / "ee" / "ff" / "11111111-2222-3333-4444-555555555506").write_bytes(
        _part10_bytes(dup_same2))
    exp["dup_same_sop"] = "1.2.3.200.1"

    # -- vxvue: pid rule + dup-diff-content + invalid/missing UIDs
    vxd = _mk_ds(CR_SOP, "1.2.3.500.1", "1.2.3.500", "1.2.3.500.9", "DX",
                 "VX-00123", "Vieworks^Test")
    _write(vxd, vx / "v1.dcm")
    exp["vx_sop"] = "1.2.3.500.1"
    dup_diff_a = _mk_ds(CR_SOP, "1.2.3.600.1", "1.2.3.600", "1.2.3.600.9", "DX",
                        "VX-00124", "Dup^Diff")
    _write(dup_diff_a, vx / "v2.dcm")
    dup_diff_b = _mk_ds(CR_SOP, "1.2.3.600.1", "1.2.3.600", "1.2.3.600.9", "DX",
                        "VX-00124", "Dup^Diff")
    dup_diff_b.PixelData = bytes(reversed(range(64)))
    _write(dup_diff_b, vx / "v2_copy.dcm")
    exp["dup_diff_sop"] = "1.2.3.600.1"
    # same UID, different bytes, NO pixels (PR): the no-pixel-iod-diff collision path
    for label, fname in (("STATE_A", "pr_a.dcm"), ("STATE_B", "pr_b.dcm")):
        prd = _mk_ds("1.2.840.10008.5.1.4.1.1.11.1", "1.2.3.960.1", "1.2.3.960",
                     "1.2.3.960.9", "PR", "VX-00960", "Dup^State")
        del prd.PixelData
        prd.ContentLabel = label
        _write(prd, vx / fname)
    exp["dup_nopix_sop"] = "1.2.3.960.1"
    badu = _mk_ds(CR_SOP, "1.2.3.700.01", "1.2.3.700", "1.2.3.700.9", "DX",
                  "VX-00125", "Bad^Uid")  # leading zero component = invalid
    _write(badu, vx / "v3.dcm")
    exp["bad_uid_old"] = "1.2.3.700.01"
    nostudy = _mk_ds(CR_SOP, "1.2.3.800.1", None, "1.2.3.800.9", "DX",
                     "VX-00126", "No^Study")
    _write(nostudy, vx / "v4.dcm")
    exp["nostudy_sop"] = "1.2.3.800.1"
    (vx / "empty.dcm").write_bytes(b"")
    (vx / "trunc.dcm").write_bytes(_part10_bytes(vxd)[:200])

    # -- perlove: GB18030 text + decimal IS values, implicit-VR variant too
    gbname = _lat("Петренко^Тарас", "gb18030")
    pl = _mk_ds(CR_SOP, "1.2.3.900.1", "1.2.3.900", "1.2.3.900.9", "DX",
                "PL001", gbname, charset="ISO_IR 100")
    pl.ExposureTime = "40.000000"
    pl.XRayTubeCurrent = "160.000000"
    pl.Exposure = "6.300000"
    _write(pl, per / "p1.dcm")
    exp["perlove_sop"] = "1.2.3.900.1"
    exp["perlove_name"] = "Петренко^Тарас"

    # -- fuji: sites via institution + folder map; ordinals within month
    fj = [("1.2.4.10.1", "1.2.4.10", "20190312", "8K Hospital", "site_a"),
          ("1.2.4.20.1", "1.2.4.20", "20190301", "8K Hospital", "site_a"),
          ("1.2.4.30.1", "1.2.4.30", "20190405", "8K Hospital", "site_a"),
          ("1.2.4.40.1", "1.2.4.40", "20190310", "District Clinic", "site_b")]
    for sop, stu, date, inst, folder in fj:
        d = _mk_ds(CR_SOP, sop, stu, stu + ".9", "CR", "42", "Fuji Patient",
                   study_date=date, institution=inst)
        _write(d, fuji / folder / f"{sop}.dcm")
    exp["fuji"] = {"1.2.4.20.1": "8K319001", "1.2.4.10.1": "8K319002",
                   "1.2.4.30.1": "8K419001", "1.2.4.40.1": "9K319001"}
    noflag = _mk_ds(CR_SOP, "1.2.4.50.1", "1.2.4.50", "1.2.4.50.9", "CR", "43",
                    "Fuji Nosite", study_date="20190320", institution="Plain")
    _write(noflag, fuji / "site_c" / "x.dcm")
    exp["fuji_nosite_study"] = "1.2.4.50"
    return exp


SELFTEST_TOML = """
[general]
db_path     = "{base}/state.db"
report_dir  = "{base}/reports"
sidecar_dir = "{base}/sidecars"
log_file    = ""
pause_file  = "{base}/PAUSE"
scan_backend = "threads"

[server]
host = "127.0.0.1"
dimse_timeout = 30
[server.destinations.ct]
port = {port}
called_aet = "LOOPBACK"
[server.destinations.xray]
port = {port}
called_aet = "LOOPBACK"
[server.destinations.other]
port = {port}
called_aet = "LOOPBACK"

[routing]
ct   = ["CT"]
xray = ["CR", "DX"]
precedence = ["ct", "xray", "other"]

[network]
max_associations = 2
rate_limit_mbit = 0
active_hours = ""
instances_per_association = 100

[[source]]
name = "sante_ct"
adapter = "filetree"
roots = ['{base}/tree/src_sante']
calling_aet = "MIG_SANTE"
priority = 10
caret_repair = true
max_readers = 2

[[source]]
name = "orthanc_blob"
adapter = "orthanc"
roots = ['{base}/tree/src_orthanc']
calling_aet = "MIG_ORTHANC"
priority = 20
charset_detect_order = ["utf-8", "cp1251", "latin-1"]
max_readers = 2

[[source]]
name = "vxvue"
adapter = "filetree"
roots = ['{base}/tree/src_vxvue']
calling_aet = "MIG_VXVUE"
priority = 30
caret_repair = true
pid_rules_mode = "optional"
max_readers = 2
[[source.patient_id_rule]]
id = "vx-strip"
match = '^VX[-_ ]?(?P<num>\\d+)$'
template = "VX{{num}}"

[[source]]
name = "perlove"
adapter = "filetree"
roots = ['{base}/tree/src_perlove']
calling_aet = "MIG_PERLOVE"
priority = 40
charset_policy = "keep-gb"
charset_source = "gb18030"
is_fix_tags = ["00181150", "00181151", "00181152"]
max_readers = 2

[[source]]
name = "fuji_cr"
adapter = "filetree"
roots = ['{base}/tree/src_fuji']
calling_aet = "MIG_FUJI"
priority = 50
pid_rules_mode = "required"
max_readers = 2
[source.fuji]
enabled = true
sites = ["8K", "9K"]
fallback_site = "XK"
[source.fuji.folder_site_map]
'site_b' = "9K"
"""


class _Args:
    """argparse.Namespace stand-in for driving commands programmatically."""

    def __init__(self, **kw):
        defaults = dict(source=None, rescan="new", limit=0, force_threads=True,
                        study_uid=None, dry_run=False, dry_run_dir=None,
                        ignore_window=True, single_aet=None, ack=None, note=None,
                        arm=False, problem=None, file_id=None, path_glob=None,
                        resolve_dup_priority=False, fallback_ledger=False,
                        host=None, port=None, called_aet=None, calling_aet=None)
        defaults.update(kw)
        self.__dict__.update(defaults)


def _selftest_units(check) -> None:
    check(sniff_kind(b"{" + b" " * 500, 501) == K_ORTHANC_JSON, "sniff json")
    z = zlib.compress(b"x" * 100)
    check(sniff_kind(len(b"x" * 100).to_bytes(8, "little") + z, 108 + 0) in
          (K_ZLIB_PART10,), "sniff zlib")
    check(repair_caret("Ivanov Ivan")[0] == "Ivanov^Ivan", "caret 2 tokens")
    check(repair_caret("Ivanov^Ivan")[1] is False, "caret already ok")
    check(repair_caret("X 1 2 3 4 5")[1] is False, "caret too many tokens")
    codec, _ = detect_codec("Іваненко Ґудзь".encode("cp1251"), ["utf-8", "cp1251", "latin-1"])
    check(codec == "cp1251", f"detect cp1251 (got {codec})")
    codec, _ = detect_codec("Петренко".encode("utf-8"), ["utf-8", "cp1251", "latin-1"])
    check(codec == "utf-8", f"detect utf-8 (got {codec})")
    check(fuji_pid("8K", "20190301", 1) == "8K319001", "fuji pid format")
    check(fuji_pid("9K", "20191115", 23) == "9K1119023", "fuji pid Nov")
    check(fuji_pid("XK", "20190301", 1, two_digit_month=True) == "XK0319001",
          "fuji pid 2-digit month")
    check(fuji_pid("XK", "20211201", 1, two_digit_month=True)
          != fuji_pid("XK", "20220112", 1001, two_digit_month=True),
          "fuji pid ambiguity resolved (Dec-21#1 vs Jan-22#1001)")
    src = SourceCfg(name="t", roots=["x"], patient_id_rules=[
        PidRule(id="r", match=r"^VX-(?P<n>\d+)$", template="VX{n}")])
    check(apply_pid_rules(src, "VX-007", "", "") == ("VX007", "r"), "pid rule")
    check(apply_pid_rules(src, "XX-007", "", "")[0] is None, "pid rule no match")
    check(valid_uid("1.2.840.10008.1.1") and not valid_uid("1.2.03") and not valid_uid("1." + "9" * 70),
          "uid validity")
    w = ActiveWindow("20:00-07:00", ["mon", "tue", "wed", "thu", "fri", "sat", "sun"])
    check(w.is_active(_dt.datetime(2026, 7, 3, 23, 0)) and
          not w.is_active(_dt.datetime(2026, 7, 3, 12, 0)), "active window")
    check(translit_cyrillic("Щукін") == "Shchukin", "translit")
    check(placeholder_pid("City Hospital #1", "20240115", "093012.5") == "City_Hospital_1-20240115-093012-Missing",
          f"placeholder pid (got {placeholder_pid('City Hospital #1', '20240115', '093012.5')})")
    check(placeholder_pid("", "", "") == "UNKNOWN-00000000-000000-Missing", "placeholder pid empty")
    check(sop_needs_pixels("1.2.840.10008.5.1.4.1.1.2") and
          not sop_needs_pixels("1.2.840.10008.5.1.4.1.1.11.1") and
          not sop_needs_pixels("1.2.840.10008.5.1.4.1.1.88.11"), "sop_needs_pixels")


def cmd_selftest(_cfg, args) -> int:
    import tempfile
    failures: list[str] = []

    def check(cond, msg):
        if cond:
            log.debug("PASS %s", msg)
        else:
            failures.append(msg)
            log.error("FAIL %s", msg)

    _selftest_units(check)

    base = Path(tempfile.mkdtemp(prefix="dcm_migrate_selftest_"))
    log.info("selftest workspace: %s", base)
    try:
        exp = build_selftest_tree(base / "tree")
        scp = LoopbackSCP(store_dir=str(base / "received"))
        scp.start()
        cfg_path = base / "migration.toml"
        cfg_path.write_text(SELFTEST_TOML.format(base=str(base).replace("\\", "/"),
                                                 port=scp.port), encoding="utf-8")
        cfg = load_config(str(cfg_path))

        cmd_scan(cfg, _Args())
        conn = db_connect(cfg.general.db_path)
        n_inst = conn.execute("SELECT COUNT(*) FROM instances").fetchone()[0]
        check(n_inst == 22, f"scan found 22 instances (got {n_inst})")
        n_nd = conn.execute("SELECT COUNT(*) FROM files WHERE scan_status=?",
                            (FS_NONDICOM,)).fetchone()[0]
        check(n_nd == 3, f"3 non-dicom (2 decoys + 1 DICOMDIR) (got {n_nd})")
        n_dcmdir = conn.execute("SELECT COUNT(*) FROM files WHERE kind=?",
                                (K_DICOMDIR,)).fetchone()[0]
        check(n_dcmdir == 1, f"DICOMDIR skipped not errored (got {n_dcmdir})")
        n_err = conn.execute("SELECT COUNT(*) FROM files WHERE scan_status=?",
                             (FS_ERROR,)).fetchone()[0]
        check(n_err == 2, f"2 unreadable files (got {n_err})")
        kinds = {r[0] for r in conn.execute("SELECT DISTINCT kind FROM files WHERE scan_status=1")}
        check(K_PREAMBLELESS in kinds and K_ZLIB_PART10 in kinds,
              f"preambleless+zlib kinds detected (got {sorted(kinds)})")
        conn.close()

        cmd_analyze(cfg, _Args())
        conn = db_connect(cfg.general.db_path)
        probs = {r[0]: r[1] for r in conn.execute(
            "SELECT code, COUNT(*) FROM problems WHERE resolved=0 GROUP BY code")}
        check(probs.get(H_DUP_CONFLICT) == 2, f"2 dup conflicts (got {probs.get(H_DUP_CONFLICT)})")
        kinds_dup = {json.loads(r[0]).get("kind") for r in conn.execute(
            "SELECT detail FROM problems WHERE code=? AND resolved=0", (H_DUP_CONFLICT,))}
        check(kinds_dup == {"pixel-diff", "no-pixel-iod-diff"},
              f"dup-conflict kinds classified (got {kinds_dup})")
        check(probs.get(H_PID_RULE_MISS, 0) == 0,
              f"fuji fallback XK cleared site misses (got {probs.get(H_PID_RULE_MISS)})")
        check(probs.get(H_TRUNCATED, 0) + probs.get(H_UNREADABLE, 0) == 2,
              "2 hard unreadable/truncated")
        check(probs.get(H_PIXELLESS_ONLY, 0) == 0,
              f"Presentation State not flagged pixelless (got {probs.get(H_PIXELLESS_ONLY, 0)})")
        dup_skipped = conn.execute(
            "SELECT COUNT(*) FROM instances WHERE sop_uid=? AND send_status=?",
            (exp["dup_same_sop"], S_SKIPPED_DUP)).fetchone()[0]
        check(dup_skipped == 1, f"same-content dup deduped (got {dup_skipped})")
        fuji_map = {r[0]: r[1] for r in conn.execute(
            "SELECT s.study_uid, s.pid_new FROM studies s WHERE s.pid_new IS NOT NULL "
            "AND s.source='fuji_cr'")}
        for stu_sop, want in exp["fuji"].items():
            got = fuji_map.get(stu_sop.rsplit(".", 1)[0])
            check(got == want, f"fuji pid {stu_sop.rsplit('.', 1)[0]} -> {want} (got {got})")
        check(fuji_map.get(exp["fuji_nosite_study"]) == "XK0319001",
              f"fuji fallback XK id (got {fuji_map.get(exp['fuji_nosite_study'])})")
        vx_new = conn.execute("SELECT pid_new FROM studies WHERE study_uid='1.2.3.500'").fetchone()[0]
        check(vx_new == "VX00123", f"vxvue pid rule (got {vx_new})")
        det = conn.execute("SELECT detected_codec FROM instances WHERE sop_uid=?",
                           (exp["mojibake_sop"],)).fetchone()[0]
        check(det == "cp1251", f"mojibake detected as cp1251 (got {det})")
        conn.close()

        cmd_exclude(cfg, _Args(resolve_dup_priority=True))
        cmd_exclude(cfg, _Args(problem=H_PID_RULE_MISS))
        cmd_exclude(cfg, _Args(problem=H_TRUNCATED))
        cmd_exclude(cfg, _Args(problem=H_UNREADABLE))
        cmd_exclude(cfg, _Args(path_glob="*.no-such-suffix"))  # exercise, matches nothing
        conn = db_connect(cfg.general.db_path)
        ok, issues = gate_status(conn, cfg)
        warn_codes = [i.split()[1] for i in issues if i.startswith("warning ")]
        conn.close()
        cmd_approve(cfg, _Args(ack=warn_codes))
        rc = cmd_approve(cfg, _Args(arm=True))
        check(rc == 0, "gate armed")

        rc = cmd_send(cfg, _Args())
        check(rc == 0, f"send rc==0 (got {rc})")
        conn = db_connect(cfg.general.db_path)
        n_fail = conn.execute("SELECT COUNT(*) FROM instances WHERE send_status=?",
                              (S_FAILED_PERM,)).fetchone()[0]
        check(n_fail == 0, f"no permanent failures (got {n_fail})")
        n_sent = conn.execute("SELECT COUNT(*) FROM instances WHERE send_status IN (?,?)",
                              (S_SENT, S_SENT_WARN)).fetchone()[0]
        check(n_sent == 19, f"19 instances sent (got {n_sent})")
        conn.close()

        # ---- inspect received objects ----
        rx = {p.stem: p for p in Path(scp.store_dir).glob("*.dcm")}
        check(exp["dup_same_sop"] in rx and
              len([s for s in scp.stored_sops if s == exp["dup_same_sop"]]) == 1,
              "dup sent exactly once")

        d = dcmread(str(rx[exp["mojibake_sop"]]))
        check(str(d.SpecificCharacterSet) == "ISO_IR 192", "mojibake -> ISO_IR 192")
        check(str(d.PatientName) == "Сидоренко^Олена",
              f"mojibake name fixed (got {d.PatientName!r})")
        check("OriginalAttributesSequence" in d, "OAS present on modified object")
        if "OriginalAttributesSequence" in d:
            item = d.OriginalAttributesSequence[-1]
            check(str(item.get("ReasonForTheAttributeModification", "")) == "COERCE", "OAS reason")

        d = dcmread(str(rx[exp["perlove_sop"]]))
        check(str(d.SpecificCharacterSet) == "GB18030", "perlove keeps GB18030 declaration")
        check(str(d.PatientName) == exp["perlove_name"],
              f"perlove GB name intact (got {str(d.PatientName)!r})")
        check(str(d.ExposureTime) == "40" and str(d.Exposure) == "6",
              f"IS decimals rounded (got {d.ExposureTime!r}, {d.Exposure!r})")

        d = dcmread(str(rx[exp["caret_sop"]]))
        check(str(d.PatientName) == "Ivanov^Ivan", f"caret repaired (got {d.PatientName!r})")

        fuji_rx = {}
        for sop, want_pid in exp["fuji"].items():
            if sop in rx:
                fuji_rx[sop] = str(dcmread(str(rx[sop])).PatientID)
        check(fuji_rx == exp["fuji"], f"fuji PIDs on the wire (got {fuji_rx})")

        vx = dcmread(str(rx[exp["vx_sop"]]))
        check(str(vx.PatientID) == "VX00123", f"vxvue PID on the wire (got {vx.PatientID!r})")

        check(exp["bad_uid_old"] not in rx, "invalid SOP UID was regenerated")
        check(exp["nostudy_sop"] in rx and
              valid_uid(str(dcmread(str(rx[exp["nostudy_sop"]])).StudyInstanceUID)),
              "missing StudyInstanceUID synthesized")
        check(exp["bare_sop"] in rx and exp["zlib_sop"] in rx,
              "preambleless + zlib blobs delivered")
        check(exp["pr_sop"] in rx, "Presentation State (no pixels) was sent, not blocked")
        check(len([s for s in scp.stored_sops if s == exp["dup_nopix_sop"]]) == 1,
              "no-pixel dup collision resolved to exactly one sent copy")

        rc = cmd_verify(cfg, _Args())
        check(rc == 0, "verify all MATCH")
        scp.stop()
    finally:
        try:
            shutil.rmtree(base, ignore_errors=True)
        except Exception:
            pass

    if failures:
        log.error("SELFTEST FAILED: %d failure(s)", len(failures))
        for f_ in failures:
            log.error("  - %s", f_)
        return 1
    log.info("SELFTEST PASSED")
    return 0

# --------------------------------------------------------------------------
# SECTION 17 — CLI / MAIN
# --------------------------------------------------------------------------

def cmd_init(args) -> int:
    path = Path(args.path)
    if path.exists() and not args.force:
        raise SystemExit(f"{path} already exists (use --force to overwrite)")
    path.write_text(CONFIG_TEMPLATE, encoding="utf-8")
    print(f"template written to {path}")
    print("fill every REPLACE_ME, then: scan -> analyze -> (exclude/approve) -> echo -> send -> verify")
    return 0


def cmd_report(cfg: Config, args) -> int:
    conn = db_connect(cfg.general.db_path)
    run = int(meta_get(conn, "analyze_run", "0"))
    if run == 0:
        raise SystemExit("no analyze run yet")
    generate_reports(conn, cfg, run)
    conn.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog=PROG,
        description="Gated, resumable DICOM archive migration to a DIMSE PACS (see module docstring).")
    p.add_argument("--config", default="migration.toml", help="config file (default: ./migration.toml)")
    p.add_argument("--db", default=None, help="override [general].db_path")
    p.add_argument("-v", action="count", default=0, dest="verbose", help="debug logging")
    p.add_argument("-q", action="count", default=0, dest="quiet", help="warnings only")
    p.add_argument("--log-file", default=None, help="override [general].log_file")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("init", help="write a commented migration.toml template")
    sp.add_argument("--path", default="migration.toml")
    sp.add_argument("--force", action="store_true")

    sp = sub.add_parser("scan", help="inventory sources (header-only reads; incremental)")
    sp.add_argument("--source", nargs="*", help="limit to these source names")
    sp.add_argument("--rescan", choices=["new", "errors", "reclassify", "full"], default="new",
                    help="new: skip unchanged files (default); errors: also retry error files; "
                         "reclassify: also re-read files previously classified non-DICOM "
                         "(after a sniffer fix); full: re-read everything")
    sp.add_argument("--limit", type=int, default=0, help="stop each source after N files (pilot)")
    sp.add_argument("--force-threads", action="store_true", help=argparse.SUPPRESS)

    sub.add_parser("analyze", help="dedupe, classify problems, assign IDs, write reports")
    sub.add_parser("report", help="re-emit reports from the last analyze")

    sp = sub.add_parser("exclude", help="exclude files/studies or resolve problem classes")
    sp.add_argument("--problem", help="resolve+exclude everything flagged with this code")
    sp.add_argument("--study-uid", help="exclude one study")
    sp.add_argument("--path-glob", help="exclude files whose path matches this glob "
                                        "(e.g. \"*.tmp\"); case-insensitive, matched against "
                                        "the full path and the basename")
    sp.add_argument("--file-id", type=int, help="exclude one file by id (see problems.csv)")
    sp.add_argument("--resolve-dup-priority", action="store_true",
                    help="resolve every H-DUP-CONFLICT by source priority")

    sp = sub.add_parser("approve", help="acknowledge warning classes / arm the send gate")
    sp.add_argument("--ack", nargs="*", help="warning code(s) to acknowledge")
    sp.add_argument("--note", default="", help="free-text note stored with the ack")
    sp.add_argument("--arm", action="store_true", help="arm the gate (requires all checks green)")

    sp = sub.add_parser("echo", help="C-ECHO every destination with every calling AET")
    sp.add_argument("--host", default=None)
    sp.add_argument("--calling-aet", default=None)

    sp = sub.add_parser("send", help="transform in memory and C-STORE (requires armed gate)")
    sp.add_argument("--source", nargs="*", help="limit to these source names")
    sp.add_argument("--study-uid", help="send a single study")
    sp.add_argument("--limit", type=int, default=0, help="stop after N instances this run")
    sp.add_argument("--dry-run", action="store_true",
                    help="send to an in-process loopback SCP instead of the real server")
    sp.add_argument("--dry-run-dir", default=None, help="where the loopback SCP stores objects")
    sp.add_argument("--ignore-window", action="store_true", help="ignore active_hours for this run")
    sp.add_argument("--single-aet", default=None,
                    help="override all per-source calling AETs with one AE title")

    sp = sub.add_parser("verify", help="C-FIND per study: expected vs found instance counts")
    sp.add_argument("--fallback-ledger", action="store_true", help="no network; ledger summary only")
    sp.add_argument("--host", default=None)
    sp.add_argument("--port", type=int, default=None)
    sp.add_argument("--called-aet", default=None)
    sp.add_argument("--calling-aet", default=None)

    sp = sub.add_parser("probe", help="forensically inspect files classified non-DICOM/error")
    sp.add_argument("--source", help="sample from this source's recorded non-DICOM/error files")
    sp.add_argument("--sample", type=int, default=15, help="how many random files to inspect")
    sp.add_argument("--status", choices=["nondicom", "errors", "all"], default="all")
    sp.add_argument("--path", nargs="*", help="probe specific file paths instead of sampling")

    sp = sub.add_parser("dup-audit",
                        help="characterize H-DUP-CONFLICT groups (cross-root / TS / "
                             "pixel-identity) to choose a resolution policy")
    sp.add_argument("--sample", type=int, default=300, help="how many conflict groups to sample")

    sp = sub.add_parser("xcheck-orthanc",
                        help="verify every instance referenced by a metadata store "
                             "(Orthanc gzip OR DICOMweb-JSON) exists in the inventory")
    sp.add_argument("--source", nargs="*", help="sources holding attachment stores "
                                                "(default: all)")
    sp.add_argument("--dir", default=None,
                    help="cross-check a raw directory tree of metadata files "
                         "(e.g. a metadata-only store not configured as a source)")

    sub.add_parser("status", help="one-screen progress summary")
    sub.add_parser("export-mapping", help="CSV exports of ID/UID mappings")
    sub.add_parser("selftest", help="synthetic end-to-end test against a loopback SCP")

    sp = sub.add_parser("scp", help="standalone loopback storage SCP (rehearsal receiver)")
    sp.add_argument("--dir", required=True, help="directory to store received objects")
    sp.add_argument("--port", type=int, default=11119)
    return p


_NEEDS_CONFIG = {"scan", "analyze", "report", "exclude", "approve", "echo",
                 "send", "verify", "status", "export-mapping", "probe", "xcheck-orthanc",
                 "dup-audit"}


def main(argv: list[str] | None = None) -> int:
    mp.freeze_support()
    args = build_parser().parse_args(argv)
    cfg: Config | None = None
    log_file = args.log_file
    if args.cmd in _NEEDS_CONFIG:
        cfg = load_config(args.config)
        if args.db:
            cfg.general.db_path = args.db
        log_file = log_file if log_file is not None else cfg.general.log_file
    setup_logging(log_file, args.verbose - args.quiet)
    log.debug("%s %s (python %s, GIL %s)", PROG, VERSION,
              sys.version.split()[0], "on" if GIL_ENABLED else "off")
    dispatch: dict[str, Callable[[], int]] = {
        "init": lambda: cmd_init(args),
        "scan": lambda: cmd_scan(cfg, args),
        "analyze": lambda: cmd_analyze(cfg, args),
        "report": lambda: cmd_report(cfg, args),
        "exclude": lambda: cmd_exclude(cfg, args),
        "approve": lambda: cmd_approve(cfg, args),
        "echo": lambda: cmd_echo(cfg, args),
        "send": lambda: cmd_send(cfg, args),
        "verify": lambda: cmd_verify(cfg, args),
        "status": lambda: cmd_status(cfg, args),
        "export-mapping": lambda: cmd_export_mapping(cfg, args),
        "probe": lambda: cmd_probe(cfg, args),
        "dup-audit": lambda: cmd_dup_audit(cfg, args),
        "xcheck-orthanc": lambda: cmd_xcheck(cfg, args),
        "selftest": lambda: cmd_selftest(cfg, args),
        "scp": lambda: cmd_scp(cfg, args),
    }
    return dispatch[args.cmd]()


if __name__ == "__main__":
    sys.exit(main())
