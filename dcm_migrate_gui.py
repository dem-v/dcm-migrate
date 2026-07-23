# /// script
# requires-python = ">=3.11"
# dependencies = ["tomlkit>=0.13"]
# ///
"""dcm_migrate control panel — native Windows UI (tkinter).

A thin cockpit over dcm_migrate.py: it never reimplements engine logic.
Status comes from read-only queries against the state database; every action
(scan / analyze / approve / send / verify / ...) runs the CLI as a child
process with its output streamed into the console pane; pausing the senders
is the engine's own PAUSE-file mechanism.

Usage:
    py dcm_migrate_gui.py [migration.toml]

The engine (dcm_migrate.py) is expected next to this file; children are
launched with `uv run` when uv is available (PEP 723 deps resolve
automatically), else with this interpreter.
"""
from __future__ import annotations

import copy
import json
import os
import queue
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
from pathlib import Path

import tkinter as tk
from tkinter import messagebox, simpledialog, ttk

try:                       # comment-preserving TOML round-tripper (pure Python)
    import tomlkit
except ImportError:        # graceful: the structured editor degrades if it's absent
    tomlkit = None

# Mirrors of engine constants (dcm_migrate.py SECTION 2) — kept tiny on purpose.
S_PENDING, S_SENT, S_SENT_WARN, S_FAILED_PERM, S_FAILED_RETRY, \
    S_SKIPPED_DUP, S_EXCLUDED, S_SKIPPED_EXISTS = range(8)
SEV_NAMES = {0: "info", 1: "WARN", 2: "HARD"}
VERIFY_NAMES = {1: "match", 2: "MISMATCH", 3: "unavailable"}

ACTIONS = ["scan", "analyze", "report", "echo", "send", "verify",
           "status", "dup-audit", "export-mapping"]

# hover help for the action buttons (first-timer orientation)
ACTION_TIP = {
    "scan": "Inventory the source archives (header-only, incremental, resumable). "
            "Safe and read-only. Start here.",
    "analyze": "Classify problems, dedupe, assign IDs and write reports. Re-running "
               "analyze DISARMS the gate until re-approved.",
    "report": "Re-emit the HTML/CSV reports from the last analyze (no re-analysis).",
    "echo": "C-ECHO every destination with every calling AET — a quick connectivity test.",
    "send": "Transform in memory and C-STORE to the PACS. Requires an ARMED gate. "
            "IRREVERSIBLE — study transmissions cannot be recalled.",
    "verify": "C-FIND per study: compare expected (sent) vs found-on-server counts. "
              "Read-only; runs fine alongside a send.",
    "status": "One-screen progress summary (per-source counts).",
    "dup-audit": "Review duplicate groups found by analyze.",
    "export-mapping": "Write CSVs of the old->new PatientID / UID mappings.",
}

# plain-English explanation per problem code, shown when a problem row is selected
PROBLEM_HELP = {
    "H-UNREADABLE": "File could not be parsed as DICOM. Exclude it (or fix the source).",
    "H-TRUNCATED": "File ends mid-data (partial/corrupt copy). Exclude, or rescan if the "
                   "source was still being written.",
    "H-DUP-CONFLICT": "Same SOPInstanceUID but different pixels across files. Choose a "
                      "diff_content_policy (block / regenerate-uid / keep-priority).",
    "H-PID-RULE-MISS": "A source with required ID rules had a study no rule matched. Add a "
                       "rule or set a fallback (e.g. id_generator.fallback_site).",
    "H-PID-COLLISION": "Two studies were assigned the SAME generated PatientID. Use a "
                       "fixed-width id_date_format (mmyy/yymm) to avoid ambiguity.",
    "H-PIXELLESS-ONLY": "A study has only pixel-less objects (no images). Review/exclude.",
    "H-NO-PATIENT-IDENTITY": "Study has neither PatientID nor PatientName. Set "
                             "no_identity_policy=placeholder, or exclude.",
    "H-IDGEN-NO-DATE": "A generated-ID study has no usable StudyDate for the ID. Enable "
                       "id_generator.date_from_mtime, or exclude.",
    "W-CHARSET-GUESSED": "Text encoding was detected (not declared) and repaired to UTF-8. "
                         "Spot-check charset_suspects.csv before acking.",
    "W-CHARSET-LOSSY": "Some characters could not be represented and were replaced.",
    "W-PN-CARET-REPAIRED": "A space-separated name was rewritten to Family^Given form.",
    "W-PN-UNPARSEABLE": "A PatientName looked malformed and was left as-is.",
    "W-UID-REGENERATED": "An invalid/missing UID was deterministically regenerated.",
    "W-STUDYUID-SYNTHESIZED": "A missing StudyInstanceUID was synthesized so siblings reunite.",
    "W-IS-ROUNDED": "A decimal in an Integer-String tag was rounded to an integer.",
    "W-DUP-CROSS-SOURCE": "The same instance exists in more than one source; the "
                          "highest-priority source wins.",
    "W-DUP-COLLISION-REGEN": "Same-UID/diff-pixels kept by giving losers fresh UIDs.",
    "W-DUP-COLLISION-DROPPED": "Same-UID/diff-pixels: losing copies discarded (logged).",
    "W-PID-REWRITE": "A PatientID was rewritten by a rule. Review patient_id_preview.csv.",
    "W-IDENTITY-PLACEHOLDER": "A no-identity study got a synthesized placeholder ID.",
    "W-IDGEN-MTIME-DATE": "Generated-ID date came from the file's mtime (no valid StudyDate).",
    "W-COMPANION-ROUTED": "A companion-only study (SR/PR/OT/…) adopted a sibling's archive.",
    "W-GB-DECLARATION-FIXED": "GB bytes kept; only the (0008,0005) charset declaration fixed.",
    "I-NON-DICOM": "Non-DICOM files found and skipped (informational).",
}

# guided workflow stages (order matters); state derived from the DB in refresh
STAGES = ["Config", "Scan", "Analyze", "Approve", "Send", "Verify"]


class Tooltip:
    """Lightweight hover tooltip (stdlib tkinter, no deps)."""
    def __init__(self, widget, text: str):
        self.widget, self.text, self.tip = widget, text, None
        widget.bind("<Enter>", self._show, add="+")
        widget.bind("<Leave>", self._hide, add="+")

    def _show(self, _e=None):
        if self.tip or not self.text:
            return
        x = self.widget.winfo_rootx() + 12
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 3
        self.tip = tk.Toplevel(self.widget)
        self.tip.wm_overrideredirect(True)
        self.tip.wm_geometry(f"+{x}+{y}")
        tk.Label(self.tip, text=self.text, bg="#ffffe0", fg="#222", relief="solid",
                 borderwidth=1, justify="left", wraplength=380, padx=6, pady=3).pack()

    def _hide(self, _e=None):
        if self.tip:
            self.tip.destroy(); self.tip = None
CONFIRM = {"send": "Start SENDING to the PACS?\n\nThe gate must be armed; "
                   "transmitted studies cannot be recalled.",
           "analyze": "Run analyze?\n\nA new analyze run re-derives decisions "
                      "and DISARMS the send gate until re-approved."}

# Concurrency model: status and verify are read-oriented and run in their OWN
# process slots, so they never block (and are never blocked by) the heavy
# mutating command in the "main" slot — a long verify C-FIND sweep can run
# while a send is in flight (the engine's WAL + retry-on-locked make that safe).
SLOTS = ("main", "status", "verify")
INDEP_SLOTS = {"status": "status", "verify": "verify"}


def slot_for(cmd: str) -> str:
    return INDEP_SLOTS.get(cmd, "main")


def engine_cmd(engine: Path, cfg: Path, cmd: str, extra: list[str]) -> list[str]:
    base = ["uv", "run", str(engine)] if shutil.which("uv") else [sys.executable, str(engine)]
    return base + ["--config", str(cfg), cmd] + extra


class App:
    def __init__(self, root: tk.Tk, cfg_path: Path, engine: Path):
        self.root, self.cfg_path, self.engine = root, cfg_path, engine
        self.jobs: dict[str, subprocess.Popen] = {}     # slot -> running child
        self.job_cmd: dict[str, str] = {}               # slot -> command name
        # child output: (slot, line) while running, (slot, None) with rc on exit
        self.outq: queue.Queue[tuple[str, str | None, int | None]] = queue.Queue()
        self.uiq: queue.Queue[dict] = queue.Queue()   # refresh results -> UI thread
        self.refreshing = False
        self.db_path = self.report_dir = self.pause_file = ""
        self.has_replace_me = False
        self.last_pending = 0                 # pending instances at last refresh (for ETA)
        self._eta_prev: tuple[float, int] | None = None
        self._schema: dict | None = None      # cached engine config schema
        self._load_cfg()

        root.title(f"dcm_migrate — {cfg_path}")
        root.geometry("1180x760")
        self._build()
        self.root.after(200, self._pump)
        self.refresh()

    # ---- config (only the few [general] paths; engine re-parses on every run)
    def _load_cfg(self) -> None:
        self.has_replace_me = False
        try:
            text = Path(self.cfg_path).read_text(encoding="utf-8", errors="replace")
            self.has_replace_me = "REPLACE_ME" in text
            d = tomllib.loads(text)
            g = d.get("general", {})
            self.db_path = g.get("db_path", "")
            self.report_dir = g.get("report_dir", "")
            self.pause_file = g.get("pause_file", "")
        except Exception as e:
            messagebox.showerror("config", f"cannot parse {self.cfg_path}:\n{e}")

    # ---- layout ------------------------------------------------------------
    def _build(self) -> None:
        self._build_menubar()
        top = ttk.Frame(self.root, padding=4); top.pack(fill="x")
        self.gate_lbl = tk.Label(top, text="gate: ?", font=("Segoe UI", 10, "bold"),
                                 padx=8, pady=2)
        self.gate_lbl.pack(side="left")
        pf = ttk.Button(top, text="✓ Preflight", command=lambda: self.run("doctor"))
        pf.pack(side="left", padx=(8, 2)); Tooltip(pf, "Run the doctor checklist: config, "
                "source roots, DB, disk and PACS connectivity — 'am I ready?'")
        rh = ttk.Button(top, text="Rehearsal", command=lambda: self.run("selftest"))
        rh.pack(side="left", padx=2); Tooltip(rh, "Run the built-in synthetic end-to-end "
                "test against an in-process SCP — proves the pipeline with no real PACS.")
        self.live_lbl = ttk.Label(top, text="")   # parsed `progress:` line of a running send
        self.live_lbl.pack(side="left", padx=12)
        self.refresh_lbl = ttk.Label(top, text="")
        self.refresh_lbl.pack(side="left", padx=8)
        for text, fn in [("Refresh", self.refresh),
                         ("Config editor", self.open_config_editor),
                         ("New workspace", self.new_workspace),
                         ("Edit raw TOML", lambda: os.startfile(self.cfg_path)),
                         ("Open report", self.open_report),
                         ("Open log dir", lambda: os.startfile(str(Path(self.db_path).parent)))]:
            ttk.Button(top, text=text, command=fn).pack(side="right", padx=2)

        # guided workflow stepper
        stepfr = ttk.Frame(self.root, padding=(6, 0)); stepfr.pack(fill="x")
        self.step_ui: dict[str, tk.Label] = {}
        for i, st in enumerate(STAGES):
            if i:
                tk.Label(stepfr, text="›", fg="#888").pack(side="left")
            lbl = tk.Label(stepfr, text=st, padx=8, pady=2)
            lbl.pack(side="left"); self.step_ui[st] = lbl
        self.step_hint = ttk.Label(stepfr, text="", foreground="#3366cc")
        self.step_hint.pack(side="left", padx=14)

        self.pause_btn = ttk.Button(top, command=self.toggle_pause)
        self.pause_btn.pack(side="right", padx=8)
        self._sync_pause_btn()

        pane = ttk.PanedWindow(self.root, orient="horizontal"); pane.pack(fill="both", expand=True)

        # left: status tables
        left = ttk.Frame(pane, padding=4); pane.add(left, weight=1)
        ttk.Label(left, text="Per-source instance states").pack(anchor="w")
        cols = ("pending", "sent", "warn", "failed", "skipped", "excluded")
        self.src_tree = ttk.Treeview(left, columns=cols, height=8)
        self.src_tree.heading("#0", text="source"); self.src_tree.column("#0", width=110)
        for c in cols:
            self.src_tree.heading(c, text=c); self.src_tree.column(c, width=85, anchor="e")
        self.src_tree.pack(fill="x", pady=(0, 2))
        self.empty_lbl = ttk.Label(left, text="", foreground="#3366cc", wraplength=360)
        self.empty_lbl.pack(anchor="w", pady=(0, 6))
        ttk.Label(left, text="Problems (current analyze run) — click a row for help").pack(anchor="w")
        self.prob_tree = ttk.Treeview(left, columns=("count", "acked"), height=9)
        self.prob_tree.heading("#0", text="severity / code"); self.prob_tree.column("#0", width=260)
        self.prob_tree.heading("count", text="count"); self.prob_tree.column("count", width=90, anchor="e")
        self.prob_tree.heading("acked", text="acked"); self.prob_tree.column("acked", width=60, anchor="center")
        self.prob_tree.pack(fill="both", expand=True)
        self.prob_tree.bind("<<TreeviewSelect>>", self._on_prob_select)
        self.prob_help = ttk.Label(left, text="", foreground="#555", wraplength=380, justify="left")
        self.prob_help.pack(anchor="w", pady=2)
        self.verify_lbl = ttk.Label(left, text=""); self.verify_lbl.pack(anchor="w", pady=4)

        # right: actions + console
        right = ttk.Frame(pane, padding=4); pane.add(right, weight=3)
        row = ttk.Frame(right); row.pack(fill="x")
        for cmd in ACTIONS:
            b = ttk.Button(row, text=cmd, width=9, command=lambda c=cmd: self.run(c))
            b.pack(side="left", padx=1, pady=1)
            if cmd in ACTION_TIP:
                Tooltip(b, ACTION_TIP[cmd])
        row2 = ttk.Frame(right); row2.pack(fill="x", pady=2)
        ttk.Button(row2, text="Ack warnings…", command=self.ack_dialog).pack(side="left", padx=1)
        ttk.Button(row2, text="ARM GATE", command=self.arm).pack(side="left", padx=6)
        ttk.Label(row2, text="extra args:").pack(side="left", padx=(16, 2))
        self.extra = ttk.Entry(row2, width=40); self.extra.pack(side="left")

        # DB maintenance (all run in the 'main' slot — they mutate the DB but are
        # safe alongside a send thanks to WAL; ANALYZE is the fix for a slow verify)
        maint = ttk.LabelFrame(right, text="database maintenance", padding=2)
        maint.pack(fill="x", pady=(2, 2))
        for text, extra in [("Refresh stats (ANALYZE)", ["--analyze"]),
                            ("Rebuild indexes", ["--indexes"]),
                            ("Checkpoint WAL", ["--checkpoint"]),
                            ("Full maintain", [])]:
            ttk.Button(maint, text=text,
                       command=lambda e=extra: self.run("maintain", e)).pack(side="left", padx=2, pady=1)
        ttk.Label(maint, text="(run when verify/status feel slow on a large DB)").pack(side="left", padx=8)

        # one row per concurrent process slot: live state + its own Stop button
        slots = ttk.LabelFrame(right, text="running instances (status & verify run independently of the main action)",
                               padding=2)
        slots.pack(fill="x", pady=(2, 2))
        self.slot_ui: dict[str, dict] = {}
        for slot in SLOTS:
            fr = ttk.Frame(slots); fr.pack(side="left", padx=10, pady=1)
            lbl = ttk.Label(fr, text=f"{slot}: idle", width=30, anchor="w")
            lbl.pack(side="left")
            btn = ttk.Button(fr, text="Stop", width=5, state="disabled",
                             command=lambda s=slot: self.stop_slot(s))
            btn.pack(side="left")
            self.slot_ui[slot] = {"lbl": lbl, "btn": btn}

        cbar = ttk.Frame(right); cbar.pack(fill="x")
        ttk.Label(cbar, text="console output").pack(side="left")
        self.filter_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(cbar, text="errors/warnings only", variable=self.filter_var).pack(side="left", padx=10)
        ttk.Button(cbar, text="Clear", command=self._clear_console).pack(side="right")

        conf = ttk.Frame(right); conf.pack(fill="both", expand=True)
        self.console = tk.Text(conf, bg="#111318", fg="#d6d8dd", insertbackground="#d6d8dd",
                               font=("Consolas", 9), wrap="none", state="disabled")
        ys = ttk.Scrollbar(conf, command=self.console.yview)
        self.console.configure(yscrollcommand=ys.set)
        self.console.pack(side="left", fill="both", expand=True); ys.pack(side="right", fill="y")
        self.console.tag_configure("err", foreground="#ff6b6b")
        self.console.tag_configure("warn", foreground="#ffb454")
        self.console.tag_configure("ok", foreground="#7dd97b")

    def _clear_console(self) -> None:
        self.console.configure(state="normal")
        self.console.delete("1.0", "end")
        self.console.configure(state="disabled")

    def _build_menubar(self) -> None:
        mb = tk.Menu(self.root)
        helpm = tk.Menu(mb, tearoff=0)
        docs = self.engine.parent
        helpm.add_command(label="README", command=lambda: self._open_doc(docs / "README.md"))
        helpm.add_command(label="Config reference (CONFIG.md)",
                          command=lambda: self._open_doc(docs / "CONFIG.md"))
        helpm.add_separator()
        helpm.add_command(label="How the safety gate works", command=self._about_gate)
        helpm.add_command(label="First steps", command=self._about_firststeps)
        mb.add_cascade(label="Help", menu=helpm)
        try:
            self.root.config(menu=mb)
        except tk.TclError:
            pass

    def _open_doc(self, p: Path) -> None:
        if p.exists():
            os.startfile(str(p))
        else:
            messagebox.showinfo("doc", f"not found: {p}")

    def _about_gate(self) -> None:
        messagebox.showinfo("The safety gate",
            "Nothing is transmitted until the gate is ARMED.\n\n"
            "You arm it only after: (1) a full analyze has run, (2) every hard "
            "blocker (H-*) is resolved or excluded, and (3) every warning class "
            "(W-*) is acknowledged.\n\n"
            "Any edit to content config, or a re-analyze, automatically DISARMS "
            "the gate — so what you approved is exactly what gets sent. Tuning "
            "[network] (bandwidth, hours, retries) never disarms.")

    def _about_firststeps(self) -> None:
        messagebox.showinfo("First steps",
            "1. Config editor → fill every REPLACE_ME (host, ports/AETs, source roots).\n"
            "2. ✓ Preflight → confirm config, roots and PACS connectivity.\n"
            "3. Rehearsal → watch the synthetic end-to-end run (no real PACS).\n"
            "4. Scan → Analyze → open the report and review.\n"
            "5. Ack warnings, resolve blockers, ARM the gate.\n"
            "6. Send, then Verify.\n\n"
            "The stepper across the top always highlights your next action.")

    # ---- child processes (one per slot, running concurrently) ---------------
    def run(self, cmd: str, extra: list[str] | None = None) -> None:
        slot = slot_for(cmd)
        busy = self.jobs.get(slot)
        if busy and busy.poll() is None:
            messagebox.showwarning(
                "busy", f"the '{slot}' slot is already running '{self.job_cmd.get(slot, '?')}'"
                        f" — Stop it or wait for it to finish")
            return
        if cmd in CONFIRM and not messagebox.askyesno(cmd, CONFIRM[cmd]):
            return
        argv = engine_cmd(self.engine, self.cfg_path, cmd,
                          (extra if extra is not None else self.extra.get().split()))
        self._console(f"\n$ [{slot}] {' '.join(argv)}\n", "ok")
        flags = 0
        if os.name == "nt":
            flags = subprocess.CREATE_NEW_PROCESS_GROUP
            import ctypes
            if not ctypes.windll.kernel32.GetConsoleWindow():
                # launched via pythonw: suppress flashing child consoles
                # (CTRL_BREAK then can't be delivered; Stop falls back to terminate)
                flags |= subprocess.CREATE_NO_WINDOW
        # unbuffered child stdout: a PIPE is block-buffered (unlike a terminal),
        # which otherwise delays the first visible line by minutes on quiet
        # commands.  PYTHONUNBUFFERED + bufsize=1 make lines appear as produced.
        child_env = dict(os.environ, PYTHONUNBUFFERED="1")
        try:
            proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                    stdin=subprocess.DEVNULL, bufsize=1,
                                    text=True, encoding="utf-8", errors="replace",
                                    env=child_env,
                                    creationflags=flags, cwd=str(self.engine.parent))
        except Exception as e:
            self._console(f"launch failed: {e}\n", "err"); return
        self.jobs[slot] = proc
        self.job_cmd[slot] = cmd
        if cmd == "send":
            self._eta_prev = None            # fresh ETA baseline for this send
        self._set_slot(slot, f"{cmd} running…", running=True)
        threading.Thread(target=self._reader, args=(slot, proc), daemon=True).start()

    def _reader(self, slot: str, proc: subprocess.Popen) -> None:
        assert proc.stdout
        for line in proc.stdout:
            self.outq.put((slot, line, None))
        self.outq.put((slot, None, proc.wait()))   # sentinel: child exited

    def _pump(self) -> None:
        try:
            while True:
                self._refresh_apply(self.uiq.get_nowait())
        except queue.Empty:
            pass
        try:
            while True:
                slot, line, rc = self.outq.get_nowait()
                if line is None:
                    self._console(f"[{slot} exit {rc}]\n", "ok" if rc == 0 else "err")
                    self._set_slot(slot, f"done (rc={rc})", running=False)
                    self.jobs.pop(slot, None)
                    self.refresh()
                    continue
                tag = ("err" if " ERROR " in line else
                       "warn" if " WARNING " in line else None)
                # log filter: when on, only ERROR/WARNING lines reach the console
                # (progress/ETA/slot labels still update below)
                show = (not self.filter_var.get()) or tag is not None
                if slot == "main":
                    if " progress: " in line:
                        snap = line.split(" progress: ", 1)[1].strip()
                        self.live_lbl.config(text=snap + self._eta(snap))
                    if show:
                        self._console(line, tag)
                else:
                    if line.strip():
                        self._set_slot(slot, self._short(line), running=True)
                    if show:
                        self._console(f"[{slot}] {line}", tag)
        except queue.Empty:
            pass
        self.root.after(200, self._pump)

    _PROG_RE = re.compile(r"sent=(\d+) warn=(\d+) failed=(\d+) skipped=(\d+)")

    def _eta(self, snapshot: str) -> str:
        """Estimate remaining time from the delta between two progress snapshots
        and the pending count captured at the last refresh."""
        m = self._PROG_RE.search(snapshot)
        if not m:
            return ""
        done = sum(int(x) for x in m.groups())
        now = time.monotonic()
        prev = self._eta_prev
        self._eta_prev = (now, done)
        if not prev or self.last_pending <= 0:
            return ""
        dt, dn = now - prev[0], done - prev[1]
        if dt <= 0 or dn <= 0:
            return ""
        remaining = max(0, self.last_pending - done)
        secs = remaining / (dn / dt)
        h, rem = divmod(int(secs), 3600)
        mnt = rem // 60
        return f"   ·   ~{remaining:,} left, ETA {h}h{mnt:02d}m at {dn/dt:.0f}/s"

    @staticmethod
    def _short(line: str) -> str:
        s = line.split("] ", 1)[-1].strip() if "] " in line else line.strip()
        return (s[:34] + "…") if len(s) > 35 else s

    def _set_slot(self, slot: str, text: str, *, running: bool) -> None:
        ui = self.slot_ui[slot]
        ui["lbl"].config(text=f"{slot}: {text}")
        ui["btn"].config(state="normal" if running else "disabled")

    def stop_slot(self, slot: str) -> None:
        proc = self.jobs.get(slot)
        if not (proc and proc.poll() is None):
            return
        cmd = self.job_cmd.get(slot, "?")
        mode = self._ask_stop_mode(slot, cmd)
        if mode == "graceful":
            self._stop_graceful(slot, proc, cmd)
        elif mode == "force":
            self._stop_force(slot, proc)

    def _ask_stop_mode(self, slot: str, cmd: str) -> str | None:
        """Modal: Graceful vs Force vs Cancel.  Graceful lets the engine run its
        drain-and-commit shutdown (no PAUSE file needed); Force kills the tree."""
        dlg = tk.Toplevel(self.root)
        dlg.title(f"Stop  {slot}: {cmd}")
        dlg.transient(self.root); dlg.resizable(False, False)
        heavy = " (for a send this may take up to ~2 min while it finishes the " \
                "associations in flight)" if cmd == "send" else ""
        msg = (f"Stop '{cmd}' in the {slot} slot.\n\n"
               f"• Graceful — signal it to finish the work in flight, then shut "
               f"down and commit state cleanly{heavy}. No PAUSE file needed.\n\n"
               f"• Force kill — terminate the whole process tree immediately. "
               f"State stays recoverable (WAL journal) and you can re-run to "
               f"resume, but in-flight work is abandoned.")
        ttk.Label(dlg, text=msg, wraplength=440, justify="left", padding=12).pack()
        choice: dict[str, str | None] = {"v": None}
        bar = ttk.Frame(dlg, padding=(12, 0, 12, 12)); bar.pack(fill="x")

        def pick(v: str | None) -> None:
            choice["v"] = v; dlg.destroy()
        ttk.Button(bar, text="Graceful", command=lambda: pick("graceful")).pack(side="left")
        ttk.Button(bar, text="Force kill", command=lambda: pick("force")).pack(side="left", padx=6)
        ttk.Button(bar, text="Cancel", command=lambda: pick(None)).pack(side="right")
        dlg.bind("<Escape>", lambda _e: pick(None))
        dlg.grab_set()
        self.root.wait_window(dlg)
        return choice["v"]

    def _stop_graceful(self, slot: str, proc: subprocess.Popen, cmd: str) -> None:
        try:
            if os.name == "nt":
                proc.send_signal(signal.CTRL_BREAK_EVENT)   # -> SIGBREAK -> engine KeyboardInterrupt
            else:
                proc.terminate()                             # SIGTERM -> engine KeyboardInterrupt
        except Exception as e:
            self._console(f"[{slot}: graceful stop failed: {e} — try Force]\n", "err"); return
        self._set_slot(slot, f"{cmd} stopping (graceful)…", running=True)
        self._console(f"[{slot}: graceful stop requested — draining in-flight work, "
                      f"then it exits and commits. Click Stop again → Force to kill now.]\n", "warn")

    def _stop_force(self, slot: str, proc: subprocess.Popen) -> None:
        if os.name == "nt":
            # kill the whole tree: the child may be a `uv run` wrapper around the
            # real python engine — proc.kill() alone would orphan the sender
            try:
                subprocess.run(["taskkill", "/T", "/F", "/PID", str(proc.pid)],
                               capture_output=True)
            except Exception:
                proc.kill()
        else:
            proc.kill()
        self._console(f"[{slot}: force-killed — state recoverable (WAL); re-run to resume]\n", "err")

    def _console(self, text: str, tag: str | None = None) -> None:
        self.console.configure(state="normal")
        self.console.insert("end", text, tag or ())
        if int(self.console.index("end-1c").split(".")[0]) > 8000:
            self.console.delete("1.0", "2000.0")
        self.console.see("end")
        self.console.configure(state="disabled")

    # ---- PAUSE file ----------------------------------------------------------
    def toggle_pause(self) -> None:
        if not self.pause_file:
            messagebox.showinfo("pause", "no pause_file configured in [general]"); return
        p = Path(self.pause_file)
        if p.exists():
            p.unlink()
            self._console("[PAUSE removed — senders resume]\n", "ok")
        else:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text("paused from GUI")
            self._console("[PAUSE created — senders drain and idle]\n", "warn")
        self._sync_pause_btn()

    def _sync_pause_btn(self) -> None:
        paused = bool(self.pause_file) and Path(self.pause_file).exists()
        self.pause_btn.config(text="RESUME senders" if paused else "PAUSE senders")

    # ---- status refresh (read-only DB, off the UI thread) -------------------
    def refresh(self) -> None:
        if self.refreshing or not self.db_path:
            return
        self.refreshing = True
        self.refresh_lbl.config(text="refreshing…")
        threading.Thread(target=self._refresh_worker, daemon=True).start()

    def _refresh_worker(self) -> None:
        out: dict = {}
        try:
            conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, timeout=30)
            conn.row_factory = sqlite3.Row
            meta = dict(conn.execute("SELECT key, value FROM meta"))
            out["gate"] = meta.get("gate", "never armed")
            out["run"] = int(meta.get("analyze_run", 0) or 0)
            out["src"] = conn.execute(
                "SELECT source, send_status, COUNT(*) n FROM instances "
                "WHERE canonical=1 GROUP BY source, send_status").fetchall()
            out["prob"] = conn.execute(
                "SELECT severity, code, COUNT(*) n FROM problems WHERE run_id=? "
                "GROUP BY severity, code ORDER BY severity DESC, n DESC",
                (out["run"],)).fetchall()
            out["acked"] = {r[0] for r in conn.execute(
                "SELECT DISTINCT code FROM acks WHERE analyze_run=?", (out["run"],))}
            out["verify"] = dict(conn.execute(
                "SELECT verify_status, COUNT(*) FROM studies WHERE verify_status>0 "
                "GROUP BY verify_status"))
            conn.close()
        except Exception as e:
            out["error"] = str(e)
        # NEVER touch tkinter from a worker thread — post to the UI pump instead
        self.uiq.put(out)

    def _refresh_apply(self, out: dict) -> None:
        self.refreshing = False
        self.refresh_lbl.config(text="")
        self._sync_pause_btn()
        if "error" in out:
            self._console(f"[status refresh failed: {out['error']}]\n", "err"); return
        gate = out["gate"]
        armed = gate.startswith("armed")
        self.gate_lbl.config(text=f"gate: {gate.split(':')[0]}   analyze run: {out['run']}",
                             bg="#1d5c2d" if armed else "#6e1f1f", fg="white")
        agg: dict[str, dict[int, int]] = {}
        for r in out["src"]:
            agg.setdefault(r["source"], {})[r["send_status"]] = r["n"]
        self.src_tree.delete(*self.src_tree.get_children())
        pending = sent = total = 0
        for src in sorted(agg):
            c = agg[src]
            pend = c.get(S_PENDING, 0) + c.get(S_FAILED_RETRY, 0)
            snt = c.get(S_SENT, 0) + c.get(S_SENT_WARN, 0)
            pending += pend; sent += snt; total += sum(c.values())
            self.src_tree.insert("", "end", text=src, values=(
                f"{pend:,}", f"{c.get(S_SENT, 0):,}", f"{c.get(S_SENT_WARN, 0):,}",
                f"{c.get(S_FAILED_PERM, 0):,}",
                f"{c.get(S_SKIPPED_DUP, 0) + c.get(S_SKIPPED_EXISTS, 0):,}",
                f"{c.get(S_EXCLUDED, 0):,}"))
        self.last_pending = pending
        self.prob_tree.delete(*self.prob_tree.get_children())
        nblock = 0
        for r in out["prob"]:
            if r["severity"] == 2:
                nblock += r["n"]
            acked = "yes" if r["code"] in out["acked"] else ("" if r["severity"] != 1 else "NO")
            self.prob_tree.insert("", "end",
                                  text=f"{SEV_NAMES.get(r['severity'], r['severity'])}  {r['code']}",
                                  values=(f"{r['n']:,}", acked))
        v = out["verify"]
        self.verify_lbl.config(text="verify: " + ", ".join(
            f"{v.get(k, 0):,} {n}" for k, n in VERIFY_NAMES.items()) if v else "verify: not run")

        # empty-state hint
        if total == 0:
            self.empty_lbl.config(text="No inventory yet. Fill the config (Config editor), "
                                       "then run Scan to build it." if self.has_replace_me
                                  else "No inventory yet — run Scan to build it.")
        else:
            self.empty_lbl.config(text="")
        self._update_stepper(out, total, pending, sent, nblock, armed)

    def _update_stepper(self, out, total, pending, sent, nblock, armed):
        run = out["run"]
        unacked = any(r["severity"] == 1 and r["code"] not in out["acked"] for r in out["prob"])
        done = {
            "Config": not self.has_replace_me,
            "Scan": total > 0,
            "Analyze": run > 0,
            "Approve": armed,
            "Send": sent > 0 and pending == 0,
            "Verify": bool(out["verify"]),
        }
        hints = {
            "Config": "Fill every REPLACE_ME (Config editor), then Preflight.",
            "Scan": "Run Scan to inventory the sources (safe, read-only).",
            "Analyze": "Run Analyze, then open the report and review.",
            "Approve": (f"Resolve {nblock:,} hard blocker(s), " if nblock else "") +
                       ("ack warning class(es), then ARM the gate." if unacked
                        else "ARM the gate to enable sending."),
            "Send": f"Gate armed — run Send ({pending:,} pending).",
            "Verify": "Run Verify to reconcile server counts.",
        }
        current = next((s for s in STAGES if not done[s]), None)
        for s in STAGES:
            if done[s]:
                self.step_ui[s].config(text=f"✓ {s}", fg="white", bg="#1d5c2d")
            elif s == current:
                self.step_ui[s].config(text=f"▸ {s}", fg="white", bg="#3366cc")
            else:
                self.step_ui[s].config(text=f"{s}", fg="#888", bg=self.root.cget("bg"))
        self.step_hint.config(text=hints.get(current, "Migration complete — all stages done. ✓"))

    def _on_prob_select(self, _e=None):
        sel = self.prob_tree.selection()
        if not sel:
            return
        code = self.prob_tree.item(sel[0])["text"].split()[-1]
        self.prob_help.config(text=f"{code}: {PROBLEM_HELP.get(code, 'no description available.')}")

    # ---- gate actions --------------------------------------------------------
    def ack_dialog(self) -> None:
        rows = [self.prob_tree.item(i) for i in self.prob_tree.get_children()]
        codes = [r["text"].split()[-1] for r in rows
                 if r["text"].startswith("WARN") and r["values"][1] != "yes"]
        if not codes:
            messagebox.showinfo("ack", "no unacknowledged warning classes"); return
        dlg = tk.Toplevel(self.root); dlg.title("acknowledge warning classes")
        dlg.transient(self.root)
        ttk.Label(dlg, text="Select one or more warning classes to acknowledge in a "
                            "single approve call:", wraplength=360, padding=(12, 8, 12, 2),
                  justify="left").pack(anchor="w")
        vars_: dict[str, tk.BooleanVar] = {}
        for c in codes:
            vars_[c] = tk.BooleanVar(value=True)   # default: ack them all (the common case)
            ttk.Checkbutton(dlg, text=c, variable=vars_[c]).pack(anchor="w", padx=16, pady=1)

        def set_all(val: bool) -> None:
            for v in vars_.values():
                v.set(val)
        bar = ttk.Frame(dlg, padding=(12, 6, 12, 12)); bar.pack(fill="x")
        ttk.Button(bar, text="Select all", command=lambda: set_all(True)).pack(side="left")
        ttk.Button(bar, text="Select none", command=lambda: set_all(False)).pack(side="left", padx=4)

        def go() -> None:
            sel = [c for c, v in vars_.items() if v.get()]
            dlg.destroy()
            if sel:
                # one approve invocation acks every selected class at once
                self.run("approve", ["--ack", *sel, "--note", "acked via GUI"])
        ttk.Button(bar, text="Acknowledge selected", command=go).pack(side="right")

    def arm(self) -> None:
        if messagebox.askyesno(
                "ARM GATE",
                "Arm the send gate?\n\nThis is the point of no return: the next "
                "`send` will transmit to the PACS and transmitted studies cannot "
                "be recalled.\n\nArming succeeds only if every hard blocker is "
                "resolved and every warning class acknowledged.", icon="warning"):
            self.run("approve", ["--arm"])

    def open_report(self) -> None:
        try:
            runs = sorted(Path(self.report_dir).glob("run_*"),
                          key=lambda p: int(re.sub(r"\D", "", p.name) or 0))
            os.startfile(str(runs[-1] / "summary.html"))
        except Exception as e:
            messagebox.showinfo("report", f"no report found: {e}")

    def _get_schema(self) -> dict | None:
        """Fetch the config schema from the engine (cached).  The GUI can't import
        the engine (it may lack pydicom), so we shell out to `dcm_migrate schema`."""
        if getattr(self, "_schema", None) is not None:
            return self._schema
        argv = engine_cmd(self.engine, self.cfg_path, "schema", [])
        # `schema` ignores --config, but engine_cmd always inserts it — harmless
        try:
            r = subprocess.run(argv, capture_output=True, text=True, timeout=120,
                               cwd=str(self.engine.parent))
            self._schema = json.loads(r.stdout)
            return self._schema
        except Exception as e:
            messagebox.showerror("config editor", f"could not load the config schema "
                                                  f"from the engine:\n{e}")
            return None

    def open_config_editor(self) -> None:
        if any(p.poll() is None for p in self.jobs.values()):
            if not messagebox.askyesno("config editor",
                                       "An engine command is running. Editing config "
                                       "won't affect it, but saving a gate-affecting "
                                       "change disarms the gate for future runs. Open anyway?"):
                return
        schema = self._get_schema()
        if schema:
            ConfigEditor(self, schema)

    def new_workspace(self) -> None:
        schema = self._get_schema()
        if schema:
            ConfigEditor(self, schema, new_workspace=True)

    def reload_config(self) -> None:
        """Re-read [general] paths after the editor saved (paths may have changed)."""
        self._load_cfg()
        self.refresh()


# ---------------------------------------------------------------------------
# Schema-driven, comment-preserving config editor.  The field/structure schema
# comes from the engine (`dcm_migrate schema`); the document model is tomlkit,
# so comments, ordering and formatting survive every edit — including adding,
# removing and reordering whole sources / archives / rules.
# ---------------------------------------------------------------------------
class ConfigEditor:
    def __init__(self, app: "App", schema: dict, new_workspace: bool = False):
        self.app = app
        self.path = Path(app.cfg_path)
        self.schema = schema
        if tomlkit is None:
            messagebox.showinfo(
                "config editor",
                "The structured editor needs the pure-Python 'tomlkit' package.\n\n"
                "Launch via dcm_migrate_gui.cmd or `uv run dcm_migrate_gui.py` (which "
                "installs it automatically), or `pip install tomlkit`.\n\n"
                "Meanwhile, use 'Edit raw TOML'.")
            return
        try:
            text = "" if new_workspace else self.path.read_text(encoding="utf-8")
            self.doc = tomlkit.parse(text)
        except Exception as e:
            messagebox.showerror("config editor", f"cannot read/parse {self.path}:\n{e}")
            return
        self.new_workspace = new_workspace
        self.sec_by_key = {s["key"]: s for s in schema["sections"]}
        if new_workspace:
            self._seed()
        else:
            self._migrate_legacy()
        self.orig_nn = self._nn_of_doc()          # baseline for gate-disarm detection
        self.binders: list = []
        self.gate_widgets: list = []
        self.win = tk.Toplevel(app.root)
        self.win.title(("New workspace — " if new_workspace else "Config editor — ") + self.path.name)
        self.win.geometry("900x900")
        self.gate_var = tk.BooleanVar(value=new_workspace)   # a fresh workspace is all-editable
        self._build()

    # ---- helpers -----------------------------------------------------------
    @staticmethod
    def _nn(d: dict) -> str:
        return json.dumps({k: v for k, v in d.items() if k != "network"}, sort_keys=True, default=str)

    def _nn_of_doc(self) -> str:
        return self._nn(tomllib.loads(tomlkit.dumps(self.doc)))

    def _default_for(self, field):
        d = field["default"]
        if d is not None:
            return d
        return {"csv": [], "lines": []}.get(field["kind"], "")

    def _defaults_table(self, fields):
        t = tomlkit.table()
        for f in fields:
            t[f["name"]] = self._default_for(f)
        return t

    def _seed(self):
        S = self.sec_by_key
        self.doc["general"] = self._defaults_table(S["general"]["fields"])
        srv = self._defaults_table(S["server"]["fields"])
        dchild = next(c for c in S["server"]["children"] if c["key"] == "destinations")
        dests = tomlkit.table()
        dests["other"] = self._defaults_table(dchild["value_fields"])
        srv["destinations"] = dests
        self.doc["server"] = srv
        self.doc["network"] = self._defaults_table(S["network"]["fields"])
        rt = self._defaults_table(S["routing"]["fields"])
        # cover the engine's default modality groups so the skeleton validates;
        # every group falls back to the 'other' archive until the user adds more.
        rt["precedence"] = ["ct", "xray", "xa", "other"]
        self.doc["routing"] = rt
        src = self._defaults_table(S["source"]["fields"])
        src["name"] = "source1"
        src["roots"] = ["REPLACE_ME"]        # roots must be non-empty; user fills it
        aot = tomlkit.aot(); aot.append(src)
        self.doc["source"] = aot

    def _migrate_legacy(self):
        """Rename the legacy [source.fuji] block to [source.id_generator] so the
        schema-driven form finds it.  tomlkit preserves the block's contents and
        comments across the rename; the engine still accepts either key."""
        for src in self.doc.get("source", []) or []:
            if "fuji" in src and "id_generator" not in src:
                src["id_generator"] = src.pop("fuji")

    def _ensure_table(self, parent, key):
        if key not in parent:
            parent[key] = tomlkit.table()
        return parent[key]

    def _ensure_aot(self, parent, key):
        if key not in parent:
            parent[key] = tomlkit.aot()
        return parent[key]

    # ---- widgets -----------------------------------------------------------
    def _row(self, parent, field, container, gate, label=None):
        kind, name = field["kind"], field["name"]
        cur = container.get(name, self._default_for(field))
        fr = ttk.Frame(parent); fr.pack(fill="x", padx=(16, 8), pady=1)
        ttk.Label(fr, text=label or name, width=24, anchor="w").pack(side="left")
        if kind == "bool":
            var = tk.BooleanVar(value=bool(cur))
            w = ttk.Checkbutton(fr, variable=var); w.pack(side="left")
            get = lambda: bool(var.get())
        elif kind == "enum":
            var = tk.StringVar(value="" if cur is None else str(cur))
            w = ttk.Combobox(fr, textvariable=var, values=field.get("choices") or [],
                             width=24, state="readonly"); w.pack(side="left")
            get = lambda: var.get()
        elif kind == "csv":
            var = tk.StringVar(value=", ".join(str(x) for x in (cur or [])))
            w = ttk.Entry(fr, textvariable=var); w.pack(side="left", fill="x", expand=True)
            get = lambda: [x.strip() for x in var.get().split(",") if x.strip()]
        elif kind == "lines":
            w = tk.Text(fr, height=3, width=48)
            w.insert("1.0", "\n".join(str(x) for x in (cur or [])))
            w.pack(side="left", fill="x", expand=True)
            get = lambda: [x.strip() for x in w.get("1.0", "end").splitlines() if x.strip()]
        elif kind in ("int", "float"):
            var = tk.StringVar(value="" if cur is None else str(cur))
            w = ttk.Entry(fr, textvariable=var, width=16); w.pack(side="left")
            conv = int if kind == "int" else float
            def get(_v=var, _c=conv, _l=(label or name)):
                s = _v.get().strip()
                try:
                    return _c(s)
                except ValueError:
                    raise ValueError(f"{_l}: expected {'an integer' if _c is int else 'a number'}, got {s!r}")
        else:  # str / path
            var = tk.StringVar(value="" if cur is None else str(cur))
            w = ttk.Entry(fr, textvariable=var); w.pack(side="left", fill="x", expand=True)
            get = lambda: var.get()
        if field.get("help"):
            ttk.Label(fr, text=field["help"], foreground="#999").pack(side="left", padx=6)
        if isinstance(cur, str) and "REPLACE_ME" in cur:
            try:
                w.configure(foreground="#d33")
            except tk.TclError:
                pass
            tk.Label(fr, text="⚠ set me", fg="#d33").pack(side="left", padx=4)
        if gate:
            self.gate_widgets.append(w)
        self.binders.append(lambda _c=container, _n=name, _g=get: _c.__setitem__(_n, _g()))

    def _section_header(self, parent, label, gate):
        bg = "#7a3b00" if gate else "#1d5c2d"
        tag = ("GATE-AFFECTING — saving disarms the gate" if gate
               else "SAFE — retune without disarming")
        tk.Label(parent, text=f"  {label}   —   {tag}", bg=bg, fg="white", anchor="w",
                 font=("Segoe UI", 9, "bold")).pack(fill="x", pady=(12, 2))

    def _titlebar(self, parent, text, on_remove=None, on_up=None, on_down=None, color="#446"):
        bar = tk.Frame(parent, bg=color); bar.pack(fill="x", padx=8, pady=(6, 0))
        tk.Label(bar, text=text, bg=color, fg="white", anchor="w").pack(side="left", padx=4)
        if on_down:
            b = ttk.Button(bar, text="↓", width=2, command=lambda: self._struct(on_down)); b.pack(side="right")
            if self.gate_var.get() is False: b.state(["disabled"])
        if on_up:
            b = ttk.Button(bar, text="↑", width=2, command=lambda: self._struct(on_up)); b.pack(side="right")
            if self.gate_var.get() is False: b.state(["disabled"])
        if on_remove:
            b = ttk.Button(bar, text="remove", width=7, command=lambda: self._struct(on_remove)); b.pack(side="right", padx=2)
            if self.gate_var.get() is False: b.state(["disabled"])
        return bar

    # ---- structural mutation (flush widgets -> doc, mutate, rebuild) -------
    def _struct(self, mutate):
        try:
            self._flush()
        except ValueError as e:
            messagebox.showerror("invalid input", str(e)); return
        mutate()
        self._rebuild()

    def _flush(self):
        for b in self.binders:
            b()

    def _add_button(self, parent, text, mutate):
        b = ttk.Button(parent, text=text, command=lambda: self._struct(mutate))
        b.pack(anchor="w", padx=16, pady=2)
        if not self.gate_var.get():
            b.state(["disabled"])
        self.gate_widgets.append(b)

    # ---- rendering ---------------------------------------------------------
    def _build(self):
        for c in list(self.win.children.values()):
            c.destroy()
        self.binders = []; self.gate_widgets = []

        top = ttk.Frame(self.win, padding=6); top.pack(fill="x")
        ttk.Checkbutton(top, variable=self.gate_var, command=self._toggle_gate,
                        text="Enable editing of GATE-AFFECTING fields/structure "
                             "(saving disarms the gate — re-analyze & re-arm)").pack(anchor="w")
        ttk.Label(top, foreground="#999",
                  text="green = [network] (safe)   ·   amber = content config (disarms on save)   ·   "
                       "comments & formatting are preserved on save (tomlkit)").pack(anchor="w")

        outer = ttk.Frame(self.win); outer.pack(fill="both", expand=True)
        canvas = tk.Canvas(outer, borderwidth=0, highlightthickness=0)
        vsb = ttk.Scrollbar(outer, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y"); canvas.pack(side="left", fill="both", expand=True)
        body = ttk.Frame(canvas)
        canvas.create_window((0, 0), window=body, anchor="nw", width=850)
        body.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        wheel = lambda e: canvas.yview_scroll(int(-e.delta / 120), "units")
        canvas.bind_all("<MouseWheel>", wheel)
        self.win.bind("<Destroy>", lambda e: canvas.unbind_all("<MouseWheel>"))

        for sec in self.schema["sections"]:
            self._render_section(body, sec)

        bar = ttk.Frame(self.win, padding=6); bar.pack(fill="x")
        ttk.Button(bar, text="Validate", command=self._on_validate).pack(side="left")
        ttk.Button(bar, text="Save (backup + write)", command=self._on_save).pack(side="left", padx=6)
        if not self.new_workspace:
            ttk.Button(bar, text="Reload from disk", command=self._reload).pack(side="left")
        ttk.Button(bar, text="Close", command=self.win.destroy).pack(side="right")
        self._toggle_gate()

    def _render_section(self, parent, sec):
        self._section_header(parent, sec["label"], sec["gate"])
        card = sec["cardinality"]
        if card == "table":
            container = self._ensure_table(self.doc, sec["key"])
            for f in sec["fields"]:
                self._row(parent, f, container, sec["gate"])
            for child in sec.get("children", []):
                if child["cardinality"] == "table_map":
                    self._render_table_map(parent, child, container)
            if "groups" in sec:
                self._render_groups(parent, container, sec["groups"])
        elif card == "array_table":
            self._render_array_table(parent, sec, self.doc, top=True)

    def _render_table_map(self, parent, child, parent_container):
        container = self._ensure_table(parent_container, child["key"])
        ttk.Label(parent, text=f"  {child['label']}", foreground="#66a").pack(anchor="w", padx=12)
        for k in list(container):
            self._titlebar(parent, f"{child['key']}.{k}",
                           on_remove=lambda kk=k: container.__delitem__(kk))
            for f in child["value_fields"]:
                self._row(parent, f, container[k], child["gate"], label=f"  {f['name']}")
        def add():
            key = simpledialog.askstring(child["key"], f"New {child.get('key_hint', 'key')}:", parent=self.win)
            if key:
                container[key] = self._defaults_table(child["value_fields"])
        self._add_button(parent, f"+ add {child['key'][:-1] if child['key'].endswith('s') else child['key']}", add)

    def _render_groups(self, parent, routing, meta):
        reserved = {"precedence", "companion"}
        groups = [k for k in routing if k not in reserved and isinstance(routing.get(k), list)]
        ttk.Label(parent, text=f"  {meta['label']}", foreground="#66a").pack(anchor="w", padx=12)
        for g in groups:
            fr = ttk.Frame(parent); fr.pack(fill="x", padx=(16, 8), pady=1)
            ttk.Label(fr, text=f"  {g}", width=24, anchor="w").pack(side="left")
            var = tk.StringVar(value=", ".join(str(x) for x in routing.get(g, [])))
            w = ttk.Entry(fr, textvariable=var); w.pack(side="left", fill="x", expand=True)
            rm = ttk.Button(fr, text="remove", width=7, command=lambda gg=g: self._struct(lambda: routing.__delitem__(gg)))
            rm.pack(side="left", padx=2)
            self.gate_widgets += [w, rm]
            self.binders.append(lambda _r=routing, _g=g, _v=var:
                                _r.__setitem__(_g, [x.strip() for x in _v.get().split(",") if x.strip()]))
        def add():
            key = simpledialog.askstring("routing group", f"New {meta.get('key_hint', 'group')}:", parent=self.win)
            if key:
                routing[key] = []
        self._add_button(parent, "+ add group", add)

    def _render_array_table(self, parent, sec, parent_container, top=False):
        container = self._ensure_aot(parent_container, sec["key"])
        n = len(container)
        for i in range(n):
            item = container[i]
            kf = sec.get("key_field")
            title = f"{sec['key']}[{i}]" + (f"  {item.get(kf, '')}" if kf else "")
            def mk_remove(idx=i):
                return lambda: container.__delitem__(idx)
            def mk_up(idx=i):
                return lambda: container.body.insert(idx - 1, container.body.pop(idx))
            def mk_down(idx=i):
                return lambda: container.body.insert(idx + 1, container.body.pop(idx))
            self._titlebar(parent, title, on_remove=mk_remove(),
                           on_up=mk_up() if i > 0 else None,
                           on_down=mk_down() if i < n - 1 else None, color="#356")
            for f in sec["fields"]:
                self._row(parent, f, item, sec["gate"])
            for child in sec.get("children", []):
                if child["cardinality"] == "opt_table":
                    self._render_opt_table(parent, child, item)
                elif child["cardinality"] == "array_table":
                    ttk.Label(parent, text=f"  {child['label']}", foreground="#66a").pack(anchor="w", padx=12)
                    self._render_array_table(parent, child, item)
        self._add_button(parent, f"+ add {sec['key']}",
                         lambda: container.append(self._new_item(sec)))

    def _new_item(self, sec):
        t = self._defaults_table(sec["fields"])
        kf = sec.get("key_field")
        if kf and not t.get(kf):
            t[kf] = f"{sec['key']}{len(self.doc.get(sec['key'], []))}"
        return t

    def _render_opt_table(self, parent, child, item):
        if child["key"] in item:
            self._titlebar(parent, child["label"], on_remove=lambda: item.__delitem__(child["key"]), color="#454")
            block = item[child["key"]]
            for f in child["fields"]:
                self._row(parent, f, block, child["gate"], label=f"  {f['name']}")
            for gc in child.get("children", []):
                if gc["cardinality"] == "scalar_map":
                    self._render_scalar_map(parent, gc, block)
        else:
            self._add_button(parent, f"+ add {child['label']}",
                             lambda: item.__setitem__(child["key"], self._defaults_table(child["fields"])))

    def _render_scalar_map(self, parent, child, block):
        container = self._ensure_table(block, child["key"])
        ttk.Label(parent, text=f"    {child['label']}", foreground="#66a").pack(anchor="w", padx=16)
        for k in list(container):
            fr = ttk.Frame(parent); fr.pack(fill="x", padx=(24, 8), pady=1)
            ttk.Label(fr, text=f"  {k}", width=28, anchor="w").pack(side="left")
            var = tk.StringVar(value=str(container.get(k, "")))
            w = ttk.Entry(fr, textvariable=var, width=16); w.pack(side="left")
            rm = ttk.Button(fr, text="remove", width=7, command=lambda kk=k: self._struct(lambda: container.__delitem__(kk)))
            rm.pack(side="left", padx=2)
            self.gate_widgets += [w, rm]
            self.binders.append(lambda _c=container, _k=k, _v=var: _c.__setitem__(_k, _v.get()))
        def add():
            key = simpledialog.askstring(child["key"], f"New {child.get('key_hint', 'key')}:", parent=self.win)
            if key:
                container[key] = ""
        self._add_button(parent, f"+ add mapping", add)

    def _toggle_gate(self):
        on = self.gate_var.get()
        for w in self.gate_widgets:
            try:
                if isinstance(w, ttk.Combobox):
                    w.configure(state="readonly" if on else "disabled")
                elif isinstance(w, ttk.Button):
                    w.state(["!disabled"] if on else ["disabled"])
                else:
                    w.configure(state="normal" if on else "disabled")
            except tk.TclError:
                pass

    # ---- validate / save ---------------------------------------------------
    def _dump(self) -> str:
        self._flush()
        return tomlkit.dumps(self.doc)

    def _validate_text(self, text):
        tmp = Path(tempfile.gettempdir()) / f"dcm_cfg_check_{os.getpid()}.toml"
        tmp.write_text(text, encoding="utf-8")
        argv = engine_cmd(self.app.engine, tmp, "checkconfig", [])
        try:
            r = subprocess.run(argv, capture_output=True, text=True, timeout=180,
                               cwd=str(self.app.engine.parent))
            return r.returncode == 0, (r.stdout + r.stderr).strip()
        except Exception as e:
            return False, f"could not run checkconfig: {e}"
        finally:
            try:
                tmp.unlink()
            except OSError:
                pass

    def _on_validate(self):
        try:
            text = self._dump()
        except ValueError as e:
            messagebox.showerror("invalid input", str(e)); return
        ok, out = self._validate_text(text)
        (messagebox.showinfo if ok else messagebox.showerror)(
            "validation " + ("passed" if ok else "FAILED"), out or ("ok" if ok else "unknown error"))

    def _on_save(self):
        try:
            text = self._dump()
        except ValueError as e:
            messagebox.showerror("invalid input", str(e)); return
        ok, out = self._validate_text(text)
        if not ok:
            messagebox.showerror("validation failed",
                                 "The engine rejected this config — NOT saved:\n\n" + out)
            return
        target = self.path
        if self.new_workspace:
            chosen = filedialog.asksaveasfilename(
                parent=self.win, defaultextension=".toml", initialfile="migration.toml",
                filetypes=[("TOML", "*.toml")], title="Save new workspace config as")
            if not chosen:
                return
            target = Path(chosen)
        disarms = (not target.exists()) or (self._nn(tomllib.loads(text)) != self.orig_nn)
        if disarms and target.exists() and not messagebox.askyesno(
                "disarm the gate?",
                "These edits change content config (not just [network]).\n\n"
                "Saving DISARMS the send gate — re-run analyze and approve --arm "
                "before the next send.\n\nSave anyway?", icon="warning"):
            return
        try:
            if target.exists():
                shutil.copy2(target, target.with_name(target.name + ".bak"))
            target.write_text(text, encoding="utf-8")
        except Exception as e:
            messagebox.showerror("write failed", str(e)); return
        self.path = target
        self.orig_nn = self._nn(tomllib.loads(text))
        self.new_workspace = False
        if str(target) == str(self.app.cfg_path):
            self.app.reload_config()
        note = ("gate DISARMED — re-analyze & re-arm" if disarms
                else "network-only — restart send to apply")
        self.app._console(f"[config saved -> {target.name} (comments preserved); {note}]\n",
                          "warn" if disarms else "ok")
        messagebox.showinfo("saved", f"Saved and validated: {target.name}\n\n{note}.\n"
                                     f"(comments and formatting preserved)")

    def _reload(self):
        try:
            self.doc = tomlkit.parse(self.path.read_text(encoding="utf-8"))
            self.orig_nn = self._nn_of_doc()
        except Exception as e:
            messagebox.showerror("reload", str(e)); return
        self._build()

    def _rebuild(self):
        self._build()


def main() -> int:
    cfg = Path(sys.argv[1] if len(sys.argv) > 1 else "migration.toml").resolve()
    engine = Path(__file__).with_name("dcm_migrate.py")
    if not cfg.exists():
        print(f"config not found: {cfg}\nusage: py dcm_migrate_gui.py [migration.toml]")
        return 2
    if not engine.exists():
        print(f"engine not found next to the GUI: {engine}")
        return 2
    root = tk.Tk()
    try:
        root.state("zoomed")
    except tk.TclError:
        pass
    App(root, cfg, engine)
    root.mainloop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
