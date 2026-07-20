"""dcm_migrate control panel — native Windows UI (tkinter, stdlib only).

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

import os
import queue
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import tomllib
from pathlib import Path

import tkinter as tk
from tkinter import messagebox, ttk

# Mirrors of engine constants (dcm_migrate.py SECTION 2) — kept tiny on purpose.
S_PENDING, S_SENT, S_SENT_WARN, S_FAILED_PERM, S_FAILED_RETRY, \
    S_SKIPPED_DUP, S_EXCLUDED, S_SKIPPED_EXISTS = range(8)
SEV_NAMES = {0: "info", 1: "WARN", 2: "HARD"}
VERIFY_NAMES = {1: "match", 2: "MISMATCH", 3: "unavailable"}

ACTIONS = ["scan", "analyze", "report", "echo", "send", "verify",
           "status", "dup-audit", "export-mapping"]
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
        self._load_cfg()

        root.title(f"dcm_migrate — {cfg_path}")
        root.geometry("1180x760")
        self._build()
        self.root.after(200, self._pump)
        self.refresh()

    # ---- config (only the few [general] paths; engine re-parses on every run)
    def _load_cfg(self) -> None:
        try:
            with open(self.cfg_path, "rb") as f:
                d = tomllib.load(f)
            g = d.get("general", {})
            self.db_path = g.get("db_path", "")
            self.report_dir = g.get("report_dir", "")
            self.pause_file = g.get("pause_file", "")
        except Exception as e:
            messagebox.showerror("config", f"cannot parse {self.cfg_path}:\n{e}")

    # ---- layout ------------------------------------------------------------
    def _build(self) -> None:
        top = ttk.Frame(self.root, padding=4); top.pack(fill="x")
        self.gate_lbl = tk.Label(top, text="gate: ?", font=("Segoe UI", 10, "bold"),
                                 padx=8, pady=2)
        self.gate_lbl.pack(side="left")
        self.live_lbl = ttk.Label(top, text="")   # parsed `progress:` line of a running send
        self.live_lbl.pack(side="left", padx=12)
        self.refresh_lbl = ttk.Label(top, text="")
        self.refresh_lbl.pack(side="left", padx=8)
        for text, fn in [("Refresh", self.refresh),
                         ("Edit config", lambda: os.startfile(self.cfg_path)),
                         ("Open report", self.open_report),
                         ("Open log dir", lambda: os.startfile(str(Path(self.db_path).parent)))]:
            ttk.Button(top, text=text, command=fn).pack(side="right", padx=2)
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
        self.src_tree.pack(fill="x", pady=(0, 8))
        ttk.Label(left, text="Problems (current analyze run)").pack(anchor="w")
        self.prob_tree = ttk.Treeview(left, columns=("count", "acked"), height=10)
        self.prob_tree.heading("#0", text="severity / code"); self.prob_tree.column("#0", width=260)
        self.prob_tree.heading("count", text="count"); self.prob_tree.column("count", width=90, anchor="e")
        self.prob_tree.heading("acked", text="acked"); self.prob_tree.column("acked", width=60, anchor="center")
        self.prob_tree.pack(fill="both", expand=True)
        self.verify_lbl = ttk.Label(left, text=""); self.verify_lbl.pack(anchor="w", pady=4)

        # right: actions + console
        right = ttk.Frame(pane, padding=4); pane.add(right, weight=3)
        row = ttk.Frame(right); row.pack(fill="x")
        for cmd in ACTIONS:
            ttk.Button(row, text=cmd, width=9,
                       command=lambda c=cmd: self.run(c)).pack(side="left", padx=1, pady=1)
        row2 = ttk.Frame(right); row2.pack(fill="x", pady=2)
        ttk.Button(row2, text="Ack warnings…", command=self.ack_dialog).pack(side="left", padx=1)
        ttk.Button(row2, text="ARM GATE", command=self.arm).pack(side="left", padx=6)
        ttk.Label(row2, text="extra args:").pack(side="left", padx=(16, 2))
        self.extra = ttk.Entry(row2, width=40); self.extra.pack(side="left")

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

        conf = ttk.Frame(right); conf.pack(fill="both", expand=True)
        self.console = tk.Text(conf, bg="#111318", fg="#d6d8dd", insertbackground="#d6d8dd",
                               font=("Consolas", 9), wrap="none", state="disabled")
        ys = ttk.Scrollbar(conf, command=self.console.yview)
        self.console.configure(yscrollcommand=ys.set)
        self.console.pack(side="left", fill="both", expand=True); ys.pack(side="right", fill="y")
        self.console.tag_configure("err", foreground="#ff6b6b")
        self.console.tag_configure("warn", foreground="#ffb454")
        self.console.tag_configure("ok", foreground="#7dd97b")

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
        try:
            proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                    stdin=subprocess.DEVNULL,
                                    text=True, encoding="utf-8", errors="replace",
                                    creationflags=flags, cwd=str(self.engine.parent))
        except Exception as e:
            self._console(f"launch failed: {e}\n", "err"); return
        self.jobs[slot] = proc
        self.job_cmd[slot] = cmd
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
                if slot == "main":
                    if " progress: " in line:
                        self.live_lbl.config(text=line.split(" progress: ", 1)[1].strip())
                    self._console(line, tag)
                else:
                    # independent slots: attribute in the console AND surface the
                    # latest line in their own slot label (the "different portion")
                    if line.strip():
                        self._set_slot(slot, self._short(line), running=True)
                    self._console(f"[{slot}] {line}", tag)
        except queue.Empty:
            pass
        self.root.after(200, self._pump)

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
        if os.name == "nt":
            import signal
            try:
                proc.send_signal(signal.CTRL_BREAK_EVENT)
            except Exception:
                pass
            self.root.after(5000, lambda: proc.poll() is None and proc.terminate())
        else:
            proc.terminate()
        hint = " — tip: PAUSE drains senders gracefully first" if slot == "main" else ""
        self._console(f"[{slot}: stop requested{hint}]\n", "warn")

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
        for src in sorted(agg):
            c = agg[src]
            self.src_tree.insert("", "end", text=src, values=(
                f"{c.get(S_PENDING, 0) + c.get(S_FAILED_RETRY, 0):,}",
                f"{c.get(S_SENT, 0):,}", f"{c.get(S_SENT_WARN, 0):,}",
                f"{c.get(S_FAILED_PERM, 0):,}",
                f"{c.get(S_SKIPPED_DUP, 0) + c.get(S_SKIPPED_EXISTS, 0):,}",
                f"{c.get(S_EXCLUDED, 0):,}"))
        self.prob_tree.delete(*self.prob_tree.get_children())
        for r in out["prob"]:
            acked = "yes" if r["code"] in out["acked"] else ("" if r["severity"] != 1 else "NO")
            self.prob_tree.insert("", "end",
                                  text=f"{SEV_NAMES.get(r['severity'], r['severity'])}  {r['code']}",
                                  values=(f"{r['n']:,}", acked))
        v = out["verify"]
        self.verify_lbl.config(text="verify: " + ", ".join(
            f"{v.get(k, 0):,} {n}" for k, n in VERIFY_NAMES.items()) if v else "verify: not run")

    # ---- gate actions --------------------------------------------------------
    def ack_dialog(self) -> None:
        rows = [self.prob_tree.item(i) for i in self.prob_tree.get_children()]
        codes = [r["text"].split()[-1] for r in rows
                 if r["text"].startswith("WARN") and r["values"][1] != "yes"]
        if not codes:
            messagebox.showinfo("ack", "no unacknowledged warning classes"); return
        dlg = tk.Toplevel(self.root); dlg.title("acknowledge warning classes")
        vars_: dict[str, tk.BooleanVar] = {}
        for c in codes:
            vars_[c] = tk.BooleanVar(value=False)
            ttk.Checkbutton(dlg, text=c, variable=vars_[c]).pack(anchor="w", padx=12, pady=1)
        def go():
            sel = [c for c, v in vars_.items() if v.get()]
            dlg.destroy()
            if sel:
                self.run("approve", ["--ack", *sel, "--note", "acked via GUI"])
        ttk.Button(dlg, text="Acknowledge selected", command=go).pack(pady=8)

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
