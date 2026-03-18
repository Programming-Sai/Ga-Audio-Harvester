"""
Multi-Agent Pipeline TUI  v3
============================
Agents   : Discovery (cyan) → Download (green) → Resilience (magenta)
Layout   : Active agent = large left panel (~68%)
           Other two   = stacked right      (~32%)
           On DONE     → Summary screen (key 4) shown by default
                         Right col has all 3 agents stacked

Shortcuts (running):
  q / Q    graceful / force quit  (both confirm)
  p        pause / resume queue
  + / -    worker slots  (1–6 hard cap)
  1 2 3    focus agent panel
  e        expand focused panel full-screen
  ↑ ↓      scroll log

Shortcuts (done):
  4        summary  (default on completion)
  1 2 3    focus agent panel
  e        expand
  ↑ ↓      scroll
  o        open output folder
  x        export JSON summary
  r        run again  (confirm)
  q / Q    quit  (confirm)
"""

from __future__ import annotations

import os
import queue
import subprocess
import sys
import threading
import time
import json
from collections  import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses  import dataclass, field
from enum         import Enum, auto
from typing       import List, Optional

IS_WINDOWS = sys.platform == "win32"
if IS_WINDOWS:
    import msvcrt
else:
    import termios, tty

from rich.console import Console, Group
from rich.layout  import Layout
from rich.live    import Live
from rich.panel   import Panel
from rich.text    import Text
from rich.align   import Align
from rich.table   import Table
from rich         import box as rbox


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

REFRESH_RATE   = 8
LOG_MAXLEN     = 500
WORKER_MIN     = 1
WORKER_MAX     = 6
WORKER_DEFAULT = 4

C_CYAN    = "bright_cyan"
C_GREEN   = "bright_green"
C_MAGENTA = "magenta"
C_RED     = "bright_red"
C_WHITE   = "white"
C_DIM     = "dim white"
C_YELLOW  = "yellow"


# ─────────────────────────────────────────────────────────────────────────────
# ENUMS
# ─────────────────────────────────────────────────────────────────────────────

class AgentState(Enum):
    STANDBY = auto()
    RUNNING = auto()
    DONE    = auto()
    ERROR   = auto()

class FocusPanel(Enum):
    DISCOVERY  = 1
    DOWNLOAD   = 2
    RESILIENCE = 3
    SUMMARY    = 4

class ModalKind(Enum):
    NONE          = auto()
    QUIT_GRACEFUL = auto()
    QUIT_FORCE    = auto()
    RUN_AGAIN     = auto()

class PipelinePhase(Enum):
    DISCOVERY = auto()
    DOWNLOAD  = auto()
    DONE      = auto()


# ─────────────────────────────────────────────────────────────────────────────
# STATE DATACLASSES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ScrollState:
    offset:    int  = 0
    following: bool = True

    def scroll_up(self):
        self.offset   += 1
        self.following = False

    def scroll_down(self):
        self.offset = max(0, self.offset - 1)
        if self.offset == 0:
            self.following = True

    def tail(self):
        self.offset    = 0
        self.following = True


@dataclass
class WorkerSlot:
    idx:    int
    name:   str   = ""
    pct:    float = 0.0
    speed:  float = 0.0
    active: bool  = False


@dataclass
class CompletedDownload:
    name:   str
    size_mb: int
    source: str   = "unknown"  # query / channel / playlist / direct
    status: str   = "OK"       # OK / ERR / SKIP
    reason: str   = ""


@dataclass
class DiscoveryState:
    state:    AgentState = AgentState.STANDBY
    n_inputs: int        = 0
    types:    dict       = field(default_factory=dict)
    res_log:  deque      = field(default_factory=lambda: deque(maxlen=LOG_MAXLEN))
    resolved: dict       = field(default_factory=lambda: {
                               "query": 0, "channel": 0,
                               "playlist": 0, "direct": 0})
    skipped:  int   = 0
    errors:   int   = 0
    total:    int   = 0
    elapsed:  float = 0.0
    lock:     threading.Lock = field(default_factory=threading.Lock)


@dataclass
class DownloadState:
    state:      AgentState = AgentState.STANDBY
    workers:    List[WorkerSlot] = field(
        default_factory=lambda: [WorkerSlot(i + 1) for i in range(WORKER_MAX)])
    comp_log:   deque = field(default_factory=lambda: deque(maxlen=LOG_MAXLEN))
    completed:  List[CompletedDownload] = field(default_factory=list)
    done:       int   = 0
    total:      int   = 0
    errors:     int   = 0
    total_mb:   int   = 0
    speed:      float = 0.0
    eta:        float = 0.0
    lock:       threading.Lock = field(default_factory=threading.Lock)


@dataclass
class ResilienceState:
    state:      AgentState = AgentState.STANDBY
    log:        deque      = field(default_factory=lambda: deque(maxlen=LOG_MAXLEN))
    retries:    int   = 0
    failures:   int   = 0
    uptime_pct: float = 100.0
    lock:       threading.Lock = field(default_factory=threading.Lock)


@dataclass
class UIState:
    focus:        FocusPanel  = FocusPanel.DISCOVERY
    modal:        ModalKind   = ModalKind.NONE
    expanded:     bool        = False
    scroll:       dict        = field(default_factory=lambda: {
                                   FocusPanel.DISCOVERY:  ScrollState(),
                                   FocusPanel.DOWNLOAD:   ScrollState(),
                                   FocusPanel.RESILIENCE: ScrollState(),
                                   FocusPanel.SUMMARY:    ScrollState(),
                               })
    worker_slots: int         = WORKER_DEFAULT
    paused:       bool        = False
    events:       queue.Queue = field(default_factory=queue.Queue)
    lock:         threading.Lock = field(default_factory=threading.Lock)


@dataclass
class PipelineState:
    phase:      PipelinePhase  = PipelinePhase.DISCOVERY
    discovery:  DiscoveryState = field(default_factory=DiscoveryState)
    download:   DownloadState  = field(default_factory=DownloadState)
    resilience: ResilienceState= field(default_factory=ResilienceState)
    ui:         UIState        = field(default_factory=UIState)
    start_time: float          = field(default_factory=time.time)
    end_time:   float          = 0.0
    output_dir: str            = "./media"


# ─────────────────────────────────────────────────────────────────────────────
# KEYBOARD
# ─────────────────────────────────────────────────────────────────────────────

def _dispatch_key(ch: str, ui: UIState):
    c = ch.lower()
    if   ch == 'q': ui.events.put('quit_graceful')
    elif ch == 'Q': ui.events.put('quit_force')
    elif c  == 'p': ui.events.put('pause_toggle')
    elif ch == '+': ui.events.put('workers_inc')
    elif ch == '-': ui.events.put('workers_dec')
    elif ch == '1': ui.events.put('focus_1')
    elif ch == '2': ui.events.put('focus_2')
    elif ch == '3': ui.events.put('focus_3')
    elif ch == '4': ui.events.put('focus_4')
    elif c  == 'e': ui.events.put('expand_toggle')
    elif c  == 'o': ui.events.put('open_output')
    elif c  == 'x': ui.events.put('export_summary')
    elif c  == 'r': ui.events.put('run_again')
    elif c  == 'y': ui.events.put('modal_yes')
    elif c  == 'n': ui.events.put('modal_no')


def keyboard_thread(ui: UIState):
    if IS_WINDOWS:
        _kb_windows(ui)
    else:
        _kb_unix(ui)


def _kb_windows(ui: UIState):
    try:
        while True:
            ch = msvcrt.getwch()
            if ch in ('\x00', '\xe0'):
                scan = msvcrt.getwch()
                if   scan == 'H': ui.events.put('scroll_up')
                elif scan == 'P': ui.events.put('scroll_down')
                continue
            if ch == '\x03':
                ui.events.put('quit_force')
                continue
            _dispatch_key(ch, ui)
    except Exception:
        pass


def _kb_unix(ui: UIState):
    fd  = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        while True:
            ch = sys.stdin.read(1)
            if not ch:
                continue
            if ch == '\x1b':
                rest = sys.stdin.read(2)
                if   rest == '[A': ui.events.put('scroll_up')
                elif rest == '[B': ui.events.put('scroll_down')
                continue
            if ch == '\x03':
                ui.events.put('quit_force')
                continue
            _dispatch_key(ch, ui)
    except Exception:
        pass
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)


# ─────────────────────────────────────────────────────────────────────────────
# EVENT HANDLER
# ─────────────────────────────────────────────────────────────────────────────

def handle_events(ps: PipelineState, kill_flag: threading.Event,
                  worker_sem: threading.Semaphore,
                  restart_flag: threading.Event) -> bool:
    ui = ps.ui
    while not ui.events.empty():
        try:
            ev = ui.events.get_nowait()
        except queue.Empty:
            break

        with ui.lock:
            # modal responses
            if ev == 'modal_yes':
                if ui.modal == ModalKind.QUIT_GRACEFUL:
                    kill_flag.set()
                    ui.modal = ModalKind.NONE
                    return True
                elif ui.modal == ModalKind.QUIT_FORCE:
                    os._exit(0)
                elif ui.modal == ModalKind.RUN_AGAIN:
                    restart_flag.set()
                    ui.modal = ModalKind.NONE
                ui.modal = ModalKind.NONE
                continue
            if ev == 'modal_no':
                ui.modal = ModalKind.NONE
                continue

            if ui.modal != ModalKind.NONE:
                continue

            # quit
            if   ev == 'quit_graceful': ui.modal = ModalKind.QUIT_GRACEFUL
            elif ev == 'quit_force':    ui.modal = ModalKind.QUIT_FORCE

            # run again (only when done)
            elif ev == 'run_again':
                if ps.phase == PipelinePhase.DONE:
                    ui.modal = ModalKind.RUN_AGAIN

            # pause
            elif ev == 'pause_toggle':
                ui.paused = not ui.paused
                tag = "[PAUSE]" if ui.paused else "[RESUME]"
                msg = "Queue paused." if ui.paused else "Queue resumed."
                with ps.resilience.lock:
                    ps.resilience.log.append((tag, msg))

            # worker slots
            elif ev == 'workers_inc':
                if ui.worker_slots < WORKER_MAX:
                    ui.worker_slots += 1
                    worker_sem.release()
                    with ps.resilience.lock:
                        ps.resilience.log.append(
                            ("[CFG]", f"Workers → {ui.worker_slots}/{WORKER_MAX}"))
            elif ev == 'workers_dec':
                if ui.worker_slots > WORKER_MIN:
                    if worker_sem.acquire(blocking=False):
                        ui.worker_slots -= 1
                        with ps.resilience.lock:
                            ps.resilience.log.append(
                                ("[CFG]", f"Workers → {ui.worker_slots}/{WORKER_MAX}"))

            # focus
            elif ev == 'focus_1': ui.focus = FocusPanel.DISCOVERY
            elif ev == 'focus_2': ui.focus = FocusPanel.DOWNLOAD
            elif ev == 'focus_3': ui.focus = FocusPanel.RESILIENCE
            elif ev == 'focus_4':
                if ps.phase == PipelinePhase.DONE:
                    ui.focus = FocusPanel.SUMMARY

            # expand / scroll
            elif ev == 'expand_toggle': ui.expanded = not ui.expanded
            elif ev == 'scroll_up':
                ui.scroll[ui.focus].scroll_up()
            elif ev == 'scroll_down':
                ui.scroll[ui.focus].scroll_down()

            # output / export
            elif ev == 'open_output':
                _open_folder(ps.output_dir)
            elif ev == 'export_summary':
                if ps.phase == PipelinePhase.DONE:
                    _export_summary(ps)

    return False


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def ts() -> str:
    return time.strftime("%H:%M:%S")


def _fmt_duration(seconds: float) -> str:
    s = int(seconds)
    if s < 60:   return f"{s}s"
    if s < 3600: return f"{s//60}m {s%60:02d}s"
    return f"{s//3600}h {(s%3600)//60:02d}m"


def _open_folder(path: str):
    os.makedirs(path, exist_ok=True)
    try:
        if   sys.platform == "darwin": subprocess.Popen(["open",     path])
        elif sys.platform == "win32":  subprocess.Popen(["explorer", path])
        else:                          subprocess.Popen(["xdg-open", path])
    except Exception:
        pass


def _export_summary(ps: PipelineState):
    elapsed = (ps.end_time or time.time()) - ps.start_time
    summary = {
        "run_start":  time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(ps.start_time)),
        "duration_s": round(elapsed, 1),
        "discovery":  {"total_resolved": ps.discovery.total,
                       "skipped":        ps.discovery.skipped,
                       "errors":         ps.discovery.errors},
        "download":   {"total":    ps.download.total,
                       "done":     ps.download.done,
                       "errors":   ps.download.errors,
                       "total_mb": ps.download.total_mb},
        "resilience": {"retries":  ps.resilience.retries,
                       "failures": ps.resilience.failures},
        "files": [{"name": c.name, "size_mb": c.size_mb,
                   "source": c.source, "status": c.status,
                   "reason": c.reason}
                  for c in ps.download.completed],
    }
    fname = time.strftime("run_%Y%m%d_%H%M%S.json", time.localtime(ps.start_time))
    path  = os.path.join(ps.output_dir, fname)
    os.makedirs(ps.output_dir, exist_ok=True)
    with open(path, "w") as f:
        json.dump(summary, f, indent=2)
    with ps.resilience.lock:
        ps.resilience.log.append(("[EXPORT]", f"Summary → {path}"))


def _trim(log: deque, n: int, scroll: ScrollState) -> list:
    items = list(log)
    if scroll.following or scroll.offset == 0:
        return items[-n:] if len(items) > n else items
    end   = max(0, len(items) - scroll.offset)
    start = max(0, end - n)
    return items[start:end]


def _bar(pct: float, width: int, color: str, bg: str) -> Text:
    """Bar from percentage 0-100."""
    filled = max(0, min(width, int(pct / 100 * width)))
    t = Text()
    t.append("█" * filled,         style=color)
    t.append("░" * (width-filled), style=bg)
    return t


def _bar_n(n: int, cap: int, color: str, bg: str, width: int = 20) -> Text:
    """Bar from raw count n against a cap.
    cap==0 (direct/no cap): full bar if n>0, empty if n==0.
    Prevents over-filled bars regardless of actual count.
    """
    if cap <= 0:
        pct = 100.0 if n > 0 else 0.0
    else:
        pct = min(100.0, n / cap * 100.0)
    return _bar(pct, width, color, bg)


def _badge(state: AgentState, color: str) -> Text:
    t = Text()
    if   state == AgentState.STANDBY: t.append(" ○ STANDBY ", style=f"dim {color}")
    elif state == AgentState.RUNNING:  t.append(" ● RUNNING ", style=f"bold {color}")
    elif state == AgentState.DONE:     t.append(" ✓ DONE    ", style=f"dim {color}")
    elif state == AgentState.ERROR:    t.append(" ✗ ERROR   ", style=f"bold {C_RED}")
    return t


# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY SCREEN RENDERER
# ─────────────────────────────────────────────────────────────────────────────

def render_summary_large(ps: PipelineState, scroll: ScrollState,
                         height: int) -> Panel:
    """Summary screen — yellow themed, Table-based stat cards."""
    color   = C_YELLOW
    ds      = ps.discovery
    dl      = ps.download
    rs      = ps.resilience
    elapsed = (ps.end_time or time.time()) - ps.start_time

    # ── STAT CARDS — one Table row, 5 columns ──
    # Each card is a bordered cell: label / big value / sub
    cards = [
        ("DOWNLOADED", str(dl.done),
         f"of {dl.total} resolved", C_GREEN),
        ("ERRORS",     str(dl.errors),
         "age-restr / unavail",     C_RED),
        ("TOTAL SIZE",
         f"{dl.total_mb/1024:.1f} GB" if dl.total_mb >= 1024 else f"{dl.total_mb} MB",
         f"avg {dl.total_mb//max(dl.done,1)} MB/file", C_WHITE),
        ("DURATION",   _fmt_duration(elapsed),
         f"avg {dl.total_mb/max(elapsed,1):.0f} MB/s", C_CYAN),
        ("RETRIES",    str(rs.retries),
         "0 permanent failures",    C_MAGENTA),
    ]

    cards_table = Table(
        box=rbox.SIMPLE_HEAD,
        show_header=True,
        show_edge=False,
        padding=(0, 2),
        expand=True,
        border_style=f"dim {color}",
    )
    for label, val, sub, col in cards:
        # column header = label
        cards_table.add_column(
            Text(label, style=f"dim {col}"),
            justify="left",
            no_wrap=True,
        )

    # row 1: big values
    val_row = [Text(val, style=f"bold {col}") for _, val, _, col in cards]
    cards_table.add_row(*val_row)

    # row 2: sub labels
    sub_row = [Text(sub, style=f"dim {col}") for _, _, sub, col in cards]
    cards_table.add_row(*sub_row)

    # ── DISCOVERY BREAKDOWN ────────────────────
    disc_text = Text()
    disc_text.append("\n  DISCOVERY BREAKDOWN\n", style=f"dim {C_CYAN}")
    disc_text.append(f"  {'─'*66}\n",              style=f"dim {C_CYAN}")

    # caps per type — bar fills proportionally against cap
    # direct URLs have no cap → bar is full if any resolved, empty otherwise
    caps  = {"query": 10, "channel": 10, "playlist": 20, "direct": 0}
    BAR_W = 20
    with ds.lock:
        resolved = dict(ds.resolved)
        total    = ds.total
        skipped  = ds.skipped

    # ds.types = {type: (count_of_sources, cap_per_source)}
    for t in ["query", "channel", "playlist", "direct"]:
        n          = resolved.get(t, 0)
        label      = t.capitalize().ljust(10)
        cap_each   = caps[t]
        n_sources  = ds.types.get(t, (1, cap_each))[0] if ds.types else 1
        # total possible = n_sources × cap_each
        total_cap  = n_sources * cap_each if cap_each > 0 else 0
        note       = "no cap" if t == "direct" else f"cap:{cap_each}/src"
        disc_text.append(f"  {label}", style=f"dim {C_CYAN}")
        disc_text.append(f" {str(n).rjust(3)} ", style=f"bold {C_CYAN}")
        disc_text.append_text(_bar_n(n, total_cap, C_CYAN, "color(23)", width=BAR_W))
        disc_text.append(f"  {note}\n", style=f"dim {C_CYAN}")

    disc_text.append(f"  {'─'*66}\n", style=f"dim {C_CYAN}")
    disc_text.append("  Total resolved: ", style=f"dim {C_CYAN}")
    disc_text.append(f"{total}",           style=f"bold {C_CYAN}")
    disc_text.append("   skipped: ",       style=f"dim {C_CYAN}")
    disc_text.append(f"{skipped}",         style=f"bold {C_CYAN}")
    disc_text.append("   parse errors: ",  style=f"dim {C_CYAN}")
    disc_text.append(f"{ds.errors}\n",     style=f"bold {C_CYAN}")

    # ── DOWNLOAD RESULTS + OUTPUT side by side ─
    # Left: scrollable results table  Right: output info
    with dl.lock:
        completed = list(dl.completed)

    fixed_lines = 18  # cards(4) + breakdown(8) + headers(3) + padding
    log_height  = max(3, height - fixed_lines - 4)

    sc = scroll
    if sc.following or sc.offset == 0:
        visible = completed[-log_height:] if len(completed) > log_height else completed
    else:
        end     = max(0, len(completed) - sc.offset)
        start   = max(0, end - log_height)
        visible = completed[start:end]

    # Build results as Text (left column of the bottom table)
    dl_text = Text()
    dl_text.append(f"DOWNLOAD RESULTS\n", style=f"dim {color}")
    dl_text.append(f"{'─'*52}\n",          style=f"dim {color}")
    dl_text.append(f"{'VIDEO':<30}", style=f"dim {color}")
    dl_text.append(f"{'SIZE':>7}",   style=f"dim {color}")
    dl_text.append(f"  {'SRC':<10}", style=f"dim {color}")
    dl_text.append(f"  ST\n",        style=f"dim {color}")
    dl_text.append(f"{'─'*52}\n",    style=f"dim {color}")

    for c in visible:
        name_s = (c.name[:28] + "…") if len(c.name) > 29 else c.name
        size_s = f"{c.size_mb}MB" if c.size_mb else "—"
        src_s  = c.source[:9]
        if   c.status == "OK":  st_s = f"bold {C_GREEN}"
        elif c.status == "ERR": st_s = f"bold {C_RED}"
        else:                   st_s = "dim white"
        row_s = C_WHITE if c.status == "OK" else C_DIM
        dl_text.append(f"{name_s:<30}", style=row_s)
        dl_text.append(f"{size_s:>7}",  style=f"dim {color}")
        dl_text.append(f"  {src_s:<10}",style="dim white")
        dl_text.append(f"  {c.status}\n", style=st_s)

    if not sc.following and len(completed) > log_height:
        dl_text.append(f"↑ scrolled  (↓ to follow)\n", style=f"dim {color}")

    # Build output info as Text (right column)
    fname = time.strftime("run_%Y%m%d_%H%M%S.json",
                          time.localtime(ps.start_time))
    out_text = Text()
    out_text.append(f"OUTPUT\n",   style=f"dim {color}")
    out_text.append(f"{'─'*28}\n", style=f"dim {color}")
    out_text.append("Directory\n", style=f"dim {color}")
    out_text.append(f"{os.path.abspath(ps.output_dir)}\n\n", style=color)
    out_text.append("Summary file\n", style=f"dim {color}")
    out_text.append(f"{fname}\n\n",   style=color)
    # quick stats recap
    out_text.append(f"{'─'*28}\n",     style=f"dim {color}")
    out_text.append("downloaded  ", style=f"dim {color}")
    out_text.append(f"{dl.done}\n", style=f"bold {C_GREEN}")
    out_text.append("errors      ", style=f"dim {color}")
    out_text.append(f"{dl.errors}\n", style=f"bold {C_RED}" if dl.errors else f"bold {color}")
    out_text.append("total size  ", style=f"dim {color}")
    size_val = (f"{dl.total_mb/1024:.1f} GB"
                if dl.total_mb >= 1024 else f"{dl.total_mb} MB")
    out_text.append(f"{size_val}\n", style=f"bold {color}")
    out_text.append("duration    ", style=f"dim {color}")
    out_text.append(f"{_fmt_duration(elapsed)}\n", style=f"bold {color}")

    # side-by-side table
    bottom_table = Table(
        box=None,
        show_header=False,
        show_edge=False,
        padding=(0, 1),
        expand=True,
    )
    bottom_table.add_column(ratio=68)   # results left
    bottom_table.add_column(ratio=32)   # output right
    bottom_table.add_row(dl_text, out_text)

    completed_ts = time.strftime(
        "%H:%M:%S", time.localtime(ps.end_time or time.time()))

    return Panel(
        Group(cards_table, disc_text, bottom_table),
        title=Text.assemble(("  RUN SUMMARY  ", f"bold {color}")),
        title_align="left",
        subtitle=Text.assemble((
            f" run #0001  ─  completed {completed_ts} ",
            f"dim {color}")),
        subtitle_align="right",
        border_style=color,
        padding=(0, 0),
    )


def render_summary_sidebar_panel(label: str, color: str,
                                 state: AgentState,
                                 lines: list, footer_l: str,
                                 footer_r: str) -> Panel:
    """Compact done-state panel for the summary right column."""
    body = Text()
    for (tag, msg) in lines[-6:]:
        if tag in ("[ERR]", "[WARN]"):
            tag_s = f"bold {C_RED}"
        else:
            tag_s = f"dim {color}"
        short = (msg[:28] + "…") if len(msg) > 29 else msg
        body.append(f"  {tag:>8} ", style=tag_s)
        body.append(f"{short}\n",   style=f"dim white")

    return Panel(
        body,
        title=Text.assemble((f" {label} ", f"bold dim {color}")),
        title_align="left",
        subtitle=_badge(state, color),
        subtitle_align="right",
        border_style=f"dim {color}",
        padding=(0, 0),
    )


# ─────────────────────────────────────────────────────────────────────────────
# AGENT PANEL RENDERERS  (unchanged from v2, reproduced for completeness)
# ─────────────────────────────────────────────────────────────────────────────

def render_discovery_large(ds: DiscoveryState, scroll: ScrollState,
                           height: int) -> Panel:
    color  = C_CYAN
    dim_bg = "color(23)"
    lbl    = f"dim {color}"
    body   = Text()

    body.append("  INPUT PARSING\n", style=lbl)
    body.append(f"  {'─'*54}\n",      style=f"dim {color}")
    if ds.n_inputs:
        body.append("  [PARSE] ", style=f"bold {color}")
        body.append(f"Loaded queries — {ds.n_inputs} inputs found\n", style=C_WHITE)
        for t, (cnt, cap) in ds.types.items():
            cap_s = "no cap" if t == "direct" else f"cap:{cap}"
            body.append("  [TYPE]  ", style=f"bold {color}")
            body.append(f"{cnt}× {t:<10} {cap_s}\n", style=C_WHITE)
    else:
        body.append("  Waiting for input file...\n", style=C_DIM)

    body.append(f"\n  RESOLUTION LOG\n", style=lbl)
    body.append(f"  {'─'*54}\n",         style=f"dim {color}")

    parse_lines   = 3 + len(ds.types)
    summary_lines = 8
    log_height    = max(3, height - parse_lines - summary_lines - 7)

    with ds.lock:
        lines = _trim(ds.res_log, log_height, scroll)

    if lines:
        for (tag, msg) in lines:
            body.append(f"  {tag:>8} ", style=f"bold {color}")
            body.append(f"{msg}\n",     style=C_WHITE)
    else:
        body.append("  Waiting for resolution...\n", style=C_DIM)

    if not scroll.following:
        body.append(f"  ↑ scrolled  (↓ to follow tail)\n", style=f"dim {color}")

    body.append(f"\n  QUEUE SUMMARY\n", style=lbl)
    body.append(f"  {'─'*54}\n",         style=f"dim {color}")

    caps = {"query": 10, "channel": 10, "playlist": 20, "direct": 0}
    maxn = 20

    with ds.lock:
        resolved = dict(ds.resolved)
        total    = ds.total
        skipped  = ds.skipped
        errors   = ds.errors

    for t in ["query", "channel", "playlist", "direct"]:
        n     = resolved.get(t, 0)
        label = t.capitalize().ljust(9)
        note  = "no cap" if t == "direct" else f"cap:{caps[t]}"
        body.append(f"  {label}", style=f"dim {color}")
        body.append(f" {str(n).rjust(3)} ", style=f"bold {color}")
        body.append_text(_bar(n, maxn, color, dim_bg))
        body.append(f"  {note}\n", style=f"dim {color}")

    body.append(f"  {'─'*54}\n",     style=f"dim {color}")
    body.append("  TOTAL ENQUEUED: ", style=f"dim {color}")
    body.append(f"{total}",           style=f"bold {color}")
    body.append(" → dispatching to download workers\n", style=f"dim {color}")

    return Panel(
        body,
        title=Text.assemble(("  AGENT 01 — DISCOVERY  ", f"bold {color}")),
        title_align="left",
        subtitle=_badge(ds.state, color),
        subtitle_align="right",
        border_style=color if ds.state == AgentState.RUNNING else f"dim {color}",
        padding=(0, 0),
    )


def render_download_large(dl: DownloadState, ui: UIState,
                          scroll: ScrollState, height: int) -> Panel:
    color  = C_GREEN
    dim_bg = "color(22)"
    lbl    = f"dim {color}"
    body   = Text()

    with dl.lock:
        workers = [(w.idx, w.name, w.pct, w.speed, w.active)
                   for w in dl.workers]
        done    = dl.done
        total   = dl.total
        errors  = dl.errors
        speed   = dl.speed
        eta     = dl.eta

    pause_s = "  [PAUSED]" if ui.paused else ""
    body.append(f"  WORKERS  {ui.worker_slots}/{WORKER_MAX} slots{pause_s}\n", style=lbl)
    body.append(f"  {'─'*54}\n", style=f"dim {color}")

    for idx, name, pct, spd, active in workers[:ui.worker_slots]:
        if not active:
            body.append(f"  [W{idx}] ", style=f"bold dim {color}")
            body.append("idle — awaiting job\n", style=C_DIM)
        else:
            short = (name[:42] + "…") if len(name) > 43 else name.ljust(43)
            body.append(f"  [W{idx}] ", style=f"bold {color}")
            body.append(f"{short}", style=C_WHITE)
            body.append(f"  {spd:5.1f} MB/s\n", style=f"dim {color}")
            body.append("        ")
            body.append_text(_bar(pct, 30, color, dim_bg))
            body.append(f"  {pct:5.1f}%\n", style=f"bold {color}")

    body.append(f"\n  COMPLETIONS\n", style=lbl)
    body.append(f"  {'─'*54}\n",       style=f"dim {color}")

    worker_lines = ui.worker_slots * 2 + 3
    log_height   = max(3, height - worker_lines - 7)

    with dl.lock:
        lines = _trim(dl.comp_log, log_height, scroll)

    if lines:
        for (tag, msg) in lines:
            tag_s = f"bold {color}" if tag == "[OK]" else f"bold {C_RED}"
            body.append(f"  {tag:>6} ", style=tag_s)
            body.append(f"{msg}\n",     style=C_WHITE)
    else:
        body.append("  No completions yet...\n", style=C_DIM)

    if not scroll.following:
        body.append(f"  ↑ scrolled  (↓ to follow tail)\n", style=f"dim {color}")

    return Panel(
        body,
        title=Text.assemble(("  AGENT 02 — DOWNLOAD  ", f"bold {color}")),
        title_align="left",
        subtitle=_badge(dl.state, color),
        subtitle_align="right",
        border_style=color if dl.state == AgentState.RUNNING else f"dim {color}",
        padding=(0, 0),
    )


def render_resilience_large(rs: ResilienceState, scroll: ScrollState,
                            height: int) -> Panel:
    color = C_MAGENTA
    lbl   = f"dim {color}"
    body  = Text()

    body.append(f"  WATCHDOG LOG\n",  style=lbl)
    body.append(f"  {'─'*54}\n",       style=f"dim {color}")

    log_height = max(3, height - 8)
    with rs.lock:
        lines    = _trim(rs.log, log_height, scroll)
        retries  = rs.retries
        failures = rs.failures

    if lines:
        for (tag, msg) in lines:
            tag_s = (f"bold {C_RED}" if tag in ("[WARN]", "[ERR]")
                     else f"bold dim {color}")
            body.append(f"  {tag:>9} ", style=tag_s)
            body.append(f"{msg}\n",     style=C_WHITE)
    else:
        body.append("  Watchdog standing by...\n", style=C_DIM)

    if not scroll.following:
        body.append(f"  ↑ scrolled  (↓ to follow tail)\n", style=f"dim {color}")

    body.append(f"\n  {'─'*54}\n",     style=f"dim {color}")
    body.append("  retries: ",          style=f"dim {color}")
    body.append(f"{retries}",           style=f"bold {color}")
    body.append("   failures: ",        style=f"dim {color}")
    body.append(f"{failures}\n",
                style=f"bold {C_RED}" if failures else f"bold {color}")

    return Panel(
        body,
        title=Text.assemble(("  AGENT 03 — RESILIENCE  ", f"bold {color}")),
        title_align="left",
        subtitle=_badge(rs.state, color),
        subtitle_align="right",
        border_style=color,
        padding=(0, 0),
    )


def render_small(label: str, state: AgentState,
                 log: deque, color: str, height: int) -> Panel:
    body       = Text()
    log_height = max(2, height - 4)

    if state == AgentState.STANDBY:
        body.append("\n  Waiting to start...\n", style=f"dim {color}")
        body.append("  Standing by.\n",           style=f"dim {color}")
    else:
        lines = list(log)[-log_height:]
        for (tag, msg) in lines:
            tag_s = (f"bold {C_RED}" if tag in ("[WARN]", "[ERR]")
                     else f"bold {'dim ' if state == AgentState.DONE else ''}{color}")
            short = (msg[:28] + "…") if len(msg) > 29 else msg
            body.append(f"  {tag:>8} ", style=tag_s)
            body.append(f"{short}\n",
                        style=C_WHITE if state == AgentState.RUNNING else C_DIM)

    border = color if state == AgentState.RUNNING else f"dim {color}"
    return Panel(
        body,
        title=Text.assemble((f" {label} ", f"bold {border}")),
        title_align="left",
        subtitle=_badge(state, color),
        subtitle_align="right",
        border_style=border,
        padding=(0, 0),
    )


# ─────────────────────────────────────────────────────────────────────────────
# MODAL
# ─────────────────────────────────────────────────────────────────────────────

def render_modal(kind: ModalKind) -> Panel:
    t = Text(justify="center")
    t.append("\n")
    if kind == ModalKind.QUIT_GRACEFUL:
        t.append("  GRACEFUL QUIT\n\n",      style=f"bold {C_CYAN}")
        t.append("  Finish active downloads then exit.\n", style=C_WHITE)
        t.append("  Remaining queue will be abandoned.\n\n", style=C_DIM)
    elif kind == ModalKind.QUIT_FORCE:
        t.append("  FORCE QUIT\n\n",         style=f"bold {C_RED}")
        t.append("  Kill immediately. Active downloads\n", style=C_WHITE)
        t.append("  will be incomplete / corrupted.\n\n",  style=C_DIM)
    elif kind == ModalKind.RUN_AGAIN:
        t.append("  RUN AGAIN\n\n",           style=f"bold {C_GREEN}")
        t.append("  Start a new pipeline run with the\n", style=C_WHITE)
        t.append("  same configuration?\n\n", style=C_DIM)
    t.append("  [y] confirm     [n] cancel\n", style=f"bold {C_WHITE}")
    return Panel(t,
                 border_style=(C_RED if kind == ModalKind.QUIT_FORCE
                                else C_CYAN if kind == ModalKind.QUIT_GRACEFUL
                                else C_GREEN),
                 padding=(0, 4))


# ─────────────────────────────────────────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────────────────────────────────────────

def render_footer(ps: PipelineState) -> Text:
    ui = ps.ui
    t  = Text()

    def key(k, active=False):
        style = "bold bright_green on color(22)" if active else "bold white on color(235)"
        t.append(f" {k} ", style=style)

    def label(l):
        t.append(f" {l} ", style="dim white")

    def sep():
        t.append(" │ ", style="dim color(240)")

    if ui.modal != ModalKind.NONE:
        key("y"); label("confirm"); sep(); key("n"); label("cancel")
        return t

    # quit always available
    key("q"); label("quit"); sep()
    key("Q"); label("force quit"); sep()

    if ps.phase == PipelinePhase.DONE:
        key("4", active=ui.focus == FocusPanel.SUMMARY); label("summary"); sep()

    key("1", active=ui.focus == FocusPanel.DISCOVERY);  label("discovery"); sep()
    key("2", active=ui.focus == FocusPanel.DOWNLOAD);   label("download");  sep()
    key("3", active=ui.focus == FocusPanel.RESILIENCE); label("resilience"); sep()

    if ps.phase == PipelinePhase.DOWNLOAD:
        pause_l = "resume" if ui.paused else "pause"
        key("p"); label(pause_l); sep()
        key("+"); label("workers+"); sep()
        key("-"); label("workers-"); sep()

    key("e"); label("expand"); sep()
    key("↑↓"); label("scroll")

    if ps.phase == PipelinePhase.DONE:
        sep(); key("o"); label("open folder")
        sep(); key("x"); label("export")
        sep(); key("r"); label("run again")

    return t


# ─────────────────────────────────────────────────────────────────────────────
# LAYOUT BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_layout(ps: PipelineState, console: Console) -> Layout:
    width, height = console.size
    ui            = ps.ui
    body_h        = height - 2   # topbar + footer

    root = Layout()
    root.split_column(
        Layout(name="topbar", size=1),
        Layout(name="body"),
        Layout(name="footer", size=1),
    )

    # ── TOPBAR ────────────────────────────────
    elapsed = (ps.end_time or time.time()) - ps.start_time
    topbar  = Text(overflow="ellipsis", no_wrap=True)
    topbar.append(" ▌ PIPELINE ", style="bold white")
    topbar.append("│ ", style="dim white")

    phase_colors = {PipelinePhase.DISCOVERY: C_CYAN,
                    PipelinePhase.DOWNLOAD:  C_GREEN,
                    PipelinePhase.DONE:      C_YELLOW}
    pc = phase_colors[ps.phase]

    if ps.phase == PipelinePhase.DONE:
        topbar.append(" ✓ COMPLETE ", style=f"bold {C_YELLOW}")
    else:
        topbar.append(f" {ps.phase.name} ", style=f"bold {pc}")

    topbar.append("│ ", style="dim white")
    topbar.append(f"workers:{ui.worker_slots}/{WORKER_MAX} ", style="dim white")
    if ui.paused:
        topbar.append("[PAUSED] ", style=f"bold {C_RED}")
    topbar.append("│ ", style="dim white")
    topbar.append(f"elapsed:{_fmt_duration(elapsed)} ", style="dim white")
    topbar.append("│ ", style="dim white")
    topbar.append(ts(), style="dim white")
    root["topbar"].update(topbar)

    # ── FOOTER ────────────────────────────────
    root["footer"].update(render_footer(ps))

    # ── MODAL ─────────────────────────────────
    if ui.modal != ModalKind.NONE:
        root["body"].update(Align.center(render_modal(ui.modal), vertical="middle"))
        return root

    # ── EXPANDED ──────────────────────────────
    if ui.expanded:
        scr = ui.scroll[ui.focus]
        if   ui.focus == FocusPanel.DISCOVERY:  p = render_discovery_large(ps.discovery, scr, body_h)
        elif ui.focus == FocusPanel.DOWNLOAD:   p = render_download_large(ps.download, ui, scr, body_h)
        elif ui.focus == FocusPanel.RESILIENCE: p = render_resilience_large(ps.resilience, scr, body_h)
        elif ui.focus == FocusPanel.SUMMARY:    p = render_summary_large(ps, scr, body_h)
        else:                                   p = render_summary_large(ps, scr, body_h)
        root["body"].update(p)
        return root

    # ── SUMMARY SCREEN (4) ────────────────────
    if ui.focus == FocusPanel.SUMMARY and ps.phase == PipelinePhase.DONE:
        root["body"].split_row(
            Layout(name="left",  ratio=68),
            Layout(name="right", ratio=32),
        )
        root["body"]["right"].split_column(
            Layout(name="rt"),
            Layout(name="rm"),
            Layout(name="rb"),
        )
        panel_h = body_h // 3

        root["body"]["left"].update(
            render_summary_large(ps, ui.scroll[FocusPanel.SUMMARY], body_h))

        with ps.discovery.lock:
            disc_log = deque(ps.discovery.res_log)
        with ps.download.lock:
            dl_log = deque(ps.download.comp_log)
        with ps.resilience.lock:
            res_log = deque(ps.resilience.log)

        root["body"]["rt"].update(
            render_summary_sidebar_panel(
                "DISCOVERY", C_CYAN, ps.discovery.state,
                list(disc_log)[-6:],
                f"resolved:{ps.discovery.total}",
                f"skipped:{ps.discovery.skipped}"))
        root["body"]["rm"].update(
            render_summary_sidebar_panel(
                "DOWNLOAD", C_GREEN, ps.download.state,
                list(dl_log)[-6:],
                f"done:{ps.download.done}/{ps.download.total}",
                f"err:{ps.download.errors}"))
        root["body"]["rb"].update(
            render_summary_sidebar_panel(
                "RESILIENCE", C_MAGENTA, ps.resilience.state,
                list(res_log)[-6:],
                f"retries:{ps.resilience.retries}",
                f"failures:{ps.resilience.failures}"))
        return root

    # ── NORMAL 68/32 SPLIT ────────────────────
    root["body"].split_row(
        Layout(name="left",  ratio=68),
        Layout(name="right", ratio=32),
    )
    root["body"]["right"].split_column(
        Layout(name="rt"),
        Layout(name="rb"),
    )
    panel_h = body_h // 2
    focus   = ui.focus

    scr = ui.scroll.get(focus, ScrollState())

    def _small_disc():
        with ps.discovery.lock:
            log_copy = deque(ps.discovery.res_log)
        return render_small("DISCOVERY",  ps.discovery.state,  log_copy, C_CYAN,    panel_h)

    def _small_dl():
        with ps.download.lock:
            log_copy = deque(ps.download.comp_log)
        return render_small("DOWNLOAD",   ps.download.state,   log_copy, C_GREEN,   panel_h)

    def _small_res():
        with ps.resilience.lock:
            log_copy = deque(ps.resilience.log)
        return render_small("RESILIENCE", ps.resilience.state, log_copy, C_MAGENTA, panel_h)

    if focus == FocusPanel.DISCOVERY or focus == FocusPanel.SUMMARY:
        root["body"]["left"].update(render_discovery_large(ps.discovery, scr, body_h))
        root["body"]["rt"].update(_small_dl())
        root["body"]["rb"].update(_small_res())
    elif focus == FocusPanel.DOWNLOAD:
        root["body"]["left"].update(render_download_large(ps.download, ui, scr, body_h))
        root["body"]["rt"].update(_small_disc())
        root["body"]["rb"].update(_small_res())
    elif focus == FocusPanel.RESILIENCE:
        root["body"]["left"].update(render_resilience_large(ps.resilience, scr, body_h))
        root["body"]["rt"].update(_small_disc())
        root["body"]["rb"].update(_small_dl())

    return root


# ─────────────────────────────────────────────────────────────────────────────
# MOCK PIPELINE  — replace with real yt-dlp logic
# ─────────────────────────────────────────────────────────────────────────────

def run_discovery(ps: PipelineState):
    ds = ps.discovery
    ds.state    = AgentState.RUNNING
    ds.n_inputs = 6
    ds.types    = {"query":(2,10),"channel":(1,10),"playlist":(2,20),"direct":(1,0)}
    start       = time.time()

    sources = [
        ("query",    "[QUERY]", '"ghana news today"',   10),
        ("query",    "[QUERY]", '"accra morning news"', 10),
        ("channel",  "[CHAN]",  "/c/GHNews",             10),
        ("playlist", "[LIST]",  "PLxxxx",                18),
        ("playlist", "[WARN]",  "PLyyyy → private, skipping", 0),
        ("direct",   "[URL]",   "youtu.be/abc123 → verified", 1),
    ]
    for qtype, tag, label, n in sources:
        time.sleep(0.5)
        with ds.lock: ds.res_log.append((tag, f"{label} → resolving..."))
        time.sleep(0.7)
        if n > 0:
            with ds.lock:
                ds.res_log.append((tag, f"{label} → {n} URLs resolved"))
                ds.resolved[qtype] = ds.resolved.get(qtype, 0) + n
                ds.total = sum(ds.resolved.values())
        else:
            with ds.lock:
                ds.res_log.append((tag, f"{label}"))
                ds.skipped += 1
        with ds.lock:
            ds.elapsed = time.time() - start

    with ds.lock:
        ds.res_log.append(("[META]", "Fetching metadata for all resolved URLs..."))
    time.sleep(1.0)
    with ds.lock:
        ds.res_log.append(("[META]", f"Metadata collected → {ds.total} JSON records ready"))
        ds.res_log.append(("[DONE]", f"Discovery complete. {ds.total} items dispatched."))
    ds.state   = AgentState.DONE
    ps.phase   = PipelinePhase.DOWNLOAD
    ps.ui.focus = FocusPanel.DOWNLOAD


MOCK_VIDEOS = [
    ("Ga News Bulletin #441",    284, "channel"),
    ("Accra Metro Report Live",  512, "playlist"),
    ("GTV Evening News 13-Mar",  611, "query"),
    ("GBC 9 OClock News",        312, "query"),
    ("Morning Starr 103.5 Clip",  97, "channel"),
    ("Joy FM Super Morning",     189, "query"),
    ("Citi FM Morning Drive",    203, "playlist"),
    ("TV3 Midday News",          445, "query"),
    ("Parliament Wrap Mar 14",   178, "direct"),
    ("Joy News At 7",            290, "channel"),
    ("GHOne TV Morning Show",    388, "channel"),
    ("Starr FM Drive Time",      221, "query"),
    ("Metro Morning Brief",      165, "playlist"),
    ("Citi Newsroom PM Edition", 334, "query"),
    ("GTV Breakfast Show",       412, "playlist"),
    ("Adom TV News Night",         0, "channel"),   # will ERR
    ("Parliament Session 14-Mar",  0, "query"),      # will ERR
]


def run_download(ps: PipelineState, worker_sem: threading.Semaphore):
    dl  = ps.download
    rs  = ps.resilience
    ui  = ps.ui
    total = ps.discovery.total

    with dl.lock:
        dl.total = total
        dl.state = AgentState.RUNNING

    vid_list = (MOCK_VIDEOS * ((total // len(MOCK_VIDEOS)) + 1))[:total]
    vid_queue = list(vid_list)
    q_lock    = threading.Lock()

    def worker(slot_idx: int):
        while True:
            while ui.paused:
                time.sleep(0.2)
            worker_sem.acquire()
            try:
                with q_lock:
                    if not vid_queue:
                        return
                    name, size_mb, source = vid_queue.pop(0)

                ok = size_mb > 0

                with dl.lock:
                    w        = dl.workers[slot_idx]
                    w.name   = name
                    w.pct    = 0.0
                    w.speed  = 0.0
                    w.active = True

                steps = 20
                for i in range(steps):
                    while ui.paused:
                        time.sleep(0.2)
                    time.sleep(0.18)
                    pct = (i + 1) / steps * 100
                    spd = 14 + slot_idx * 2 + (i % 4)
                    with dl.lock:
                        w.pct   = pct
                        w.speed = spd

                    if i == 7 and slot_idx == 1 and dl.done < 2:
                        with rs.lock:
                            rs.log.append(("[WARN]", f"W{slot_idx+1} stall >2s — retry"))
                            rs.retries += 1
                        time.sleep(0.4)
                        with rs.lock:
                            rs.log.append(("[OK]", f"W{slot_idx+1} resumed"))

                tag = "[OK]" if ok else "[ERR]"
                msg = f"{name[:30]}  {size_mb}MB  meta:{'OK' if ok else 'FAIL'}"

                cd = CompletedDownload(
                    name=name, size_mb=size_mb, source=source,
                    status="OK" if ok else "ERR",
                    reason="" if ok else "age-restricted")

                with dl.lock:
                    dl.comp_log.append((tag, msg))
                    dl.completed.append(cd)
                    w.active  = False
                    w.pct     = 100.0
                    dl.done  += 1
                    if not ok: dl.errors += 1
                    dl.total_mb += size_mb
                    active_speeds = [x.speed for x in dl.workers if x.active]
                    dl.speed = sum(active_speeds)
                    rem = dl.total - dl.done
                    dl.eta = (rem / max(len(active_speeds), 1) * 3.6)

                with rs.lock:
                    rs.log.append(("[HBEAT]", f"DL {dl.done}/{dl.total}"))
            finally:
                worker_sem.release()

    with ThreadPoolExecutor(max_workers=WORKER_MAX) as pool:
        futs = [pool.submit(worker, i) for i in range(WORKER_MAX)]
        for f in as_completed(futs):
            try:
                f.result()
            except Exception as exc:
                with rs.lock:
                    rs.log.append(("[ERR]", f"Worker exception: {exc}"))

    with dl.lock:
        dl.state = AgentState.DONE
        dl.eta   = 0

    ps.end_time = time.time()
    ps.phase    = PipelinePhase.DONE
    ps.ui.focus = FocusPanel.SUMMARY   # ← auto-navigate to summary

    with rs.lock:
        rs.log.append(("[DONE]", "All downloads complete. Pipeline finished."))


def run_resilience_watchdog(ps: PipelineState, stop_evt: threading.Event):
    rs = ps.resilience
    rs.state = AgentState.RUNNING
    with rs.lock:
        rs.log.append(("[INIT]",  "Watchdog started"))
        rs.log.append(("[WATCH]", "Monitoring all pipeline agents"))

    while not stop_evt.is_set():
        time.sleep(10)
        if stop_evt.is_set():
            break
        disc_s = ps.discovery.state.name
        dl_s   = ps.download.state.name
        with rs.lock:
            rs.log.append(("[HBEAT]", f"DISC:{disc_s}  DL:{dl_s}  uptime:100%"))

    rs.state = AgentState.DONE


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    console      = Console()
    ps           = PipelineState()
    kill_flag    = threading.Event()
    restart_flag = threading.Event()
    stop_watch   = threading.Event()
    worker_sem   = threading.Semaphore(WORKER_DEFAULT)

    kb = threading.Thread(target=keyboard_thread, args=(ps.ui,), daemon=True)
    kb.start()

    def pipeline():
        t_watch = threading.Thread(
            target=run_resilience_watchdog, args=(ps, stop_watch), daemon=True)
        t_watch.start()
        run_discovery(ps)
        if not kill_flag.is_set():
            run_download(ps, worker_sem)
        stop_watch.set()
        t_watch.join(timeout=2)

    t_pipe = threading.Thread(target=pipeline, daemon=True)
    t_pipe.start()

    with Live(
        build_layout(ps, console),
        console=console,
        screen=True,
        refresh_per_second=REFRESH_RATE,
    ) as live:
        while True:
            should_exit = handle_events(ps, kill_flag, worker_sem, restart_flag)
            live.update(build_layout(ps, console))
            time.sleep(1 / REFRESH_RATE)

            if should_exit or kill_flag.is_set():
                break

            if restart_flag.is_set():
                # TODO: reset state and rerun — placeholder
                restart_flag.clear()
                break


if __name__ == "__main__":
    main()