"""
Multi-Agent Pipeline TUI  v2
============================
Agents   : Discovery  (cyan)  →  Download  (green)  →  Resilience  (magenta)
Layout   : Active agent = large left panel (~68%)
           Inactive two = stacked right (~32%)
           Resilience always on the right

Shortcuts:
  q        graceful quit  (confirm)     Q   force quit     (confirm)
  p        pause / resume queue
  + / -    worker slots  (1–6 hard cap)
  1 2 3    focus panel   (decouple spotlight from active agent)
  e        expand focused panel full-screen  (toggle)
  ↑ ↓      scroll log of focused panel
  o        open output folder in file manager
  x        export run summary  (when done)
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

# ── platform detection ────────────────────────────────────────────────────────
IS_WINDOWS = sys.platform == "win32"

if IS_WINDOWS:
    import msvcrt
else:
    import termios
    import tty

from rich.console import Console
from rich.layout  import Layout
from rich.live    import Live
from rich.panel   import Panel
from rich.text    import Text
from rich.align   import Align


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

REFRESH_RATE  = 8
LOG_MAXLEN    = 500
WORKER_MIN    = 1
WORKER_MAX    = 6
WORKER_DEFAULT= 4

C_CYAN    = "bright_cyan"
C_GREEN   = "bright_green"
C_MAGENTA = "magenta"
C_RED     = "bright_red"
C_WHITE   = "white"
C_DIM     = "dim white"


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

class ModalKind(Enum):
    NONE            = auto()
    QUIT_GRACEFUL   = auto()
    QUIT_FORCE      = auto()

class PipelinePhase(Enum):
    DISCOVERY = auto()
    DOWNLOAD  = auto()
    DONE      = auto()


# ─────────────────────────────────────────────────────────────────────────────
# SHARED STATE
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ScrollState:
    offset:    int  = 0   # lines from bottom (0 = follow tail)
    following: bool = True

    def scroll_up(self):
        self.offset    += 1
        self.following  = False

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
    state:   AgentState  = AgentState.STANDBY
    workers: List[WorkerSlot] = field(
        default_factory=lambda: [WorkerSlot(i + 1) for i in range(WORKER_MAX)])
    comp_log: deque = field(default_factory=lambda: deque(maxlen=LOG_MAXLEN))
    done:    int    = 0
    total:   int    = 0
    errors:  int    = 0
    speed:   float  = 0.0
    eta:     float  = 0.0
    lock:    threading.Lock = field(default_factory=threading.Lock)


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
    # which panel is spotlighted in the large slot
    focus:          FocusPanel  = FocusPanel.DISCOVERY
    # modal
    modal:          ModalKind   = ModalKind.NONE
    # expand mode (focused panel fills full screen)
    expanded:       bool        = False
    # per-panel scroll
    scroll:         dict        = field(default_factory=lambda: {
                                      FocusPanel.DISCOVERY:  ScrollState(),
                                      FocusPanel.DOWNLOAD:   ScrollState(),
                                      FocusPanel.RESILIENCE: ScrollState(),
                                  })
    # worker semaphore count (user-visible)
    worker_slots:   int         = WORKER_DEFAULT
    # pause flag
    paused:         bool        = False
    # events from keyboard thread → main loop
    events:         queue.Queue = field(default_factory=queue.Queue)
    lock:           threading.Lock = field(default_factory=threading.Lock)


@dataclass
class PipelineState:
    phase:      PipelinePhase = PipelinePhase.DISCOVERY
    discovery:  DiscoveryState  = field(default_factory=DiscoveryState)
    download:   DownloadState   = field(default_factory=DownloadState)
    resilience: ResilienceState = field(default_factory=ResilienceState)
    ui:         UIState         = field(default_factory=UIState)
    start_time: float           = field(default_factory=time.time)
    output_dir: str             = "./media"


# ─────────────────────────────────────────────────────────────────────────────
# KEYBOARD INPUT  (runs in its own daemon thread)
# ─────────────────────────────────────────────────────────────────────────────

def _dispatch_key(ch: str, ui: UIState):
    """Map a single character to an event and enqueue it."""
    c = ch.lower()
    if   ch == 'q':  ui.events.put('quit_graceful')
    elif ch == 'Q':  ui.events.put('quit_force')
    elif c  == 'p':  ui.events.put('pause_toggle')
    elif ch == '+':  ui.events.put('workers_inc')
    elif ch == '-':  ui.events.put('workers_dec')
    elif ch == '1':  ui.events.put('focus_1')
    elif ch == '2':  ui.events.put('focus_2')
    elif ch == '3':  ui.events.put('focus_3')
    elif c  == 'e':  ui.events.put('expand_toggle')
    elif c  == 'o':  ui.events.put('open_output')
    elif c  == 'x':  ui.events.put('export_summary')
    elif c  == 'y':  ui.events.put('modal_yes')
    elif c  == 'n':  ui.events.put('modal_no')


def keyboard_thread(ui: UIState):
    """
    Cross-platform raw keyboard reader.
    Windows  → msvcrt.getwch()  (no setup needed)
    Unix/Mac → termios raw mode
    """
    if IS_WINDOWS:
        _keyboard_thread_windows(ui)
    else:
        _keyboard_thread_unix(ui)


def _keyboard_thread_windows(ui: UIState):
    """Windows keyboard reader using msvcrt."""
    try:
        while True:
            # msvcrt.getwch() blocks until a key is pressed, returns immediately
            ch = msvcrt.getwch()

            # arrow keys and special keys come as two-character sequences:
            # first char is '\x00' or '\xe0', second is the scan code
            if ch in ('\x00', '\xe0'):
                scan = msvcrt.getwch()
                if   scan == 'H':  ui.events.put('scroll_up')    # up arrow
                elif scan == 'P':  ui.events.put('scroll_down')   # down arrow
                # ignore other special keys
                continue

            # Ctrl+C
            if ch == '\x03':
                ui.events.put('quit_force')
                continue

            _dispatch_key(ch, ui)
    except Exception:
        pass


def _keyboard_thread_unix(ui: UIState):
    """Unix/Mac keyboard reader using termios raw mode."""
    fd  = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        while True:
            ch = sys.stdin.read(1)
            if not ch:
                continue

            # escape sequence → arrow keys
            if ch == '\x1b':
                rest = sys.stdin.read(2)
                if   rest == '[A': ui.events.put('scroll_up')
                elif rest == '[B': ui.events.put('scroll_down')
                continue

            # Ctrl+C
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
                  worker_sem: threading.Semaphore) -> bool:
    """Process all pending events. Returns True if we should exit."""
    ui = ps.ui

    while not ui.events.empty():
        try:
            ev = ui.events.get_nowait()
        except queue.Empty:
            break

        with ui.lock:
            # ── modal responses ──────────────────────────
            if ev == 'modal_yes':
                if ui.modal == ModalKind.QUIT_GRACEFUL:
                    kill_flag.set()
                    ui.modal = ModalKind.NONE
                    return True
                elif ui.modal == ModalKind.QUIT_FORCE:
                    os._exit(0)
                ui.modal = ModalKind.NONE
                continue
            if ev == 'modal_no':
                ui.modal = ModalKind.NONE
                continue

            # block most actions while modal is open
            if ui.modal != ModalKind.NONE:
                continue

            # ── quit ─────────────────────────────────────
            if ev == 'quit_graceful':
                ui.modal = ModalKind.QUIT_GRACEFUL
            elif ev == 'quit_force':
                ui.modal = ModalKind.QUIT_FORCE

            # ── pause / resume ───────────────────────────
            elif ev == 'pause_toggle':
                ui.paused = not ui.paused
                tag = "[PAUSE]" if ui.paused else "[RESUME]"
                msg = "Download queue paused." if ui.paused else "Download queue resumed."
                with ps.resilience.lock:
                    ps.resilience.log.append((tag, msg))

            # ── worker slots ─────────────────────────────
            elif ev == 'workers_inc':
                if ui.worker_slots < WORKER_MAX:
                    ui.worker_slots += 1
                    worker_sem.release()
                    with ps.resilience.lock:
                        ps.resilience.log.append(
                            ("[CFG]", f"Workers → {ui.worker_slots}/{WORKER_MAX}"))
            elif ev == 'workers_dec':
                if ui.worker_slots > WORKER_MIN:
                    acquired = worker_sem.acquire(blocking=False)
                    if acquired:
                        ui.worker_slots -= 1
                        with ps.resilience.lock:
                            ps.resilience.log.append(
                                ("[CFG]", f"Workers → {ui.worker_slots}/{WORKER_MAX}"))

            # ── focus ─────────────────────────────────────
            elif ev == 'focus_1':
                ui.focus = FocusPanel.DISCOVERY
            elif ev == 'focus_2':
                ui.focus = FocusPanel.DOWNLOAD
            elif ev == 'focus_3':
                ui.focus = FocusPanel.RESILIENCE

            # ── expand ───────────────────────────────────
            elif ev == 'expand_toggle':
                ui.expanded = not ui.expanded

            # ── scroll ───────────────────────────────────
            elif ev == 'scroll_up':
                ui.scroll[ui.focus].scroll_up()
            elif ev == 'scroll_down':
                ui.scroll[ui.focus].scroll_down()

            # ── open output ──────────────────────────────
            elif ev == 'open_output':
                _open_folder(ps.output_dir)

            # ── export summary ───────────────────────────
            elif ev == 'export_summary':
                if ps.phase == PipelinePhase.DONE:
                    _export_summary(ps)

    return False


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def ts() -> str:
    return time.strftime("%H:%M:%S")


def _open_folder(path: str):
    os.makedirs(path, exist_ok=True)
    try:
        if sys.platform == "darwin":
            subprocess.Popen(["open", path])
        elif sys.platform == "win32":
            subprocess.Popen(["explorer", path])
        else:
            subprocess.Popen(["xdg-open", path])
    except Exception:
        pass


def _export_summary(ps: PipelineState):
    summary = {
        "run_start":   time.strftime("%Y-%m-%dT%H:%M:%S",
                                     time.localtime(ps.start_time)),
        "duration_s":  round(time.time() - ps.start_time, 1),
        "discovery": {
            "total_resolved": ps.discovery.total,
            "skipped":        ps.discovery.skipped,
            "errors":         ps.discovery.errors,
        },
        "download": {
            "total":   ps.download.total,
            "done":    ps.download.done,
            "errors":  ps.download.errors,
        },
        "resilience": {
            "retries":  ps.resilience.retries,
            "failures": ps.resilience.failures,
        },
    }
    path = os.path.join(ps.output_dir, "run_summary.json")
    os.makedirs(ps.output_dir, exist_ok=True)
    with open(path, "w") as f:
        json.dump(summary, f, indent=2)
    with ps.resilience.lock:
        ps.resilience.log.append(("[EXPORT]", f"Summary → {path}"))


def _trim(log: deque, n: int, scroll: ScrollState) -> list:
    """Slice log respecting scroll offset."""
    items = list(log)
    if scroll.following or scroll.offset == 0:
        return items[-n:] if len(items) > n else items
    # scrolled up
    end   = max(0, len(items) - scroll.offset)
    start = max(0, end - n)
    return items[start:end]


def _bar(pct: float, width: int, color: str, bg: str) -> Text:
    filled = max(0, min(width, int(pct / 100 * width)))
    empty  = width - filled
    t = Text()
    t.append("█" * filled, style=color)
    t.append("░" * empty,  style=bg)
    return t


def _badge(state: AgentState, color: str) -> Text:
    t = Text()
    if   state == AgentState.STANDBY: t.append(" ○ STANDBY ", style=f"dim {color}")
    elif state == AgentState.RUNNING:  t.append(" ● RUNNING ", style=f"bold {color}")
    elif state == AgentState.DONE:     t.append(" ✓ DONE    ", style=f"dim {color}")
    elif state == AgentState.ERROR:    t.append(" ✗ ERROR   ", style=f"bold {C_RED}")
    return t


# ─────────────────────────────────────────────────────────────────────────────
# PANEL RENDERERS
# ─────────────────────────────────────────────────────────────────────────────

def render_discovery_large(ds: DiscoveryState, scroll: ScrollState,
                           height: int) -> Panel:
    color  = C_CYAN
    dim_bg = "color(23)"
    lbl    = f"dim {color}"
    body   = Text()

    # ── INPUT PARSING ──────────────────────────
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

    # ── RESOLUTION LOG ─────────────────────────
    body.append(f"\n  RESOLUTION LOG\n", style=lbl)
    body.append(f"  {'─'*54}\n",         style=f"dim {color}")

    parse_lines   = 3 + len(ds.types)
    summary_lines = 8
    footer_lines  = 2
    log_height    = max(3, height - parse_lines - summary_lines - footer_lines - 5)

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

    # ── QUEUE SUMMARY ──────────────────────────
    body.append(f"\n  QUEUE SUMMARY\n", style=lbl)
    body.append(f"  {'─'*54}\n",         style=f"dim {color}")

    caps  = {"query": 10, "channel": 10, "playlist": 20, "direct": 0}
    maxn  = 20

    with ds.lock:
        resolved = dict(ds.resolved)
        total    = ds.total
        skipped  = ds.skipped
        errors   = ds.errors
        elapsed  = ds.elapsed

    for t in ["query", "channel", "playlist", "direct"]:
        n      = resolved.get(t, 0)
        label  = t.capitalize().ljust(9)
        bar    = _bar(n, maxn, color, dim_bg)
        note   = "no cap" if t == "direct" else f"cap:{caps[t]}"
        body.append(f"  {label}", style=f"dim {color}")
        body.append(f" {str(n).rjust(3)} ", style=f"bold {color}")
        body.append_text(bar)
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

    active_n = sum(1 for *_, a in workers if a)
    pause_s  = "  [PAUSED]" if ui.paused else ""
    body.append(f"  WORKERS  {ui.worker_slots}/{WORKER_MAX} slots{pause_s}\n",
                style=lbl)
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

    # ── COMPLETIONS ────────────────────────────
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

    eta_s = f"ETA ~{eta:.0f}s" if eta > 0 else ("done" if dl.state == AgentState.DONE else "…")

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
        uptime   = rs.uptime_pct

    if lines:
        for (tag, msg) in lines:
            if tag in ("[WARN]", "[ERR]"):
                tag_s = f"bold {C_RED}"
            elif tag == "[OK]":
                tag_s = f"bold {color}"
            else:
                tag_s = f"bold dim {color}"
            body.append(f"  {tag:>9} ", style=tag_s)
            body.append(f"{msg}\n",     style=C_WHITE)
    else:
        body.append("  Watchdog standing by...\n", style=C_DIM)

    if not scroll.following:
        body.append(f"  ↑ scrolled  (↓ to follow tail)\n", style=f"dim {color}")

    body.append(f"\n  {'─'*54}\n", style=f"dim {color}")
    body.append("  retries: ",      style=f"dim {color}")
    body.append(f"{retries}",        style=f"bold {color}")
    body.append("   failures: ",    style=f"dim {color}")
    body.append(f"{failures}",       style=f"bold {C_RED}" if failures else f"bold {color}")
    body.append("   uptime: ",      style=f"dim {color}")
    body.append(f"{uptime:.0f}%\n", style=f"bold {color}")

    return Panel(
        body,
        title=Text.assemble(("  AGENT 03 — RESILIENCE  ", f"bold {color}")),
        title_align="left",
        subtitle=_badge(rs.state, color),
        subtitle_align="right",
        border_style=color,
        padding=(0, 0),
    )


def render_small(label: str, agent_num: int, state: AgentState,
                 log: deque, color: str, height: int,
                 extra_footer: str = "") -> Panel:
    body       = Text()
    log_height = max(2, height - 4)

    if state == AgentState.STANDBY:
        body.append(f"\n  Waiting to start...\n",  style=f"dim {color}")
        body.append(f"  Standing by.\n",            style=f"dim {color}")
    else:
        lines = list(log)[-log_height:]
        for (tag, msg) in lines:
            if tag in ("[WARN]", "[ERR]"):
                tag_s = f"bold {C_RED}"
            else:
                tag_s = f"bold {'dim ' if state == AgentState.DONE else ''}{color}"
            short = (msg[:28] + "…") if len(msg) > 29 else msg
            body.append(f"  {tag:>8} ", style=tag_s)
            body.append(f"{short}\n",   style=C_WHITE if state == AgentState.RUNNING else C_DIM)

    border = color if state == AgentState.RUNNING else f"dim {color}"
    sub    = _badge(state, color)

    return Panel(
        body,
        title=Text.assemble((f" {label} ", f"bold {border}")),
        title_align="left",
        subtitle=sub,
        subtitle_align="right",
        border_style=border,
        padding=(0, 0),
    )


# ─────────────────────────────────────────────────────────────────────────────
# MODAL RENDERER
# ─────────────────────────────────────────────────────────────────────────────

def render_modal(kind: ModalKind) -> Text:
    t = Text(justify="center")
    t.append("\n")
    if kind == ModalKind.QUIT_GRACEFUL:
        t.append("  GRACEFUL QUIT\n\n", style=f"bold {C_CYAN}")
        t.append("  Finish active downloads then exit.\n", style=C_WHITE)
        t.append("  Remaining queue will be abandoned.\n\n", style=C_DIM)
    elif kind == ModalKind.QUIT_FORCE:
        t.append("  FORCE QUIT\n\n", style=f"bold {C_RED}")
        t.append("  Kill immediately. Active downloads\n", style=C_WHITE)
        t.append("  will be incomplete / corrupted.\n\n", style=C_DIM)
    t.append("  [y] confirm     [n] cancel\n", style=f"bold {C_WHITE}")
    return t


# ─────────────────────────────────────────────────────────────────────────────
# FOOTER RENDERER
# ─────────────────────────────────────────────────────────────────────────────

def render_footer(ps: PipelineState) -> Text:
    ui    = ps.ui
    t     = Text(justify="left")
    phase = ps.phase

    def key(k):   t.append(f" {k} ",  style="bold white on color(235)")
    def label(l): t.append(f" {l} ",  style="dim white")
    def sep():    t.append(" │ ",      style="dim color(240)")

    if ui.modal != ModalKind.NONE:
        key("y"); label("confirm")
        sep()
        key("n"); label("cancel")
        return t

    key("q"); label("quit")
    sep()
    key("Q"); label("force quit")
    sep()

    if phase == PipelinePhase.DOWNLOAD:
        pause_l = "resume" if ui.paused else "pause"
        key("p"); label(pause_l)
        sep()
        key("+"); label("more workers")
        sep()
        key("-"); label("fewer workers")
        sep()

    key("1"); label("discovery")
    sep()
    key("2"); label("download")
    sep()
    key("3"); label("resilience")
    sep()
    key("e"); label("expand")
    sep()
    key("↑↓"); label("scroll")

    if phase == PipelinePhase.DONE:
        sep()
        key("o"); label("open folder")
        sep()
        key("x"); label("export summary")

    return t


# ─────────────────────────────────────────────────────────────────────────────
# LAYOUT BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_layout(ps: PipelineState, console: Console) -> Layout:
    width, height = console.size
    ui            = ps.ui

    root = Layout()
    root.split_column(
        Layout(name="topbar",  size=1),
        Layout(name="body"),
        Layout(name="footer",  size=1),
    )

    # ── TOPBAR ────────────────────────────────
    elapsed = time.time() - ps.start_time
    topbar  = Text(overflow="ellipsis", no_wrap=True)
    topbar.append(" ▌ PIPELINE ", style="bold white")
    topbar.append("│ ", style="dim white")
    phase_color = {
        PipelinePhase.DISCOVERY: C_CYAN,
        PipelinePhase.DOWNLOAD:  C_GREEN,
        PipelinePhase.DONE:      C_MAGENTA,
    }[ps.phase]
    topbar.append(f" {ps.phase.name} ", style=f"bold {phase_color}")
    topbar.append("│ ", style="dim white")
    topbar.append(f"workers:{ui.worker_slots}/{WORKER_MAX} ", style="dim white")
    if ui.paused:
        topbar.append("[PAUSED] ", style=f"bold {C_RED}")
    topbar.append("│ ", style="dim white")
    topbar.append(f"elapsed:{elapsed:.0f}s ", style="dim white")
    topbar.append("│ ", style="dim white")
    topbar.append(ts(), style="dim white")
    root["topbar"].update(topbar)

    # ── FOOTER ────────────────────────────────
    root["footer"].update(render_footer(ps))

    # body height
    body_h = height - 2

    # ── MODAL overlay ─────────────────────────
    if ui.modal != ModalKind.NONE:
        modal_panel = Panel(
            render_modal(ui.modal),
            border_style=C_RED if ui.modal == ModalKind.QUIT_FORCE else C_CYAN,
            padding=(0, 4),
        )
        root["body"].update(Align.center(modal_panel, vertical="middle"))
        return root

    # ── EXPANDED mode ─────────────────────────
    if ui.expanded:
        scr = ui.scroll[ui.focus]
        if ui.focus == FocusPanel.DISCOVERY:
            p = render_discovery_large(ps.discovery, scr, body_h)
        elif ui.focus == FocusPanel.DOWNLOAD:
            p = render_download_large(ps.download, ui, scr, body_h)
        else:
            p = render_resilience_large(ps.resilience, scr, body_h)
        root["body"].update(p)
        return root

    # ── NORMAL 68/32 split ────────────────────
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

    # determine which agent is "large"
    # default: follow pipeline phase; override with focus key
    if focus == FocusPanel.DISCOVERY:
        large = "discovery"
    elif focus == FocusPanel.DOWNLOAD:
        large = "download"
    else:
        large = "resilience"

    def small_disc():
        with ps.discovery.lock:
            log_copy = deque(ps.discovery.res_log)
        return render_small("DISCOVERY", 1, ps.discovery.state,
                            log_copy, C_CYAN, panel_h)

    def small_dl():
        with ps.download.lock:
            log_copy = deque(ps.download.comp_log)
        return render_small("DOWNLOAD", 2, ps.download.state,
                            log_copy, C_GREEN, panel_h)

    def small_res():
        with ps.resilience.lock:
            log_copy = deque(ps.resilience.log)
        return render_small("RESILIENCE", 3, ps.resilience.state,
                            log_copy, C_MAGENTA, panel_h)

    scr = ui.scroll[focus]

    if large == "discovery":
        root["body"]["left"].update(
            render_discovery_large(ps.discovery, scr, body_h))
        root["body"]["rt"].update(small_dl())
        root["body"]["rb"].update(small_res())

    elif large == "download":
        root["body"]["left"].update(
            render_download_large(ps.download, ui, scr, body_h))
        root["body"]["rt"].update(small_disc())
        root["body"]["rb"].update(small_res())

    else:  # resilience focused
        root["body"]["left"].update(
            render_resilience_large(ps.resilience, scr, body_h))
        root["body"]["rt"].update(small_disc())
        root["body"]["rb"].update(small_dl())

    return root


# ─────────────────────────────────────────────────────────────────────────────
# MOCK PIPELINE  — replace internals with real yt-dlp logic
# ─────────────────────────────────────────────────────────────────────────────

def run_discovery(ps: PipelineState):
    ds = ps.discovery
    rs = ps.resilience

    ds.state    = AgentState.RUNNING
    ds.n_inputs = 6
    ds.types    = {
        "query":    (2, 10),
        "channel":  (1, 10),
        "playlist": (2, 20),
        "direct":   (1, 0),
    }
    start = time.time()

    def log(tag, msg):
        with ds.lock:
            ds.res_log.append((tag, msg))

    def add_res(qtype, n):
        with ds.lock:
            ds.resolved[qtype] = ds.resolved.get(qtype, 0) + n
            ds.total = sum(ds.resolved.values())

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
        log(tag, f"{label} → resolving...")
        time.sleep(0.7)
        if n > 0:
            log(tag, f"{label} → {n} URLs resolved")
            add_res(qtype, n)
        else:
            log(tag, f"{label}")
            with ds.lock:
                ds.skipped += 1
        with ds.lock:
            ds.elapsed = time.time() - start

    log("[META]", "Fetching metadata for all resolved URLs...")
    time.sleep(1.0)
    with ds.lock:
        total = ds.total
    log("[META]", f"Metadata collected → {total} JSON records ready")
    time.sleep(0.3)
    log("[DONE]", f"Discovery complete. {total} items dispatched.")
    ds.state  = AgentState.DONE
    ps.phase  = PipelinePhase.DOWNLOAD
    ps.ui.focus = FocusPanel.DOWNLOAD  # auto-shift spotlight


def run_download(ps: PipelineState, worker_sem: threading.Semaphore):
    dl  = ps.download
    rs  = ps.resilience
    ui  = ps.ui
    total = ps.discovery.total

    with dl.lock:
        dl.total = total
        dl.state = AgentState.RUNNING

    videos = ([
        ("Ga News Bulletin #441",    284),
        ("Accra Metro Report Live",  512),
        ("GTV Evening News 13-Mar",  611),
        ("GBC 9 OClock News",        312),
        ("Morning Starr 103.5 Clip",  97),
        ("Joy FM Super Morning",     189),
        ("Citi FM Morning Drive",    203),
        ("TV3 Midday News",          445),
        ("Parliament Wrap Mar 14",   178),
        ("Joy News At 7",            290),
    ] * ((total // 10) + 1))[:total]

    vid_queue  = list(videos)
    q_lock     = threading.Lock()

    def worker(slot_idx: int):
        while True:
            # pause support — block here if paused
            while ui.paused:
                time.sleep(0.2)

            worker_sem.acquire()
            try:
                with q_lock:
                    if not vid_queue:
                        return
                    name, size_mb = vid_queue.pop(0)

                with dl.lock:
                    w       = dl.workers[slot_idx]
                    w.name  = name
                    w.pct   = 0.0
                    w.speed = 0.0
                    w.active= True

                steps = 20
                for i in range(steps):
                    while ui.paused:
                        time.sleep(0.2)
                    time.sleep(0.2)
                    pct = (i + 1) / steps * 100
                    spd = 14 + slot_idx * 2 + (i % 4)
                    with dl.lock:
                        w.pct   = pct
                        w.speed = spd

                    # simulate stall on slot 1, early on
                    if i == 7 and slot_idx == 1 and dl.done < 2:
                        with rs.lock:
                            rs.log.append(("[WARN]",
                                f"W{slot_idx+1} stall >2s — issuing retry"))
                            rs.retries += 1
                        time.sleep(0.5)
                        with rs.lock:
                            rs.log.append(("[OK]",
                                f"W{slot_idx+1} resumed after retry"))

                ok  = True
                tag = "[OK]" if ok else "[ERR]"
                msg = f"{name[:30]}  {size_mb}MB  meta:{'OK' if ok else 'FAIL'}"

                with dl.lock:
                    dl.comp_log.append((tag, msg))
                    w.active = False
                    w.pct    = 100.0
                    dl.done += 1
                    if not ok:
                        dl.errors += 1
                    active_speeds = [x.speed for x in dl.workers if x.active]
                    dl.speed = sum(active_speeds)
                    rem      = dl.total - dl.done
                    dl.eta   = (rem / max(len(active_speeds), 1) * 4)

                with rs.lock:
                    rs.log.append(("[HBEAT]",
                        f"DL {dl.done}/{dl.total} — {dl.speed:.0f} MB/s"))
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
                    dl.errors += 1

    with dl.lock:
        dl.state = AgentState.DONE
        dl.eta   = 0
    ps.phase = PipelinePhase.DONE
    with rs.lock:
        rs.log.append(("[DONE]", "All downloads complete. Pipeline finished."))


def run_resilience_watchdog(ps: PipelineState, stop_evt: threading.Event):
    rs = ps.resilience
    rs.state = AgentState.RUNNING
    with rs.lock:
        rs.log.append(("[INIT]",  "Watchdog process started"))
        rs.log.append(("[WATCH]", "Monitoring all pipeline agents"))

    while not stop_evt.is_set():
        time.sleep(10)
        if stop_evt.is_set():
            break
        disc_s = ps.discovery.state.name
        dl_s   = ps.download.state.name
        with rs.lock:
            rs.log.append(("[HBEAT]",
                f"DISC:{disc_s}  DL:{dl_s}  uptime:100%"))

    rs.state = AgentState.DONE


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    console    = Console()
    ps         = PipelineState()
    kill_flag  = threading.Event()
    stop_watch = threading.Event()

    # semaphore controls active worker slots
    worker_sem = threading.Semaphore(WORKER_DEFAULT)

    # keyboard thread
    kb = threading.Thread(
        target=keyboard_thread, args=(ps.ui,), daemon=True)
    kb.start()

    # pipeline thread
    def pipeline():
        t_watch = threading.Thread(
            target=run_resilience_watchdog,
            args=(ps, stop_watch),
            daemon=True)
        t_watch.start()

        run_discovery(ps)

        if not kill_flag.is_set():
            run_download(ps, worker_sem)

        stop_watch.set()
        t_watch.join(timeout=2)

    t_pipe = threading.Thread(target=pipeline, daemon=True)
    t_pipe.start()

    # render loop
    with Live(
        build_layout(ps, console),
        console=console,
        screen=True,
        refresh_per_second=REFRESH_RATE,
    ) as live:
        while True:
            should_exit = handle_events(ps, kill_flag, worker_sem)
            live.update(build_layout(ps, console))
            time.sleep(1 / REFRESH_RATE)

            if should_exit:
                break

            if kill_flag.is_set():
                break

            if (ps.discovery.state == AgentState.DONE
                    and ps.download.state == AgentState.DONE
                    and ps.resilience.state == AgentState.DONE):
                # stay on screen until user quits
                while True:
                    handle_events(ps, kill_flag, worker_sem)
                    live.update(build_layout(ps, console))
                    time.sleep(1 / REFRESH_RATE)
                    if kill_flag.is_set():
                        break


if __name__ == "__main__":
    main()