"""
Multi-Agent Pipeline TUI
========================
Three agents: Discovery, Download, Resilience
- Active agent  → large left panel  (~68% width)
- Other two     → stacked right     (~32% width)
- Resilience    → always on the right, always watching
- Rich Live + Layout, screen=True, responsive to terminal resize
"""

from __future__ import annotations

import time
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import List, Optional

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.text import Text
from rich.style import Style


# ─────────────────────────────────────────────
# ENUMS & CONSTANTS
# ─────────────────────────────────────────────

class AgentState(Enum):
    STANDBY  = auto()
    RUNNING  = auto()
    DONE     = auto()
    ERROR    = auto()


class ActiveAgent(Enum):
    DISCOVERY = auto()
    DOWNLOAD  = auto()


MAX_WORKERS     = 4
REFRESH_RATE    = 8   # redraws per second
LOG_MAXLEN      = 200 # max lines kept in memory per agent

# Rich colour tokens
C_CYAN    = "bright_cyan"
C_GREEN   = "bright_green"
C_MAGENTA = "magenta"
C_DIM     = "dim"


# ─────────────────────────────────────────────
# SHARED STATE  (written by workers, read by renderer)
# ─────────────────────────────────────────────

@dataclass
class WorkerSlot:
    idx:      int
    name:     str  = ""
    pct:      float = 0.0
    speed:    float = 0.0   # MB/s
    active:   bool  = False


@dataclass
class DiscoveryState:
    state:        AgentState = AgentState.STANDBY
    # parsing
    n_inputs:     int = 0
    types:        dict = field(default_factory=dict)  # {type: count}
    # resolution log
    res_log:      deque = field(default_factory=lambda: deque(maxlen=LOG_MAXLEN))
    # summary counters
    resolved:     dict  = field(default_factory=lambda: {"query":0,"channel":0,"playlist":0,"direct":0})
    skipped:      int   = 0
    errors:       int   = 0
    total:        int   = 0
    elapsed:      float = 0.0
    lock:         threading.Lock = field(default_factory=threading.Lock)


@dataclass
class DownloadState:
    state:        AgentState = AgentState.STANDBY
    workers:      List[WorkerSlot] = field(default_factory=lambda: [WorkerSlot(i+1) for i in range(MAX_WORKERS)])
    comp_log:     deque = field(default_factory=lambda: deque(maxlen=LOG_MAXLEN))
    done:         int   = 0
    total:        int   = 0
    errors:       int   = 0
    speed:        float = 0.0   # aggregate MB/s
    eta:          float = 0.0   # seconds
    lock:         threading.Lock = field(default_factory=threading.Lock)


@dataclass
class ResilienceState:
    state:        AgentState = AgentState.STANDBY
    log:          deque = field(default_factory=lambda: deque(maxlen=LOG_MAXLEN))
    retries:      int   = 0
    failures:     int   = 0
    uptime_pct:   float = 100.0
    lock:         threading.Lock = field(default_factory=threading.Lock)


@dataclass
class PipelineState:
    active:       ActiveAgent    = ActiveAgent.DISCOVERY
    discovery:    DiscoveryState = field(default_factory=DiscoveryState)
    download:     DownloadState  = field(default_factory=DownloadState)
    resilience:   ResilienceState = field(default_factory=ResilienceState)
    start_time:   float           = field(default_factory=time.time)


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def ts() -> str:
    return time.strftime("%H:%M:%S")


def make_bar(pct: float, width: int = 22) -> Text:
    filled = int(pct / 100 * width)
    empty  = width - filled
    t = Text()
    t.append("█" * filled, style=C_GREEN)
    t.append("░" * empty,  style="color(22)")   # very dark green
    return t


def make_bar_cyan(pct: float, width: int = 22) -> Text:
    filled = int(pct / 100 * width)
    empty  = width - filled
    t = Text()
    t.append("█" * filled, style=C_CYAN)
    t.append("░" * empty,  style="color(23)")
    return t


def trim_log(log: deque, n_lines: int) -> list:
    """Return the last n_lines from a deque."""
    items = list(log)
    return items[-n_lines:] if len(items) > n_lines else items


# ─────────────────────────────────────────────
# RENDERERS  — each returns a Panel
# ─────────────────────────────────────────────

def render_discovery_large(ds: DiscoveryState, height: int) -> Panel:
    """Full-size Discovery panel."""
    color  = C_CYAN
    dim_c  = "color(23)"
    label_style = f"dim {color}"

    body = Text()

    # ── INPUT PARSING ──────────────────────────
    body.append("  INPUT PARSING\n", style=label_style)
    body.append("  " + "─" * 54 + "\n", style=f"dim {color}")
    if ds.n_inputs:
        body.append(f"  [PARSE]", style=f"bold {color}")
        body.append(f"  Loaded queries — {ds.n_inputs} inputs found\n", style="white")
        for t, c in ds.types.items():
            cap_hint = "" if t == "direct" else f"  cap: {c[1]}"
            body.append(f"  [TYPE] ", style=f"bold {color}")
            body.append(f" {c[0]}× {t}{cap_hint}\n", style="white")
    else:
        body.append("  Waiting for input file...\n", style="dim white")

    # ── RESOLUTION LOG ─────────────────────────
    body.append("\n  RESOLUTION LOG\n", style=label_style)
    body.append("  " + "─" * 54 + "\n", style=f"dim {color}")

    # How many lines can the log take?
    # Fixed lines above: ~7 (parse section) + 2 (section header) + summary below ~8
    fixed_lines  = 14
    summary_lines = 7
    log_height   = max(3, height - fixed_lines - summary_lines - 4)

    with ds.lock:
        lines = trim_log(ds.res_log, log_height)

    if lines:
        for (tag, msg) in lines:
            body.append(f"  {tag:>8} ", style=f"bold {color}")
            body.append(f" {msg}\n",    style="white")
    else:
        body.append("  Waiting for resolution to begin...\n", style="dim white")

    # ── QUEUE SUMMARY ──────────────────────────
    body.append("\n  QUEUE SUMMARY\n", style=label_style)
    body.append("  " + "─" * 54 + "\n", style=f"dim {color}")

    caps  = {"query": 10, "channel": 10, "playlist": 20, "direct": 0}
    types = ["query", "channel", "playlist", "direct"]
    maxn  = 20   # max for bar scaling

    with ds.lock:
        resolved = dict(ds.resolved)
        total    = ds.total
        skipped  = ds.skipped
        errors   = ds.errors

    for t in types:
        n      = resolved.get(t, 0)
        label  = t.capitalize().ljust(9)
        bar_w  = 22
        filled = int(n / maxn * bar_w) if maxn else 0
        empty  = bar_w - filled
        note   = f"no cap" if t == "direct" else f"cap:{caps[t]}"
        body.append(f"  {label}", style=f"dim {color}")
        body.append(f" {str(n).rjust(3)} ", style=f"bold {color}")
        body.append("█" * filled, style=color)
        body.append("░" * empty,  style=dim_c)
        body.append(f"  {note}\n", style=f"dim {color}")

    body.append("  " + "─" * 54 + "\n", style=f"dim {color}")
    body.append(f"  TOTAL ENQUEUED: ", style=f"dim {color}")
    body.append(f"{total}", style=f"bold {color}")
    body.append(" → dispatching to download workers\n", style=f"dim {color}")

    # footer text
    state_str = ds.state.name
    badge_style = f"bold {color}" if ds.state == AgentState.RUNNING else f"dim {color}"
    footer = Text()
    footer.append(f"parsed:{ds.n_inputs} ", style=f"dim {color}")
    footer.append("│ ", style=f"dim {color}")
    footer.append(f"resolved:{total} ", style=f"dim {color}")
    footer.append("│ ", style=f"dim {color}")
    footer.append(f"skipped:{skipped} ", style=f"dim {color}")
    footer.append("│ ", style=f"dim {color}")
    footer.append(f"errors:{errors} ", style=f"dim {color}")
    footer.append("│ ", style=f"dim {color}")
    footer.append(f"elapsed:{ds.elapsed:.1f}s", style=f"dim {color}")

    badge = _badge(ds.state, color)

    return Panel(
        body,
        title=Text.assemble(("  AGENT 01 — DISCOVERY  ", f"bold {color}")),
        title_align="left",
        subtitle=badge,
        subtitle_align="right",
        border_style=color if ds.state == AgentState.RUNNING else f"dim {color}",
        padding=(0, 0),
    )


def render_download_large(dl: DownloadState, height: int) -> Panel:
    color    = C_GREEN
    dim_c    = "color(22)"
    label_style = f"dim {color}"

    body = Text()

    # ── WORKERS ────────────────────────────────
    with dl.lock:
        workers = [(w.idx, w.name, w.pct, w.speed, w.active) for w in dl.workers]
        done    = dl.done
        total   = dl.total
        errors  = dl.errors
        speed   = dl.speed
        eta     = dl.eta

    active_count = sum(1 for *_, a in workers if a)
    body.append(f"  WORKERS  {active_count}/{MAX_WORKERS} active\n", style=label_style)
    body.append("  " + "─" * 54 + "\n", style=f"dim {color}")

    for idx, name, pct, spd, active in workers:
        if not active:
            body.append(f"  [W{idx}] ", style=f"bold dim {color}")
            body.append("idle — awaiting job\n", style="dim white")
        else:
            body.append(f"  [W{idx}] ", style=f"bold {color}")
            short = (name[:38] + "…") if len(name) > 39 else name.ljust(39)
            body.append(f"{short}", style="white")
            body.append(f"  {spd:5.1f} MB/s\n", style=f"dim {color}")
            filled = int(pct / 100 * 28)
            empty  = 28 - filled
            body.append("       ")
            body.append("█" * filled, style=color)
            body.append("░" * empty,  style=dim_c)
            body.append(f"  {pct:5.1f}%\n", style=f"bold {color}")

    # ── COMPLETIONS ────────────────────────────
    body.append("\n  COMPLETIONS\n", style=label_style)
    body.append("  " + "─" * 54 + "\n", style=f"dim {color}")

    # Worker block is ~2 lines per worker + 3 header = 11 fixed
    fixed_lines  = 11
    log_height   = max(3, height - fixed_lines - 5)

    with dl.lock:
        lines = trim_log(dl.comp_log, log_height)

    if lines:
        for (tag, msg) in lines:
            tag_style = f"bold {color}" if tag == "[OK]" else "bold red"
            body.append(f"  {tag:>6} ", style=tag_style)
            body.append(f" {msg}\n",    style="white")
    else:
        body.append("  No completions yet...\n", style="dim white")

    # footer
    eta_str = f"ETA ~{eta:.0f}s" if eta > 0 else "wrapping up..."

    return Panel(
        body,
        title=Text.assemble(("  AGENT 02 — DOWNLOAD  ", f"bold {color}")),
        title_align="left",
        subtitle=_badge(dl.state, color),
        subtitle_align="right",
        border_style=color if dl.state == AgentState.RUNNING else f"dim {color}",
        padding=(0, 0),
    )


def render_discovery_small(ds: DiscoveryState, height: int) -> Panel:
    color = C_CYAN
    body  = Text()

    if ds.state == AgentState.STANDBY:
        body.append("\n  Waiting to start...\n", style=f"dim {color}")
        body.append("  Workers ready.\n",        style=f"dim {color}")
        body.append("  Queue empty.\n",           style=f"dim {color}")
    elif ds.state in (AgentState.RUNNING, AgentState.DONE):
        log_height = max(2, height - 5)
        with ds.lock:
            lines = trim_log(ds.res_log, log_height)
        if lines:
            for (tag, msg) in lines:
                body.append(f" {tag:>8} ", style=f"bold {'dim ' if ds.state==AgentState.DONE else ''}{color}")
                short = (msg[:26] + "…") if len(msg) > 27 else msg
                body.append(f"{short}\n", style="white" if ds.state==AgentState.RUNNING else f"dim white")
        with ds.lock:
            total   = ds.total
            skipped = ds.skipped
    else:
        body.append("  Error.\n", style="bold red")

    border = color if ds.state == AgentState.RUNNING else f"dim {color}"
    return Panel(
        body,
        title=Text.assemble(("DISCOVERY", f"bold {border}")),
        title_align="left",
        subtitle=_badge(ds.state, color),
        subtitle_align="right",
        border_style=border,
        padding=(0, 0),
    )


def render_download_small(dl: DownloadState, height: int) -> Panel:
    color = C_GREEN
    body  = Text()

    if dl.state == AgentState.STANDBY:
        body.append("\n  Workers ready.\n",              style=f"dim {color}")
        body.append("  Awaiting Discovery output.\n",   style=f"dim {color}")
        body.append(f"  {MAX_WORKERS} slots standing by.\n", style=f"dim {color}")
    elif dl.state in (AgentState.RUNNING, AgentState.DONE):
        log_height = max(2, height - 5)
        with dl.lock:
            lines = trim_log(dl.comp_log, log_height)
            done  = dl.done
            total = dl.total
        if lines:
            for (tag, msg) in lines:
                tag_style = f"bold {color}" if tag == "[OK]" else "bold red"
                short = (msg[:26] + "…") if len(msg) > 27 else msg
                body.append(f" {tag:>6} ", style=tag_style if dl.state==AgentState.RUNNING else f"dim {color}")
                body.append(f"{short}\n",  style="white" if dl.state==AgentState.RUNNING else "dim white")
        else:
            body.append("  No completions yet.\n", style=f"dim {color}")
    else:
        body.append("  Error.\n", style="bold red")

    border = color if dl.state == AgentState.RUNNING else f"dim {color}"
    return Panel(
        body,
        title=Text.assemble(("DOWNLOAD", f"bold {border}")),
        title_align="left",
        subtitle=_badge(dl.state, color),
        subtitle_align="right",
        border_style=border,
        padding=(0, 0),
    )


def render_resilience_small(rs: ResilienceState, height: int) -> Panel:
    color = C_MAGENTA
    body  = Text()

    log_height = max(2, height - 5)
    with rs.lock:
        lines = trim_log(rs.log, log_height)

    if lines:
        for (tag, msg) in lines:
            tag_style = f"bold {color}"
            if tag in ("[WARN]", "[ERR]"):
                tag_style = "bold red"
            elif tag == "[OK]":
                tag_style = f"bold {color}"
            short = (msg[:26] + "…") if len(msg) > 27 else msg
            body.append(f" {tag:>8} ", style=tag_style)
            body.append(f"{short}\n",  style="white")
    else:
        body.append("\n  Watchdog started.\n",        style=f"dim {color}")
        body.append("  Monitoring pipeline...\n",     style=f"dim {color}")

    with rs.lock:
        retries  = rs.retries
        failures = rs.failures
        uptime   = rs.uptime_pct

    return Panel(
        body,
        title=Text.assemble(("RESILIENCE", f"bold {color}")),
        title_align="left",
        subtitle=Text.assemble((f"retries:{retries} fail:{failures}", f"dim {color}")),
        subtitle_align="right",
        border_style=color,
        padding=(0, 0),
    )


def _badge(state: AgentState, color: str) -> Text:
    t = Text()
    if state == AgentState.STANDBY:
        t.append("○ STANDBY",  style=f"dim {color}")
    elif state == AgentState.RUNNING:
        t.append("● RUNNING",  style=f"bold {color}")
    elif state == AgentState.DONE:
        t.append("✓ DONE",     style=f"dim {color}")
    elif state == AgentState.ERROR:
        t.append("✗ ERROR",    style="bold red")
    return t


# ─────────────────────────────────────────────
# LAYOUT BUILDER
# ─────────────────────────────────────────────

def build_layout(ps: PipelineState, console: Console) -> Layout:
    width, height = console.size
    h_inner = height - 2   # minus topbar + possible border

    root = Layout()
    root.split_column(
        Layout(name="top",  size=1),
        Layout(name="body"),
    )

    # topbar
    topbar = Text()
    topbar.append("▌ MULTI-AGENT PIPELINE  ", style="bold white")
    topbar.append("│ ", style="dim white")
    topbar.append("RUN #0001 ", style=f"bold {C_CYAN}")
    topbar.append("│ ", style="dim white")

    active_label = ps.active.name
    active_color = C_CYAN if ps.active == ActiveAgent.DISCOVERY else C_GREEN
    topbar.append("ACTIVE ", style="dim white")
    topbar.append(f"{active_label} ", style=f"bold {active_color}")
    topbar.append("│ ", style="dim white")
    elapsed = time.time() - ps.start_time
    topbar.append(f"elapsed:{elapsed:.0f}s ", style="dim white")
    topbar.append("│ ", style="dim white")
    topbar.append(time.strftime("%H:%M:%S"), style="dim white")

    root["top"].update(topbar)

    # body: left (large) + right (stacked small)
    root["body"].split_row(
        Layout(name="left",  ratio=68),
        Layout(name="right", ratio=32),
    )
    root["body"]["right"].split_column(
        Layout(name="right_top"),
        Layout(name="right_bot"),
    )

    panel_h = h_inner // 2   # approximate half height for small panels

    if ps.active == ActiveAgent.DISCOVERY:
        root["body"]["left"].update(
            render_discovery_large(ps.discovery, h_inner)
        )
        root["body"]["right_top"].update(
            render_download_small(ps.download, panel_h)
        )
        root["body"]["right_bot"].update(
            render_resilience_small(ps.resilience, panel_h)
        )
    else:  # DOWNLOAD active
        root["body"]["left"].update(
            render_download_large(ps.download, h_inner)
        )
        root["body"]["right_top"].update(
            render_discovery_small(ps.discovery, panel_h)
        )
        root["body"]["right_bot"].update(
            render_resilience_small(ps.resilience, panel_h)
        )

    return root


# ─────────────────────────────────────────────
# MOCK PIPELINE  (simulates real work)
# Replace these functions with your actual logic.
# Each function writes into PipelineState and returns.
# ─────────────────────────────────────────────

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
        with rs.lock:
            rs.log.append(("[HBEAT]", f"Discovery active"))

    def add_res(qtype, n):
        with ds.lock:
            ds.resolved[qtype] += n
            ds.total            = sum(ds.resolved.values())

    # simulate resolution
    queries = [
        ("query",    '[QUERY]', '"ghana news today"',   10),
        ("query",    '[QUERY]', '"accra morning news"', 10),
        ("channel",  '[CHAN]',  '/c/GHNews',             10),
        ("playlist", '[LIST]',  'PLxxxx',                18),
        ("playlist", '[WARN]',  'PLyyyy → private, skipping', 0),
        ("direct",   '[URL]',   'youtu.be/abc123 → verified', 1),
    ]

    for qtype, tag, label, n in queries:
        time.sleep(0.6)
        log(tag, f"{label} → resolving...")
        time.sleep(0.8)
        if n > 0:
            log(tag, f"{label} → {n} URLs resolved")
            add_res(qtype, n)
        else:
            log(tag, f"{label}")
            with ds.lock:
                ds.skipped += 1
        with ds.lock:
            ds.elapsed = time.time() - start

    # metadata fetch
    with ds.lock:
        items = list(ds.res_log)[-5:]
    log("[META]", "Fetching metadata for all resolved URLs...")
    time.sleep(1.2)
    log("[META]", f"Metadata collected → {ds.total} JSON records ready")
    time.sleep(0.3)
    log("[DONE]", f"Discovery complete. {ds.total} items dispatched.")

    ds.state    = AgentState.DONE
    ps.active   = ActiveAgent.DOWNLOAD
    with ds.lock:
        ds.elapsed = time.time() - start


def run_download(ps: PipelineState):
    dl = ps.download
    rs = ps.resilience

    total = ps.discovery.total
    with dl.lock:
        dl.total = total
        dl.state = AgentState.RUNNING

    # fake video list
    videos = [
        ("Ga News Bulletin #441",         284),
        ("Accra Metro Report — Live",     512),
        ("GTV Evening News 13-Mar",       611),
        ("GBC 9 O'Clock News",            312),
        ("Morning Starr 103.5 Clip",       97),
        ("Joy FM Super Morning",          189),
        ("Citi FM Morning Drive",         203),
        ("TV3 Midday News",               445),
        ("Parliament Wrap Mar 14",        178),
        ("Joy News At 7",                 290),
    ] * (total // 10 + 1)
    videos = videos[:total]

    queue     = list(videos)
    q_lock    = threading.Lock()
    done_lock = threading.Lock()

    def worker(slot_idx: int):
        while True:
            with q_lock:
                if not queue:
                    break
                name, size_mb = queue.pop(0)

            # assign slot
            with dl.lock:
                dl.workers[slot_idx].name   = name
                dl.workers[slot_idx].pct    = 0.0
                dl.workers[slot_idx].speed  = 0.0
                dl.workers[slot_idx].active = True

            # simulate download progress
            pct   = 0.0
            steps = 20
            for i in range(steps):
                time.sleep(0.25)
                pct = (i + 1) / steps * 100
                spd = 15 + (slot_idx * 3) + (i % 5)
                with dl.lock:
                    dl.workers[slot_idx].pct   = pct
                    dl.workers[slot_idx].speed = spd

                # resilience: simulate occasional stall
                if i == 8 and slot_idx == 1 and dl.done < 3:
                    with rs.lock:
                        rs.log.append(("[WARN]",  f"W{slot_idx+1} stall >2s detected"))
                        rs.retries += 1
                    time.sleep(0.4)
                    with rs.lock:
                        rs.log.append(("[OK]",    f"W{slot_idx+1} resumed normally"))

            # complete
            ok = True  # 90% success rate simulation
            tag = "[OK]" if ok else "[ERR]"
            msg = f"{name[:28]}  {size_mb} MB  meta:{'OK' if ok else 'FAIL'}"
            with dl.lock:
                dl.comp_log.append((tag, msg))
                dl.workers[slot_idx].active = False
                dl.workers[slot_idx].pct    = 100.0
                dl.done += 1
                if not ok:
                    dl.errors += 1
                # recalc aggregate speed
                active_speeds = [w.speed for w in dl.workers if w.active]
                dl.speed = sum(active_speeds)
                remaining = dl.total - dl.done
                dl.eta    = (remaining / len(active_speeds) * 5) if active_speeds else 0

            with rs.lock:
                rs.log.append(("[HBEAT]", f"Download {dl.done}/{dl.total} complete"))

    # run workers in thread pool
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = [pool.submit(worker, i) for i in range(MAX_WORKERS)]
        for f in as_completed(futs):
            f.result()

    with dl.lock:
        dl.state = AgentState.DONE
        dl.eta   = 0

    with rs.lock:
        rs.log.append(("[DONE]", "All downloads complete. Pipeline finished."))


def run_resilience_watchdog(ps: PipelineState):
    """Runs in background the whole time, emits heartbeats."""
    rs = ps.resilience
    rs.state = AgentState.RUNNING

    with rs.lock:
        rs.log.append(("[INIT]",  "Watchdog started"))
        rs.log.append(("[WATCH]", "Monitoring pipeline agents"))

    while ps.discovery.state != AgentState.DONE or ps.download.state != AgentState.DONE:
        time.sleep(8)
        disc_s = ps.discovery.state.name
        dl_s   = ps.download.state.name
        with rs.lock:
            rs.log.append(("[HBEAT]", f"DISC:{disc_s}  DL:{dl_s}"))

    with rs.lock:
        rs.log.append(("[OK]", "Pipeline complete. All agents done."))
    rs.state = AgentState.DONE


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    console = Console()
    ps      = PipelineState()

    # kick off pipeline in background threads
    def pipeline():
        # resilience runs the whole time
        t_res = threading.Thread(target=run_resilience_watchdog, args=(ps,), daemon=True)
        t_res.start()

        # discovery first
        run_discovery(ps)

        # then download
        run_download(ps)

        t_res.join(timeout=2)

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
            live.update(build_layout(ps, console))
            time.sleep(1 / REFRESH_RATE)

            # stop when everything is done
            if (
                ps.discovery.state == AgentState.DONE
                and ps.download.state == AgentState.DONE
                and ps.resilience.state == AgentState.DONE
            ):
                time.sleep(3)  # show final state briefly
                break


if __name__ == "__main__":
    main()