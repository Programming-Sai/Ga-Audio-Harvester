"""
agents/resilience/agent.py
===========================
ResilienceAgent — SPADE agent, Phase A (standalone).

Responsibilities (Prometheus agent descriptor):
  Goals     : MaintainPipelineHealth, DetectStalls, ReportFailures
  Percepts  : worker slot pct values, agent state flags, error counts
  Actions   : LogWarning, RecordRetry, RecordFailure, EmitHeartbeat
  Data      : resilience_state, watched_agents refs
  Comms     : none in Phase A  (Phase B: receives INFORM from
               DownloadAgent, sends REQUEST retry to DownloadAgent)

Run standalone:
    python -m agents.resilience.agent
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import spade
import spade.agent
from dotenv import load_dotenv

from agents.resilience.behaviours import (
    HeartbeatBehaviour,
    StallDetectorBehaviour,
    MessageReceiverBehaviour,
    HEARTBEAT_PERIOD,
    STALL_PERIOD,
)

load_dotenv()
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# INTERNAL STATE
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ResilienceInternalState:
    """Mirrors tui_v3 ResilienceState for Phase C bridge."""
    state_flag:      str   = "standby"
    log:             deque = field(default_factory=lambda: deque(maxlen=500))
    retries:         int   = 0
    failures:        int   = 0
    errors:          int   = 0
    uptime_pct:      float = 100.0
    heartbeat_count: int   = 0
    last_known_errors: int = 0

    # per-job stall tracking (used by StallDetectorBehaviour)
    job_last_pct:     dict = field(default_factory=dict)
    job_last_move:    dict = field(default_factory=dict)
    job_stall_warned: set  = field(default_factory=set)

    lock: threading.Lock = field(default_factory=threading.Lock)


# ─────────────────────────────────────────────────────────────────────────────
# RESILIENCE AGENT
# ─────────────────────────────────────────────────────────────────────────────

class ResilienceAgent(spade.agent.Agent):
    """
    Watchdog agent — monitors Discovery and Download agents for stalls,
    errors, and failures.  Runs two periodic behaviours:
      - HeartbeatBehaviour    : overall pipeline health check (10s)
      - StallDetectorBehaviour: per-worker stall detection (5s)

    Phase A: reads sibling agent state objects directly via watch().
    Phase B: receives INFORM messages from DownloadAgent instead.
    """

    def __init__(
        self,
        jid: str,
        password: str,
        verify_security: bool = False,
    ):
        super().__init__(jid, password, verify_security=verify_security)

        # XMPP target (Phase B)
        self.download_jid = os.getenv("DOWNLOAD_JID") or None

        # references to sibling agents (Phase A only)
        self.watched_agents: dict = {}

        # internal state
        self.resilience_state = ResilienceInternalState()

        # completion signal (Phase B)
        self.all_done_event: asyncio.Event = asyncio.Event()

    # ── SPADE LIFECYCLE ───────────────────────────────────────────────────

    async def setup(self):
        logger.info("ResilienceAgent [%s] starting up", self.jid)
        self.resilience_state.state_flag = "running"

        self._log("[INIT]",  "Watchdog process started")
        self._log("[WATCH]", "Monitoring pipeline agents")

        self.add_behaviour(HeartbeatBehaviour(period=HEARTBEAT_PERIOD))
        self.add_behaviour(StallDetectorBehaviour(period=STALL_PERIOD))
        self.add_behaviour(MessageReceiverBehaviour())

    # ── PUBLIC API ────────────────────────────────────────────────────────

    def watch(self, role: str, agent):
        """
        Register a sibling agent to monitor.
        role: "discovery" | "download"
        """
        self.watched_agents[role] = agent
        self._log("[WATCH]", f"Now monitoring: {role} ({agent.jid})")

    def mark_done(self):
        """Called when the pipeline finishes."""
        rs = self.resilience_state
        with rs.lock:
            retries  = rs.retries
            failures = rs.failures
        rs.state_flag = "done"
        self._log("[DONE]", f"Pipeline complete — retries:{retries}  failures:{failures}")

    # ── HELPERS ───────────────────────────────────────────────────────────

    def _log(self, tag: str, msg: str):
        with self.resilience_state.lock:
            self.resilience_state.log.append((tag, msg))
        logger.info("RESILIENCE %s %s", tag, msg)



# ─────────────────────────────────────────────────────────────────────────────
# STANDALONE ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

async def _run_standalone():
    """
    Phase A standalone test.
    Starts all three agents, wires resilience to watch the others,
    runs the full pipeline, then prints the resilience report.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    # ── credentials ───────────────────────────────────────────────────
    disc_jid  = os.environ["DISCOVERY_JID"]
    disc_pass = os.environ["DISCOVERY_PASSWORD"]
    dl_jid    = os.environ["DOWNLOAD_JID"]
    dl_pass   = os.environ["DOWNLOAD_PASSWORD"]
    res_jid   = os.environ["RESILIENCE_JID"]
    res_pass  = os.environ["RESILIENCE_PASSWORD"]

    query_file = Path("assets/input/queries.txt")
    if not query_file.exists():
        query_file = Path("test_query.txt")
        query_file.write_text("ghana news today\n")

    # ── create agents ─────────────────────────────────────────────────
    from agents.discovery.agent import DiscoveryAgent
    from agents.download.agent  import DownloadAgent

    disc = DiscoveryAgent(
        disc_jid, disc_pass,
        query_file=query_file,
        output_dir="./output",
        max_items={"search": 2, "channel": 2, "playlist": 3},
    )
    dl = DownloadAgent(
        dl_jid, dl_pass,
        output_dir="./output",
        worker_slots=2,
        retries=1,
    )
    res = ResilienceAgent(res_jid, res_pass)

    # ── start all three ───────────────────────────────────────────────
    try:
        await asyncio.wait_for(disc.start(auto_register=False), timeout=15.0)
        await asyncio.wait_for(dl.start(auto_register=False), timeout=15.0)
        await asyncio.wait_for(res.start(auto_register=False), timeout=15.0)
    except asyncio.TimeoutError:
        print("[Pipeline] START TIMEOUT (XMPP connect hang?)")
        return
    except Exception as exc:
        print(f"[Pipeline] START FAILED: {exc}")
        return

    print(f"\n[ResilienceAgent] started as {res_jid}")
    print("[ResilienceAgent] watching Discovery and Download\n")

    # wire resilience to watch the other two
    res.watch("discovery", disc)
    res.watch("download",  dl)

    # ── wait for discovery ────────────────────────────────────────────
    print("[Pipeline] Waiting for Discovery...")
    try:
        await asyncio.wait_for(disc.resolution_done.wait(), timeout=120)
    except asyncio.TimeoutError:
        print("[Pipeline] Discovery timed out")

    jobs = disc.get_jobs()
    print(f"[Pipeline] Discovery done — {len(jobs)} jobs queued\n")

    # ── feed jobs to download ─────────────────────────────────────────
    dl.add_jobs(jobs)
    print(f"[Pipeline] Downloading {len(jobs)} files...\n")

    try:
        await asyncio.wait_for(dl.all_done_event.wait(), timeout=600)
    except asyncio.TimeoutError:
        print("[Pipeline] Download timed out")

    res.mark_done()

    # ── final report ──────────────────────────────────────────────────
    rs = res.resilience_state
    ds = dl.download_state

    print(f"\n{'─'*60}")
    print(f"  PIPELINE COMPLETE — RESILIENCE REPORT")
    print(f"{'─'*60}")
    print(f"  Heartbeats : {rs.heartbeat_count}")
    print(f"  Retries    : {rs.retries}")
    print(f"  Failures   : {rs.failures}")
    print(f"  Downloads  : {ds.done} OK  {ds.errors} ERR  ({ds.total} total)")
    print(f"\n  Watchdog log (last 20 entries):")
    for tag, msg in list(rs.log)[-20:]:
        print(f"    {tag:>9}  {msg}")
    print(f"{'─'*60}\n")

    # silence slixmpp before stopping all agents
    logging.getLogger("slixmpp").setLevel(logging.CRITICAL)
    await disc.stop()
    await dl.stop()
    await res.stop()
    await asyncio.sleep(0.5)
    print("[Pipeline] All agents stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(_run_standalone())
    except KeyboardInterrupt:
        print("\n[Pipeline] interrupted by user.")
