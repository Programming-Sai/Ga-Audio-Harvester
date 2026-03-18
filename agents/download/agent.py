"""
agents/download/agent.py
=========================
DownloadAgent — SPADE agent, Phase A (standalone).

Responsibilities (Prometheus agent descriptor):
  Goals     : DownloadAudio, TrackProgress, ReportCompletion
  Percepts  : job_queue items, yt-dlp progress hooks
  Actions   : DownloadVideo, UpdateWorkerSlot, RecordCompletion,
               PauseQueue, ResumeQueue, AdjustWorkers
  Data      : download_state, pending_queue, completed list
  Comms     : none in Phase A  (Phase B: receives INFORM from
               DiscoveryAgent, sends INFORM to ResilienceAgent)

Run standalone:
    python -m agents.download.agent
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Any

import spade
import spade.agent
from dotenv import load_dotenv

from agents.download.behaviours import QueueConsumerBehaviour

load_dotenv()
logger = logging.getLogger(__name__)

# hard ceiling — never exceed this regardless of user setting
WORKER_MAX = 6
WORKER_MIN = 1
WORKER_DEFAULT = 4


# ─────────────────────────────────────────────────────────────────────────────
# INTERNAL STATE
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class WorkerSlot:
    idx:    int
    name:   str   = ""
    pct:    float = 0.0
    speed:  float = 0.0   # MB/s
    active: bool  = False
    job:    Any   = None  # current Job object — read by ResilienceAgent for retry


@dataclass
class CompletedDownload:
    name:    str
    size_mb: int  = 0
    source:  str  = "unknown"
    status:  str  = "OK"     # OK | ERR | SKIP
    reason:  str  = ""


@dataclass
class DownloadInternalState:
    """Mirrors tui_v3 DownloadState for Phase C bridge."""
    state_flag: str   = "standby"
    workers:    list  = field(
        default_factory=lambda: [WorkerSlot(i+1) for i in range(WORKER_MAX)])
    comp_log:   deque = field(default_factory=lambda: deque(maxlen=500))
    completed:  list  = field(default_factory=list)
    done:       int   = 0
    total:      int   = 0
    errors:     int   = 0
    total_mb:   int   = 0
    speed:      float = 0.0
    eta:        float = 0.0
    lock:       threading.Lock = field(default_factory=threading.Lock)


# ─────────────────────────────────────────────────────────────────────────────
# DOWNLOAD AGENT
# ─────────────────────────────────────────────────────────────────────────────

class DownloadAgent(spade.agent.Agent):
    """
    Consumes jobs from pending_queue and downloads them with bounded
    concurrency controlled by worker_sem.

    Phase A: jobs are added directly via agent.add_job().
    Phase B: jobs arrive as FIPA-ACL INFORM messages from DiscoveryAgent.
    """

    def __init__(
        self,
        jid: str,
        password: str,
        output_dir: str | Path = "./output",
        worker_slots: int = WORKER_DEFAULT,
        retries: int = 2,
        verify_security: bool = False,
    ):
        super().__init__(jid, password, verify_security=verify_security)

        self.output_dir   = Path(output_dir)
        self.worker_slots = max(WORKER_MIN, min(WORKER_MAX, worker_slots))
        self.retries      = retries

        # pause flag — checked by QueueConsumerBehaviour
        self.paused = False

        # internal state
        self.download_state = DownloadInternalState()

        # all asyncio primitives created in setup() — event loop is live there
        self.pending_queue:  asyncio.Queue     = None  # type: ignore
        self.worker_sem:     asyncio.Semaphore = None  # type: ignore
        self.all_done_event: asyncio.Event     = None  # type: ignore

    # ── SPADE LIFECYCLE ───────────────────────────────────────────────────

    async def setup(self):
        logger.info("DownloadAgent [%s] starting up", self.jid)
        self.pending_queue   = asyncio.Queue()
        self.worker_sem      = asyncio.Semaphore(self.worker_slots)
        self.all_done_event  = asyncio.Event()
        self.download_state.state_flag = "running"
        self.add_behaviour(QueueConsumerBehaviour())

    # ── PUBLIC API ────────────────────────────────────────────────────────

    def add_job(self, job):
        """Add a job to the pending queue (call from async context only)."""
        self.pending_queue.put_nowait(job)
        with self.download_state.lock:
            self.download_state.total += 1

    def add_jobs(self, jobs: list):
        """Add multiple jobs at once."""
        for j in jobs:
            self.add_job(j)

    def set_worker_slots(self, n: int):
        self.worker_slots = max(WORKER_MIN, min(WORKER_MAX, n))
        logger.info("Worker slots adjusted to %d", self.worker_slots)

    def pause(self):
        self.paused = True
        logger.info("DownloadAgent paused")

    def resume(self):
        self.paused = False
        logger.info("DownloadAgent resumed")

    # ── HELPERS ───────────────────────────────────────────────────────────

    def _log(self, tag: str, msg: str):
        with self.download_state.lock:
            self.download_state.comp_log.append((tag, msg))
        logger.info("DOWNLOAD %s %s", tag, msg)

    def _make_completed(self, job, ok: bool, size_mb: int = 0) -> CompletedDownload:
        return CompletedDownload(
            name=job.title or job.url,
            size_mb=size_mb,
            source=job.source,
            status="OK" if ok else "ERR",
        )


# ─────────────────────────────────────────────────────────────────────────────
# STANDALONE ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

async def _run_standalone():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    jid      = os.environ["DOWNLOAD_JID"]
    password = os.environ["DOWNLOAD_PASSWORD"]

    agent = DownloadAgent(
        jid=jid,
        password=password,
        output_dir="./output",
        worker_slots=2,
        retries=1,
    )

    try:
        await asyncio.wait_for(agent.start(auto_register=False), timeout=15.0)
        print(f"\n[DownloadAgent] started as {jid}")
    except asyncio.TimeoutError:
        print("\n[DownloadAgent] START TIMEOUT (XMPP connect hang?)")
        return
    except Exception as exc:
        print(f"\n[DownloadAgent] START FAILED: {exc}")
        return

    # ── use Discovery to build jobs ────────────────────────────────────
    from agents.discovery.agent import DiscoveryAgent

    disc_jid  = os.environ["DISCOVERY_JID"]
    disc_pass = os.environ["DISCOVERY_PASSWORD"]

    query_file = Path("assets/input/queries.txt")
    if not query_file.exists():
        query_file = Path("test_query.txt")
        query_file.write_text("ghana news today\n")

    disc = DiscoveryAgent(
        jid=disc_jid,
        password=disc_pass,
        query_file=query_file,
        output_dir="./output",
        max_items={"search": 2, "channel": 2, "playlist": 3},
    )
    try:
        await asyncio.wait_for(disc.start(auto_register=False), timeout=15.0)
        print("[DiscoveryAgent] resolving inputs...")
    except asyncio.TimeoutError:
        print("[DiscoveryAgent] START TIMEOUT (XMPP connect hang?)")
        return
    except Exception as exc:
        print(f"[DiscoveryAgent] START FAILED: {exc}")
        return

    try:
        await asyncio.wait_for(disc.resolution_done.wait(), timeout=90)
    except asyncio.TimeoutError:
        print("[DiscoveryAgent] resolution timed out")

    jobs = disc.get_jobs()
    print(f"[DiscoveryAgent] resolved {len(jobs)} jobs — feeding to DownloadAgent")

    logging.getLogger("slixmpp").setLevel(logging.CRITICAL)
    await disc.stop()
    await asyncio.sleep(0.5)

    agent.add_jobs(jobs)
    print(f"[DownloadAgent] downloading {len(jobs)} files with {agent.worker_slots} workers...")

    try:
        await asyncio.wait_for(agent.all_done_event.wait(), timeout=600)
    except asyncio.TimeoutError:
        print("[DownloadAgent] TIMEOUT")

    ds = agent.download_state
    print(f"\n{'─'*60}")
    print(f"  DOWNLOAD RESULTS")
    print(f"{'─'*60}")
    print(f"  State  : {ds.state_flag}")
    print(f"  Done   : {ds.done}  Errors: {ds.errors}  Total: {ds.total}")
    print(f"  Size   : {ds.total_mb} MB")
    print(f"\n  Completed:")
    for c in ds.completed:
        print(f"  [{c.status}] {c.name[:50]}  {c.size_mb}MB  ({c.source})")
    print(f"{'─'*60}\n")

    logging.getLogger("slixmpp").setLevel(logging.CRITICAL)
    await agent.stop()
    await asyncio.sleep(0.5)
    print("[DownloadAgent] stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(_run_standalone())
    except KeyboardInterrupt:
        print("\n[DownloadAgent] interrupted by user.")
