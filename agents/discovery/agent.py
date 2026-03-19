"""
agents/discovery/agent.py
==========================
DiscoveryAgent — SPADE agent, Phase A (standalone).

Responsibilities (Prometheus agent descriptor):
  Goals     : ResolveInputs, BuildJobQueue
  Percepts  : QueryFile contents, YouTube API responses
  Actions   : ClassifyQuery, ResolveURL, SearchYouTube,
               FetchChannelURLs, FetchPlaylistURLs, EnqueueJob
  Data      : job_queue, discovery_state
  Comms     : none in Phase A  (Phase B: INFORM → DownloadAgent)

Run standalone:
    python -m agents.discovery.agent
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
from dataclasses import dataclass, field
from collections import deque
from pathlib import Path
from typing import Optional

import spade
import spade.agent
from dotenv import load_dotenv

from agents.discovery.behaviours import ResolveBehaviour

load_dotenv()
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# INTERNAL STATE  (mirrors tui_v3 DiscoveryState for Phase C bridge)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class DiscoveryInternalState:
    """
    Mirrors the TUI DiscoveryState dataclass so Phase C can simply
    copy fields across without transformation.
    """
    state_flag: str   = "standby"   # standby | running | done | error
    n_inputs:   int   = 0
    types:      dict  = field(default_factory=dict)
    res_log:    deque = field(default_factory=lambda: deque(maxlen=500))
    resolved:   dict  = field(default_factory=lambda: {
                            "search": 0, "channel": 0,
                            "playlist": 0, "direct": 0})
    skipped:    int   = 0
    errors:     int   = 0
    total:      int   = 0
    elapsed:    float = 0.0
    lock:       threading.Lock = field(default_factory=threading.Lock)


# ─────────────────────────────────────────────────────────────────────────────
# JOB  (unit of work passed to DownloadAgent)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Job:
    url:       str
    source:    str   # search | channel | playlist | direct
    query_key: str   # original query / channel URL / playlist URL
    title:     str   = ""
    output_dir:str   = ""


# ─────────────────────────────────────────────────────────────────────────────
# DISCOVERY AGENT
# ─────────────────────────────────────────────────────────────────────────────

class DiscoveryAgent(spade.agent.Agent):
    """
    Perceives the query file, resolves all input types into a flat list
    of downloadable URLs, and exposes them via self.job_queue.

    Phase A: standalone — no XMPP messaging.
    Phase B: will INFORM DownloadAgent for each job, then send
             discovery.done when queue is fully populated.
    """

    def __init__(
        self,
        jid: str,
        password: str,
        query_file: str | Path,
        output_dir: str | Path = "./output",
        max_items: Optional[dict] = None,
        verify_security: bool = False,
        use_xmpp: bool = False,
        download_jid: Optional[str] = None,
        xmpp_debug: bool = False,
    ):
        super().__init__(jid, password, verify_security=verify_security)

        self.query_file  = str(query_file)
        self.output_dir  = Path(output_dir)
        self.max_items   = max_items or {
            "search":   10,
            "channel":  10,
            "playlist": 20,
        }
        self.use_xmpp = use_xmpp
        self.download_jid = download_jid
        self.xmpp_debug = xmpp_debug

        # populated by ResolveBehaviour
        self.job_queue: list[Job] = []
        self.job_lock = threading.Lock()

        # internal state (read by TUI bridge in Phase C)
        self.discovery_state = DiscoveryInternalState()

        # created in setup() once the event loop is running
        self.resolution_done: asyncio.Event = None  # type: ignore

    # ── SPADE LIFECYCLE ───────────────────────────────────────────────────

    async def setup(self):
        logger.info("DiscoveryAgent [%s] starting up", self.jid)
        self.resolution_done = asyncio.Event()   # created here — loop is live
        self.discovery_state.state_flag = "running"
        self.add_behaviour(ResolveBehaviour())

    # ── HELPERS ───────────────────────────────────────────────────────────

    def _log(self, tag: str, msg: str):
        """Append to internal log and Python logger."""
        with self.discovery_state.lock:
            self.discovery_state.res_log.append((tag, msg))
        logger.info("DISCOVERY %s %s", tag, msg)

    async def _enqueue_job(
        self,
        url: str,
        source: str,
        query_key: str,
        title: str = "",
    ):
        from download_layer.utils import sanitize_query_to_filename
        from urllib.parse import urlparse

        if title:
            folder = sanitize_query_to_filename(title[:40])
        elif source == "channel":
            # extract @handle or channel ID from URL
            path = urlparse(query_key).path.rstrip("/")
            raw  = path.split("/")[-1].lstrip("@")
            folder = sanitize_query_to_filename(raw[:40]) or "unknown_channel"
        else:
            folder = sanitize_query_to_filename(query_key[:40])

        if not folder:
            folder = "unknown"

        job = Job(
            url=url,
            source=source,
            query_key=query_key,
            title=title,
            output_dir=str(
                self.output_dir / source /
                (folder if source != "direct" else "single_videos")
            ),
        )
        # always store locally for inspection and TUI mirroring
        with self.job_lock:
            self.job_queue.append(job)

        return job

    def get_jobs(self) -> list[Job]:
        """Return a snapshot of the current job queue."""
        with self.job_lock:
            return list(self.job_queue)



# ─────────────────────────────────────────────────────────────────────────────
# STANDALONE ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

async def _run_standalone():
    """
    Phase A standalone test.
    Runs the agent, waits for resolution to complete, prints the job list.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    jid      = os.environ["DISCOVERY_JID"]
    password = os.environ["DISCOVERY_PASSWORD"]

    query_file = Path("assets/input/queries.txt")
    if not query_file.exists():
        # fallback for testing outside project root
        query_file = Path("test_query.txt")
        query_file.write_text(
            "https://www.youtube.com/@OBonuTV\n"
            "ghana news today\n"
        )

    agent = DiscoveryAgent(
        jid=jid,
        password=password,
        query_file=query_file,
        output_dir="./output",
        max_items={"search": 3, "channel": 3, "playlist": 5},
    )

    try:
        await asyncio.wait_for(agent.start(auto_register=False), timeout=15.0)
        print(f"\n[DiscoveryAgent] started as {jid}")
    except asyncio.TimeoutError:
        print("\n[DiscoveryAgent] START TIMEOUT (XMPP connect hang?)")
        return
    except Exception as exc:
        print(f"\n[DiscoveryAgent] START FAILED: {exc}")
        return
    print("[DiscoveryAgent] resolving inputs...\n")

    # wait for resolution or timeout
    try:
        await asyncio.wait_for(agent.resolution_done.wait(), timeout=120)
    except asyncio.TimeoutError:
        print("[DiscoveryAgent] TIMEOUT — resolution did not complete in 120s")

    jobs = agent.get_jobs()
    ds   = agent.discovery_state

    print(f"\n{'─'*60}")
    print(f"  DISCOVERY RESULTS")
    print(f"{'─'*60}")
    print(f"  State   : {ds.state_flag}")
    print(f"  Inputs  : {ds.n_inputs}")
    print(f"  Total   : {ds.total}  skipped:{ds.skipped}  errors:{ds.errors}")
    print(f"  Elapsed : {ds.elapsed:.1f}s")
    print(f"\n  Resolved counts:")
    for t, n in ds.resolved.items():
        print(f"    {t:<12} {n}")
    print(f"\n  Job queue ({len(jobs)} items):")
    for i, j in enumerate(jobs[:20], 1):
        print(f"    {i:>3}. [{j.source:<8}] {j.url[:60]}")
    if len(jobs) > 20:
        print(f"         ... and {len(jobs)-20} more")
    print(f"{'─'*60}\n")

    logging.getLogger("slixmpp").setLevel(logging.CRITICAL)
    await agent.stop()
    await asyncio.sleep(0.5)
    print("[DiscoveryAgent] stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(_run_standalone())
    except KeyboardInterrupt:
        print("\n[DiscoveryAgent] interrupted by user.")
    except RuntimeError as e:
        if "Executor shutdown" in str(e):
            pass  # slixmpp reconnect loop firing after event loop closes — harmless
        else:
            raise
    finally:
        # suppress the RuntimeWarning about executor threads not joining
        import concurrent.futures
        import logging
        logging.getLogger("asyncio").setLevel(logging.CRITICAL)
