"""
agents/resilience/behaviours.py
================================
SPADE behaviours for the ResilienceAgent.

Behaviour map (Prometheus capability ? SPADE behaviour):
  MonitorPipeline  ? HeartbeatBehaviour   (PeriodicBehaviour, 10s)
    Logs overall pipeline health and error growth.

  WatchDownloads   ? StallDetectorBehaviour  (PeriodicBehaviour, 5s)
    Watches each active job for zero-progress stalls.
    If a job hasn't moved in STALL_THRESHOLD seconds, decides
    whether to retry the job based on error type.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import spade.behaviour
from spade.message import Message

from agents.shared.ontology import (
    REQUEST,
    RetryMsg,
    encode,
    ONTOLOGY,
    MSG_DL_PROGRESS,
    MSG_DL_FINISHED,
    MSG_DL_FAILED,
    MSG_DL_ALL_DONE,
    decode,
    msg_type,
)

if TYPE_CHECKING:
    from .agent import ResilienceAgent

logger = logging.getLogger(__name__)

STALL_THRESHOLD  = 15   # seconds without progress before acting
HEARTBEAT_PERIOD = 10   # seconds between heartbeat checks
STALL_PERIOD     = 5    # seconds between stall checks
MAX_RETRIES      = 3    # maximum retries per job before giving up


class MessageReceiverBehaviour(spade.behaviour.CyclicBehaviour):
    """
    Receive download progress from DownloadAgent over XMPP.
    Updates internal progress maps and error counters.
    """

    async def run(self):
        agent: ResilienceAgent = self.agent
        rs = agent.resilience_state

        msg = await self.receive(timeout=1)
        if not msg:
            return
        if msg.metadata.get("ontology") != ONTOLOGY:
            return

        body = msg.body or ""
        mtype = msg_type(body)
        data = decode(body)
        now = time.time()

        if mtype == MSG_DL_PROGRESS:
            url = data.get("url", "")
            pct = float(data.get("pct", 0.0))
            with rs.lock:
                last_pct = rs.job_last_pct.get(url, -1)
                if pct != last_pct:
                    rs.job_last_pct[url] = pct
                    rs.job_last_move[url] = now
                    if url in rs.job_stall_warned:
                        rs.job_stall_warned.discard(url)
                        agent._log("[OK]", f"{url[:30]} resumed ? stall cleared")
        elif mtype == MSG_DL_FINISHED:
            url = data.get("url", "")
            with rs.lock:
                rs.job_last_pct.pop(url, None)
                rs.job_last_move.pop(url, None)
                rs.job_stall_warned.discard(url)
        elif mtype == MSG_DL_FAILED:
            url = data.get("url", "")
            with rs.lock:
                rs.errors += 1
            agent._log("[WARN]", f"Download failed: {url[:40]}")
        elif mtype == MSG_DL_ALL_DONE:
            with rs.lock:
                rs.state_flag = "done"
            agent._log("[DONE]", "Pipeline complete ? download all done")
            agent.all_done_event.set()


class HeartbeatBehaviour(spade.behaviour.PeriodicBehaviour):
    """
    Logs heartbeat and error counts based on XMPP updates.
    """

    async def run(self):
        agent: ResilienceAgent = self.agent
        rs = agent.resilience_state

        with rs.lock:
            active = len(rs.job_last_pct)
            errs = rs.errors

        if errs > rs.last_known_errors:
            delta = errs - rs.last_known_errors
            rs.last_known_errors = errs
            agent._log("[WARN]", f"{delta} new download error(s) ? total: {errs}")

        agent._log("[HBEAT]", f"DL:RUNNING  active:{active}  errors:{errs}")

        with rs.lock:
            rs.heartbeat_count += 1


class StallDetectorBehaviour(spade.behaviour.PeriodicBehaviour):
    """
    Detects stalled downloads based on XMPP progress updates.
    Decision logic:
      - pct > 0 at stall time  ? transient, retry (up to MAX_RETRIES)
      - pct == 0 at stall time ? permanent, skip
    """


    async def _send_retry(self, agent, url: str, reason: str = "stall"):
        if not agent.download_jid:
            return
        msg = Message(to=agent.download_jid)
        msg.set_metadata("ontology", ONTOLOGY)
        msg.set_metadata("performative", REQUEST)
        msg.body = encode(RetryMsg(url=url, reason=reason))
        await self.send(msg)
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._retry_counts: dict[str, int] = {}

    async def run(self):
        agent: ResilienceAgent = self.agent
        rs = agent.resilience_state

        now = time.time()

        with rs.lock:
            items = list(rs.job_last_pct.items())
            last_moves = dict(rs.job_last_move)
            warned = set(rs.job_stall_warned)

        for url, pct in items:
            last_move = last_moves.get(url, now)
            stall_secs = now - last_move
            if stall_secs < STALL_THRESHOLD or url in warned:
                continue

            with rs.lock:
                rs.job_stall_warned.add(url)

            retry_count = self._retry_counts.get(url, 0)

            if pct > 0 and retry_count < MAX_RETRIES:
                self._retry_counts[url] = retry_count + 1
                with rs.lock:
                    rs.retries += 1
                agent._log(
                    "[RETRY]",
                    f"stall at {pct:.0f}% ? re-queuing {url[:30]} (attempt {retry_count + 1}/{MAX_RETRIES})"
                )
                await self._send_retry(agent, url, reason="stall")

            elif pct > 0 and retry_count >= MAX_RETRIES:
                with rs.lock:
                    rs.failures += 1
                agent._log(
                    "[FAIL]",
                    f"max retries ({MAX_RETRIES}) exhausted: {url[:30]}"
                )

            else:
                with rs.lock:
                    rs.retries += 1
                agent._log(
                    "[SKIP]",
                    f"stall at 0% ? not retrying (likely 403/bad path): {url[:30]}"
                )
