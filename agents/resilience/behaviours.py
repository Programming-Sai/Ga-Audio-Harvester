"""
agents/resilience/behaviours.py
================================
SPADE behaviours for the ResilienceAgent.

Behaviour map (Prometheus capability â†’ SPADE behaviour):
  MonitorPipeline  â†’ HeartbeatBehaviour   (PeriodicBehaviour, 10s)
    Checks that Discovery and Download agents are alive and
    progressing.  Detects stalls and emits warnings.

  WatchDownloads   â†’ StallDetectorBehaviour  (PeriodicBehaviour, 5s)
    Watches each active worker slot for zero-progress stalls.
    If a slot hasn't moved in STALL_THRESHOLD seconds, decides
    whether to retry the job or skip it based on error type:

    RETRYABLE  â€” slot made some progress (pct > 0) before stalling.
                 This suggests a transient network issue. Job is
                 re-queued into agent.pending_queue.

    NOT RETRYABLE â€” slot stalled at exactly 0% (HTTP 403, bad path,
                    unavailable video). Retrying is pointless.
                    Logged as skipped.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import spade.behaviour

if TYPE_CHECKING:
    from .agent import ResilienceAgent

logger = logging.getLogger(__name__)

STALL_THRESHOLD  = 15   # seconds without progress before acting
HEARTBEAT_PERIOD = 10   # seconds between heartbeat checks
STALL_PERIOD     = 5    # seconds between stall checks
MAX_RETRIES      = 3    # maximum retries per job before giving up


class HeartbeatBehaviour(spade.behaviour.PeriodicBehaviour):
    """
    Percept  : state_flag of watched agents
    Goal     : confirm all agents are progressing
    Action   : log status, count uptime ticks, warn on new errors
    """

    async def run(self):
        agent: ResilienceAgent = self.agent
        rs = agent.resilience_state

        disc = agent.watched_agents.get("discovery")
        dl   = agent.watched_agents.get("download")

        disc_status = disc.discovery_state.state_flag.upper() if disc else "â€”"
        dl_status   = dl.download_state.state_flag.upper()    if dl   else "â€”"

        if dl:
            with dl.download_state.lock:
                errs = dl.download_state.errors

            if errs > rs.last_known_errors:
                delta = errs - rs.last_known_errors
                rs.last_known_errors = errs
                agent._log(
                    "[WARN]",
                    f"{delta} new download error(s) â€” total: {errs}"
                )

        agent._log("[HBEAT]", f"DISC:{disc_status}  DL:{dl_status}  uptime:100%")

        with rs.lock:
            rs.heartbeat_count += 1


class StallDetectorBehaviour(spade.behaviour.PeriodicBehaviour):
    """
    Percept  : worker slot pct + job values over time
    Goal     : detect stalls, retry if transient, skip if permanent
    Decision logic:
      - pct > 0 at stall time  â†’ transient, re-queue job (up to MAX_RETRIES)
      - pct == 0 at stall time â†’ permanent (403 / bad path), skip
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # track retry counts per job URL to enforce MAX_RETRIES
        self._retry_counts: dict[str, int] = {}

    async def run(self):
        agent: ResilienceAgent = self.agent
        rs = agent.resilience_state

        dl = agent.watched_agents.get("download")
        if not dl:
            return

        now = time.time()

        with dl.download_state.lock:
            workers = [
                (w.idx, w.name, w.pct, w.active, w.job)
                for w in dl.download_state.workers
            ]

        for idx, name, pct, active, job in workers:
            if not active:
                # clear stall record if slot went idle
                rs.slot_last_pct.pop(idx, None)
                rs.slot_last_move.pop(idx, None)
                rs.slot_stall_warned.discard(idx)
                continue

            last_pct  = rs.slot_last_pct.get(idx, -1)
            last_move = rs.slot_last_move.get(idx, now)

            if pct != last_pct:
                # progress detected â€” clear any stall warning
                rs.slot_last_pct[idx]  = pct
                rs.slot_last_move[idx] = now
                if idx in rs.slot_stall_warned:
                    rs.slot_stall_warned.discard(idx)
                    agent._log("[OK]", f"W{idx} resumed â€” stall cleared")
            else:
                stall_secs = now - last_move
                if stall_secs < STALL_THRESHOLD or idx in rs.slot_stall_warned:
                    continue

                # â”€â”€ STALL DETECTED â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
                rs.slot_stall_warned.add(idx)

                if job is None:
                    # no job reference available â€” can't retry
                    agent._log("[WARN]", f"W{idx} stall >{STALL_THRESHOLD}s â€” no job ref, skipping")
                    continue

                job_key = job.url
                retry_count = self._retry_counts.get(job_key, 0)

                if pct > 0 and retry_count < MAX_RETRIES:
                    # â”€â”€ RETRYABLE: made some progress, transient issue â”€â”€
                    self._retry_counts[job_key] = retry_count + 1
                    with rs.lock:
                        rs.retries += 1

                    agent._log(
                        "[RETRY]",
                        f"W{idx} stall at {pct:.0f}% â€” re-queuing: "
                        f"{(job.title or job.url)[:30]}  "
                        f"(attempt {retry_count + 1}/{MAX_RETRIES})"
                    )

                    # re-queue the job â€” total stays the same since
                    # it was already counted when first added
                    dl.pending_queue.put_nowait(job)

                elif pct > 0 and retry_count >= MAX_RETRIES:
                    # â”€â”€ EXHAUSTED RETRIES â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
                    with rs.lock:
                        rs.failures += 1
                    agent._log(
                        "[FAIL]",
                        f"W{idx} â€” max retries ({MAX_RETRIES}) exhausted: "
                        f"{(job.title or job.url)[:30]}"
                    )

                else:
                    # â”€â”€ NOT RETRYABLE: stalled at 0% â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
                    # 403 Forbidden, bad path, unavailable video etc.
                    # Retrying the same URL will produce the same result.
                    with rs.lock:
                        rs.retries += 1  # count it but don't re-queue
                    agent._log(
                        "[SKIP]",
                        f"W{idx} stall at 0% â€” not retrying "
                        f"(likely 403/bad path): "
                        f"{(job.title or job.url)[:30]}"
                    )


class XmppInboxBehaviour(spade.behaviour.CyclicBehaviour):
    """
    Receives XMPP status messages from DownloadAgent.
    """

    async def run(self):
        agent: ResilienceAgent = self.agent
        msg = await self.receive(timeout=1)
        if not msg:
            return

        from agents.shared.ontology import (
            ONTOLOGY,
            MSG_DL_FINISHED,
            MSG_DL_FAILED,
            MSG_DL_ALL_DONE,
            decode,
        )
        if msg.get_metadata("ontology") != ONTOLOGY:
            return

        body = decode(msg.body)
        if agent.xmpp_debug:
            logger.info("XMPP RECV <- %s | %s", msg.sender, msg.body)
        mtype = body.get("type")
        rs = agent.resilience_state

        if mtype == MSG_DL_FINISHED:
            agent._log("[OK]", f"Finished: {body.get('url', '')[:50]}")
        elif mtype == MSG_DL_FAILED:
            with rs.lock:
                rs.failures += 1
            agent._log("[FAIL]", f"Failed: {body.get('url', '')[:50]}")
        elif mtype == MSG_DL_ALL_DONE:
            rs.state_flag = "done"
            agent._log("[DONE]", "Download agent reported all done")
