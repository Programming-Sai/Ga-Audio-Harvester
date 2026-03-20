"""
agents/download/behaviours.py
==============================
SPADE behaviours for the DownloadAgent.

Behaviour map (Prometheus capability â†’ SPADE behaviour):
  ConsumeQueue  â†’ QueueConsumerBehaviour  (CyclicBehaviour)
    Pulls jobs from agent.pending_queue, runs up to MAX_WORKERS
    concurrent downloads using asyncio.Semaphore, updates
    agent.download_state in real time via the yt-dlp progress hook.

The semaphore size is controlled by agent.worker_slots so the TUI
can change it at runtime (+ / - keys in Phase C).
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING

import spade.behaviour
from spade.message import Message

if TYPE_CHECKING:
    from .agent import DownloadAgent

logger = logging.getLogger(__name__)

# how often the cyclic behaviour polls for new jobs (seconds)
POLL_INTERVAL = 0.5
XMPP_PROGRESS_PERIOD = 2.5  # seconds between progress publishes (XMPP mode)


class QueueConsumerBehaviour(spade.behaviour.CyclicBehaviour):
    """
    Percept  : agent.pending_queue  (jobs added by Discovery or directly)
    Goal     : download every job, update agent.download_state
    Action   : acquire semaphore slot â†’ call download_video() in thread
               â†’ update worker slot â†’ release semaphore
    Decision : skip if paused; retry logic handled by ResilienceAgent
    """

    async def run(self):
        agent: DownloadAgent = self.agent

        # drain whatever is currently in the pending queue
        jobs_to_start = []
        while not agent.pending_queue.empty():
            try:
                job = agent.pending_queue.get_nowait()
                jobs_to_start.append(job)
            except asyncio.QueueEmpty:
                break

        # fire each job as a separate async task (bounded by semaphore)
        for job in jobs_to_start:
            asyncio.create_task(self._run_job(agent, job))

        await asyncio.sleep(POLL_INTERVAL)

    async def _run_job(self, agent: DownloadAgent, job):
        """Download one job inside the worker semaphore."""
        ds = agent.download_state

        # honour pause flag
        while agent.paused:
            await asyncio.sleep(0.3)

        async with agent.worker_sem:
            slot_idx = self._acquire_slot(agent, job)
            if slot_idx is None:
                # all visible slots occupied â€” put job back
                await agent.pending_queue.put(job)
                return

            try:
                await self._download(agent, job, slot_idx)
            finally:
                # always release the slot and clear job reference
                with ds.lock:
                    w = ds.workers[slot_idx]
                    w.active = False
                    w.pct    = 0.0
                    w.name   = ""
                    w.speed  = 0.0
                    w.job    = None  # clear job reference on slot release
                if getattr(job, "url", ""):
                    agent.active_urls.discard(job.url)

    def _acquire_slot(self, agent: DownloadAgent, job) -> int | None:
        """Return the index of the first idle worker slot, or None."""
        ds = agent.download_state
        with ds.lock:
            for i, w in enumerate(ds.workers):
                if not w.active and i < agent.worker_slots:
                    w.active = True
                    w.name   = job.title or job.url
                    w.job    = job   # store job on slot so ResilienceAgent can read it
                    if getattr(job, "url", ""):
                        agent.active_urls.add(job.url)
                    return i
        return None

    async def _download(self, agent: DownloadAgent, job, slot_idx: int):
        """Run yt-dlp in a thread, map progress hook â†’ worker slot."""
        ds    = agent.download_state
        start = time.time()

        with ds.lock:
            w = ds.workers[slot_idx]
            w.pct   = 0.0
            w.speed = 0.0

        agent._log(
            "[START]",
            f"[W{slot_idx+1}] {(job.title or job.url)[:50]}"
        )

        # â”€â”€ PROGRESS HOOK â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        def _progress(ev: dict):
            status = ev.get("status")
            with ds.lock:
                w = ds.workers[slot_idx]
                if status == "downloading":
                    w.pct   = float(ev.get("progress", w.pct))
                    raw_spd = ev.get("speed") or 0
                    w.speed = raw_spd / (1024 * 1024) if raw_spd else w.speed
                elif status == "finished":
                    w.pct   = 100.0
                # track hook activity so XMPP progress can detect stale telemetry
                agent.slot_last_hook[slot_idx] = time.time()

        # â”€â”€ RUN IN THREAD â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        from download_layer.core import download_video

        out_dir = Path(job.output_dir) if job.output_dir else agent.output_dir

        try:
            # Poll the thread future so XMPP-mode resilience can request a retry.
            # Note: the underlying thread cannot be force-killed; this is best-effort.
            fut = asyncio.create_task(asyncio.to_thread(
                download_video,
                job.url,
                output_dir=out_dir,
                audio_only=True,
                event_callback=_progress,
                retries=agent.retries,
            ))
            ok = None

            def _silence_task(t: asyncio.Task):
                try:
                    t.result()
                except Exception:
                    return

            while True:
                done, _pending = await asyncio.wait({fut}, timeout=0.5)
                if done:
                    ok = bool(list(done)[0].result())
                    break
                if (time.time() - start) > 300.0:
                    fut.add_done_callback(_silence_task)
                    raise asyncio.TimeoutError()
                if agent.use_xmpp and job.url in agent.cancel_requested:
                    fut.add_done_callback(_silence_task)
                    ok = False
                    break
        except asyncio.TimeoutError:
            ok = False
            agent._log("[ERR]", f"[W{slot_idx+1}] timed out after 300s: {job.url[:50]}")
        except Exception as exc:
            ok = False
            agent._log("[ERR]", f"[W{slot_idx+1}] exception: {exc}")

        elapsed = time.time() - start

        # If resilience requested a retry, treat this attempt as transient and re-queue
        # the same job without incrementing totals/errors.
        if agent.use_xmpp and job.url in agent.cancel_requested:
            agent.cancel_requested.discard(job.url)
            agent._log("[RETRY]", f"[W{slot_idx+1}] retry requested — re-queuing: {job.url[:50]}")
            await agent.pending_queue.put(job)
            return

        # â”€â”€ UPDATE STATE â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        with ds.lock:
            w    = ds.workers[slot_idx]
            name = w.name

            if ok:
                ds.done += 1
                tag = "[OK]"
                msg = f"{name[:30]}  {elapsed:.0f}s  meta:OK"
                ds.comp_log.append((tag, msg))
                est_mb = int((w.speed or 0) * elapsed)
                ds.total_mb += est_mb
                cd = agent._make_completed(job, ok=True, size_mb=est_mb)
            else:
                ds.errors += 1
                tag = "[ERR]"
                msg = f"{name[:30]}  failed after {elapsed:.0f}s"
                ds.comp_log.append((tag, msg))
                cd = agent._make_completed(job, ok=False)

            ds.completed.append(cd)

            # recalc aggregate speed
            active_speeds = [x.speed for x in ds.workers if x.active]
            ds.speed = sum(active_speeds)
            rem   = ds.total - ds.done
            ds.eta = (rem / max(len(active_speeds), 1)) * 5 if active_speeds else 0

        agent._log(tag, msg)

        if agent.use_xmpp:
            from agents.shared.ontology import (
                DownloadFinishedMsg,
                DownloadFailedMsg,
            )
            if ok:
                await self._send_resilience_msg(DownloadFinishedMsg(
                    url=job.url,
                    filename=job.title or "",
                    size_mb=cd.size_mb,
                ))
            else:
                await self._send_resilience_msg(DownloadFailedMsg(
                    url=job.url,
                    reason="download_failed",
                    attempt=0,
                ))

        # check if all done
        with ds.lock:
            all_done = ds.done + ds.errors >= ds.total and ds.total > 0

        if all_done:
            with ds.lock:
                ds.state_flag = "done"
            agent.all_done_event.set()
            agent._log("[DONE]", f"All downloads complete â€” {ds.done} OK  {ds.errors} ERR")
            if agent.use_xmpp:
                from agents.shared.ontology import DownloadAllDoneMsg
                await self._send_resilience_msg(DownloadAllDoneMsg(
                    done=ds.done,
                    errors=ds.errors,
                    total_mb=ds.total_mb,
                ))

    async def _send_resilience_msg(self, msg_dc):
        agent = self.agent
        if not agent.use_xmpp or not agent.resilience_jid:
            return
        try:
            from agents.shared.ontology import INFORM, ONTOLOGY, encode
            msg = Message(to=agent.resilience_jid)
            msg.set_metadata("performative", INFORM)
            msg.set_metadata("ontology", ONTOLOGY)
            msg.body = encode(msg_dc)
            if agent.xmpp_debug:
                logger.info("XMPP SEND -> %s | %s", agent.resilience_jid, msg.body)
            await self.send(msg)
        except Exception as exc:
            logger.warning("Failed to send to Resilience via XMPP: %s", exc)


class XmppProgressPublisherBehaviour(spade.behaviour.PeriodicBehaviour):
    """
    Periodically publishes per-worker progress to ResilienceAgent in XMPP mode.

    This is throttled and designed to support ResilienceAgent stall detection
    without flooding the server with stanzas.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._last_sent: dict[str, tuple[float, float]] = {}  # url -> (ts, pct)

    async def run(self):
        agent: DownloadAgent = self.agent
        if not agent.use_xmpp or not agent.resilience_jid:
            return

        from agents.shared.ontology import INFORM, ONTOLOGY, DownloadProgressMsg, encode

        now = time.time()
        with agent.download_state.lock:
            snap = []
            for idx, w in enumerate(agent.download_state.workers):
                if not w.active or not w.job:
                    continue
                url = getattr(w.job, "url", "") or ""
                if not url:
                    continue
                hook_ts = float(agent.slot_last_hook.get(idx, 0.0) or 0.0)
                snap.append((url, float(w.pct), float(w.speed), getattr(w.job, "title", "") or "", hook_ts))

        for url, pct, speed_mb, title, hook_ts in snap:
            last_ts, last_pct = self._last_sent.get(url, (0.0, -1.0))
            if (now - last_ts) < XMPP_PROGRESS_PERIOD and pct == last_pct:
                continue

            self._last_sent[url] = (now, pct)
            speed_bytes = speed_mb * 1024 * 1024  # schema expects bytes/s
            # If we haven't seen a hook update for a while, treat speed as 0 to help stall detection.
            if hook_ts and (now - hook_ts) > 8.0:
                speed_bytes = 0.0
            msg = Message(to=agent.resilience_jid)
            msg.set_metadata("performative", INFORM)
            msg.set_metadata("ontology", ONTOLOGY)
            msg.body = encode(DownloadProgressMsg(
                url=url,
                pct=pct,
                speed=speed_bytes,
                eta=0.0,
                filename=title,
            ))
            if agent.xmpp_debug:
                logger.info("XMPP SEND progress -> %s | %s", agent.resilience_jid, msg.body)
            try:
                await self.send(msg)
            except Exception as exc:
                logger.warning("Failed to send progress via XMPP: %s", exc)


class XmppInboxBehaviour(spade.behaviour.CyclicBehaviour):
    """
    Receives XMPP messages and converts them into local download jobs.
    """

    async def run(self):
        agent: DownloadAgent = self.agent
        msg = await self.receive(timeout=1)
        if not msg:
            return

        from agents.shared.ontology import (
            ONTOLOGY,
            MSG_JOB_ENQUEUE,
            MSG_DISCOVERY_DONE,
            MSG_RETRY,
            decode,
        )
        if msg.get_metadata("ontology") != ONTOLOGY:
            return

        body = decode(msg.body)
        if agent.xmpp_debug:
            logger.info("XMPP RECV <- %s | %s", msg.sender, msg.body)
        mtype = body.get("type")
        if mtype == MSG_JOB_ENQUEUE:
            from agents.download.agent import DownloadJob
            job = DownloadJob(
                url=body.get("url", ""),
                source=body.get("source", ""),
                query_key=body.get("query_key", ""),
                title=body.get("title", ""),
                output_dir=body.get("output_dir", ""),
            )
            if job.url:
                agent.add_job(job)
        elif mtype == MSG_DISCOVERY_DONE:
            agent.discovery_done_event.set()
        elif mtype == MSG_RETRY:
            url = body.get("url", "")
            if not url:
                return

            # If currently active, mark cancel requested; QueueConsumerBehaviour will requeue.
            if url in agent.active_urls:
                agent.cancel_requested.add(url)
                agent._log("[RETRY]", f"Resilience requested retry (active): {url[:50]}")
                return

            # Otherwise, requeue immediately using remembered metadata (no total increment).
            job = agent.job_templates.get(url)
            if job is None:
                from agents.download.agent import DownloadJob
                job = DownloadJob(url=url, source="retry", query_key=url, title="", output_dir="")
                agent.job_templates[url] = job

            await agent.pending_queue.put(job)
            agent._log("[RETRY]", f"Resilience requested retry — queued: {url[:50]}")
