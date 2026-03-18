"""
agents/download/behaviours.py
==============================
SPADE behaviours for the DownloadAgent.

Behaviour map (Prometheus capability → SPADE behaviour):
  ConsumeQueue  → QueueConsumerBehaviour  (CyclicBehaviour)
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

from agents.shared.ontology import (
    ONTOLOGY,
    MSG_JOB_ENQUEUE,
    MSG_DISCOVERY_DONE,
    MSG_RETRY,
    MSG_PAUSE,
    MSG_RESUME,
    DownloadProgressMsg,
    DownloadFinishedMsg,
    DownloadFailedMsg,
    DownloadAllDoneMsg,
    INFORM,
    decode,
    msg_type,
    encode,
)

if TYPE_CHECKING:
    from .agent import DownloadAgent

logger = logging.getLogger(__name__)

# how often the cyclic behaviour polls for new jobs (seconds)
POLL_INTERVAL = 0.5


class QueueConsumerBehaviour(spade.behaviour.CyclicBehaviour):
    """
    Percept  : agent.pending_queue  (jobs added by Discovery or directly)
    Goal     : download every job, update agent.download_state
    Action   : acquire semaphore slot → call download_video() in thread
               → update worker slot → release semaphore
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
                # all visible slots occupied — put job back
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

    def _acquire_slot(self, agent: DownloadAgent, job) -> int | None:
        """Return the index of the first idle worker slot, or None."""
        ds = agent.download_state
        with ds.lock:
            for i, w in enumerate(ds.workers):
                if not w.active and i < agent.worker_slots:
                    w.active = True
                    w.name   = job.title or job.url
                    w.job    = job   # store job on slot so ResilienceAgent can read it
                    return i
        return None

    async def _download(self, agent: DownloadAgent, job, slot_idx: int):
        """Run yt-dlp in a thread, map progress hook → worker slot."""
        ds    = agent.download_state
        start = time.time()
        loop  = asyncio.get_running_loop()

        with ds.lock:
            w = ds.workers[slot_idx]
            w.pct   = 0.0
            w.speed = 0.0

        agent._log(
            "[START]",
            f"[W{slot_idx+1}] {(job.title or job.url)[:50]}"
        )
        await self._send_progress(agent, job.url, pct=0.0)

        # ── PROGRESS HOOK ─────────────────────────────────────────────
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

            if status == "downloading":
                pct = float(ev.get("progress", 0.0))
                speed = float(ev.get("speed") or 0.0)
                eta = float(ev.get("eta") or 0.0)
                filename = ev.get("filename") or ""
                loop.call_soon_threadsafe(
                    asyncio.create_task,
                    self._send_progress(agent, job.url, pct, speed=speed, eta=eta, filename=filename),
                )

        # ── RUN IN THREAD ─────────────────────────────────────────────
        from download_layer.core import download_video

        out_dir = Path(job.output_dir) if job.output_dir else agent.output_dir

        failed_sent = False
        try:
            ok = await asyncio.wait_for(
                asyncio.to_thread(
                    download_video,
                    job.url,
                    output_dir=out_dir,
                    audio_only=True,
                    event_callback=_progress,
                    retries=agent.retries,
                ),
                timeout=300.0  # 5 min max per download
            )
        except asyncio.TimeoutError:
            ok = False
            agent._log("[ERR]", f"[W{slot_idx+1}] timed out after 300s: {job.url[:50]}")
            await self._send_failed(agent, job.url, reason="timeout")
            failed_sent = True
        except Exception as exc:
            ok = False
            agent._log("[ERR]", f"[W{slot_idx+1}] exception: {exc}")
            await self._send_failed(agent, job.url, reason=str(exc))
            failed_sent = True

        elapsed = time.time() - start

        # ── UPDATE STATE ──────────────────────────────────────────────
        finished_mb = 0
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
                finished_mb = est_mb
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
        if ok:
            await self._send_finished(agent, job.url, size_mb=finished_mb)
        elif not failed_sent:
            await self._send_failed(agent, job.url, reason="failed")

        # check if all done
        with ds.lock:
            all_done = ds.done + ds.errors >= ds.total and ds.total > 0

        if all_done:
            with ds.lock:
                ds.state_flag = "done"
            agent.all_done_event.set()
            agent._log("[DONE]", f"All downloads complete ? {ds.done} OK  {ds.errors} ERR")
            await self._send_all_done(agent, ds.done, ds.errors, ds.total_mb)

    async def _send_progress(self, agent: DownloadAgent, url: str, pct: float, speed: float = 0.0, eta: float = 0.0, filename: str = ""):
        if not agent.resilience_jid:
            return
        msg = Message(to=agent.resilience_jid)
        msg.set_metadata("ontology", ONTOLOGY)
        msg.set_metadata("performative", INFORM)
        msg.body = encode(DownloadProgressMsg(
            url=url, pct=pct, speed=speed, eta=eta, filename=filename
        ))
        await self.send(msg)

    async def _send_finished(self, agent: DownloadAgent, url: str, filename: str = "", size_mb: int = 0):
        if not agent.resilience_jid:
            return
        msg = Message(to=agent.resilience_jid)
        msg.set_metadata("ontology", ONTOLOGY)
        msg.set_metadata("performative", INFORM)
        msg.body = encode(DownloadFinishedMsg(
            url=url, filename=filename, size_mb=size_mb
        ))
        await self.send(msg)

    async def _send_failed(self, agent: DownloadAgent, url: str, reason: str = "", attempt: int = 0):
        if not agent.resilience_jid:
            return
        msg = Message(to=agent.resilience_jid)
        msg.set_metadata("ontology", ONTOLOGY)
        msg.set_metadata("performative", INFORM)
        msg.body = encode(DownloadFailedMsg(
            url=url, reason=reason, attempt=attempt
        ))
        await self.send(msg)

    async def _send_all_done(self, agent: DownloadAgent, done: int, errors: int, total_mb: int):
        if not agent.resilience_jid:
            return
        msg = Message(to=agent.resilience_jid)
        msg.set_metadata("ontology", ONTOLOGY)
        msg.set_metadata("performative", INFORM)
        msg.body = encode(DownloadAllDoneMsg(
            done=done, errors=errors, total_mb=total_mb
        ))
        await self.send(msg)

class MessageReceiverBehaviour(spade.behaviour.CyclicBehaviour):
    """
    Receives XMPP messages from Discovery (jobs) and Resilience (retry/pause/resume).
    """

    async def run(self):
        agent: DownloadAgent = self.agent
        msg = await self.receive(timeout=1)
        if not msg:
            return
        if msg.metadata.get("ontology") != ONTOLOGY:
            return

        body = msg.body or ""
        mtype = msg_type(body)
        data = decode(body)

        if mtype == MSG_JOB_ENQUEUE:
            job = agent.make_job(
                url=data.get("url", ""),
                source=data.get("source", ""),
                query_key=data.get("query_key", ""),
                title=data.get("title", ""),
                output_dir=data.get("output_dir", ""),
            )
            agent.add_job(job)
        elif mtype == MSG_DISCOVERY_DONE:
            agent.discovery_done = True
        elif mtype == MSG_RETRY:
            url = data.get("url", "")
            job = agent.job_cache.get(url)
            if job:
                await agent.pending_queue.put(job)
        elif mtype == MSG_PAUSE:
            agent.pause()
        elif mtype == MSG_RESUME:
            agent.resume()

