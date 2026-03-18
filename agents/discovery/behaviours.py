"""
agents/discovery/behaviours.py
===============================
SPADE behaviours for the DiscoveryAgent.

Behaviour map (Prometheus capability → SPADE behaviour):
  ResolveInputs  → ResolveBehaviour   (OneShotBehaviour)
    Parses the query file, classifies each entry, resolves URLs /
    search results, applies the cap, emits per-item log events.

The behaviour writes resolved jobs into agent.job_queue (a list) and
updates agent.discovery_state directly so the TUI bridge (Phase C) can
read it at any time.
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
    INFORM,
    JobEnqueueMsg,
    DiscoveryDoneMsg,
    encode,
)

if TYPE_CHECKING:
    from .agent import DiscoveryAgent

logger = logging.getLogger(__name__)


class ResolveBehaviour(spade.behaviour.OneShotBehaviour):
    """
    Percept  : query file path  (agent.query_file)
    Goal     : populate agent.job_queue with resolved URLs
    Action   : load → classify → resolve → cap → enqueue
    Decision : skip private/unavailable sources, warn on errors
    """

    async def run(self):
        agent: DiscoveryAgent = self.agent
        ds = agent.discovery_state
        self._start = time.time()  # stored on self so helpers can reference it

        agent._log("[INIT]", f"Discovery started — input: {agent.query_file}")

        # ── 1. LOAD QUERIES ───────────────────────────────────────────────
        try:
            from input_layer.loader import load_queries
            raw_queries = load_queries(agent.query_file)
        except Exception as exc:
            agent._log("[ERR]", f"Failed to load query file: {exc}")
            ds.state_flag = "error"
            agent.resolution_done.set()  # unblock the waiter even on error
            return

        with ds.lock:
            ds.n_inputs = len(raw_queries)

        agent._log("[PARSE]", f"Loaded {len(raw_queries)} inputs from {agent.query_file}")

        # ── 2. CLASSIFY ───────────────────────────────────────────────────
        from download_layer.utils import detect_query_type

        classified: dict[str, list[str]] = {
            "search": [], "channel": [], "playlist": [], "direct": []
        }
        for q in raw_queries:
            qtype = detect_query_type(q)
            if qtype == "video":
                qtype = "direct"
            if qtype not in classified:
                agent._log("[WARN]", f"Unknown query type '{qtype}' for '{q}' — treating as direct")
                qtype = "direct"
            classified[qtype].append(q)

        caps = {
            "search":   agent.max_items.get("search",   10),
            "channel":  agent.max_items.get("channel",  10),
            "playlist": agent.max_items.get("playlist", 20),
            "direct":   0,  # no cap for direct URLs
        }

        with ds.lock:
            ds.types = {
                k: (len(v), caps[k])
                for k, v in classified.items()
                if v
            }

        for qtype, items in classified.items():
            if items:
                cap = caps[qtype]
                cap_s = "no cap" if qtype == "direct" else f"cap:{cap}"
                agent._log(
                    "[TYPE]",
                    f"{len(items)}× {qtype}  {cap_s}"
                )

        # ── 3. RESOLVE ────────────────────────────────────────────────────
        await self._resolve_all(agent, classified, caps)

        # ── 4. DONE ───────────────────────────────────────────────────────
        with ds.lock:
            total   = ds.total
            skipped = ds.skipped
            errors  = ds.errors
            ds.elapsed = time.time() - self._start  # single authoritative elapsed
            ds.state_flag = "done"

        agent._log(
            "[DONE]",
            f"Discovery complete — {total} items dispatched  "
            f"skipped:{skipped}  errors:{errors}"
        )

        # notify DownloadAgent that discovery is complete (Phase B)
        await self._send_discovery_done(agent, total=total, skipped=skipped, errors=errors)

        # Signal the agent that resolution is finished
        agent.resolution_done.set()

    # ── RESOLUTION HELPERS ────────────────────────────────────────────────

    async def _resolve_all(self, agent, classified, caps):
        ds = agent.discovery_state

        # search queries
        for query in classified["search"]:
            await self._resolve_search(agent, query, caps["search"])

        # channels
        for url in classified["channel"]:
            await self._resolve_channel(agent, url, caps["channel"])

        # playlists
        for url in classified["playlist"]:
            await self._resolve_playlist(agent, url, caps["playlist"])

        # direct video URLs — no resolution needed
        for url in classified["direct"]:
            agent._log("[URL]", f"{url} → verified direct link")
            job = agent._enqueue_job(url, "direct", url)
            await self._send_job(agent, job)
            with ds.lock:
                ds.resolved["direct"] = ds.resolved.get("direct", 0) + 1
                ds.total += 1
            # NOTE: do NOT update ds.elapsed here — that is done once in run()

    async def _resolve_search(self, agent, query: str, cap: int):
        ds = agent.discovery_state
        agent._log("[QUERY]", f'"{query}" → resolving, cap={cap}')

        try:
            from input_layer.search import search_youtube
            result = await asyncio.wait_for(
                asyncio.to_thread(search_youtube, query, max_results=cap),
                timeout=30.0
            )
            videos = result.get("videos", [])

            agent._log("[QUERY]", f'"{query}" → {len(videos)} URLs resolved')

            for v in videos:
                url = v.get("url")
                if url:
                    job = agent._enqueue_job(
                        url, "search", query,
                        title=v.get("title", ""),
                    )
                    await self._send_job(agent, job)
                    with ds.lock:
                        ds.resolved["search"] = ds.resolved.get("search", 0) + 1
                        ds.total += 1

        except asyncio.TimeoutError:
            agent._log("[ERR]", f'"{query}" → search timed out after 30s')
            with ds.lock:
                ds.errors += 1
        except Exception as exc:
            agent._log("[ERR]", f'"{query}" → resolution failed: {exc}')
            with ds.lock:
                ds.errors += 1

    async def _resolve_channel(self, agent, url: str, cap: int):
        ds = agent.discovery_state
        agent._log("[CHAN]", f"{url} → fetching latest, cap={cap}")

        try:
            from download_layer.channel import (
                get_channel_name, fetch_channel_video_urls
            )
            name = await asyncio.wait_for(
                asyncio.to_thread(get_channel_name, url),
                timeout=30.0
            ) or url
            urls = await asyncio.wait_for(
                asyncio.to_thread(fetch_channel_video_urls, url),
                timeout=30.0
            )
            urls = urls[:cap]

            agent._log("[CHAN]", f"{name} → {len(urls)} URLs resolved")

            for video_url in urls:
                job = agent._enqueue_job(video_url, "channel", url, title=name)
                await self._send_job(agent, job)
                with ds.lock:
                    ds.resolved["channel"] = ds.resolved.get("channel", 0) + 1
                    ds.total += 1

        except asyncio.TimeoutError:
            agent._log("[ERR]", f"{url} → channel fetch timed out after 30s")
            with ds.lock:
                ds.errors += 1
        except Exception as exc:
            agent._log("[ERR]", f"{url} → channel resolution failed: {exc}")
            with ds.lock:
                ds.errors += 1

    async def _resolve_playlist(self, agent, url: str, cap: int):
        ds = agent.discovery_state
        agent._log("[LIST]", f"{url} ? resolving playlist, cap={cap}")

        try:
            from download_layer.playlist import (
                get_playlist_name, fetch_playlist_urls
            )
            name = await asyncio.wait_for(
                asyncio.to_thread(get_playlist_name, url),
                timeout=60.0
            ) or url
            urls = await asyncio.wait_for(
                asyncio.to_thread(fetch_playlist_urls, url),
                timeout=60.0
            )
            urls = urls[:cap]

            agent._log("[LIST]", f"{name} ? {len(urls)} URLs resolved")

            for video_url in urls:
                job = agent._enqueue_job(video_url, "playlist", url, title=name)
                await self._send_job(agent, job)
                with ds.lock:
                    ds.resolved["playlist"] = ds.resolved.get("playlist", 0) + 1
                    ds.total += 1

        except asyncio.TimeoutError:
            agent._log("[WARN]", f"{url} ? playlist fetch timed out after 30s ? skipping")
            with ds.lock:
                ds.skipped += 1
        except Exception as exc:
            # private / unavailable playlist
            agent._log("[WARN]", f"{url} ? {exc} ? skipping")
            with ds.lock:
                ds.skipped += 1

    async def _send_job(self, agent, job):
        if not agent.download_jid:
            return
        msg = Message(to=agent.download_jid)
        msg.set_metadata("ontology", ONTOLOGY)
        msg.set_metadata("performative", INFORM)
        msg.body = encode(JobEnqueueMsg(
            url=job.url,
            source=job.source,
            query_key=job.query_key,
            title=job.title,
            output_dir=job.output_dir,
        ))
        await self.send(msg)

    async def _send_discovery_done(self, agent, total: int, skipped: int, errors: int):
        if not agent.download_jid:
            return
        msg = Message(to=agent.download_jid)
        msg.set_metadata("ontology", ONTOLOGY)
        msg.set_metadata("performative", INFORM)
        msg.body = encode(DiscoveryDoneMsg(
            total=total,
            skipped=skipped,
            errors=errors,
        ))
        await self.send(msg)
