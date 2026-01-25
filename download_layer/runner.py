# download_layer/runner.py

import asyncio
import logging
from pathlib import Path
from typing import Optional, Callable

from download_layer.config import BASE_OUTPUT_DIR, MAX_ITEMS
from download_layer.core import download_video
from download_layer.playlist import download_playlist
from download_layer.channel import download_from_channel
from download_layer.utils import sanitize_query_to_filename, detect_query_type
from input_layer.loader import load_queries
from input_layer.search import search_youtube

logger = logging.getLogger(__name__)

# concurrency ONLY across queries
# MAX_CONCURRENT_QUERIES = 1



def _wrap_callback(cb: Optional[Callable], job_id: str) -> Optional[Callable]:
    """Attach job_id as query_key to every emitted event."""
    if cb is None:
        return None

    def wrapped(ev: dict):
        try:
            ev2 = dict(ev)
            ev2.setdefault("query_key", job_id)
            cb(ev2)
        except Exception:
            pass  # never break runner because of UI

    return wrapped


# -------------------------
# SEARCH (SEQUENTIAL videos)
# -------------------------
async def download_search_results(query: str, semaphore: asyncio.Semaphore, event_callback=None, max_items=MAX_ITEMS["search"], base_output_dir="", retries=None):
    job_id = f"search:{sanitize_query_to_filename(query)}"
    wrapped_cb = _wrap_callback(event_callback, job_id)

    # Perform search FIRST (your previous version used js before assignment)
    js = search_youtube(
        query,
        max_results=max_items,
        event_callback=wrapped_cb,
    )

    videos = js.get("videos", [])

    # Empty job
    if not videos:
        wrapped_cb({
            "type": "job",
            "status": "empty",
        })
        return

    # Announce job metadata
    wrapped_cb({
        "type": "meta",
        "kind": "search",
        "title": query,
        "count": len(videos),
    })

    out_dir = (base_output_dir or BASE_OUTPUT_DIR) / "search" / sanitize_query_to_filename(query)

    success = 0
    fail = 0

    async with semaphore:
        for v in videos:
            url = v.get("url")
            if not url:
                fail += 1
                continue

            try:
                ok = await asyncio.to_thread(
                    download_video,
                    url,
                    output_dir=out_dir,
                    audio_only=True,
                    event_callback=wrapped_cb,
                    retries=retries
                )

                if ok:
                    success += 1
                else:
                    fail += 1
                    wrapped_cb({
                        "type": "download",
                        "status": "final_failed",
                        "url": url,
                    })

            except Exception:
                fail += 1
                wrapped_cb({
                    "type": "download",
                    "status": "final_failed",
                    "url": url,
                })

    # TERMINAL JOB EVENT (always exactly once)
    wrapped_cb({
        "type": "job",
        "status": "completed" if success > 0 else "failed",
    })


# -------------------------
# PLAYLIST
# -------------------------
async def download_playlist_async(url: str, semaphore: asyncio.Semaphore, event_callback=None, max_items=MAX_ITEMS["playlist"], base_output_dir="", retries=None):
    job_id = f"playlist:{sanitize_query_to_filename(url)}"
    wrapped_cb = _wrap_callback(event_callback, job_id)
    out_dir = (base_output_dir or BASE_OUTPUT_DIR) / "playlists"

    try:
        async with semaphore:
            await asyncio.to_thread(
                download_playlist,
                url,
                output_dir=out_dir,
                max_downloads=max_items,
                event_callback=wrapped_cb,
                retries=retries
            )

        wrapped_cb({
            "type": "job",
            "status": "completed",
        })

    except Exception:
        wrapped_cb({
            "type": "job",
            "status": "failed",
        })


# -------------------------
# CHANNEL
# -------------------------
async def download_channel_async(url: str, semaphore: asyncio.Semaphore, event_callback=None, max_items=MAX_ITEMS["channel"], base_output_dir="", retries=None):
    job_id = f"channel:{sanitize_query_to_filename(url)}"
    wrapped_cb = _wrap_callback(event_callback, job_id)
    out_dir = (base_output_dir or BASE_OUTPUT_DIR) / "channels"

    try:
        async with semaphore:
            await asyncio.to_thread(
                download_from_channel,
                url,
                output_dir=out_dir,
                max_videos=max_items,
                event_callback=wrapped_cb,
                retries=retries
            )

        wrapped_cb({
            "type": "job",
            "status": "completed",
        })

    except Exception:
        wrapped_cb({
            "type": "job",
            "status": "failed",
        })


# -------------------------
# SINGLE VIDEO
# -------------------------
async def download_video_async(url: str, semaphore: asyncio.Semaphore, event_callback=None, base_output_dir="", retries=None):
    job_id = f"single:{sanitize_query_to_filename(url)}"
    wrapped_cb = _wrap_callback(event_callback, job_id)
    out_dir =(base_output_dir or BASE_OUTPUT_DIR) / "single_videos"

    try:
        async with semaphore:
            ok = await asyncio.to_thread(
                download_video,
                url,
                output_dir=out_dir,
                audio_only=True,
                event_callback=wrapped_cb,
                retries=retries
            )

        if ok:
            wrapped_cb({
                "type": "job",
                "status": "completed",
            })
        else:
            wrapped_cb({
                "type": "download",
                "status": "final_failed",
                "url": url,
            })
            wrapped_cb({
                "type": "job",
                "status": "failed",
            })

    except Exception:
        wrapped_cb({
            "type": "job",
            "status": "failed",
        })


# -------------------------
# MAIN ENTRY
# -------------------------
async def run_all_downloads(query_file: str, event_callback: Optional[Callable] = None, config=None):
    config = config or {}
    queries = load_queries(query_file)
    concurrency_cfg = config.get("concurrency", {})
    retires = config.get("retires", {})
    max_concurrent_queries = concurrency_cfg.get("queries", 1)

    semaphore = asyncio.Semaphore(max_concurrent_queries)
    base_output_dir = Path(config.get("output") or BASE_OUTPUT_DIR)
    max_items_cfg = config.get("max_items", {}) 

    tasks = []
    for q in queries:
        q_type = detect_query_type(q)

        if q_type == "search":
            tasks.append(
                download_search_results(
                    q,
                    semaphore,
                    event_callback,
                    max_items=max_items_cfg.get("search", MAX_ITEMS["search"]),
                    base_output_dir=base_output_dir,
                    retries=retires
                )
            )
        elif q_type == "playlist":
            tasks.append(
                download_playlist_async(
                    q,
                    semaphore,
                    event_callback,
                    max_items=max_items_cfg.get("playlist", MAX_ITEMS["playlist"]),
                    base_output_dir=base_output_dir,
                    retries=retires
                )
            )
        elif q_type == "channel":
            tasks.append(
                download_channel_async(
                    q,
                    semaphore,
                    event_callback,
                    max_items=max_items_cfg.get("channel", MAX_ITEMS["channel"]),
                    base_output_dir=base_output_dir,
                    retries=retires
                )
            )
        else:
            tasks.append(
                download_video_async(
                    q,
                    semaphore,
                    event_callback,
                    base_output_dir=base_output_dir,
                    retries=retires
                )
            )

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for r, q in zip(results, queries):
        if isinstance(r, Exception):
            logger.error("Query task crashed: %s → %s", q, r)
