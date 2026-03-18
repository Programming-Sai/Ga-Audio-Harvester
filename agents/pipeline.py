"""
agents/pipeline.py
===================
Top-level pipeline runner for Phase A.

Starts all three agents, wires them together, and runs the full
pipeline from query file → resolved URLs → downloaded files.

Usage:
    python -m agents.pipeline
    python -m agents.pipeline --input assets/input/queries.txt
    python -m agents.pipeline --input queries.txt --workers 4 --search 5
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path


# also suppress the RuntimeWarning about executor threads
import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)
from dotenv import load_dotenv

load_dotenv()
logging.getLogger("slixmpp").setLevel(logging.CRITICAL)
logging.getLogger("asyncio").setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)


async def run_pipeline(
    query_file:   str | Path,
    output_dir:   str | Path = "./output",
    worker_slots: int        = 4,
    retries:      int        = 2,
    max_items:    dict       = None,
):
    """
    Orchestrate all three agents for one full pipeline run.

    Phase A: direct object references between agents.
    Phase B: replace with XMPP message passing.
    """
    from agents.discovery.agent  import DiscoveryAgent
    from agents.download.agent   import DownloadAgent
    from agents.resilience.agent import ResilienceAgent

    max_items = max_items or {"search": 10, "channel": 10, "playlist": 20}

    disc_jid  = os.environ["DISCOVERY_JID"]
    disc_pass = os.environ["DISCOVERY_PASSWORD"]
    dl_jid    = os.environ["DOWNLOAD_JID"]
    dl_pass   = os.environ["DOWNLOAD_PASSWORD"]
    res_jid   = os.environ["RESILIENCE_JID"]
    res_pass  = os.environ["RESILIENCE_PASSWORD"]

    # ── CREATE ────────────────────────────────────────────────────────
    disc = DiscoveryAgent(
        disc_jid, disc_pass,
        query_file=query_file,
        output_dir=output_dir,
        max_items=max_items,
    )
    dl = DownloadAgent(
        dl_jid, dl_pass,
        output_dir=output_dir,
        worker_slots=worker_slots,
        retries=retries,
    )
    res = ResilienceAgent(res_jid, res_pass)

    # ── START ─────────────────────────────────────────────────────────
    logger.info("Starting all agents...")
    await _start_agent_with_timeout("DiscoveryAgent", disc, 15.0)
    await _start_agent_with_timeout("DownloadAgent", dl, 15.0)
    await _start_agent_with_timeout("ResilienceAgent", res, 15.0)

    # wire resilience to watch the others
    res.watch("discovery", disc)
    res.watch("download",  dl)

    logger.info("All agents running. Pipeline in progress...")

    # ── DISCOVERY PHASE ───────────────────────────────────────────────
    logger.info("Waiting for Discovery to resolve inputs...")
    try:
        await asyncio.wait_for(disc.resolution_done.wait(), timeout=180)
    except asyncio.TimeoutError:
        logger.error("Discovery timed out after 180s")
        await _stop_all(disc, dl, res)
        return

    jobs = disc.get_jobs()
    logger.info("Discovery complete — %d jobs resolved", len(jobs))

    if not jobs:
        logger.warning("No jobs resolved — nothing to download")
        await _stop_all(disc, dl, res)
        return

    # ── DOWNLOAD PHASE ────────────────────────────────────────────────
    dl.add_jobs(jobs)
    logger.info(
        "Download started — %d jobs  workers:%d  retries:%d",
        len(jobs), worker_slots, retries
    )

    try:
        await asyncio.wait_for(dl.all_done_event.wait(), timeout=3600)
    except asyncio.TimeoutError:
        logger.error("Download timed out after 3600s")

    # ── WRAP UP ───────────────────────────────────────────────────────
    res.mark_done()
    await _stop_all(disc, dl, res)

    # ── PRINT SUMMARY ─────────────────────────────────────────────────
    ds = dl.download_state
    rs = res.resilience_state

    print(f"\n{'═'*62}")
    print(f"  PIPELINE COMPLETE")
    print(f"{'═'*62}")
    print(f"  Downloaded : {ds.done}  Errors: {ds.errors}  Total: {ds.total}")
    print(f"  Size       : {ds.total_mb} MB")
    print(f"  Retries    : {rs.retries}  Failures: {rs.failures}")
    print(f"  Output     : {Path(output_dir).resolve()}")
    print(f"{'═'*62}\n")


async def _stop_all(*agents):
    for a in agents:
        try:
            await asyncio.wait_for(a.stop(), timeout=3.0)
        except Exception:
            pass


async def _start_agent_with_timeout(label: str, agent, timeout_s: float):
    """
    Start a SPADE agent with a hard timeout so we can detect
    XMPP connection hangs quickly.
    """
    logger.info("Starting %s...", label)
    try:
        await asyncio.wait_for(agent.start(auto_register=False), timeout=timeout_s)
        logger.info("%s started", label)
    except asyncio.TimeoutError:
        logger.error("%s start timed out after %.0fs (XMPP connect hang?)", label, timeout_s)
        raise
    except Exception as exc:
        logger.error("%s failed to start: %s", label, exc)
        raise


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(prog="agents.pipeline")
    p.add_argument("-i", "--input",
                   default="assets/input/queries.txt",
                   help="Path to query file")
    p.add_argument("-o", "--output",
                   default="./output",
                   help="Output directory")
    p.add_argument("-w", "--workers",
                   type=int, default=4,
                   help=f"Worker slots (1–6, default 4)")
    p.add_argument("-r", "--retries",
                   type=int, default=2,
                   help="Download retries per video")
    p.add_argument("--search",
                   type=int, default=10,
                   help="Max results per search query")
    p.add_argument("--channel",
                   type=int, default=10,
                   help="Max videos per channel")
    p.add_argument("--playlist",
                   type=int, default=20,
                   help="Max videos per playlist")
    return p.parse_args()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    args = parse_args()

    qfile = Path(args.input)
    if not qfile.exists():
        sys.exit(f"❌  Input file not found: {qfile}")

    max_items = {
        "search":   args.search,
        "channel":  args.channel,
        "playlist": args.playlist,
    }

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run_pipeline(
            query_file   = qfile,
            output_dir   = args.output,
            worker_slots = args.workers,
            retries      = args.retries,
            max_items    = max_items,
        ))
    finally:
        # cancel all remaining tasks so slixmpp reconnect loops die
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(
                asyncio.gather(*pending, return_exceptions=True)
            )
        loop.close()
        # hard exit — don't let slixmpp's threads delay shutdown
        os._exit(0)


if __name__ == "__main__":
    main()
