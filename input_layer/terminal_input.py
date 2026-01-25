import argparse
from pathlib import Path
import sys
import threading
import time
import asyncio

from download_layer.runner import run_all_downloads
from input_layer.loader import load_config
from terminal_layer.ui import Dashboard


# =========================
# CLI DISPATCH
# =========================
def cli_dispatch():
    args = parse_cli()
    cli_cfg = normalize_args(args)

    config = load_config("config.json", cli_config=cli_cfg)

    input_file = Path(config.get("input") or "assets/input/queries.txt")
    if not input_file.exists():
        sys.exit(f"❌ Input file not found: {input_file}")

    dashboard = Dashboard()
    dashboard_thread = threading.Thread(target=dashboard.start, daemon=True)
    dashboard_thread.start()
    time.sleep(0.5)

    try:
        asyncio.run(
            run_all_downloads(
                str(input_file),
                event_callback=dashboard.send_event,
                config=config,
            )
        )
    finally:
        dashboard.stop()
        if dashboard_thread.is_alive():
            dashboard_thread.join(timeout=2.0)


# =========================
# CLI PARSING
# =========================
def parse_cli():
    parser = argparse.ArgumentParser(prog="gaudio")

    parser.add_argument("-i", "--input", help="Input file")
    parser.add_argument("-o", "--output", help="Output directory")

    sub = parser.add_subparsers(dest="command")

    cfg = sub.add_parser("config")
    cfg.add_argument("-r", "--retries", type=int)
    cfg.add_argument("-y", "--yt-output")
    cfg.add_argument("-C", "--concurrency", type=int, help="Max concurrent queries")

    cfg.add_argument("-a", "--all", type=int)
    cfg.add_argument("-s", "--search", type=int)
    cfg.add_argument("-p", "--playlist", type=int)
    cfg.add_argument("-c", "--channel", type=int)


    args = parser.parse_args()
    validate_args(args)
    return args


# =========================
# ARG NORMALIZATION
# =========================
def normalize_args(args):
    cfg = {}

    if getattr(args, "input", None):
        cfg["input"] = args.input

    if getattr(args, "output", None):
        cfg["output"] = args.output

    if getattr(args, "retries", None) is not None:
        cfg["retries"] = args.retries

    if getattr(args, "yt_output", None):
        cfg["yt_output"] = args.yt_output

    if getattr(args, "concurrency", None) is not None:
        cfg["concurrency"] = {
            "queries": args.concurrency
        }

    max_items = {}
    for name in ("all", "search", "playlist", "channel"):
        if hasattr(args, name):
            val = getattr(args, name)
            if val is not None:
                max_items[name] = val

    if max_items:
        cfg["max_items"] = max_items

    return cfg


# =========================
# VALIDATION
# =========================
def validate_args(args):
    if args.input and not Path(args.input).exists():
        sys.exit(f"❌ Input file not found: {args.input}")

    if args.output:
        out = Path(args.output)
        if out.exists() and not out.is_dir():
            sys.exit("❌ Output path exists and is not a directory")

    if args.command == "config":
        if args.retries is not None and args.retries < 0:
            sys.exit("❌ retries must be >= 0")

        for name in ("all", "search", "playlist", "channel"):
            val = getattr(args, name)
            if val is not None and val <= 0:
                sys.exit(f"❌ --{name} must be > 0")

        if not any([
            args.retries,
            args.yt_output,
            args.concurrency,
            args.all,
            args.search,
            args.playlist,
            args.channel,
        ]):
            sys.exit("❌ config called with no options")
