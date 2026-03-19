#!/usr/bin/env python3
"""
Ga Audio Harvester - Main entry point for agent pipeline.
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

# Import the pipeline runner
from agents.pipeline import run_pipeline


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )
    # Suppress verbose logs from libraries
    logging.getLogger("slixmpp").setLevel(logging.CRITICAL)
    logging.getLogger("asyncio").setLevel(logging.CRITICAL)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Ga Audio Harvester - Multi-agent YouTube audio downloader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s -i assets/input/queries.txt
  %(prog)s -i queries.txt -w 4 --search 5
        """,
    )
    parser.add_argument(
        "-i", "--input",
        default="assets/input/queries.txt",
        help="Path to query file (default: assets/input/queries.txt)"
    )
    parser.add_argument(
        "-o", "--output",
        default="./output",
        help="Output directory for downloads (default: ./output)"
    )
    parser.add_argument(
        "-w", "--workers",
        type=int, default=4,
        help="Number of worker slots (1-6, default: 4)"
    )
    parser.add_argument(
        "-r", "--retries",
        type=int, default=2,
        help="Download retries per video (default: 2)"
    )
    parser.add_argument(
        "--search",
        type=int, default=10,
        help="Max results per search query (default: 10)"
    )
    parser.add_argument(
        "--channel",
        type=int, default=10,
        help="Max videos per channel (default: 10)"
    )
    parser.add_argument(
        "--playlist",
        type=int, default=20,
        help="Max videos per playlist (default: 20)"
    )
    return parser.parse_args()


def main():
    setup_logging()
    args = parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"❌ Error: Input file not found: {input_path}")
        sys.exit(1)

    output_path = Path(args.output)
    output_path.mkdir(parents=True, exist_ok=True)

    max_items = {
        "search": args.search,
        "channel": args.channel,
        "playlist": args.playlist,
    }

    print(f"\n🎵 Ga Audio Harvester")
    print(f"{'='*50}")
    print(f"Input file : {input_path}")
    print(f"Output dir : {output_path}")
    print(f"Workers    : {args.workers}")
    print(f"Retries    : {args.retries}")
    print(f"Search max : {args.search}")
    print(f"Channel max: {args.channel}")
    print(f"Playlist max: {args.playlist}")
    print(f"{'='*50}\n")

    try:
        asyncio.run(run_pipeline(
            query_file=input_path,
            output_dir=output_path,
            worker_slots=args.workers,
            retries=args.retries,
            max_items=max_items,
        ))
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()