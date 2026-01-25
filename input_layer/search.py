# input_layer/search.py
import json
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Optional, Callable
import logging
logger = logging.getLogger(__name__)


def _emit(cb: Optional[Callable], ev: dict):
    if cb:
        try:
            cb(ev)
        except Exception:
            pass

def fix_duration(time):
    if time is None:
        return None
    hours, minutes, seconds = time // 3600, (time % 3600) // 60, time % 60
    return (f"{str(hours).zfill(2)+':' if hours else ''}{str(minutes).zfill(2)}:{str(seconds).zfill(2)}")

def search_youtube(
    query: str,
    max_results: int = 10,
    save: bool = False,
    output_dir: Path | str = "input_layer/search_results",
    event_callback: Optional[Callable] = None,
) -> dict:
    """
    Search YouTube and return structured payload.
    Emits a meta event containing number of videos found.
    """
    cmd = [
        "yt-dlp",
        f"ytsearch{max_results}:{query}",
        "--skip-download",
        "--dump-json",
        "--quiet"
    ]

    results = []
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8"
    )

    assert process.stdout is not None
    for line in process.stdout:
        try:
            data = json.loads(line)
            duration = fix_duration(data.get("duration"))
            results.append({
                "id": data.get("id"),
                "title": data.get("title"),
                "url": data.get("webpage_url"),
                "channel": data.get("channel"),
                "duration": duration
            })
        except json.JSONDecodeError:
            continue

    payload = {
        "query": query,
        "fetched_at": datetime.utcnow().isoformat(),
        "videos": results
    }

    # emit meta
    _emit(event_callback, {"type": "meta", "kind": "search", "query": query, "count": len(results)})

    if save:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        safe_name = query.lower().replace(" ", "_")
        output_file = output_dir / f"{safe_name}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)

    return payload
