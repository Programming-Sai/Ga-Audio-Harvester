# download_layer/playlist.py
import json
import subprocess
import logging
from pathlib import Path
from typing import Optional, Callable

from download_layer.config import BASE_OUTPUT_DIR, AUDIO_FORMAT
from download_layer.utils import ensure_dir, sanitize_name

logger = logging.getLogger(__name__)

def _emit(cb: Optional[Callable], ev: dict):
    if cb:
        try:
            cb(ev)
        except Exception:
            logger.exception("event callback error")

def get_playlist_name(playlist_url: str) -> Optional[str]:
    try:
        result = subprocess.run(
            ["yt-dlp", "-J", playlist_url],
            capture_output=True,
            text=True,
            check=True,
        )
        data = json.loads(result.stdout)
        title = data.get("title") or data.get("id")
        return title
    except Exception as e:
        logger.warning("Error fetching playlist info: %s", e)
        return None

def fetch_playlist_urls(playlist_url: str) -> list[str]:
    result = subprocess.run(
        [
            "yt-dlp", 
            "--flat-playlist", 
            "--print", 
            "webpage_url", 
            playlist_url
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    urls = [u.strip() for u in result.stdout.splitlines() if u.strip()]
    return urls

def download_playlist(
    playlist_url: str,
    output_dir: Optional[Path | str] = None,
    audio_only: bool = True,
    audio_format: str = AUDIO_FORMAT,
    max_downloads: Optional[int] = None,
    event_callback: Optional[Callable] = None,
    retries: int = None
) -> bool:
    base_outdir = Path(output_dir) if output_dir else BASE_OUTPUT_DIR
    ensure_dir(base_outdir)

    playlist_title = get_playlist_name(playlist_url) or "unknown_playlist"
    folder_name = sanitize_name(playlist_title)
    outdir = ensure_dir(base_outdir / folder_name)

    try:
        urls = fetch_playlist_urls(playlist_url)
    except subprocess.CalledProcessError as e:
        logger.error("Failed to fetch playlist JSON: %s", e)
        _emit(event_callback, {"type": "meta", "kind": "playlist", "url": playlist_url, "title": playlist_title, "count": 0})
        return False

    if max_downloads is not None:
        urls = urls[:max_downloads]

    _emit(event_callback, {"type": "meta", "kind": "playlist", "url": playlist_url, "title": playlist_title, "count": len(urls)})

    from .core import download_video

    succeeded = 0
    failed = 0
    for url in urls:
        ok = download_video(url, output_dir=outdir, audio_only=audio_only, audio_format=audio_format, event_callback=event_callback, retries=None)
        if ok:
            succeeded += 1
        else:
            failed += 1

    _emit(event_callback, {"type": "meta", "kind": "playlist_finished", "url": playlist_url, "title": playlist_title, "succeeded": succeeded, "failed": failed})
    logger.info("Playlist download finished: %d succeeded, %d failed", succeeded, failed)
    return True



