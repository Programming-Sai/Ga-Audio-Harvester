# download_layer/channel.py
import json
import subprocess
import logging
from pathlib import Path
from typing import Optional, Callable
logger = logging.getLogger(__name__)


from download_layer.config import BASE_OUTPUT_DIR, AUDIO_FORMAT
from download_layer.utils import ensure_dir, sanitize_name


def _emit(cb: Optional[Callable], ev: dict):
    if cb:
        try:
            cb(ev)
        except Exception:
            logger.exception("event callback error")

def get_channel_name(channel_url: str) -> Optional[str]:
    try:
        result = subprocess.run(
            ["yt-dlp", "-J", "--flat-playlist", channel_url],
            capture_output=True,
            text=True,
            check=True,
        )
        data = json.loads(result.stdout)
        # data may contain title/uploader
        title = data.get("title") or data.get("uploader") or data.get("id")
        return title
    except Exception as e:
        logger.warning("Error fetching channel info: %s", e)
        return None

def fetch_channel_video_urls(channel_url: str) -> list[str]:
    """
    Returns list of webpage_url strings for the channel (flat playlist).
    """
    result = subprocess.run(
        [
            "yt-dlp",
            "--flat-playlist",
            "--print",
            "webpage_url",
            channel_url,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    urls = [u.strip() for u in result.stdout.splitlines() if u.strip()]
    return urls

def download_from_channel(
    channel_url: str,
    output_dir: Optional[Path | str] = None,
    audio_only: bool = True,
    audio_format: str = AUDIO_FORMAT,
    max_videos: Optional[int] = None,
    event_callback: Optional[Callable] = None,
    retries=None
) -> bool:
    base_outdir = Path(output_dir) if output_dir else BASE_OUTPUT_DIR
    ensure_dir(base_outdir)

    # get channel title
    channel_title = get_channel_name(channel_url) or "unknown_channel"
    folder_name = sanitize_name(channel_title)
    outdir = ensure_dir(base_outdir / folder_name)

    try:
        urls = fetch_channel_video_urls(channel_url)
    except subprocess.CalledProcessError as e:
        logger.error("Failed to fetch channel videos: %s", e)
        _emit(event_callback, {"type": "meta", "kind": "channel", "url": channel_url, "title": channel_title, "count": 0})
        return False

    if max_videos:
        urls = urls[:max_videos]

    _emit(event_callback, {"type": "meta", "kind": "channel", "url": channel_url, "title": channel_title, "count": len(urls)})

    # Pass URLs to core.download_video via whatever runner uses (this function retains previous behavior: download sequentially)
    from .core import download_video  # local import to avoid cycles

    succeeded = 0
    failed = 0
    for url in urls:
        ok = download_video(url, output_dir=outdir, audio_only=audio_only, audio_format=audio_format, event_callback=event_callback, retries=retries)
        if ok:
            succeeded += 1
        else:
            failed += 1

    _emit(event_callback, {"type": "meta", "kind": "channel_finished", "url": channel_url, "title": channel_title, "succeeded": succeeded, "failed": failed})
    logger.info("Channel download finished: %d succeeded, %d failed", succeeded, failed)
    return True
