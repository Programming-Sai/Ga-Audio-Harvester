# downloader/utils.py
from pathlib import Path
import re
from typing import Tuple
from urllib.parse import urlparse

def ensure_dir(p: Path) -> Path:
    p = Path(p)
    p.mkdir(parents=True, exist_ok=True)
    return p


def seconds_to_hhmmss(duration: int) -> str:
    """
    Convert seconds to padded HH:MM:SS (hours may be 00).
    """
    h = duration // 3600
    m = (duration % 3600) // 60
    s = duration % 60
    return f"{h:02}:{m:02}:{s:02}"


def sanitize_query_to_filename(query: str) -> str:
    """
    Produce a filesystem-safe-ish filename from a query.
    We keep it simple: lower, replace spaces with underscores,
    drop chars that are obviously unsafe.
    """
    s = query.strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_\-\.]", "", s)
    return s[:200]


def is_playlist_url(url: str) -> bool:
    """
    Heuristic check for playlist URLs or 'list=' parameter in url.
    """
    if not isinstance(url, str):
        return False
    u = url.lower()
    return "list=" in u or "playlist" in u or "youtube.com/playlist" in u


def detect_query_type(query: str) -> str:
    """
    Returns one of:
      - "search"    → plain text query
      - "video"     → single video URL
      - "playlist"  → playlist URL
      - "channel"   → channel URL
    """
    query = query.strip()
    if query.startswith("http"):
        if is_playlist_url(query):
            return "playlist"
        elif any(x in query for x in ["/channel/", "/c/", "youtube.com/@"]):
            return "channel"
        else:
            return "video"
    return "search"


def sanitize_name(name: str) -> str:
    import re
    sanitized = re.sub(r'[\\/:*?"<>|]', '_', name)
    sanitized = re.sub(r'[\s_]+', '_', sanitized)
    return sanitized.strip('_')