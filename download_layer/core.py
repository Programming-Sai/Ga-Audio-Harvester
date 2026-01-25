# download_layer/core.py
import subprocess
import time
import logging
import json
import shutil
from pathlib import Path
from typing import Optional, Callable

logger = logging.getLogger(__name__)

try:
    from yt_dlp import YoutubeDL  # type: ignore
    HAVE_YTDLP = True
except Exception:
    HAVE_YTDLP = False

from .config import BASE_OUTPUT_DIR, AUDIO_FORMAT, DOWNLOAD_RETRIES, YT_OUTPUT_TEMPLATE
from .utils import ensure_dir

# Event callback type: event_callback(dict)
# Events:
# - {"type": "meta", "kind": "download", "url": url}
# - {"type": "download", "status": "started|downloading|finished|error", "url": url, ...progress fields...}
# - {"type": "log", "line": "...", "url": url}

def _emit(cb: Optional[Callable], ev: dict):
    if cb:
        try:
            cb(ev)
        except Exception:
            logger.exception("event callback error")


def _build_base_cmd(url: str, out_template: str, audio_only: bool, audio_format: str) -> list[str]:
    cmd = [
        "yt-dlp",
        url,
        "-o",
        out_template,
        "--restrict-filenames",
        "--no-mtime",
        "--newline",
        "--no-overwrites",  # do not overwrite existing files
    ]
    if audio_only:
        cmd += ["--extract-audio", "--audio-format", audio_format]
    return cmd


def download_video(
    url: str,
    output_dir: Optional[Path | str] = None,
    audio_only: bool = True,
    audio_format: str = AUDIO_FORMAT,
    retries: int = DOWNLOAD_RETRIES,
    yt_template: str = YT_OUTPUT_TEMPLATE,
    event_callback: Optional[Callable] = None,
) -> bool:
    """
    Download a single video. Emits events via event_callback (if given).
    Returns True on success, False on failure.
    """
    outdir = ensure_dir(Path(output_dir) if output_dir else BASE_OUTPUT_DIR)
    out_template = str(outdir / yt_template)
    attempt = 0
    retries = retries or DOWNLOAD_RETRIES

    # Emit queued event
    _emit(event_callback, {"type": "download", "status": "queued", "url": url})

    while attempt <= retries:
        attempt += 1
        if HAVE_YTDLP:
            # Flag to track actual success
            success_flag = False

            def _progress_hook(d):
                nonlocal success_flag

                status = d.get("status")
                if status == "finished":
                    success_flag = True

                ev = {
                    "type": "download",
                    "url": url,
                    "status": status,
                }

                # raw values
                downloaded = d.get("downloaded_bytes")
                total = d.get("total_bytes") or d.get("total_bytes_estimate")

                if downloaded is not None and total:
                    ev["downloaded_bytes"] = downloaded
                    ev["total_bytes"] = total
                    ev["progress"] = round((downloaded / total) * 100, 2)

                # optional extras
                if "_percent_str" in d:
                    ev["percent_str"] = d["_percent_str"]
                if "speed" in d:
                    ev["speed"] = d["speed"]
                if "eta" in d:
                    ev["eta"] = d["eta"]
                if "filename" in d:
                    ev["filename"] = d["filename"]

                _emit(event_callback, ev)

            opts = {
                "outtmpl": out_template,
                "restrictfilenames": True,
                "no_mtime": True,
                "noplaylist": True,
                "quiet": True,
                "no_warnings": True,
                "ignoreerrors": True,
                "progress_hooks": [_progress_hook],
                "http_headers": {
                      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/143.0.0.0 Safari/537.36"
    },
            }
            if audio_only:
                opts["format"] = "bestaudio/best"
                opts["postprocessors"] = [{"key": "FFmpegExtractAudio", "preferredcodec": audio_format}]

            try:
                _emit(event_callback, {"type": "download", "status": "started", "url": url, "attempt": attempt})
                with YoutubeDL(opts) as ydl:
                    ydl.download([url])

                if success_flag:
                    _emit(event_callback, {"type": "download", "status": "finished", "url": url})
                    logger.info("Download succeeded: %s", url)
                    return True
                else:
                    raise RuntimeError("yt-dlp did not finish the download successfully")

            except Exception as e:
                logger.warning("Download failed (attempt %d) %s -- %s", attempt, url, e)
                _emit(event_callback, {"type": "download", "status": "error", "url": url, "error": str(e)})
                if attempt > retries:
                    logger.error("Giving up on %s after %d attempts", url, retries + 1)
                    return False
                time.sleep(1 + attempt)
                continue

        else:
            # subprocess fallback
            cmd = _build_base_cmd(url, out_template, audio_only, audio_format)
            _emit(event_callback, {"type": "download", "status": "started", "url": url, "attempt": attempt})
            try:
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8")
                assert proc.stdout is not None
                failed_flag = False
                for raw in proc.stdout:
                    line = raw.rstrip("\n")
                    _emit(event_callback, {"type": "log", "line": line, "url": url})
                    if "ERROR" in line or "HTTP Error" in line:
                        failed_flag = True
                ret = proc.wait()
                if ret == 0 and not failed_flag:
                    _emit(event_callback, {"type": "download", "status": "finished", "url": url})
                    logger.info("Download succeeded: %s", url)
                    return True
                else:
                    raise subprocess.CalledProcessError(ret, cmd)
            except subprocess.CalledProcessError as e:
                logger.warning("Download failed (attempt %d): %s -- %s", attempt, url, e)
                _emit(event_callback, {"type": "download", "status": "error", "url": url, "error": str(e)})
                if attempt > retries:
                    logger.error("Giving up on %s after %d attempts", url, retries + 1)
                    return False
                time.sleep(1 + attempt)
                continue
            except FileNotFoundError:
                logger.exception("yt-dlp executable not found (fallback). Make sure yt-dlp is installed.")
                _emit(event_callback, {"type": "download", "status": "error", "url": url, "error": "yt-dlp not installed"})
                return False

    return False



# add to bottom of download_layer/core.py

if __name__ == "__main__":
    # from logging_config import setup_logging
    # setup_logging()

    TEST_URL = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"

    ok = download_video(
        TEST_URL,
        output_dir="debug_downloads/core",
        audio_only=True,
        event_callback=lambda ev: print(ev)
    )

    print("RESULT:", ok)
