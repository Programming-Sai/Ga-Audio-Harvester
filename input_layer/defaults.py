from pathlib import Path

DEFAULT_CONFIG = {
    # IO
    "input": None,
    "output": Path("./output"),

    # yt-dlp
    "audio_format": "mp3",
    "yt_output": "%(title)s - %(id)s.%(ext)s",

    # retries
    "retries": 2,

    # concurrency
    "concurrency": {
        "queries": 1,
    },

    # limits
    "max_items": {
        "search": 1,
        "playlist": 1,
        "channel": 1,
    },
}
