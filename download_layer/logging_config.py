# logging_config.py
import logging
from logging.handlers import RotatingFileHandler
import sys
from pathlib import Path

def setup_logging(
    log_dir: Path | str = "logs",
    log_file_name: str = "ga_audio_harvester.log",
    console_level: int = logging.INFO,
    file_level: int = logging.DEBUG,
):
    log_dir = Path(log_dir)
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / log_file_name

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # allow handlers to filter

    # remove any existing handlers (important)
    if root.handlers:
        root.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s  %(levelname)s  %(name)s  %(message)s"
    )

    # console handler (clean, UI-friendly)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(console_level)
    ch.setFormatter(formatter)

    # rotating file handler (debug everything)
    fh = RotatingFileHandler(
        log_file,
        maxBytes=5_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    fh.setLevel(file_level)
    fh.setFormatter(formatter)

    root.addHandler(ch)
    root.addHandler(fh)
