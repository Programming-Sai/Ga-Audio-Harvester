from pathlib import Path
from input_layer.defaults import DEFAULT_CONFIG

# Base output directory (resolved once)
BASE_OUTPUT_DIR = Path(DEFAULT_CONFIG["output"])

# yt-dlp related
AUDIO_FORMAT = DEFAULT_CONFIG["audio_format"]
DOWNLOAD_RETRIES = DEFAULT_CONFIG["retries"]
YT_OUTPUT_TEMPLATE = DEFAULT_CONFIG["yt_output"]

# limits
MAX_ITEMS = DEFAULT_CONFIG["max_items"]

# concurrency
MAX_CONCURRENT_QUERIES = DEFAULT_CONFIG["concurrency"]["queries"]
