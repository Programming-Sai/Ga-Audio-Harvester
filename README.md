# Ga-Audio-Harvester

Automated pipeline to search, batch-download, and organize Ga-language audio content from YouTube for local curation and listening.

---

## Overview

Ga-Audio-Harvester is designed to facilitate the collection of Ga-language audio material from YouTube. The primary goal is to enable raw listening and fluency practice without focusing on grammar or structured exercises. This version (v1) provides a command-line interface (CLI) for managing downloads efficiently.

---

## Features

- Batch-download audio from channels, playlists, or search queries.
- CLI-based configuration of concurrency and download limits.
- Basic dashboard displaying ongoing download progress.
- Automatic handling of network interruptions; downloads resume after pauses.
- Configurable output structure and file naming via `config.json` or CLI arguments.

---

## Limitations

- Text-based user interface (TUI) is minimal and limited in features.
- Concurrency is currently applied at the query level only.
- Downloads may be slower for large playlists or channels due to query-level concurrency.
- Some downloads may fail if network issues occur mid-stream, though successful downloads are preserved.

---

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/Programming-Sai/Ga-Audio-Harvester.git
   ```

2. Ensure Python 3.12+ is installed.
3. Install required packages:

   ```bash
   pip install -r requirements.txt
   ```

4. Ensure `ffmpeg` is installed and available in your system PATH.

---

## Usage

Prepare an input file containing URLs or search queries (one per line).

Run the main CLI:

```bash
python main.py -i <input_file> -o <output_dir> config [options]
```

## CLI Options

#### Global options

- `-i INPUT, --input INPUT` : Input file with URLs or search queries.
- `-o OUTPUT, --output OUTPUT` : Output directory.
- `-h, --help` : Show help message and exit.

#### Config subcommand

Use `config` to specify download parameters:

```bash
python main.py config -h
```

Available options:

- `-r RETRIES, --retries RETRIES` : Number of retries per download.
- `-y YT_OUTPUT, --yt-output YT_OUTPUT` : Output filename template for downloads.
- `-C CONCURRENCY, --concurrency CONCURRENCY` : Maximum concurrent queries.
- `-a ALL, --all ALL` : Maximum items for "all" queries.
- `-s SEARCH, --search SEARCH` : Maximum items per search query.
- `-p PLAYLIST, --playlist PLAYLIST` : Maximum items per playlist.
- `-c CHANNEL, --channel CHANNEL` : Maximum items per channel.

---

## Output Structure

Downloads are organized under the output directory:

```
output/
├─ channels/
├─ playlists/
├─ search/
└─ single_videos/
```

Files are named using the configured `yt-dlp` template, which includes title and ID to prevent collisions.
