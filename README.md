# Ga-Audio-Harvester

Ga-Audio-Harvester is a multi-agent YouTube audio curator for Ga-language content. It blends an agentic pipeline (Discovery, Download, Resilience) with a practical CLI, making it useful for language-resource curation, experimental agent architectures, and reproducible academic work.

**Abstract**
This project treats audio collection as a coordinated multi-agent system: one agent resolves inputs into actionable URLs, another executes downloads with bounded concurrency, and a watchdog agent monitors health and failure signals. The system accepts search queries, channels, playlists, and direct video URLs, then curates the resulting audio into a consistent output layout for downstream analysis or listening.

**Project Goals**

- Build a reproducible, agent-driven pipeline for Ga-language audio curation.
- Demonstrate a modular multi-agent architecture with explicit roles and shared ontology.
- Keep the workflow accessible to researchers and practitioners through a CLI entry point.

**System Overview**

- `agents/`: SPADE agents implementing Discovery (resolve inputs), Download (execute jobs), and Resilience (monitor health) with a shared ontology.
- `input_layer`: parses query files, merges CLI and file-based config, and feeds the agent pipeline.
- `download_layer`: runs `yt-dlp` downloads, applies retries, and structures outputs by source type.
- `assets/input/queries.txt`: example query file with search terms and YouTube URLs.

**How It Works (Agentic Flow)**

1. The Discovery agent ingests a query file and classifies each line as search, channel, playlist, or single video.
2. It resolves each query into concrete download jobs (URLs + target folders).
3. The Download agent consumes the job queue with bounded worker slots and executes `yt-dlp` audio extraction.
4. The Resilience agent observes health signals and records retries/failures.
5. Curated audio is written into type-specific folders under the output directory.

**Quick Start**
Requirements:

- Python 3.12 or newer.
- `yt-dlp` installed (Python package and CLI).
- `ffmpeg` available on PATH for audio extraction.

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the CLI with the sample inputs:

```bash
python main.py -i assets/input/queries.txt -o output config -C 2 -s 5 -p 10 -c 10
```

**CLI Usage**
Entry point: `main.py` with the `config` subcommand (single-process runner).

Global options:

- `-i`, `--input`: Path to query file.
- `-o`, `--output`: Output directory root.

Config options:

- `-r`, `--retries`: Retry count per download.
- `-y`, `--yt-output`: `yt-dlp` output template.
- `-C`, `--concurrency`: Max concurrent queries.
- `-a`, `--all`: Max items for catch-all queries.
- `-s`, `--search`: Max items per search query.
- `-p`, `--playlist`: Max items per playlist.
- `-c`, `--channel`: Max items per channel.

Example:

```bash
python main.py -i assets/input/queries.txt -o output config -r 2 -C 2 -s 10 -p 20 -c 20
```

**Multi-Agent Pipeline (SPADE)**
The agent pipeline is the primary research path. It runs Discovery, Download, and Resilience as autonomous agents and is designed to evolve toward full XMPP message passing.

Run the pipeline:

```bash
python -m agents.pipeline --input assets/input/queries.txt --output output --workers 4 --search 10 --channel 10 --playlist 20
```

Environment variables (examples in `.env`):

- `DISCOVERY_JID`, `DISCOVERY_PASSWORD`
- `DOWNLOAD_JID`, `DOWNLOAD_PASSWORD`
- `RESILIENCE_JID`, `RESILIENCE_PASSWORD`

**Input Format**
Each line in the input file is either:

- A plain text search query.
- A YouTube video URL.
- A YouTube channel URL (including `@handle`).
- A YouTube playlist URL.

**Output Layout**
Downloads are organized under the output directory:

```
output/
|-- channels/
|-- playlists/
|-- search/
|-- single_videos/
```

**Logging**
The CLI initializes rotating logs in `logs/ga_audio_harvester.log`. Console output is kept compact for interactive runs.

**Project Structure**

- `main.py`: CLI entry point.
- `input_layer/`: config parsing and query loading.
- `download_layer/`: download orchestration and `yt-dlp` integration.
- `agents/`: SPADE agents and shared message ontology.
- `assets/input/`: sample query files for quick testing.

**Acknowledgments**
Built on `yt-dlp` and SPADE, with `ffmpeg` for audio extraction.
