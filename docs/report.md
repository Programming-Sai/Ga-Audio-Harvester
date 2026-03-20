# Ga-Audio-Harvester - DCIT 403 Report (Prometheus, Phase 1-5)

This project implements an intelligent multi-agent system for curating Ga-language audio from YouTube. It treats audio harvesting as a pipeline of responsibilities: Discovery resolves user intents into download jobs, Download executes those jobs with bounded worker capacity, and Resilience monitors progress and failures. The emphasis is design quality and reasoning rather than complexity.

## Phase 1 - System Specification (What the system should do)

**Problem description.** Ga-language audio is abundant on YouTube but difficult to collect consistently for listening practice and curation. Manual collection is slow and does not scale across channels, playlists, and topical searches. An agent solution is appropriate because the task decomposes into roles (resolve, execute, monitor) that react to events (new inputs, progress, stalls) and make decisions (classification, caps, retry vs fail).

**Users and stakeholders.** Primary users are students/researchers curating language resources and individual learners building listening corpora. Evaluators need clear Prometheus artifacts and traceable design decisions.

**Goal specification.** Top-level goal: curate Ga-language audio into an organized local corpus. Sub-goals: resolve mixed input types, enforce caps, download audio reliably, track completion, and expose progress/health signals.

**System capabilities.** The system accepts a query file containing search terms and YouTube URLs (video, channel, playlist). It classifies each input, resolves it into URLs when needed, downloads audio-only artifacts, and writes outputs into a structured directory layout. It supports bounded concurrency (worker slots) and resilience via retries (plus stall detection in direct mode).

**Environment.** The environment includes YouTube as the external source, the local filesystem for persistence, and an optional XMPP server for message-based coordination.

## Phase 2 - Architectural Design (Structure of the agent system)

**Agent types.**
- DiscoveryAgent: perceives the query file, classifies items, resolves searches/channels/playlists into URLs, and emits jobs.
- DownloadAgent: consumes jobs, downloads audio using yt-dlp/ffmpeg, tracks per-worker progress, and emits completion/failure signals.
- ResilienceAgent: monitors health signals; in direct mode it detects stalls and decides to re-queue (retry) or skip; in XMPP mode it consumes status messages.

**Why multiple agents.** The roles have different percepts, actions, and decision policies. Separating them clarifies reasoning and supports evolution from direct references to message passing.

**Grouping functionalities.** "Resolve inputs" is grouped under Discovery; "execute downloads" under Download; "monitor and intervene" under Resilience. This grouping maps cleanly to Prometheus capabilities and supports traceable design decisions.

## Phase 3 - Interaction Design (How agents communicate)

The system supports two modes. In `direct` mode, the pipeline passes job objects in-process and Resilience reads Download state directly. In `xmpp` mode, Discovery sends `job.enqueue` and `discovery.done` messages to Download, and Download sends `download.finished`, `download.failed`, and `download.all_done` to Resilience. Message schemas are centralized in `agents/shared/ontology.py`.

## Phase 4 - Detailed Design (Agent internals)

**Capabilities and triggers.** Capabilities are implemented as SPADE behaviours. Discovery runs once at startup, Download runs cyclically over queued jobs, and Resilience either polls state (direct mode) or consumes messages (XMPP mode).

**Plans.** Discovery plans include: classify -> resolve -> cap -> enqueue. Download plans include: acquire worker slot -> download -> update state -> emit status. Resilience plans include: detect stall -> decide retry/skip (direct mode) and log/aggregate status (XMPP mode).

**Data (beliefs/knowledge).** The system stores internal state as dataclasses (e.g., job queue, per-worker slots, completion logs, retry counters, resilience metrics). It also stores configuration such as caps, retries, worker slots, and output paths.

**Percepts and actions.** Percepts include query file lines, resolved URL lists, download progress events, worker slot percent changes, and status messages. Actions include enqueuing jobs, downloading audio, writing files, emitting messages, and re-queuing stalled work (direct mode).

## Phase 5 - Implementation (Prototype and simulation)

The prototype is implemented in Python 3.12 using SPADE for agents and yt-dlp for YouTube extraction. It demonstrates the perceive -> decide -> act loop: inputs are perceived from a query file, decisions classify and resolve them into jobs, and actions download audio and persist results. A simple simulation is running the pipeline with a mixed query file and observing job dispatch, downloads, and resilience outputs.

**Mapping from design to code.** Agent responsibilities and capabilities are declared in the agent modules and implemented as behaviours: `agents/discovery/*`, `agents/download/*`, and `agents/resilience/*`. The message contract is defined in `agents/shared/ontology.py`. The downloader integrates with yt-dlp and ffmpeg in `download_layer/*`. The CLI provides runnable setup and parameters in `main.py`.

**Challenges and limitations.** YouTube resolution can time out and may require tuning timeouts/caps. In XMPP mode, resilience focuses on status aggregation rather than stall-based intervention (direct mode has richer stall detection). Progress reporting is best-effort and depends on yt-dlp hooks and network stability.
