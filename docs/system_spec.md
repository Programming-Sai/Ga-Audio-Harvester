# System Specification (Phase 1)

## Problem

Collecting Ga-language audio from YouTube is often manual and inconsistent. Learners and researchers need a repeatable way to curate audio from multiple sources (searches, channels, playlists, and direct links) into a structured corpus. Manual workflows do not scale, and failures (timeouts, partial downloads) are hard to manage without persistent progress and retry logic.

## Why An Agent System

The domain decomposes into agent-like responsibilities:
- Resolve intentions into concrete resources (what should be downloaded).
- Execute downloads with bounded capacity (how much can run at once).
- Monitor, detect failures/stalls, and decide recovery actions.

This separation enables clear reasoning, modular policies, and an evolution path from direct references to message passing.

## Stakeholders

- Student developer: needs traceable Prometheus artifacts and a working prototype.
- Instructor/evaluator: needs evidence of correct agent concepts, phases, and rationale.
- Language learner: needs an easy-to-run curator for listening practice.
- Research user: needs reproducible corpus building and structured outputs.

## Goals And Sub-goals

Top-level goal: **Curate Ga-language audio into a local, organized corpus**.

Sub-goals:
- Ingest a query file with mixed input types.
- Classify each input (search, channel, playlist, direct video).
- Resolve non-direct inputs into concrete video URLs (with caps).
- Download audio-only media reliably with retries.
- Track progress, completion, and failures.
- Provide a communication model for multi-agent coordination (direct and XMPP modes).

## Functionalities

- Load query file (one entry per line).
- Detect query type: search term vs YouTube URL (video/channel/playlist).
- Search YouTube via yt-dlp "ytsearchN" and select up to N results.
- Fetch channel and playlist items (flat playlist) and cap results.
- Download audio-only files via yt-dlp + ffmpeg extraction.
- Organize outputs by source type and query key.
- Emit progress and status signals to support monitoring and evaluation.

## Scenarios (3-5)

- Scenario 1: Direct URL curation
  - A user provides one YouTube video URL.
  - The system verifies it is a direct link, enqueues one job, downloads audio, and stores it in the direct output folder.

- Scenario 2: Search-based curation with cap
  - A user provides a search term like "Ga news".
  - Discovery resolves up to N results, enqueues N jobs, and Download executes them sequentially or with bounded workers.

- Scenario 3: Channel curation of latest items
  - A user provides a channel handle URL.
  - Discovery fetches channel items, caps to N, and enqueues jobs into a channel-specific folder.

- Scenario 4: Playlist curation
  - A user provides a playlist URL.
  - Discovery resolves playlist items (flat), caps to N, and enqueues jobs under a playlist folder.

- Scenario 5: Failure and recovery
  - A download starts and stalls mid-way due to a transient network issue.
  - Resilience detects no progress (direct mode) and re-queues the job up to a retry limit; otherwise it records a failure.

## Environment Description

Environment components:
- External content source: YouTube (accessed via yt-dlp).
- Execution tools: yt-dlp, ffmpeg (audio extraction).
- Persistence: local filesystem (output folders, logs).
- Coordination channel (optional): XMPP server for message-based agent communication.

Percepts (what the system perceives):
- Lines in the query file.
- Resolved URL lists from search/channel/playlist resolution.
- Download progress events (percent, speed, completion).
- Worker slot activity and stall signals (direct mode).
- XMPP message bodies (XMPP mode).

Actions (what the system can do):
- Enqueue jobs with output targets.
- Download audio-only media and write files.
- Log state transitions, progress, and errors.
- Send/receive structured inter-agent messages (XMPP mode).
- Re-queue jobs (retry) or mark failures (direct mode).

