# Detailed Design (Phase 4)

This document describes capabilities, plans, data (beliefs), percepts, and actions for the agent system.

## Capabilities (Grouped Behaviors)

- DiscoveryAgent capabilities
  - `ResolveInputs`: load query file and classify each entry.
  - `ResolveURLs`: for searches/channels/playlists, obtain concrete video URLs with caps.
  - `EnqueueJobs`: create job objects and (in XMPP mode) send `job.enqueue`.

- DownloadAgent capabilities
  - `ConsumeQueue`: poll pending queue and start downloads bounded by worker slots.
  - `DownloadAudio`: execute yt-dlp audio extraction into the target directory.
  - `TrackProgress`: update worker slot percent/speed and aggregate counts.
  - `ReportStatus`: emit `download.finished/failed/all_done` in XMPP mode.

- ResilienceAgent capabilities
  - `Heartbeat`: periodically report basic health (direct mode).
  - `DetectStalls`: detect worker slots with no progress and decide retry/skip (direct mode).
  - `IngestStatus`: consume status messages and update resilience metrics (XMPP mode).

## Plans (How Goals Are Achieved)

- Discovery plan (Resolve -> Enqueue)
  - Perceive: read query file lines.
  - Decide: classify each line into `search/channel/playlist/direct`.
  - Act: resolve URLs for non-direct entries (apply caps); enqueue jobs; send messages (XMPP mode).
  - Termination: send `discovery.done` and set `resolution_done`.

- Download plan (Execute -> Record -> Signal)
  - Perceive: job arrives in pending queue (direct add or XMPP inbox).
  - Decide: wait for worker semaphore and acquire a free worker slot.
  - Act: run `download_layer.core.download_video()`; update internal state.
  - React: on completion/failure, record in completed list; emit status (XMPP mode).
  - Termination: when all jobs are done/errors, set `all_done_event`.

- Resilience plan (Monitor -> Decide -> Recover)
  - Perceive: worker slot progress (direct) or status messages (XMPP).
  - Decide: if stalled and progress was made, retry up to `MAX_RETRIES`; if stalled at 0%, skip.
  - Act: re-queue the job (direct) or record failure (XMPP).

## Data / Beliefs (Knowledge Structures)

- Discovery state
  - `DiscoveryInternalState`: counts, types, resolved totals, logs, elapsed time.
  - `job_queue`: list/deque of Job objects (url, source, query_key, title, output_dir).

- Download state
  - `DownloadInternalState`: worker slots, completed list, totals, errors, speed, eta.
  - `pending_queue`: asyncio queue of jobs.
  - `worker_sem`: semaphore sized by worker slots.

- Resilience state
  - `ResilienceInternalState`: retries, failures, heartbeat count, stall tracking.
  - `retry_counts`: per-URL retry counters (direct mode stall detector).

## Percepts (Explicit)

- Query file contents (`assets/input/*.txt`).
- Query type signals derived from URL structure (video/channel/playlist) or plain text.
- URL resolution results from yt-dlp (search and flat playlist fetches).
- Download progress events (`status`, percent, speed, eta).
- Worker slot state (active flag, pct, job reference) in direct mode.
- XMPP message bodies in XMPP mode.

## Actions (Explicit)

- Create and enqueue jobs with output targets.
- Resolve searches, channels, and playlists into URL lists (apply caps).
- Download audio-only media and write files to disk.
- Update internal state (counts, logs, completed items).
- Send and receive XMPP messages defined by the ontology.
- Re-queue jobs (retry) or mark failures/skip based on decision policy.

