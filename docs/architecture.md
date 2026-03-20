# Architectural Design (Phase 2)

## Agent Types And Justification

- DiscoveryAgent
  - Purpose: convert user intents into a queue of concrete download jobs.
  - Why: resolving search/channel/playlist inputs is a distinct capability with its own percepts (query lines) and actions (resolve URLs, cap lists).

- DownloadAgent
  - Purpose: execute download jobs with bounded worker slots and track progress.
  - Why: download execution is resource-bound and benefits from an explicit concurrency policy and progress/state tracking.

- ResilienceAgent
  - Purpose: monitor health signals and handle failure logic.
  - Why: resilience policies (stall detection, retry limits, aggregation) are separate from resolution and execution and are easier to reason about as a dedicated agent.

## Grouping Functionalities (Capabilities)

- Discovery capability group
  - Load and classify inputs.
  - Resolve searches/channels/playlists into URLs.
  - Enqueue jobs and report completion.

- Download capability group
  - Consume job queue.
  - Acquire worker slot and run yt-dlp downloads.
  - Update per-worker and aggregate state.
  - Emit completion/failure status.

- Resilience capability group
  - Monitor progress and errors.
  - Detect stalls and decide retry/skip (direct mode).
  - Aggregate status messages (XMPP mode).

## Acquaintance Diagram (Communication And Information Flow)

Mermaid (conceptual acquaintance / flow):

```mermaid
flowchart LR
  Q[Query File] --> D1[DiscoveryAgent]
  D1 -->|job objects (direct mode)| D2[DownloadAgent]
  D1 -->|job.enqueue + discovery.done (XMPP)| D2
  D2 -->|download.finished/failed/all_done (XMPP)| R[ResilienceAgent]
  D2 -->|shared state refs (direct mode)| R
  D2 --> FS[(Filesystem Output)]
  R --> LOG[(Logs / Health Signals)]
```

## Mode Notes

- `direct` mode: agents are co-located; jobs and state are passed by reference in-process.
- `xmpp` mode: inter-agent coordination uses XMPP messages; Discovery sends jobs to Download; Download sends status to Resilience.

