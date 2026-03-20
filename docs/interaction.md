# Interaction Design (Phase 3)

This document describes message flows and message fields for the Ga-Audio-Harvester multi-agent system.

## Interaction Diagram (XMPP Mode)

```mermaid
sequenceDiagram
  autonumber
  participant U as User
  participant P as Pipeline (launcher)
  participant Disc as DiscoveryAgent
  participant Dl as DownloadAgent
  participant Res as ResilienceAgent

  U->>P: run with --mode xmpp
  P->>Dl: start()
  P->>Res: start()
  P->>Disc: start()
  Disc->>Disc: perceive + decide (classify/resolve)
  loop For each resolved job
    Disc-->>Dl: INFORM job.enqueue {url, source, query_key, title, output_dir}
  end
  Disc-->>Dl: INFORM discovery.done {total, skipped, errors}
  loop For each job
    Dl->>Dl: act (download audio)
    alt success
      Dl-->>Res: INFORM download.finished {url, filename, size_mb}
    else failure
      Dl-->>Res: INFORM download.failed {url, reason, attempt}
    end
  end
  Dl-->>Res: INFORM download.all_done {done, errors, total_mb}
```

## Interaction Diagram (Direct Mode)

```mermaid
sequenceDiagram
  autonumber
  participant P as Pipeline
  participant Disc as DiscoveryAgent
  participant Dl as DownloadAgent
  participant Res as ResilienceAgent

  P->>Disc: start()
  P->>Dl: start()
  P->>Res: start()
  Disc->>Disc: resolve inputs -> build jobs
  P->>Dl: add_jobs(jobs)
  Res->>Dl: observe worker slot state
  Dl->>Dl: download jobs
  Res->>Res: decide retry/skip on stall
```

## Message Fields (JSON Bodies)

Messages are JSON bodies with a `type` field. Types are centralized in `agents/shared/ontology.py`.

### Discovery -> Download

- `job.enqueue`
  - `type`: `"job.enqueue"`
  - `url`: string
  - `source`: `"search" | "channel" | "playlist" | "direct"`
  - `query_key`: string (original query or URL)
  - `title`: string (optional)
  - `output_dir`: string (suggested output directory)

- `discovery.done`
  - `type`: `"discovery.done"`
  - `total`: int
  - `skipped`: int
  - `errors`: int

### Download -> Resilience

- `download.finished`
  - `type`: `"download.finished"`
  - `url`: string
  - `filename`: string (best-effort label)
  - `size_mb`: int (best-effort estimate)

- `download.failed`
  - `type`: `"download.failed"`
  - `url`: string
  - `reason`: string
  - `attempt`: int

- `download.all_done`
  - `type`: `"download.all_done"`
  - `done`: int
  - `errors`: int
  - `total_mb`: int

