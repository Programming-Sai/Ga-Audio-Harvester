"""
agents/shared/ontology.py
=========================
Shared message schemas, performatives, and ontology constants for the
Ga Audio Harvester multi-agent system.

All inter-agent messages use JSON bodies with a "type" field that maps
to one of the MSG_* constants below.  This gives us a single source of
truth for the message contract so every agent speaks the same language.
"""

from __future__ import annotations
import json
from dataclasses import dataclass, field, asdict
from typing import Any, Optional

# ─────────────────────────────────────────────────────────────────────────────
# FIPA-ACL PERFORMATIVES
# ─────────────────────────────────────────────────────────────────────────────
INFORM    = "INFORM"     # share information
REQUEST   = "REQUEST"    # ask another agent to do something
CFP       = "CFP"        # call for proposals (not used in Phase A)
AGREE     = "AGREE"      # accept a request
REFUSE    = "REFUSE"     # reject a request
FAILURE   = "FAILURE"    # report failure
CONFIRM   = "CONFIRM"    # confirm receipt / completion

# ─────────────────────────────────────────────────────────────────────────────
# ONTOLOGY NAME  (set on every SPADE message)
# ─────────────────────────────────────────────────────────────────────────────
ONTOLOGY = "ga-audio-harvester"

# ─────────────────────────────────────────────────────────────────────────────
# MESSAGE TYPE CONSTANTS  (value of the "type" key in JSON body)
# ─────────────────────────────────────────────────────────────────────────────

# Discovery → Download
MSG_JOB_ENQUEUE      = "job.enqueue"       # one download job ready
MSG_DISCOVERY_DONE   = "discovery.done"    # all jobs dispatched

# Download → Resilience
MSG_DL_STARTED       = "download.started"
MSG_DL_PROGRESS      = "download.progress"
MSG_DL_FINISHED      = "download.finished"
MSG_DL_FAILED        = "download.failed"
MSG_DL_ALL_DONE      = "download.all_done"

# Resilience → Download
MSG_RETRY            = "resilience.retry"  # ask download to retry a URL
MSG_PAUSE            = "resilience.pause"
MSG_RESUME           = "resilience.resume"

# Any → Any  (heartbeat / status)
MSG_HEARTBEAT        = "heartbeat"
MSG_STATUS           = "status"


# ─────────────────────────────────────────────────────────────────────────────
# MESSAGE BODY SCHEMAS  (dataclasses → JSON)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class JobEnqueueMsg:
    """Discovery sends one of these per resolved URL."""
    type:      str = MSG_JOB_ENQUEUE
    url:       str = ""
    source:    str = ""   # "search" | "channel" | "playlist" | "direct"
    query_key: str = ""   # human-readable label from runner._wrap_callback
    title:     str = ""   # video title if known at resolve time
    output_dir:str = ""   # suggested output subdirectory

@dataclass
class DiscoveryDoneMsg:
    type:      str = MSG_DISCOVERY_DONE
    total:     int = 0
    skipped:   int = 0
    errors:    int = 0

@dataclass
class DownloadProgressMsg:
    type:      str  = MSG_DL_PROGRESS
    url:       str  = ""
    pct:       float= 0.0
    speed:     float= 0.0   # bytes/s
    eta:       float= 0.0   # seconds
    filename:  str  = ""

@dataclass
class DownloadFinishedMsg:
    type:     str = MSG_DL_FINISHED
    url:      str = ""
    filename: str = ""
    size_mb:  int = 0

@dataclass
class DownloadFailedMsg:
    type:   str = MSG_DL_FAILED
    url:    str = ""
    reason: str = ""
    attempt:int = 0

@dataclass
class DownloadAllDoneMsg:
    type:    str = MSG_DL_ALL_DONE
    done:    int = 0
    errors:  int = 0
    total_mb:int = 0

@dataclass
class RetryMsg:
    type:    str = MSG_RETRY
    url:     str = ""
    reason:  str = ""

@dataclass
class HeartbeatMsg:
    type:   str = MSG_HEARTBEAT
    sender: str = ""
    status: str = "ok"
    detail: str = ""

@dataclass
class StatusMsg:
    type:   str = MSG_STATUS
    sender: str = ""
    phase:  str = ""
    done:   int = 0
    total:  int = 0
    errors: int = 0


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def encode(msg_dc) -> str:
    """Dataclass → JSON string for SPADE message body."""
    return json.dumps(asdict(msg_dc))


def decode(body: str) -> dict:
    """JSON string → dict."""
    try:
        return json.loads(body)
    except Exception:
        return {}


def msg_type(body: str) -> str:
    """Quick-read the 'type' field without full decode."""
    return decode(body).get("type", "")