"""
Terminal Dashboard for YT Downloader - Windows Compatible
FIXED: correct job counting, offline failures, zero-result jobs
"""

import time
import threading
import os
from typing import Dict, List, Deque
from collections import deque
from dataclasses import dataclass
import sys

from rich.console import Console, Group
from rich.text import Text
from rich.panel import Panel
from rich.table import Table
from rich import box
from rich.live import Live


@dataclass
class Task:
    url: str
    title: str = ""
    progress: float = 0.0
    status: str = "queued"
    started_at: float = 0.0
    completed_at: float = 0.0

    @property
    def duration(self):
        if not self.started_at:
            return "0s"
        end = self.completed_at or time.time()
        sec = end - self.started_at
        return f"{sec:.1f}s" if sec < 60 else f"{int(sec//60)}:{int(sec%60):02d}"


class Dashboard:
    def __init__(self):
        self.console = Console(force_terminal=True)
        self._lock = threading.Lock()
        self._running = False
        self._live = None

        self.active_tasks: Dict[str, Task] = {}
        self.completed_tasks: Deque[Task] = deque(maxlen=10)
        self.failed_tasks: List[Task] = []

        # job_id → {name, total, completed, failed}
        self.jobs: Dict[str, Dict] = {}

        self.stats = {
            "start_time": time.time(),
            "total_downloaded": 0,
            "total_failed": 0,
        }

        self._terminal_urls = set()

    # ---------------- EVENTS ----------------

    def send_event(self, ev: dict):
        with self._lock:
            t = ev.get("type")
            if t == "meta":
                self._handle_meta(ev)
            elif t == "download":
                self._handle_download(ev)
            elif t == "job":
                self._handle_job(ev)

    def _handle_meta(self, ev: dict):
        job_id = ev.get("query_key")
        if not job_id:
            return

        self.jobs.setdefault(job_id, {
            "name": ev.get("title") or job_id,
            "total": int(ev.get("count", 0)),
            "completed": 0,
            "failed": 0,
        })

    def _handle_job(self, ev: dict):
        job_id = ev.get("query_key")
        status = ev.get("status")

        if not job_id:
            return

        job = self.jobs.setdefault(job_id, {
            "name": job_id,
            "total": 0,
            "completed": 0,
            "failed": 0,
        })

        # Zero-result job → mark failed
        if status in ("empty", "failed") and job["total"] == 0:
            job["failed"] = 1
            self.stats["total_failed"] += 1

    def _handle_download(self, ev: dict):
        url = ev.get("url")
        status = ev.get("status")
        job_id = ev.get("query_key")

        if not url or url in self._terminal_urls:
            return

        task = self.active_tasks.setdefault(
            url, Task(url=url, started_at=time.time())
        )

        if status == "finished":
            self._terminal_urls.add(url)
            task.status = "finished"
            task.completed_at = time.time()

            self.completed_tasks.append(task)
            self.stats["total_downloaded"] += 1
            self.active_tasks.pop(url, None)

            if job_id in self.jobs:
                self.jobs[job_id]["completed"] += 1

        elif status == "final_failed":
            self._terminal_urls.add(url)
            task.status = "failed"
            task.completed_at = time.time()

            self.failed_tasks.append(task)
            self.stats["total_failed"] += 1
            self.active_tasks.pop(url, None)

            if job_id in self.jobs:
                self.jobs[job_id]["failed"] += 1

    # ---------------- UI ----------------

    def _bar(self, percent, width=20):
        fill = int(width * percent / 100)
        return "█" * fill + "░" * (width - fill)

    def _render(self):
        elapsed = time.time() - self.stats["start_time"]
        elapsed_str = time.strftime("%H:%M:%S", time.gmtime(elapsed))

        header = Panel(
            f"⏱ {elapsed_str}  "
            f"✅ {self.stats['total_downloaded']}  "
            f"❌ {self.stats['total_failed']}  "
            f"📁 Jobs: {len(self.jobs)}",
            style="cyan"
        )

        jobs_txt = Text()
        for j in self.jobs.values():
            total = max(j["total"], j["completed"] + j["failed"])
            done = j["completed"] + j["failed"]
            pct = (done / total * 100) if total else 100
            jobs_txt.append(
                f"\n• {j['name'][:40]}\n  "
                f"{self._bar(pct,10)} {done}/{total}"
            )

        jobs_panel = Panel(jobs_txt or "No jobs", title="Jobs")

        recent = Text()
        for t in self.completed_tasks:
            recent.append(f"\n✔ {t.title or t.url[:40]} ({t.duration})")

        for t in self.failed_tasks[-5:]:
            recent.append(f"\n✖ {t.title or t.url[:40]}")

        recent_panel = Panel(recent or "None", title="Recent")

        return Group(header, jobs_panel, recent_panel)

    # ---------------- LIFECYCLE ----------------

    def start(self):
        self._running = True
        os.system("cls")
        with Live(self._render(), refresh_per_second=4, console=self.console) as live:
            self._live = live
            while self._running:
                live.update(self._render())
                time.sleep(0.2)

    def stop(self):
        self._running = False
        os.system("cls")
        self._summary()

    def _summary(self):
        table = Table(title="Download Complete!", box=box.ROUNDED)
        table.add_column("Metric")
        table.add_column("Value")

        table.add_row("Total Time", time.strftime(
            "%H:%M:%S",
            time.gmtime(time.time() - self.stats["start_time"])
        ))
        table.add_row("Downloads Completed", str(self.stats["total_downloaded"]))
        table.add_row("Downloads Failed", str(self.stats["total_failed"]))
        table.add_row("Total Jobs", str(len(self.jobs)))

        if self.failed_tasks:
            table.add_row("", "")
            table.add_row("Failed Downloads", "")
            for t in self.failed_tasks:
                table.add_row("", f"• {t.title or t.url}")

        self.console.print(table)
