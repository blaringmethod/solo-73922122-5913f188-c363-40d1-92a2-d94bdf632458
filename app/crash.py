"""Controlled process-exit injection for the transaction demo.

Two crash points are supported:

* ``before_commit`` - exit immediately *after* the correction is durable in
  SQLite but *before* the Kafka transaction is committed.  On restart the
  source message is redelivered and the clean record is produced exactly once.
* ``after_commit`` - exit immediately *after* the transaction commits but
  *before* the blocked row is marked ``committed`` in SQLite.  On restart the
  already-committed offset means the message is not reprocessed.

The flag is one-shot and in-memory: arm it through the API, the next matching
message exits the process, and a restart starts clean again.
"""
from __future__ import annotations

import os
import threading
from typing import Callable


class CrashController:
    def __init__(self, initial: str = "",
                 exit_fn: Callable[[int], None] | None = None) -> None:
        self._lock = threading.Lock()
        self._armed = initial or ""
        # In production this is os._exit (hard stop); tests inject a stub.
        self._exit_fn = exit_fn or (lambda code: os._exit(code))
        # Set when a one-shot crash was actually triggered.
        self.triggered = False
        self.last_triggered_point = ""

    def arm(self, crash_point: str) -> None:
        with self._lock:
            self._armed = crash_point

    def disarm(self) -> None:
        with self._lock:
            self._armed = ""

    def status(self) -> str:
        with self._lock:
            return self._armed

    def maybe_exit(self, crash_point: str, context: str) -> None:
        with self._lock:
            armed = self._armed
            if armed == crash_point:
                # One-shot: nothing is persisted, so a restart is clean.
                self._armed = ""
        if armed == crash_point:
            # Hard exit by default: no finally blocks, no graceful flush,
            # simulating kill -9 / power loss at the transaction boundary.
            with self._lock:
                self.triggered = True
                self.last_triggered_point = crash_point
            print(
                f"[crash-injection] exiting at {crash_point} ({context})",
                flush=True,
            )
            self._exit_fn(42)

    def was_triggered(self) -> bool:
        with self._lock:
            return self.triggered
