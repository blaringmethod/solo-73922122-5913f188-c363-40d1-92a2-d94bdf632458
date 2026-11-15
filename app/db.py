"""SQLite-backed durable state for blocked messages and corrections.

Two tables are kept:

* ``blocked``  - one row per blocked source message (keyed by
  topic/partition/offset).  Holds the original payload, the blocker reason and
  the lifecycle status (``blocked`` -> ``corrected`` -> ``committed``).
* ``correction_attempts`` - an audit trail of every correction API call, so
  repeated corrections are visible and remain idempotent.

WAL plus ``synchronous=FULL`` guarantee that a correction acknowledged by the
API survives a process exit before the Kafka transaction is committed.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any

_SCHEMA = """
CREATE TABLE IF NOT EXISTS blocked (
    topic        TEXT NOT NULL,
    partition_no INTEGER NOT NULL,
    offset_no    INTEGER NOT NULL,
    key          TEXT,
    raw_message  TEXT NOT NULL,
    reason       TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'blocked',
    correction   TEXT,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    PRIMARY KEY (topic, partition_no, offset_no)
);
CREATE TABLE IF NOT EXISTS correction_attempts (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    topic        TEXT NOT NULL,
    partition_no INTEGER NOT NULL,
    offset_no    INTEGER NOT NULL,
    requested_scale TEXT NOT NULL,
    outcome      TEXT NOT NULL,
    detail       TEXT,
    created_at   TEXT NOT NULL
);
"""


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class CorrectionStore:
    """Thread-safe wrapper around a single SQLite database file."""

    def __init__(self, path: str) -> None:
        self.path = path
        parent = os.path.dirname(os.path.abspath(path))
        os.makedirs(parent, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------ blocked
    def upsert_blocked(self, topic: str, partition: int, offset: int,
                       key: str | None, raw_message: Any, reason: str) -> bool:
        """Insert a blocked row. Returns True if newly inserted, False if known."""
        now = _utcnow()
        with self._lock:
            cur = self._conn.execute(
                "SELECT 1 FROM blocked WHERE topic=? AND partition_no=? AND offset_no=?",
                (topic, partition, offset),
            )
            if cur.fetchone() is not None:
                return False
            self._conn.execute(
                "INSERT INTO blocked(topic, partition_no, offset_no, key, raw_message,"
                " reason, status, created_at, updated_at) VALUES (?,?,?,?,?,?,'blocked',?,?)",
                (topic, partition, offset, key,
                 json.dumps(raw_message, ensure_ascii=False, sort_keys=True),
                 reason, now, now),
            )
            self._conn.commit()
            return True

    def get_blocked(self, topic: str, partition: int, offset: int) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM blocked WHERE topic=? AND partition_no=? AND offset_no=?",
                (topic, partition, offset),
            ).fetchone()
        return dict(row) if row else None

    def list_blocked(self, status: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            if status is None:
                rows = self._conn.execute(
                    "SELECT * FROM blocked ORDER BY partition_no, offset_no"
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM blocked WHERE status=? ORDER BY partition_no, offset_no",
                    (status,),
                ).fetchall()
        return [dict(r) for r in rows]

    # -------------------------------------------------------------- corrections
    def record_attempt(self, topic: str, partition: int, offset: int,
                       requested_scale: str, outcome: str, detail: str = "") -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO correction_attempts(topic, partition_no, offset_no,"
                " requested_scale, outcome, detail, created_at) VALUES (?,?,?,?,?,?,?)",
                (topic, partition, offset, requested_scale, outcome, detail, _utcnow()),
            )
            self._conn.commit()

    def apply_correction(self, topic: str, partition: int, offset: int,
                         scale: str) -> tuple[str, dict[str, Any] | None]:
        """Idempotently attach a corrected scale to a blocked message.

        Returns ``(outcome, row)`` where outcome is one of
        ``applied`` / ``duplicate_same`` / ``conflict`` / ``already_committed``
        / ``not_found``.
        """
        with self._lock:
            row = self.get_blocked(topic, partition, offset)
            if row is None:
                return "not_found", None
            if row["status"] == "committed":
                return "already_committed", row
            if row["status"] == "corrected":
                if row["correction"] == scale:
                    return "duplicate_same", row
                return "conflict", row
            now = _utcnow()
            self._conn.execute(
                "UPDATE blocked SET status='corrected', correction=?, updated_at=?"
                " WHERE topic=? AND partition_no=? AND offset_no=?",
                (scale, now, topic, partition, offset),
            )
            self._conn.commit()
            row = self.get_blocked(topic, partition, offset)
            return "applied", row

    def mark_committed(self, topic: str, partition: int, offset: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE blocked SET status='committed', updated_at=?"
                " WHERE topic=? AND partition_no=? AND offset_no=?",
                (_utcnow(), topic, partition, offset),
            )
            self._conn.commit()

    def reconcile_on_startup(self, topic: str) -> int:
        """Re-arm ``corrected`` rows for redelivery after a restart.

        A ``corrected`` row only means the correction was durable, never that
        the Kafka transaction committed.  On restart the message is redelivered
        by Kafka; the row stays in its redelivery-pending state (the approved
        scale column is preserved) so the worker reprocesses it through the
        correction path instead of re-blocking the partition.  Returns how many
        rows were reconciled.
        """
        with self._lock:
            cur = self._conn.execute(
                "UPDATE blocked SET updated_at=? "
                "WHERE topic=? AND status='corrected' AND correction IS NOT NULL",
                (_utcnow(), topic),
            )
            self._conn.commit()
            return cur.rowcount
