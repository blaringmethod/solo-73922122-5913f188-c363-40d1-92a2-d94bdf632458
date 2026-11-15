"""SQLite persistence for the incremental clean-topic index.

The index lives in the same database file as the correction audit (WAL,
``synchronous=FULL``) but owns three dedicated tables:

* ``indexed_readings`` - one row per committed clean record already copied out
  of the ``clean-readings`` topic, keyed by its clean-topic location.  The
  original scale/reading and the Celsius correction are all retained.
* ``index_progress`` - the continuation cursor (next offset to read) per clean
  partition.  The cursor only moves forward, inside the same transaction that
  inserts the reading, so an interrupted run never loses or double-records.
* ``index_issues`` - durable, locatable problems: retention gaps, read
  failures and records whose ``sampled_utc`` cannot be parsed.  Open issues
  make every affected query report ``incomplete`` instead of pretending the
  partial result set is whole.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any

_SCHEMA = """
CREATE TABLE IF NOT EXISTS indexed_readings (
    clean_topic      TEXT NOT NULL,
    clean_partition  INTEGER NOT NULL,
    clean_offset     INTEGER NOT NULL,
    box_id           TEXT NOT NULL,
    sampled_utc      TEXT,
    sampled_epoch    REAL,
    celsius          REAL NOT NULL,
    reading          REAL NOT NULL,
    scale            TEXT NOT NULL,
    source_topic     TEXT NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset    INTEGER NOT NULL,
    indexed_at       TEXT NOT NULL,
    PRIMARY KEY (clean_topic, clean_partition, clean_offset)
);
CREATE INDEX IF NOT EXISTS idx_indexed_box_time
    ON indexed_readings(box_id, sampled_epoch);
CREATE TABLE IF NOT EXISTS index_progress (
    clean_topic   TEXT NOT NULL,
    partition_no  INTEGER NOT NULL,
    next_offset   INTEGER NOT NULL,
    status        TEXT NOT NULL DEFAULT 'active',
    high_watermark INTEGER,
    updated_at    TEXT NOT NULL,
    PRIMARY KEY (clean_topic, partition_no)
);
CREATE TABLE IF NOT EXISTS index_issues (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    clean_topic  TEXT NOT NULL,
    partition_no INTEGER NOT NULL,
    clean_offset INTEGER,
    box_id       TEXT,
    kind         TEXT NOT NULL,
    detail       TEXT,
    created_at   TEXT NOT NULL,
    resolved_at  TEXT
);
CREATE INDEX IF NOT EXISTS idx_index_issues_open
    ON index_issues(clean_topic, partition_no, kind) WHERE resolved_at IS NULL;
"""


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class IndexStore:
    """Thread-safe index persistence over its own SQLite connection."""

    def __init__(self, path: str) -> None:
        self.path = path
        parent = os.path.dirname(os.path.abspath(path))
        os.makedirs(parent, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        # Two connections may write to this file (worker audit + indexer);
        # wait out the brief WAL writer lock instead of failing immediately.
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------- progress
    def get_cursor(self, topic: str, partition: int) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM index_progress WHERE clean_topic=? AND partition_no=?",
                (topic, partition),
            ).fetchone()
        return dict(row) if row else None

    def list_cursors(self, topic: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM index_progress WHERE clean_topic=? ORDER BY partition_no",
                (topic,),
            ).fetchall()
        return [dict(r) for r in rows]

    def index_record(self, topic: str, partition: int, offset: int,
                     record: dict[str, Any], sampled_epoch: float | None) -> None:
        """Insert one reading and advance the cursor in a single transaction.

        The insert is idempotent (``ON CONFLICT DO NOTHING``); the cursor upsert
        is what makes a re-triggered run safe: the same offset can never be
        counted twice and a crash between Kafka read and commit simply leaves
        the cursor at the old position for redelivery on the next run.
        """
        now = _utcnow()
        with self._lock:
            try:
                self._conn.execute(
                    "INSERT INTO indexed_readings(clean_topic, clean_partition,"
                    " clean_offset, box_id, sampled_utc, sampled_epoch, celsius,"
                    " reading, scale, source_topic, source_partition, source_offset,"
                    " indexed_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(clean_topic, clean_partition, clean_offset)"
                    " DO NOTHING",
                    (
                        topic, partition, offset,
                        str(record.get("box_id", "")),
                        record.get("sampled_utc"),
                        sampled_epoch,
                        float(record["celsius"]),
                        float(record["reading"]),
                        str(record.get("scale", "")),
                        str(record.get("source_topic", "")),
                        int(record.get("source_partition", partition)),
                        int(record.get("source_offset", offset)),
                        now,
                    ),
                )
                self._conn.execute(
                    "INSERT INTO index_progress(clean_topic, partition_no, next_offset,"
                    " status, high_watermark, updated_at) VALUES (?,?,?, 'active', ?, ?) "
                    "ON CONFLICT(clean_topic, partition_no) DO UPDATE SET"
                    " next_offset=excluded.next_offset, status='active',"
                    " high_watermark=excluded.high_watermark, updated_at=excluded.updated_at",
                    (topic, partition, offset + 1, None, now),
                )
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise

    def set_cursor_watermark(self, topic: str, partition: int,
                             high_watermark: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE index_progress SET high_watermark=?, updated_at=? "
                "WHERE clean_topic=? AND partition_no=?",
                (high_watermark, _utcnow(), topic, partition),
            )
            self._conn.commit()

    def mark_gap(self, topic: str, partition: int, missing_from: int,
                 low_watermark: int) -> dict[str, Any]:
        """Persist a retention gap and park the partition cursor at it.

        The cursor is never moved past the gap, so the index cannot silently
        skip deleted-but-unindexed records.  Repeated runs at the same gap are
        idempotent.
        """
        now = _utcnow()
        with self._lock:
            existing = self._conn.execute(
                "SELECT * FROM index_issues WHERE clean_topic=? AND partition_no=?"
                " AND kind='retention_gap' AND resolved_at IS NULL",
                (topic, partition),
            ).fetchone()
            issue = dict(existing) if existing else None
            if issue is None:
                cur = self._conn.execute(
                    "INSERT INTO index_issues(clean_topic, partition_no, clean_offset,"
                    " box_id, kind, detail, created_at) VALUES (?,?,?,NULL,'retention_gap',?,?)",
                    (topic, partition, missing_from,
                     json.dumps({"missing_from": missing_from,
                                 "low_watermark": low_watermark}),
                     now),
                )
                issue_id = cur.lastrowid
            else:
                issue_id = issue["id"]
            self._conn.execute(
                "INSERT INTO index_progress(clean_topic, partition_no, next_offset,"
                " status, high_watermark, updated_at) VALUES (?,?,?, 'gap', ?, ?) "
                "ON CONFLICT(clean_topic, partition_no) DO UPDATE SET"
                " next_offset=excluded.next_offset, status='gap',"
                " high_watermark=excluded.high_watermark, updated_at=excluded.updated_at",
                (topic, partition, missing_from, low_watermark, now),
            )
            self._conn.commit()
            row = self._conn.execute(
                "SELECT * FROM index_issues WHERE id=?", (issue_id,)
            ).fetchone()
        return dict(row)

    # --------------------------------------------------------------- issues
    def add_issue(self, topic: str, partition: int, kind: str,
                  clean_offset: int | None, box_id: str | None,
                  detail: dict[str, Any]) -> dict[str, Any]:
        """Record a locatable issue; identical open issues are deduplicated."""
        now = _utcnow()
        with self._lock:
            existing = self._conn.execute(
                "SELECT * FROM index_issues WHERE clean_topic=? AND partition_no=?"
                " AND kind=? AND COALESCE(clean_offset,-1)=COALESCE(?,-1)"
                " AND resolved_at IS NULL",
                (topic, partition, kind, clean_offset),
            ).fetchone()
            if existing is not None:
                return dict(existing)
            cur = self._conn.execute(
                "INSERT INTO index_issues(clean_topic, partition_no, clean_offset,"
                " box_id, kind, detail, created_at) VALUES (?,?,?,?,?,?,?)",
                (topic, partition, clean_offset, box_id, kind,
                 json.dumps(detail, ensure_ascii=False, sort_keys=True), now),
            )
            self._conn.commit()
            row = self._conn.execute(
                "SELECT * FROM index_issues WHERE id=?", (cur.lastrowid,)
            ).fetchone()
        return dict(row)

    def resolve_partition_issues(self, topic: str, partition: int,
                                 kinds: tuple[str, ...]) -> None:
        """Close open issues of the given kinds after a successful recovery."""
        with self._lock:
            self._conn.execute(
                "UPDATE index_issues SET resolved_at=? WHERE clean_topic=?"
                " AND partition_no=? AND kind IN (%s) AND resolved_at IS NULL"
                % ",".join("?" for _ in kinds),
                (_utcnow(), topic, partition, *kinds),
            )
            self._conn.commit()

    def open_issues(self, topic: str, partition: int | None = None,
                    box_id: str | None = None,
                    kinds: tuple[str, ...] | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM index_issues WHERE clean_topic=? AND resolved_at IS NULL"
        params: list[Any] = [topic]
        if partition is not None:
            sql += " AND partition_no=?"
            params.append(partition)
        if box_id is not None:
            sql += " AND box_id=?"
            params.append(box_id)
        if kinds:
            sql += " AND kind IN (%s)" % ",".join("?" for _ in kinds)
            params.extend(kinds)
        sql += " ORDER BY partition_no, clean_offset, id"
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    # -------------------------------------------------------------- readings
    def query_readings(self, topic: str, box_id: str, start_epoch: float,
                       end_epoch: float, limit: int | None = None) -> list[dict[str, Any]]:
        """Return indexed readings in a UTC window, ordered by sample time then
        source location.  Rows with an unparseable ``sampled_utc`` (NULL epoch)
        are never matched here; their presence is surfaced via open issues."""
        sql = (
            "SELECT * FROM indexed_readings WHERE clean_topic=? AND box_id=?"
            " AND sampled_epoch IS NOT NULL AND sampled_epoch>=? AND sampled_epoch<=?"
            " ORDER BY sampled_epoch, source_partition, source_offset,"
            " clean_partition, clean_offset"
        )
        params: list[Any] = [topic, box_id, start_epoch, end_epoch]
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def count_readings(self, topic: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM indexed_readings WHERE clean_topic=?",
                (topic,),
            ).fetchone()
        return int(row["n"])
