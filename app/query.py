"""Box + UTC range queries over the indexed clean readings.

Trust model
-----------
A query result is only reported as ``complete`` when every committed clean
record that existed when the query **started** is already indexed.  The
readable boundary (low/high watermark of both fixed partitions) is captured up
front and compared against the durable index cursors.  When the index lags,
retention has deleted unindexed records, a read previously failed, or a
matching record has an unparseable timestamp, the answer stays ``incomplete``
and carries locatable progress/source information - partial data is returned,
but it is never misreported as the whole truth.
"""
from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from confluent_kafka import Consumer, TopicPartition

from .config import Settings
from .index_store import IndexStore

logger = logging.getLogger("coldchain.query")


def _boundary_consumer(settings: Settings) -> Any:
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": f"query-boundary-{uuid.uuid4().hex}",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
        }
    )


def snapshot_boundaries(settings: Settings,
                        consumer_factory=_boundary_consumer) -> dict[str, Any]:
    """Fix the committed-readable boundary of every clean partition.

    Returns a ``{"ok": bool, "boundaries": [...], "error": ...}`` structure so
    the API can degrade to an explicitly incomplete answer instead of failing
    the request when the broker is unreachable.
    """
    consumer = consumer_factory(settings)
    try:
        metadata = consumer.list_topics(settings.clean_topic, timeout=10)
        topic_meta = metadata.topics.get(settings.clean_topic)
        if topic_meta is None:
            return {
                "ok": False,
                "boundaries": [],
                "error": {"kind": "topic_missing",
                          "detail": {"topic": settings.clean_topic}},
            }
        boundaries = []
        for p in sorted(topic_meta.partitions.keys()):
            low, high = consumer.get_watermark_offsets(
                TopicPartition(settings.clean_topic, p), timeout=5
            )
            boundaries.append({
                "partition": p,
                "low_watermark": low,
                "high_watermark": high,
            })
        return {"ok": True, "boundaries": boundaries, "error": None}
    except Exception as exc:  # noqa: BLE001 - any broker failure -> incomplete
        logger.warning("boundary snapshot failed: %r", exc)
        return {
            "ok": False,
            "boundaries": [],
            "error": {"kind": "boundary_unavailable",
                      "detail": {"error": repr(exc)}},
        }
    finally:
        try:
            consumer.close()
        except Exception:  # noqa: BLE001
            logger.exception("boundary consumer close failed")


def _issue_payload(row: dict[str, Any]) -> dict[str, Any]:
    detail = row.get("detail")
    if isinstance(detail, str):
        try:
            detail = json.loads(detail)
        except json.JSONDecodeError:
            detail = {"raw": detail}
    return {
        "id": row["id"],
        "kind": row["kind"],
        "partition": row["partition_no"],
        "clean_offset": row["clean_offset"],
        "box_id": row["box_id"],
        "detail": detail,
        "created_at": row["created_at"],
        "resolved_at": row.get("resolved_at"),
    }


def query_readings(settings: Settings, store: IndexStore, box_id: str,
                   start_epoch: float, end_epoch: float,
                   limit: int | None,
                   boundary: dict[str, Any] | None = None) -> dict[str, Any]:
    """Assemble a query response with an honest completeness verdict."""
    if boundary is None:
        boundary = snapshot_boundaries(settings)

    rows = store.query_readings(
        settings.clean_topic, box_id, start_epoch, end_epoch, limit
    )
    readings = [
        {
            "box_id": r["box_id"],
            "sampled_utc": r["sampled_utc"],
            "celsius": r["celsius"],
            "scale": r["scale"],
            "reading": r["reading"],
            "source_topic": r["source_topic"],
            "source_partition": r["source_partition"],
            "source_offset": r["source_offset"],
            "clean_partition": r["clean_partition"],
            "clean_offset": r["clean_offset"],
        }
        for r in rows
    ]

    issues: list[dict[str, Any]] = []
    complete = True
    progress: list[dict[str, Any]] = []
    cursors = {
        c["partition_no"]: c
        for c in store.list_cursors(settings.clean_topic)
    }

    if not boundary["ok"]:
        # Cannot prove completeness without the readable boundary.
        complete = False
        issues.append(boundary["error"])
        # Durable cursors are still locatable even when the broker is down;
        # expose them with null watermark fields.
        for p_no, cursor in sorted(cursors.items()):
            progress.append({
                "partition": p_no,
                "low_watermark": None,
                "high_watermark_at_query_start": None,
                "indexed_next_offset": int(cursor["next_offset"]),
                "status": cursor["status"],
            })

    for b in boundary["boundaries"]:
        p = b["partition"]
        low, high = b["low_watermark"], b["high_watermark"]
        cursor = cursors.get(p)
        next_offset = int(cursor["next_offset"]) if cursor else low
        status = cursor["status"] if cursor else "never_indexed"
        progress.append({
            "partition": p,
            "low_watermark": low,
            "high_watermark_at_query_start": high,
            "indexed_next_offset": next_offset,
            "status": status,
        })
        if next_offset < low:
            # Retention removed records the index never reached.
            complete = False
        elif next_offset < high:
            # Index has not caught up with the committed-readable boundary.
            complete = False

    # Durable partition-scoped problems (gaps, failed reads, unreadable rows).
    for row in store.open_issues(settings.clean_topic):
        if row["kind"] == "unparseable_sampled_utc":
            # Only relevant when it concerns the queried box; the timestamp
            # cannot be range-compared, so its absence from `readings` must be
            # disclosed rather than hidden.
            if row["box_id"] != box_id:
                continue
        issues.append(_issue_payload(row))
        complete = False

    return {
        "status": "complete" if complete else "incomplete",
        "box_id": box_id,
        "count": len(readings),
        "readings": readings,
        "boundaries": [
            {
                "partition": b["partition"],
                "low_watermark": b["low_watermark"],
                "high_watermark_at_query_start": b["high_watermark"],
            }
            for b in boundary["boundaries"]
        ],
        "index_progress": progress,
        "issues": issues,
    }
