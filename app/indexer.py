"""Manually-triggered incremental indexer for the clean topic.

Each ``POST /index/run`` invocation executes one bounded indexing pass:

* a fresh ``read_committed`` consumer reads the fixed two clean partitions,
* at most ``INDEX_BATCH_SIZE`` (default 50) **committed** records are taken
  from each partition,
* every reading is written to SQLite together with that partition's
  continuation position (the next offset) in a single transaction.

Because the cursor only advances in the same transaction as the row insert,
interruptions, process restarts and repeated triggers can neither skip nor
duplicate a record.  Three conditions are reported as locatable issues instead
of being swept under the carpet:

* ``retention_gap``        - retention already deleted records before the
  cursor reached them; the partition is parked and the gap is never crossed;
* ``read_failure``         - a Kafka read failed; the cursor stays put for the
  next run;
* ``unparseable_sampled_utc`` - the reading is indexed (no data loss) but its
  sample time cannot be parsed, so UTC range queries cannot place it.
"""
from __future__ import annotations

import json
import logging
import threading
import uuid
from typing import Any

from confluent_kafka import Consumer, KafkaError, TopicPartition

from .config import Settings
from .index_store import IndexStore
from .timestamps import parse_utc_epoch

logger = logging.getLogger("coldchain.indexer")


class IndexerBusyError(RuntimeError):
    """Raised when an indexing pass is already running."""


class IncrementalIndexer:
    def __init__(self, settings: Settings, store: IndexStore,
                 consumer_factory: Any | None = None) -> None:
        self.settings = settings
        self.store = store
        # Factory injection point for tests; defaults to a real consumer.
        self._consumer_factory = consumer_factory or self._default_consumer
        self._lock = threading.Lock()
        self._running = False
        self.last_run: dict[str, Any] | None = None

    @property
    def running(self) -> bool:
        with self._lock:
            return self._running

    def _default_consumer(self) -> Any:
        return Consumer(
            {
                "bootstrap.servers": self.settings.kafka_bootstrap,
                # Unique, anonymous group per run: manual assignment is used and
                # group offsets are never committed, so runs never interfere
                # with the worker's consumer group or with each other.
                "group.id": f"indexer-{uuid.uuid4().hex}",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "isolation.level": "read_committed",
            }
        )

    # ------------------------------------------------------------------ run
    def run_once(self) -> dict[str, Any]:
        """Execute one bounded indexing pass across all clean partitions."""
        if not self._lock.acquire(blocking=False):
            raise IndexerBusyError("an indexing pass is already in progress")
        self._running = True
        s = self.settings
        report: dict[str, Any] = {
            "clean_topic": s.clean_topic,
            "batch_size": s.index_batch_size,
            "partitions": [],
            "indexed_total": 0,
            "complete": True,
            "issues": [],
        }
        consumer: Any | None = None
        try:
            try:
                consumer = self._consumer_factory()
                metadata = consumer.list_topics(s.clean_topic, timeout=10)
                topic_meta = metadata.topics.get(s.clean_topic)
                if topic_meta is None:
                    report["complete"] = False
                    report["issues"].append({
                        "kind": "topic_missing",
                        "detail": {"topic": s.clean_topic},
                    })
                    return report
                partitions = sorted(topic_meta.partitions.keys())

                # Bound the scan to the committed-readable end each partition
                # had when this pass started; records arriving afterwards
                # belong to a later trigger.
                watermarks: dict[int, tuple[int, int]] = {}
                for p in partitions:
                    low, high = consumer.get_watermark_offsets(
                        TopicPartition(s.clean_topic, p), timeout=5
                    )
                    watermarks[p] = (low, high)
            except Exception as exc:  # noqa: BLE001 - broker down -> incomplete
                logger.warning("index run could not establish boundaries: %r", exc)
                report["complete"] = False
                report["issues"].append({
                    "kind": "boundary_unavailable",
                    "detail": {"error": repr(exc)},
                })
                return report

            for p in partitions:
                part_report = self._index_partition(consumer, p, *watermarks[p])
                report["partitions"].append(part_report)
                report["indexed_total"] += part_report["indexed"]
                if not part_report["complete"]:
                    report["complete"] = False
                report["issues"].extend(part_report["issues"])
            return report
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:  # noqa: BLE001
                    logger.exception("indexer consumer close failed")
            self.last_run = report
            self._running = False
            self._lock.release()

    # ---------------------------------------------------------- one partition
    def _index_partition(self, consumer: Any, partition: int,
                         low_watermark: int, high_watermark: int) -> dict[str, Any]:
        s = self.settings
        result: dict[str, Any] = {
            "partition": partition,
            "low_watermark": low_watermark,
            "high_watermark": high_watermark,
            "start_offset": None,
            "indexed": 0,
            "next_offset": None,
            "complete": True,
            "issues": [],
        }

        cursor_row = self.store.get_cursor(s.clean_topic, partition)
        if cursor_row is None:
            # First contact: the logical start of the partition log is offset
            # 0.  If retention already deleted the head before we ever indexed
            # it, the low-watermark comparison below reports that gap instead
            # of silently starting at the retained tail.
            next_offset = 0
        else:
            next_offset = int(cursor_row["next_offset"])
        result["start_offset"] = next_offset

        # Retention already deleted records before the cursor: report the gap
        # and stop.  The cursor is deliberately NOT moved past it, so no
        # unindexed record is silently skipped.
        if next_offset < low_watermark:
            issue = self.store.mark_gap(
                s.clean_topic, partition, next_offset, low_watermark
            )
            result.update(complete=False, next_offset=next_offset)
            result["issues"].append(self._issue_payload(issue))
            logger.warning(
                "retention gap on %s p%d: offsets [%d,%d) deleted before"
                " indexing; partition parked, gap not crossed",
                s.clean_topic, partition, next_offset, low_watermark,
            )
            return result

        # Already parked on a previously recorded gap: do not cross it.
        if cursor_row is not None and cursor_row["status"] == "gap":
            open_gaps = self.store.open_issues(
                s.clean_topic, partition, kinds=("retention_gap",)
            )
            result.update(complete=False, next_offset=next_offset)
            result["issues"].extend(self._issue_payload(i) for i in open_gaps)
            return result

        consumer.assign([TopicPartition(s.clean_topic, partition, next_offset)])
        idle_polls = 0
        read_failed = False
        while next_offset < high_watermark and result["indexed"] < s.index_batch_size:
            msg = consumer.poll(s.index_poll_timeout)
            if msg is None:
                idle_polls += 1
                if idle_polls >= 10:
                    # No record although the watermark promised one: record a
                    # locatable read failure rather than a false "complete".
                    read_failed = True
                    issue = self.store.add_issue(
                        s.clean_topic, partition, "read_failure", next_offset, None,
                        {"reason": "idle_timeout", "at_offset": next_offset,
                         "expected_high_watermark": high_watermark},
                    )
                    result["issues"].append(self._issue_payload(issue))
                    break
                continue
            err = msg.error()
            if err is not None:
                if err.code == KafkaError._PARTITION_EOF:
                    # Reached the end that existed at pass start.
                    break
                if err.code == KafkaError.OFFSET_OUT_OF_RANGE:
                    # Retention raced ahead of the index mid-pass: refresh the
                    # low watermark and park on the gap, never crossing it.
                    fresh_low, _ = consumer.get_watermark_offsets(
                        TopicPartition(s.clean_topic, partition), timeout=5
                    )
                    issue = self.store.mark_gap(
                        s.clean_topic, partition, next_offset, fresh_low
                    )
                    result.update(complete=False, next_offset=next_offset)
                    result["issues"].append(self._issue_payload(issue))
                    logger.warning(
                        "retention gap on %s p%d: offset %d deleted"
                        " (low watermark now %d); parked, gap not crossed",
                        s.clean_topic, partition, next_offset, fresh_low,
                    )
                    return result
                read_failed = True
                issue = self.store.add_issue(
                    s.clean_topic, partition, "read_failure", next_offset, None,
                    {"error_code": getattr(err, "code", None),
                     "error": str(err), "at_offset": next_offset},
                )
                result["issues"].append(self._issue_payload(issue))
                logger.warning("read failure on %s p%d@%d: %s",
                               s.clean_topic, partition, next_offset, err)
                break

            idle_polls = 0
            offset = msg.offset()
            # Defensive: a gap appearing mid-pass (retention racing the index).
            if offset < low_watermark:
                issue = self.store.mark_gap(
                    s.clean_topic, partition, next_offset, low_watermark
                )
                result.update(complete=False, next_offset=next_offset)
                result["issues"].append(self._issue_payload(issue))
                return result

            record, parse_issue = self._decode(msg.value())
            if parse_issue is not None:
                # A record that is not valid JSON cannot supply the columns the
                # table requires; keep its location as an issue and stop the
                # partition instead of advancing over it blindly.
                issue = self.store.add_issue(
                    s.clean_topic, partition, "unreadable_record", offset, None,
                    {"reason": parse_issue, "at_offset": offset},
                )
                result["issues"].append(self._issue_payload(issue))
                result.update(complete=False, next_offset=next_offset)
                return result

            epoch = parse_utc_epoch(record.get("sampled_utc"))
            self.store.index_record(
                s.clean_topic, partition, offset, record, epoch
            )
            if epoch is None:
                issue = self.store.add_issue(
                    s.clean_topic, partition, "unparseable_sampled_utc",
                    offset, str(record.get("box_id", "")) or None,
                    {"at_offset": offset,
                     "sampled_utc": record.get("sampled_utc"),
                     "box_id": record.get("box_id")},
                )
                result["issues"].append(self._issue_payload(issue))

            next_offset = offset + 1
            result["indexed"] += 1

        self.store.set_cursor_watermark(s.clean_topic, partition, high_watermark)
        result["next_offset"] = next_offset

        if read_failed:
            result["complete"] = False
            # Cursor was not advanced for the unread message; it retries next run.
            return result

        # A clean pass through the whole readable span clears transient issues.
        if next_offset >= high_watermark:
            self.store.resolve_partition_issues(
                s.clean_topic, partition, ("read_failure",)
            )
            result["issues"] = [
                i for i in result["issues"] if i["kind"] != "read_failure"
            ]
        else:
            # Batch boundary reached while more committed records remain.
            result["complete"] = False
        return result

    # ---------------------------------------------------------------- helpers
    @staticmethod
    def _decode(raw: bytes) -> tuple[dict[str, Any], str | None]:
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            return {}, f"invalid_json:{type(exc).__name__}"
        if not isinstance(value, dict):
            return {}, "record_not_object"
        missing = [k for k in ("box_id", "celsius", "reading", "scale",
                               "source_topic", "source_partition", "source_offset")
                   if k not in value]
        if missing:
            return {}, f"missing_fields:{','.join(missing)}"
        return value, None

    @staticmethod
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
