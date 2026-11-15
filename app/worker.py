"""Single-worker consume/transform/publish loop with blocking semantics.

Guarantees implemented here:

1. **Ordering per partition** - every input message is handled in offset order
   and clean records are produced to the same numbered partition.
2. **Blockers pause one partition only** - a missing/unknown scale pauses that
   partition; the other partition keeps flowing.
3. **Output and progress are atomic** - each clean record and the consumed
   source offset are committed in one Kafka transaction.  Downstream readers
   using ``read_committed`` only ever see finished records.
4. **No skipping, loss or duplication across restarts** - corrections are
   durable in SQLite before the partition is resumed; offsets only move inside
   the transaction; startup reconciliation makes any pre-commit crash
   redeliver the message for exactly-once reprocessing.
"""
from __future__ import annotations

import json
import logging
import threading
from typing import Any

from confluent_kafka import KafkaError, TopicPartition

from .config import Settings
from .crash import CrashController
from .db import CorrectionStore
from .logic import build_clean_record, validate_raw
from .messaging import (
    CleanProducer,
    KafkaMessage,
    RawConsumer,
    decode_key,
    ensure_topics,
)

logger = logging.getLogger("coldchain.worker")


class Worker:
    def __init__(self, settings: Settings, store: CorrectionStore,
                 crash: CrashController,
                 consumer: RawConsumer | None = None,
                 producer: CleanProducer | None = None) -> None:
        self.settings = settings
        self.store = store
        self.crash = crash
        self._consumer = consumer
        self._producer = producer
        self._thread: threading.Thread | None = None
        self._stopping = threading.Event()
        # Partitions currently paused because of an unresolved blocker.
        self._paused: set[int] = set()
        # Latest delivery error observed inside a transaction.
        self._delivery_error: Exception | None = None
        # Fatal error that terminated the loop (includes simulated crashes).
        self.fatal_error: Exception | None = None
        self._consecutive_errors = 0

    # ---------------------------------------------------------------- lifecycle
    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stopping.clear()
        self._thread = threading.Thread(
            target=self.run, name="coldchain-worker", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 10.0) -> None:
        self._stopping.set()
        if self._thread is not None:
            self._thread.join(timeout)
        self._thread = None

    def run(self) -> None:
        s = self.settings
        ensure_topics(s.kafka_bootstrap, [s.raw_topic, s.clean_topic],
                      s.topic_partitions)
        if self._consumer is None:
            self._consumer = RawConsumer(s.kafka_bootstrap, s.group_id)
        if self._producer is None:
            self._producer = CleanProducer(s.kafka_bootstrap, s.transactional_id)

        reconciled = self.store.reconcile_on_startup(s.raw_topic)
        if reconciled:
            logger.info("reconciled %d pre-commit correction(s) on startup",
                        reconciled)

        self._consumer.subscribe(s.raw_topic)
        logger.info("worker subscribed to %s (group=%s, txn=%s)",
                    s.raw_topic, s.group_id, s.transactional_id)
        try:
            while not self._stopping.is_set():
                try:
                    self._poll_once()
                    self._consecutive_errors = 0
                except Exception as exc:
                    # A failed transaction neither published records nor moved
                    # offsets, so the next poll redelivers the same message.
                    # Retry with backoff; only a sustained failure is fatal.
                    self._consecutive_errors += 1
                    self.fatal_error = exc
                    if self.crash.was_triggered():
                        logger.error("worker loop terminated by crash injection: %r", exc)
                        break
                    if self._consecutive_errors > self.settings.max_retries:
                        logger.error("retry budget exhausted; worker terminating: %r", exc)
                        raise
                    backoff = min(
                        self.settings.retry_backoff_max,
                        self.settings.retry_backoff_base * self._consecutive_errors,
                    )
                    logger.warning(
                        "poll error (attempt %d/%d): %r; retrying in %.1fs",
                        self._consecutive_errors, self.settings.max_retries, exc, backoff,
                    )
                    self._stopping.wait(backoff)
        finally:
            try:
                self._consumer.close()
            finally:
                self._producer.flush()

    # ------------------------------------------------------------- poll handling
    def _poll_once(self) -> None:
        assert self._consumer is not None
        msg = self._consumer.poll(self.settings.poll_timeout)
        if msg is None:
            return
        if msg.error is not None:
            if msg.error.code() == KafkaError._PARTITION_EOF:
                return
            raise msg.error
        self._handle_message(msg)

    def _handle_message(self, msg: KafkaMessage) -> None:
        s = self.settings
        key = decode_key(msg.key)

        raw, parse_error = self._parse(msg.value)
        if parse_error is not None:
            self._block(msg, key, raw, parse_error)
            return

        known = self.store.get_blocked(s.raw_topic, msg.partition, msg.offset)
        if known is not None and known["status"] in ("blocked", "corrected"):
            # Redelivery of a previously blocked message.  Only a durable
            # correction on a parseable payload lets it through; otherwise keep
            # the partition paused.
            if (
                known["status"] == "corrected"
                and known["correction"] in ("C", "F")
                and isinstance(raw, dict)
            ):
                raw["scale"] = known["correction"]
                fields, reason = validate_raw(raw)
                if reason is not None:  # pragma: no cover - defensive
                    self._block(msg, key, raw, reason)
                    return
                self._emit_transaction(msg, key, fields, corrected=True)
                return
            self._pause(msg.partition)
            return

        fields, reason = validate_raw(raw)
        if reason is not None:
            self._block(msg, key, raw, reason)
            return
        self._emit_transaction(msg, key, fields, corrected=False)

    # --------------------------------------------------------------- block/pause
    def _parse(self, value: bytes) -> tuple[Any, str | None]:
        try:
            return json.loads(value.decode("utf-8")), None
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            return None, f"invalid_json:{type(exc).__name__}"

    def _block(self, msg: KafkaMessage, key: str | None,
               raw: Any, reason: str) -> None:
        inserted = self.store.upsert_blocked(
            self.settings.raw_topic, msg.partition, msg.offset, key, raw, reason
        )
        if inserted:
            logger.warning(
                "BLOCKED p%d@%d reason=%s raw=%r",
                msg.partition, msg.offset, reason, raw,
            )
        self._pause(msg.partition)

    def _pause(self, partition: int) -> None:
        assert self._consumer is not None
        if partition in self._paused:
            return
        tps = [tp for tp in self._consumer.assignment() if tp.partition == partition]
        if tps:
            self._consumer.pause_partitions(tps)
            self._paused.add(partition)
            logger.info("partition %d paused; other partition keeps consuming",
                        partition)

    def resume_due_to_correction(self, partition: int) -> None:
        """Called by the API thread after a correction is durable."""
        assert self._consumer is not None
        if partition not in self._paused:
            return
        tps = [tp for tp in self._consumer.assignment() if tp.partition == partition]
        if tps:
            self._consumer.resume_partitions(tps)
        self._paused.discard(partition)
        logger.info("partition %d resumed after correction", partition)

    @property
    def paused_partitions(self) -> set[int]:
        return set(self._paused)

    # ------------------------------------------------------------- transactional
    def _on_delivery(self, err: Any, _msg: Any) -> None:
        if err is not None:
            self._delivery_error = Exception(str(err))

    def _emit_transaction(self, msg: KafkaMessage, key: str | None,
                          fields: dict[str, Any], corrected: bool) -> None:
        s = self.settings
        assert self._consumer is not None and self._producer is not None
        self._delivery_error = None
        record_key = key or fields["box_id"]
        clean = build_clean_record(
            fields, s.raw_topic, msg.partition, msg.offset
        )

        self._producer.begin()
        try:
            # Output goes to the SAME numbered partition as the source.
            self._producer.produce(
                s.clean_topic, msg.partition, record_key, clean, self._on_delivery
            )
            # Stage the source progress; commit it together with the output below.
            self._consumer.store_message(msg)

            if corrected:
                # Crash point A: correction durable, transaction not yet committed.
                self.crash.maybe_exit(
                    "before_commit",
                    f"p{msg.partition}@o{msg.offset}",
                )

            offsets = self._consumer.staged_offsets(
                [TopicPartition(msg.topic, msg.partition)]
            )
            # Attach source progress to the open transaction, then commit output
            # records and offsets as one atomic unit.
            self._producer.send_offsets(self._consumer, offsets)
            self._producer.commit()
        except BaseException:
            # A failed/aborted transaction published nothing and moved no
            # committed offset: rewind the local staged position so the source
            # message is redelivered on the next poll.
            logger.exception("transaction failed for p%d@%d; aborting",
                             msg.partition, msg.offset)
            try:
                self._consumer.reset_stored(msg)
            except Exception:  # noqa: BLE001
                logger.exception("could not reset staged offset")
            try:
                self._producer.abort()
            except Exception:  # noqa: BLE001
                logger.exception("abort_transaction also failed")
            raise

        if self._delivery_error is not None:
            # Delivery callback reported a permanent failure: commit would have
            # raised already in practice; guard kept for defensive completeness.
            raise self._delivery_error

        if corrected:
            # Crash point B: output + offsets committed, bookkeeping not updated.
            self.crash.maybe_exit(
                "after_commit",
                f"p{msg.partition}@o{msg.offset}",
            )

        self.store.mark_committed(s.raw_topic, msg.partition, msg.offset)
        logger.info(
            "COMMIT p%d@%d -> clean p%d (celsius=%.4f, corrected=%s)",
            msg.partition, msg.offset, msg.partition,
            fields["celsius"], corrected,
        )
