"""Thin wrappers around confluent-kafka clients.

Keeping the imports in one place makes the transaction/offset semantics
explicit and lets tests swap in an in-memory fake broker.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic


@dataclass
class KafkaMessage:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes
    error: Any = None


def decode_key(key: bytes | None) -> str | None:
    if key is None:
        return None
    return key.decode("utf-8")


class RawConsumer:
    """Consumer over the fixed-partition raw topic."""

    def __init__(self, bootstrap: str, group_id: str) -> None:
        self.group_id = group_id
        self._c = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": group_id,
                # Offsets are staged explicitly with store_offsets and committed
                # only as part of a producer transaction.
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "auto.offset.reset": "earliest",
                "isolation.level": "read_committed",
            }
        )

    def subscribe(self, topic: str) -> None:
        self._c.subscribe([topic])

    def poll(self, timeout: float) -> KafkaMessage | None:
        msg = self._c.poll(timeout)
        if msg is None:
            return None
        err = msg.error()
        return KafkaMessage(
            topic=msg.topic() or "",
            partition=msg.partition(),
            offset=msg.offset(),
            key=msg.key(),
            value=msg.value() if err is None else b"",
            error=err,
        )

    def pause_partitions(self, partitions: list[TopicPartition]) -> None:
        self._c.pause(partitions)

    def resume_partitions(self, partitions: list[TopicPartition]) -> None:
        self._c.resume(partitions)

    def assignment(self) -> list[TopicPartition]:
        return self._c.assignment()

    def store_message(self, msg: KafkaMessage) -> None:
        """Stage ``offset + 1`` for the next transactional offset commit."""
        self._c.store_offsets(
            offsets=[
                TopicPartition(msg.topic, msg.partition, msg.offset + 1)
            ]
        )

    def reset_stored(self, msg: KafkaMessage) -> None:
        """Rewind the staged position after an aborted transaction.

        The group offset was never committed, but the local stored position was
        advanced by ``store_message``. Point it back at the failed message
        (``offset`` rather than ``offset + 1``) so poll redelivers it.
        """
        self._c.store_offsets(
            offsets=[TopicPartition(msg.topic, msg.partition, msg.offset)]
        )

    def staged_offsets(self, partitions: list[TopicPartition]) -> list[TopicPartition]:
        """Return the stored offsets (next message to consume per partition)."""
        # position() reflects store_offsets() when auto offset store is off.
        return [
            TopicPartition(tp.topic, tp.partition, tp.offset)
            for tp in self._c.position(partitions)
        ]

    def group_metadata(self) -> Any:
        """Opaque consumer group handle required for transactional offsets."""
        return self._c.consumer_group_metadata()

    def committed(self, partitions: list[TopicPartition],
                  timeout: float = 5.0) -> list[TopicPartition]:
        return self._c.committed(partitions, timeout)

    def close(self) -> None:
        self._c.close()


class CleanProducer:
    """Transactional producer writing clean records partition-by-partition."""

    def __init__(self, bootstrap: str, transactional_id: str) -> None:
        self._p = Producer(
            {
                "bootstrap.servers": bootstrap,
                "transactional.id": transactional_id,
                "enable.idempotence": True,
                "acks": "all",
            }
        )
        self._p.init_transactions()

    def begin(self) -> None:
        self._p.begin_transaction()

    def produce(self, topic: str, partition: int, key: str,
                value: dict[str, Any], on_delivery: Callable[[Any, Any], None]) -> None:
        self._p.produce(
            topic=topic,
            partition=partition,
            key=key.encode("utf-8"),
            value=json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8"),
            on_delivery=on_delivery,
        )

    def send_offsets(self, consumer: RawConsumer,
                     offsets: list[TopicPartition]) -> None:
        """Attach consumed offsets to the open transaction atomically.

        The offsets only become visible as committed group offsets when the
        transaction commits; an abort leaves the group position untouched.
        """
        self._p.send_offsets_to_transaction(
            offsets, consumer.group_metadata()
        )

    def commit(self) -> None:
        """Publish the produced records and the attached offsets atomically."""
        self._p.commit_transaction()

    def abort(self) -> None:
        self._p.abort_transaction()

    def flush(self) -> None:
        self._p.flush()


def ensure_topics(bootstrap: str, names: list[str], partitions: int) -> None:
    """Create the fixed-partition topics idempotently."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=10).topics
    new_topics = [
        NewTopic(name, num_partitions=partitions, replication_factor=1)
        for name in names
        if name not in existing
    ]
    if new_topics:
        for future in admin.create_topics(new_topics).values():
            future.result()
