"""Read-only access to the clean topic using a transaction-aware consumer."""
from __future__ import annotations

import json
import uuid

from confluent_kafka import Consumer, KafkaError, TopicPartition

from .config import Settings


def read_clean(settings: Settings, partition: int | None = None,
               limit: int | None = None) -> list[dict]:
    """Return committed clean records in partition order.

    A fresh consumer with ``read_committed`` and a unique group id is used per
    request, so only fully transaction-committed records are ever visible and
    requests never interfere with each other's offsets.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": f"clean-reader-{uuid.uuid4().hex}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
        }
    )
    try:
        metadata = consumer.list_topics(settings.clean_topic, timeout=10)
        topic_meta = metadata.topics.get(settings.clean_topic)
        if topic_meta is None:
            return []
        partitions = (
            [partition]
            if partition is not None
            else sorted(topic_meta.partitions.keys())
        )
        consumer.assign([
            TopicPartition(settings.clean_topic, p, 0) for p in partitions
        ])
        # High watermarks bound the scan: read until every assigned partition
        # reaches the end it had when the request started.
        watermarks = {
            p: consumer.get_watermark_offsets(
                TopicPartition(settings.clean_topic, p), timeout=5
            )[1]
            for p in partitions
        }
        positions = {p: 0 for p in partitions}
        records: list[dict] = []
        idle_polls = 0
        while idle_polls < 10:
            if all(positions[p] >= watermarks[p] for p in partitions):
                break
            msg = consumer.poll(0.5)
            if msg is None:
                idle_polls += 1
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    idle_polls += 1
                    continue
                raise msg.error()
            idle_polls = 0
            records.append(json.loads(msg.value().decode("utf-8")))
            positions[msg.partition()] = msg.offset() + 1
            if limit is not None and len(records) >= limit:
                break
        return records
    finally:
        consumer.close()
