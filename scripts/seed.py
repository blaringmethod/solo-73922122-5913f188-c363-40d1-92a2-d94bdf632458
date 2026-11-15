"""Seed the two-partition raw topic with a deterministic cold-chain sample.

Partition 0 (BOX-A): C, F, **missing scale blocker**, C behind the blocker
Partition 1 (BOX-B): F, **unknown scale blocker**, C behind the blocker, F

Messages are produced directly to explicit partitions so the demo is
deterministic regardless of key hashing.
"""
from __future__ import annotations

import json
import time

from confluent_kafka import Producer

from app.config import settings
from app.messaging import ensure_topics

SAMPLE: list[tuple[int, str, dict]] = [
    # partition, key, payload
    (0, "BOX-A-001", {"box_id": "BOX-A-001", "sampled_utc": "2026-11-13T08:00:00Z",
                      "reading": 4.8, "scale": "C"}),
    (1, "BOX-B-001", {"box_id": "BOX-B-001", "sampled_utc": "2026-11-13T08:00:05Z",
                      "reading": 40.1, "scale": "F"}),
    (0, "BOX-A-002", {"box_id": "BOX-A-002", "sampled_utc": "2026-11-13T08:00:10Z",
                      "reading": 41.0, "scale": "F"}),
    (1, "BOX-B-002", {"box_id": "BOX-B-002", "sampled_utc": "2026-11-13T08:00:15Z",
                      "reading": 5.0, "scale": "C"}),
    # Blocker on partition 0: scale missing entirely.
    (0, "BOX-A-003", {"box_id": "BOX-A-003", "sampled_utc": "2026-11-13T08:00:20Z",
                      "reading": 39.2}),
    # Blocker on partition 1: unknown scale "K" (kelvin not supported here).
    (1, "BOX-B-003", {"box_id": "BOX-B-003", "sampled_utc": "2026-11-13T08:00:25Z",
                      "reading": 278.35, "scale": "K"}),
    # Records behind each blocker: must NOT overtake it in the clean topic.
    (0, "BOX-A-004", {"box_id": "BOX-A-004", "sampled_utc": "2026-11-13T08:00:30Z",
                      "reading": 6.1, "scale": "C"}),
    (1, "BOX-B-004", {"box_id": "BOX-B-004", "sampled_utc": "2026-11-13T08:00:35Z",
                      "reading": 33.8, "scale": "F"}),
]


def main() -> None:
    ensure_topics(settings.kafka_bootstrap,
                  [settings.raw_topic, settings.clean_topic],
                  settings.topic_partitions)
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap})
    for partition, key, payload in SAMPLE:
        producer.produce(
            topic=settings.raw_topic,
            partition=partition,
            key=key.encode(),
            value=json.dumps(payload, ensure_ascii=False).encode(),
        )
    producer.flush(10)
    print(f"seeded {len(SAMPLE)} messages into {settings.raw_topic} "
          f"({settings.topic_partitions} partitions) via {settings.kafka_bootstrap}")
    time.sleep(0.2)


if __name__ == "__main__":
    main()
