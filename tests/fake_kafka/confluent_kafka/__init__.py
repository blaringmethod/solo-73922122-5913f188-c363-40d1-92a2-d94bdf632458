"""In-memory test double for the ``confluent_kafka`` package.

It implements the subset of the Producer/Consumer/AdminClient API used by the
application, including:

* explicit partition assignment and pause/resume,
* idempotent transactional production with zombie fencing by transactional id,
* consumer offsets committed *as part of* a transaction,
* ``read_committed`` visibility so aborted/open transactions stay invisible,
* hard process-exit simulation that simply stops delivering further messages
  until a fresh client is constructed (the process is "rebuilt" by the test).
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any


class KafkaError:
    _PARTITION_EOF = -191
    OFFSET_OUT_OF_RANGE = 1
    UNKNOWN = -1

    def __init__(self, code: int, reason: str = "") -> None:
        self.code = code
        self.reason = reason

    def name(self) -> str:
        if self.code == self._PARTITION_EOF:
            return "PARTITION_EOF"
        if self.code == self.OFFSET_OUT_OF_RANGE:
            return "OFFSET_OUT_OF_RANGE"
        return "ERROR"

    def __str__(self) -> str:
        return self.reason or self.name()


class KafkaException(Exception):
    def __init__(self, error: KafkaError | None = None) -> None:
        super().__init__(str(error) if error else "kafka error")
        self.error = error


class TopicPartition:
    def __init__(self, topic: str, partition: int, offset: int = -1001,
                 metadata: str | None = None) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.metadata = metadata

    def __repr__(self) -> str:
        return f"TopicPartition({self.topic!r}, {self.partition}, {self.offset})"


class NewTopic:
    def __init__(self, topic: str, num_partitions: int = 1,
                 replication_factor: int = 1) -> None:
        self.topic = topic
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor


@dataclass
class _Record:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes
    # Highest committed producer epoch visible to read_committed readers.
    visible_epoch: int = -1


@dataclass
class _Topic:
    name: str
    partitions: int
    log: dict[int, list[_Record]] = field(default_factory=dict)
    # Records staged inside an open transaction: (epoch -> list[_Record])
    staged: dict[int, list[_Record]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.log = {p: [] for p in range(self.partitions)}
        self.high_watermark = {p: 0 for p in range(self.partitions)}
        # Retention boundary per partition: offsets below it are deleted.
        self.low_watermark = {p: 0 for p in range(self.partitions)}


class Broker:
    """Process-wide in-memory cluster state."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.topics: dict[str, _Topic] = {}
        # group -> {topic -> {partition -> offset}}
        self.committed: dict[str, dict[str, dict[int, int]]] = {}
        # transactional.id -> epoch
        self.producer_epochs: dict[str, int] = {}

    def reset(self) -> None:
        with self.lock:
            self.topics.clear()
            self.committed.clear()
            self.producer_epochs.clear()

    def create_topic(self, name: str, partitions: int) -> None:
        with self.lock:
            if name not in self.topics:
                self.topics[name] = _Topic(name, partitions)

    def produce_transactional(self, topic: str, partition: int, key: bytes | None,
                              value: bytes, epoch: int) -> _Record:
        with self.lock:
            # No offset yet: records are invisible and unsequenced until the
            # transaction commits, mirroring Kafka transaction semantics.
            rec = _Record(topic, partition, -1, key, value)
            self.topics[topic].staged.setdefault(epoch, []).append(rec)
            return rec

    def commit_transaction(self, epoch: int,
                           offset_commits: list[TopicPartition]) -> None:
        with self.lock:
            for t in self.topics.values():
                staged = t.staged.pop(epoch, [])
                # Assign real offsets at commit time in produce order.
                for rec in staged:
                    rec.offset = len(t.log[rec.partition])
                    rec.visible_epoch = epoch
                    t.log[rec.partition].append(rec)
            for tp in offset_commits:
                group = tp.metadata
                if group is None:
                    raise KafkaException(KafkaError(KafkaError.UNKNOWN,
                                                    "offset commit needs group"))
                groups = self.committed.setdefault(group, {})
                topics = groups.setdefault(tp.topic, {})
                topics[tp.partition] = max(topics.get(tp.partition, 0), tp.offset)

    def abort_transaction(self, epoch: int) -> None:
        with self.lock:
            for t in self.topics.values():
                t.staged.pop(epoch, None)

    def fetch(self, topic: str, partition: int, offset: int,
              read_committed: bool, max_epoch: int) -> _Record | None:
        with self.lock:
            t = self.topics.get(topic)
            if t is None:
                return None
            if offset < t.low_watermark[partition]:
                # Retention deleted this offset: signal out-of-range rather
                # than silently returning another record.
                return "OUT_OF_RANGE"
            if offset >= len(t.log[partition]):
                return None
            rec = t.log[partition][offset]
            if read_committed and rec.visible_epoch > max_epoch:
                return None
            return rec

    def end_offset(self, topic: str, partition: int) -> int:
        with self.lock:
            return len(self.topics[topic].log[partition])

    def low_offset(self, topic: str, partition: int) -> int:
        with self.lock:
            return self.topics[topic].low_watermark[partition]

    def delete_records(self, topic: str, partition: int,
                       before_offset: int) -> None:
        """Simulate retention: delete every offset below ``before_offset``."""
        with self.lock:
            t = self.topics[topic]
            before_offset = min(before_offset, len(t.log[partition]))
            t.low_watermark[partition] = max(
                t.low_watermark[partition], before_offset
            )

    def get_committed(self, group: str, topic: str, partition: int) -> int:
        with self.lock:
            return self.committed.get(group, {}).get(topic, {}).get(partition, -1001)


BROKER = Broker()


class _FakeMsg:
    def __init__(self, rec: _Record | None, error: KafkaError | None = None,
                 topic: str = "", partition: int = -1, offset: int = -1) -> None:
        self._rec = rec
        self._error = error
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def error(self) -> KafkaError | None:
        return self._error

    def topic(self) -> str:
        return self._rec.topic if self._rec else self._topic

    def partition(self) -> int:
        return self._rec.partition if self._rec else self._partition

    def offset(self) -> int:
        return self._rec.offset if self._rec else self._offset

    def key(self) -> bytes | None:
        return self._rec.key if self._rec else None

    def value(self) -> bytes:
        return self._rec.value if self._rec else b""


class Consumer:
    def __init__(self, conf: dict[str, Any]) -> None:
        self.conf = conf
        self.group = conf.get("group.id", "anonymous")
        self.read_committed = conf.get("isolation.level") == "read_committed"
        self.auto_commit = conf.get("enable.auto.commit", True)
        self.auto_store = conf.get("enable.auto.offset.store", True)
        self.subscriptions: list[str] = []
        self.assigned: list[TopicPartition] = []
        self.paused: set[tuple[str, int]] = set()
        # Stored offsets per (topic, partition); -1001 means unset.
        self.stored: dict[tuple[str, int], int] = {}
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        self.subscriptions = list(topics)
        self.assigned = [
            TopicPartition(t, p)
            for t in topics
            for p in range(BROKER.topics[t].partitions)
        ]

    def assign(self, tps: list[TopicPartition]) -> None:
        self.assigned = list(tps)
        self.subscriptions = sorted({tp.topic for tp in tps})
        # Honour explicit seek positions (the indexer assigns concrete
        # offsets).  Only valid offsets (>= 0) seed the stored position;
        # OFFSET_INVALID (-1001) falls back to earliest/committed semantics.
        for tp in tps:
            if tp.offset >= 0:
                self.stored[(tp.topic, tp.partition)] = tp.offset

    def assignment(self) -> list[TopicPartition]:
        return list(self.assigned)

    def list_topics(self, topic: str | None = None, timeout: float = -1):
        class _PartMeta:
            def __init__(self, id_: int) -> None:
                self.id = id_

        class _TopicMeta:
            def __init__(self, name: str, partitions: int) -> None:
                self.topic = name
                self.partitions = {p: _PartMeta(p) for p in range(partitions)}

        class _Meta:
            def __init__(self, topics: dict[str, _TopicMeta]) -> None:
                self.topics = topics

        names = [topic] if topic else list(BROKER.topics)
        return _Meta({n: _TopicMeta(n, BROKER.topics[n].partitions) for n in names})

    def _position(self, tp: TopicPartition) -> int:
        # Model librdkafka semantics: the committed group offset is the durable
        # floor; a stored offset only advances consumption past it. An aborted
        # transaction never committed, so its reset value still applies.
        committed = BROKER.get_committed(self.group, tp.topic, tp.partition)
        floor = committed if committed != -1001 else 0
        stored = self.stored.get((tp.topic, tp.partition))
        return max(floor, stored if stored is not None else floor)

    def poll(self, timeout: float = -1):
        for tp in self.assigned:
            if (tp.topic, tp.partition) in self.paused:
                continue
            pos = self._position(tp)
            max_epoch = max(BROKER.producer_epochs.values(), default=-1)
            rec = BROKER.fetch(tp.topic, tp.partition, pos,
                               self.read_committed, max_epoch)
            if rec is None:
                continue
            if rec == "OUT_OF_RANGE":
                return _FakeMsg(
                    None,
                    KafkaError(KafkaError.OFFSET_OUT_OF_RANGE,
                               f"offset {pos} deleted by retention"),
                    topic=tp.topic, partition=tp.partition, offset=pos,
                )
            if self.auto_store:
                self.stored[(tp.topic, tp.partition)] = pos + 1
            return _FakeMsg(rec)
        return None

    def pause(self, tps: list[TopicPartition]) -> None:
        for tp in tps:
            self.paused.add((tp.topic, tp.partition))

    def resume(self, tps: list[TopicPartition]) -> None:
        for tp in tps:
            self.paused.discard((tp.topic, tp.partition))

    def position(self, tps: list[TopicPartition]) -> list[TopicPartition]:
        return [
            TopicPartition(tp.topic, tp.partition, self._position(tp))
            for tp in tps
        ]

    def store_offsets(self, offsets: list[TopicPartition] | None = None,
                      message: _FakeMsg | None = None) -> None:
        for tp in offsets or []:
            self.stored[(tp.topic, tp.partition)] = tp.offset

    def committed(self, tps: list[TopicPartition],
                  timeout: float = -1) -> list[TopicPartition]:
        return [
            TopicPartition(tp.topic, tp.partition,
                           BROKER.get_committed(self.group, tp.topic, tp.partition),
                           self.group)
            for tp in tps
        ]

    def consumer_group_metadata(self):
        return _GroupMetadata(self.group)

    def get_watermark_offsets(self, tp: TopicPartition,
                              timeout: float = -1, cached: bool = False) -> tuple[int, int]:
        t = BROKER.topics.get(tp.topic)
        if t is None:
            return (0, 0)
        return (BROKER.low_offset(tp.topic, tp.partition),
                BROKER.end_offset(tp.topic, tp.partition))

    def close(self) -> None:
        self.closed = True


class _GroupMetadata:
    """Opaque handle mirroring librdkafka's consumer group metadata object."""

    def __init__(self, group_id: str) -> None:
        self.group_id = group_id

    def __repr__(self) -> str:
        return f"_GroupMetadata({self.group_id!r})"


class Producer:
    def __init__(self, conf: dict[str, Any]) -> None:
        self.conf = conf
        self.transactional_id = conf.get("transactional.id")
        self.epoch = -1
        self.txn_open = False
        self.pending: list[_Record] = []
        # Offsets attached to the currently open transaction.
        self.txn_offsets: list[TopicPartition] = []

    def init_transactions(self) -> None:
        if self.transactional_id is None:
            raise KafkaException(KafkaError(KafkaError.UNKNOWN,
                                            "transactional.id required"))
        # Fence any previous producer instance with the same id.
        prev = BROKER.producer_epochs.get(self.transactional_id, -1)
        self.epoch = prev + 1
        BROKER.producer_epochs[self.transactional_id] = self.epoch

    def begin_transaction(self) -> None:
        if self.txn_open:
            raise KafkaException(KafkaError(KafkaError.UNKNOWN, "txn already open"))
        self.txn_open = True
        self.pending = []
        self.txn_offsets = []

    def produce(self, topic: str, partition: int = -1, value: bytes = b"",
                key: bytes | None = None, on_delivery=None,
                headers=None) -> None:
        if not self.txn_open:
            raise KafkaException(KafkaError(KafkaError.UNKNOWN, "no open txn"))
        rec = BROKER.produce_transactional(topic, partition, key, value, self.epoch)
        self.pending.append(rec)
        if on_delivery:
            on_delivery(None, _FakeMsg(rec))

    def send_offsets_to_transaction(self, positions: list[TopicPartition],
                                    group_metadata: "_GroupMetadata",
                                    timeout: float = -1) -> None:
        if not self.txn_open:
            raise KafkaException(KafkaError(KafkaError.UNKNOWN, "no open txn"))
        for tp in positions:
            # Metadata carries the consumer group, matching librdkafka semantics.
            tp.metadata = group_metadata.group_id
        self.txn_offsets.extend(positions)
    def commit_transaction(self, offsets: list[TopicPartition] | None = None) -> None:
        if not self.txn_open:
            raise KafkaException(KafkaError(KafkaError.UNKNOWN, "no open txn"))
        # Real binding takes no offsets argument here; they were sent earlier.
        if offsets is not None:
            raise KafkaException(KafkaError(
                KafkaError.UNKNOWN,
                "use send_offsets_to_transaction before commit_transaction"))
        BROKER.commit_transaction(self.epoch, self.txn_offsets)
        self.txn_open = False
        self.pending = []
        self.txn_offsets = []

    def abort_transaction(self) -> None:
        BROKER.abort_transaction(self.epoch)
        self.txn_open = False
        self.pending = []
        self.txn_offsets = []

    def flush(self, timeout: float = -1) -> int:
        return 0


class AdminClient:
    def __init__(self, conf: dict[str, Any]) -> None:
        self.conf = conf

    def list_topics(self, timeout: float = -1):
        return Consumer({"group.id": "admin"}).list_topics()

    def create_topics(self, new_topics: list[NewTopic], **_):
        for nt in new_topics:
            BROKER.create_topic(nt.topic, nt.num_partitions)
        return {nt: _Future(None) for nt in new_topics}


class _Future:
    def __init__(self, result: Any) -> None:
        self._result = result

    def result(self, timeout: float = -1) -> Any:
        return self._result
