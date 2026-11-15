"""Runtime configuration loaded from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
    raw_topic: str = os.environ.get("RAW_TOPIC", "raw-readings")
    clean_topic: str = os.environ.get("CLEAN_TOPIC", "clean-readings")
    group_id: str = os.environ.get("GROUP_ID", "coldchain-worker")
    # A fixed, stable transactional id gives the single worker zombie fencing.
    transactional_id: str = os.environ.get("TRANSACTIONAL_ID", "coldchain-txn-1")
    topic_partitions: int = _int("TOPIC_PARTITIONS", 2)
    poll_timeout: float = float(os.environ.get("POLL_TIMEOUT", "1.0"))
    # Retry budget for transient transaction/coordinator errors. Offsets never
    # move on a failed attempt, so retries simply reprocess the same message.
    max_retries: int = _int("MAX_RETRIES", 10)
    retry_backoff_base: float = float(os.environ.get("RETRY_BACKOFF_BASE", "0.2"))
    retry_backoff_max: float = float(os.environ.get("RETRY_BACKOFF_MAX", "5.0"))
    sqlite_path: str = os.environ.get("SQLITE_PATH", "/data/coldchain.db")
    # Manual incremental indexing: at most this many committed records are
    # copied out of each clean partition per POST /index/run trigger.
    index_batch_size: int = _int("INDEX_BATCH_SIZE", 50)
    index_poll_timeout: float = float(os.environ.get("INDEX_POLL_TIMEOUT", "0.5"))
    # When False the correction stays durable but the partition only resumes
    # via POST /demo/resume/{partition}; used for deterministic crash demos.
    auto_resume: bool = os.environ.get("AUTO_RESUME", "1") not in ("0", "false", "False")
    # If set, the process exits (os._exit) when this crash point is reached.
    crash_on: str = os.environ.get("CRASH_ON", "")


settings = Settings()
