"""Shared pytest fixtures: wire the app against the in-memory fake broker."""
from __future__ import annotations

import os
import sys

import pytest

# Make the in-memory confluent_kafka double win over any real installation.
FAKE_ROOT = os.path.join(os.path.dirname(__file__), "fake_kafka")
sys.path.insert(0, FAKE_ROOT)
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from confluent_kafka import BROKER, Consumer, Producer, TopicPartition  # noqa: E402

from app.config import Settings  # noqa: E402
from app.main import AppState, build_app  # noqa: E402
from app.messaging import ensure_topics  # noqa: E402

RAW = "raw-readings"
CLEAN = "clean-readings"


def make_settings(tmp_path) -> Settings:
    return Settings(
        kafka_bootstrap="fake://broker",
        raw_topic=RAW,
        clean_topic=CLEAN,
        group_id="test-worker",
        transactional_id="test-txn-1",
        topic_partitions=2,
        poll_timeout=0.05,
        sqlite_path=str(tmp_path / "state.db"),
        auto_resume=False,
        max_retries=10,
        retry_backoff_base=0.01,
        retry_backoff_max=0.05,
    )


class _RecordedExit(Exception):
    """Raised by the injected exit stub to unwind the worker thread only."""


def make_state(settings) -> AppState:
    state = AppState(settings)
    # Replace the hard process exit with an exception that kills the worker
    # thread while leaving the pytest process alive; SQLite and the fake broker
    # retain their state exactly as a real process image would retain disk.
    def _thread_exit(code: int) -> None:
        raise _RecordedExit(f"simulated exit code {code}")

    state.crash._exit_fn = _thread_exit
    state.crash.triggered = False
    return state


def produce_raw(partition: int, key: str, payload: dict) -> None:
    import json

    p = Producer({"bootstrap.servers": "fake", "transactional.id": f"seed-{key}"})
    # Seed producer is non-transactional in real life; the double still needs a
    # txn, so use a short one per message with a unique epoch.
    p.init_transactions()
    p.begin_transaction()
    p.produce(RAW, partition, json.dumps(payload).encode(), key.encode())
    p.commit_transaction()


def drain_clean(partition: int | None = None) -> list[dict]:
    import json

    c = Consumer({
        "group.id": f"reader-{partition}-{os.urandom(4).hex()}",
        "enable.auto.commit": False,
        "isolation.level": "read_committed",
    })
    parts = [partition] if partition is not None else [0, 1]
    c.assign([TopicPartition(CLEAN, p, 0) for p in parts])
    out: list[dict] = []
    for _ in range(100):
        msg = c.poll(0.01)
        if msg is None:
            break
        if msg.error() is not None:
            break
        out.append(json.loads(msg.value()))
    c.close()
    return out


@pytest.fixture
def env(tmp_path):
    BROKER.reset()
    ensure_topics("fake", [RAW, CLEAN], 2)
    settings = make_settings(tmp_path)
    state = make_state(settings)
    app = build_app(state)
    from fastapi.testclient import TestClient

    client = TestClient(app)  # lifespan starts the worker thread
    with client:
        env = _Env(client, state, settings)
        yield env
    # Deterministic teardown: stop every worker (including restarted ones)
    # before the next test resets the shared in-memory broker, so a lagging
    # thread can never consume messages belonging to another test.
    env.shutdown_all()
    state.worker.stop(timeout=5)
    state.store.close()
    _assert_no_worker_threads()


def _assert_no_worker_threads() -> None:
    import threading

    leaked = [t.name for t in threading.enumerate() if t.name == "coldchain-worker"]
    assert not leaked, f"worker threads leaked across tests: {leaked}"


class _Env:
    def __init__(self, client, state, settings) -> None:
        self.client = client
        self.state = state
        self.settings = settings
        # Every TestClient created (including restarts) must be exited so its
        # lifespan shutdown stops the worker thread; otherwise worker threads
        # leak across tests and race on the shared in-memory broker.
        self._clients: list = [client]

    def wait_until(self, predicate, timeout: float = 10.0) -> bool:
        import time

        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return True
            time.sleep(0.02)
        return False

    def restart(self):
        """Simulate a process restart with a brand-new worker/transactional id."""
        old = self.state.worker
        if old._thread is not None:
            old._stopping.set()
            old._thread.join(5)
            assert not old._thread.is_alive(), "old worker thread did not stop"
        self.state.store.close()
        # Close the previous TestClient so its lifespan shutdown runs.
        try:
            self._clients[-1].__exit__(None, None, None)
        except Exception:  # noqa: BLE001
            pass
        fresh = make_state(self.settings)
        app = build_app(fresh)
        from fastapi.testclient import TestClient

        client = TestClient(app)
        client.__enter__()
        self._clients.append(client)
        self.client = client
        self.state = fresh
        return self

    def shutdown_all(self) -> None:
        for client in reversed(self._clients):
            try:
                client.__exit__(None, None, None)
            except Exception:  # noqa: BLE001
                pass
        self._clients.clear()
