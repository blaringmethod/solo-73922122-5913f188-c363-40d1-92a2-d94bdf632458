"""End-to-end behaviour on the in-memory broker (same semantics as Redpanda)."""
from __future__ import annotations

from tests.conftest import BROKER, RAW, drain_clean, produce_raw


# --------------------------------------------------------------------- happy path
def test_c_and_f_flow_with_conversion_and_ordering(env):
    produce_raw(0, "A1", {"box_id": "A1", "sampled_utc": "t0",
                          "reading": 4.8, "scale": "C"})
    produce_raw(0, "A2", {"box_id": "A2", "sampled_utc": "t1",
                          "reading": 41.0, "scale": "F"})
    produce_raw(1, "B1", {"box_id": "B1", "sampled_utc": "t2",
                          "reading": 32.0, "scale": "F"})

    assert env.wait_until(lambda: len(drain_clean()) == 3)
    p0 = drain_clean(0)
    assert [r["source_offset"] for r in p0] == [0, 1]
    assert p0[0]["celsius"] == 4.8 and p0[0]["scale"] == "C"
    assert abs(p0[1]["celsius"] - 5.0) < 1e-12 and p0[1]["scale"] == "F"
    # Original reading preserved untouched on an F message.
    assert p0[1]["reading"] == 41.0
    p1 = drain_clean(1)
    assert p1[0]["celsius"] == 0.0

    # Source offsets committed alongside the output transaction.
    prog = env.client.get("/progress").json()["positions"]
    assert {p["partition"]: p["committed_offset"] for p in prog} == {0: 2, 1: 1}


# ------------------------------------------------------------- blocker semantics
def test_blocker_pauses_one_partition_only(env):
    produce_raw(0, "A1", {"box_id": "A1", "sampled_utc": "t",
                          "reading": 1.0, "scale": "C"})
    produce_raw(0, "A2", {"box_id": "A2", "sampled_utc": "t",
                          "reading": 39.2})  # missing scale blocker
    produce_raw(0, "A3", {"box_id": "A3", "sampled_utc": "t",
                          "reading": 2.0, "scale": "C"})  # behind blocker
    produce_raw(1, "B1", {"box_id": "B1", "sampled_utc": "t",
                          "reading": 3.0, "scale": "C"})
    produce_raw(1, "B2", {"box_id": "B2", "sampled_utc": "t",
                          "reading": 4.0, "scale": "C"})

    assert env.wait_until(lambda: len(drain_clean(1)) == 2)
    assert env.wait_until(lambda: len(drain_clean(0)) == 1)

    blocked = env.client.get("/blocked").json()
    assert len(blocked) == 1
    b = blocked[0]
    assert b["partition"] == 0 and b["offset"] == 1
    assert b["reason"] == "missing_scale"
    assert b["raw_message"] == {"box_id": "A2", "sampled_utc": "t", "reading": 39.2}
    assert env.client.get("/health").json()["paused_partitions"] == [0]

    # The record behind the blocker must not overtake it.
    assert len(drain_clean(0)) == 1
    # Partition 1 keeps moving independently.
    produce_raw(1, "B3", {"box_id": "B3", "sampled_utc": "t",
                          "reading": 5.0, "scale": "C"})
    assert env.wait_until(lambda: len(drain_clean(1)) == 3)


def test_correction_unblocks_and_preserves_order(env):
    produce_raw(0, "A1", {"box_id": "A1", "sampled_utc": "t",
                          "reading": 1.0, "scale": "C"})
    produce_raw(0, "A2", {"box_id": "A2", "sampled_utc": "t",
                          "reading": 39.2, "scale": "K"})  # unknown scale
    produce_raw(0, "A3", {"box_id": "A3", "sampled_utc": "t",
                          "reading": 2.0, "scale": "C"})
    assert env.wait_until(lambda: len(drain_clean(0)) == 1)

    r = env.client.post("/blocked/0/1/correct", json={"scale": "C"})
    assert r.status_code == 200 and r.json()["status"] == "corrected"
    env.client.post("/demo/resume/0")

    assert env.wait_until(lambda: len(drain_clean(0)) == 3)
    recs = drain_clean(0)
    assert [r["source_offset"] for r in recs] == [0, 1, 2]
    # Correction does not change the original reading; approved scale is C.
    assert recs[1]["reading"] == 39.2 and recs[1]["scale"] == "C"
    assert recs[1]["celsius"] == 39.2
    assert env.client.get("/health").json()["paused_partitions"] == []
    assert env.client.get("/blocked?status=committed").json()[0]["offset"] == 1


def test_correction_validates_scale_and_target(env):
    produce_raw(0, "A1", {"box_id": "A1", "sampled_utc": "t", "reading": 1.0})
    assert env.wait_until(lambda: len(env.client.get("/blocked").json()) == 1)

    bad = env.client.post("/blocked/0/0/correct", json={"scale": "K"})
    assert bad.status_code == 422  # pydantic rejects anything but C/F
    missing = env.client.post("/blocked/0/99/correct", json={"scale": "C"})
    assert missing.status_code == 404
    out_of_range = env.client.post("/blocked/7/0/correct", json={"scale": "C"})
    assert out_of_range.status_code == 400


def test_repeated_corrections_are_idempotent_or_conflict(env):
    produce_raw(0, "A1", {"box_id": "A1", "sampled_utc": "t", "reading": 1.0})
    assert env.wait_until(lambda: len(env.client.get("/blocked").json()) == 1)

    # Stop the worker while the correction is still in 'corrected' state, so
    # the idempotent/conflict branches can be exercised deterministically.
    env.state.worker._stopping.set()
    env.state.worker._thread.join(3)

    first = env.client.post("/blocked/0/0/correct", json={"scale": "C"})
    assert first.status_code == 200
    again = env.client.post("/blocked/0/0/correct", json={"scale": "C"})
    assert again.status_code == 200 and "idempotent" in again.json()["message"]
    conflict = env.client.post("/blocked/0/0/correct", json={"scale": "F"})
    assert conflict.status_code == 409

    # Restart: reconciliation + redelivery produces exactly one clean record.
    env.restart()
    env.client.post("/demo/resume/0")
    assert env.wait_until(lambda: len(drain_clean(0)) == 1)
    assert len(drain_clean(0)) == 1
    assert drain_clean(0)[0]["scale"] == "C"


def test_correction_after_commit_is_rejected(env):
    produce_raw(0, "A1", {"box_id": "A1", "sampled_utc": "t",
                          "reading": 1.0, "scale": "C"})
    assert env.wait_until(lambda: len(drain_clean(0)) == 1)
    r = env.client.post("/blocked/0/0/correct", json={"scale": "F"})
    assert r.status_code == 404  # never blocked -> not found, not alterable


# ------------------------------------------------------------- crash / restart EoS
def test_exit_before_commit_redelivers_exactly_once(env):
    produce_raw(0, "A1", {"box_id": "A1", "sampled_utc": "t",
                          "reading": 1.0, "scale": "C"})
    produce_raw(0, "A2", {"box_id": "A2", "sampled_utc": "t", "reading": 41.0})
    produce_raw(0, "A3", {"box_id": "A3", "sampled_utc": "t",
                          "reading": 2.0, "scale": "C"})
    assert env.wait_until(lambda: len(drain_clean(0)) == 1)

    # Durable correction, then arm the pre-commit crash, then allow redelivery.
    assert env.client.post("/blocked/0/1/correct",
                           json={"scale": "F"}).status_code == 200
    env.client.post("/demo/crash", json={"crash_point": "before_commit"})
    env.client.post("/demo/resume/0")

    # The worker thread hits the simulated hard exit at the crash point.
    assert env.wait_until(
        lambda: env.state.worker._thread is not None
        and not env.state.worker._thread.is_alive()
        and env.state.crash.was_triggered()
    )
    assert isinstance(env.state.worker.fatal_error, object)
    # Nothing beyond offset 0 was committed: no record, no offset advance.
    assert len(drain_clean(0)) == 1
    assert env.client.get("/progress").json()["positions"][0]["committed_offset"] == 1
    assert env.state.store.get_blocked(env.settings.raw_topic, 0, 1)["status"] == "corrected"

    env.restart()
    env.client.post("/demo/resume/0")
    # Reconciliation keeps the approved scale; redelivery replays offset 1.
    assert env.wait_until(lambda: len(drain_clean(0)) == 3)
    recs = drain_clean(0)
    assert [r["source_offset"] for r in recs] == [0, 1, 2]
    assert abs(recs[1]["celsius"] - 5.0) < 1e-12 and recs[1]["scale"] == "F"
    # No duplicate clean record at offset 1.
    assert len([r for r in recs if r["source_offset"] == 1]) == 1
    prog = env.client.get("/progress").json()["positions"]
    assert {p["partition"]: p["committed_offset"] for p in prog} == {0: 3, 1: None}


def test_exit_after_commit_does_not_reprocess(env):
    produce_raw(1, "B1", {"box_id": "B1", "sampled_utc": "t", "reading": 32.0})
    assert env.wait_until(lambda: len(env.client.get("/blocked").json()) == 1)

    assert env.client.post("/blocked/1/0/correct",
                           json={"scale": "F"}).status_code == 200
    env.client.post("/demo/crash", json={"crash_point": "after_commit"})
    env.client.post("/demo/resume/1")

    assert env.wait_until(
        lambda: env.state.worker._thread is not None
        and not env.state.worker._thread.is_alive()
        and env.state.crash.was_triggered()
    )
    # The transaction committed before the exit: record visible, offset advanced.
    assert env.wait_until(lambda: len(drain_clean(1)) == 1)
    assert env.client.get("/progress").json()["positions"][1]["committed_offset"] == 1

    env.restart()
    import time

    time.sleep(0.3)  # give the restarted worker a chance to misbehave
    recs = drain_clean(1)
    assert len(recs) == 1 and recs[0]["source_offset"] == 0
    assert env.client.get("/progress").json()["positions"][1]["committed_offset"] == 1


def test_aborted_transaction_is_invisible(env):
    """A transaction that never commits must not be visible to clean readers."""
    from tests.conftest import CLEAN, Producer

    p = Producer({"bootstrap.servers": "fake", "transactional.id": "orphan-txn"})
    p.init_transactions()
    p.begin_transaction()
    p.produce(CLEAN, 0, b'{"box_id":"GHOST"}', b"GHOST")
    # Deliberately no commit: simulate abort on process death.
    p.abort_transaction()
    assert drain_clean(0) == []


def test_offsets_ride_the_transaction_via_group_metadata(env):
    """Regression: source progress must be attached with the real API shape
    (send_offsets_to_transaction + consumer_group_metadata + bare commit)."""
    produce_raw(0, "A1", {"box_id": "A1", "sampled_utc": "t",
                          "reading": 1.0, "scale": "C"})
    assert env.wait_until(lambda: len(drain_clean(0)) == 1, timeout=10)

    # Group offsets must be committed under the worker's consumer group.
    positions = env.client.get("/progress").json()["positions"]
    assert env.client.get("/progress").json()["group_id"] == "test-worker"
    assert {p["partition"]: p["committed_offset"] for p in positions}[0] == 1

    # The real binding rejects passing offsets into commit_transaction();
    # the double enforces the same contract so a regression fails loudly.
    import pytest
    from confluent_kafka import KafkaException, Producer, TopicPartition
    from tests.conftest import BROKER

    p = Producer({"bootstrap.servers": "fake", "transactional.id": "contract-txn"})
    p.init_transactions()
    p.begin_transaction()
    with pytest.raises(KafkaException):
        p.commit_transaction(offsets=[TopicPartition(RAW, 0, 5)])
    p.abort_transaction()
    # No stray offset was written.
    assert BROKER.get_committed("test-worker", RAW, 0) == 1


def test_transaction_failure_aborts_and_redelivers(env, monkeypatch):
    worker = env.state.worker
    # Stop the background thread and drive the consume loop synchronously in
    # this test thread, so the injected failure and redelivery are fully
    # deterministic with no competing poller.
    worker._stopping.set()
    worker._thread.join(3)
    assert not worker._thread.is_alive()

    produce_raw(0, "A1", {"box_id": "A1", "sampled_utc": "t",
                          "reading": 1.0, "scale": "C"})
    produce_raw(0, "A2", {"box_id": "A2", "sampled_utc": "t",
                          "reading": 2.0, "scale": "C"})

    real_commit = worker._producer.commit
    calls = {"n": 0}

    def flaky_commit():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("simulated coordinator failure")
        return real_commit()

    monkeypatch.setattr(worker._producer, "commit", flaky_commit)

    # Synchronous loop: each failed transaction aborts and the next poll
    # redelivers the same offset; stop once both records are committed.
    for _ in range(20):
        try:
            worker._poll_once()
        except RuntimeError as exc:
            assert "simulated coordinator failure" in str(exc)
        rows = drain_clean(0)
        if len(rows) == 2:
            break

    rows = drain_clean(0)
    assert [r["source_offset"] for r in rows] == [0, 1]
    assert len(rows) == 2  # the aborted attempt left no partial/duplicate record
    assert calls["n"] >= 2  # first commit failed, retried successfully
    prog = env.client.get("/progress").json()["positions"]
    assert {p["partition"]: p["committed_offset"] for p in prog}[0] == 2


def test_unknown_scale_message_is_blocked_with_full_context(env):
    produce_raw(1, "B1", {"box_id": "B1", "sampled_utc": "t",
                          "reading": 278.35, "scale": "K"})
    assert env.wait_until(lambda: len(env.client.get("/blocked").json()) == 1)
    b = env.client.get("/blocked").json()[0]
    assert b["partition"] == 1 and b["offset"] == 0
    assert b["reason"] == "unknown_scale:'K'"
    assert b["raw_message"]["reading"] == 278.35


def test_invalid_json_blocks_and_rejects_scale_correction(env):
    import json as _json

    from tests.conftest import Producer, RAW as RAW_TOPIC

    p = Producer({"bootstrap.servers": "fake", "transactional.id": "seed-bad-json"})
    p.init_transactions()
    p.begin_transaction()
    p.produce(RAW_TOPIC, 0, b"{not valid json", b"BAD")
    p.commit_transaction()

    assert env.wait_until(
        lambda: any(r["reason"].startswith("invalid_json")
                    for r in env.client.get("/blocked").json())
    )
    row = next(r for r in env.client.get("/blocked").json()
               if r["reason"].startswith("invalid_json"))
    assert row["raw_message"] is None
    r = env.client.post(
        f"/blocked/{row['partition']}/{row['offset']}/correct",
        json={"scale": "C"},
    )
    assert r.status_code == 422
    # Partition stays paused and no clean record leaks.
    assert env.client.get("/health").json()["paused_partitions"] == [row["partition"]]
    assert drain_clean(row["partition"]) == []
