"""Tests for the manual incremental index and the box/UTC query API.

These exercise the same guarantees the operator-facing feature promises:

* bounded, manual incremental runs (max 50 committed records per partition),
* no loss or double-record across interruption, restart and re-trigger,
* honest ``incomplete`` answers while the index lags,
* retention gaps are reported and never crossed,
* read failures and unparseable timestamps are locatable,
* queries fix the two-partition readable boundary at request start.
"""
from __future__ import annotations

import json

import pytest

from confluent_kafka import BROKER, KafkaError

from tests.conftest import CLEAN, clean_record, produce_clean


def _run(client):
    r = client.post("/index/run")
    assert r.status_code == 200, r.text
    return r.json()


def _query(client, box="BOX-1", start="2026-01-01T00:00:00Z",
           end="2027-01-01T00:00:00Z", **extra):
    params = {"box_id": box, "start_utc": start, "end_utc": end, **extra}
    return client.get("/readings/query", params=params)


# --------------------------------------------------------------------- happy path
def test_index_run_copies_committed_records_with_locations(env):
    produce_clean(0, clean_record("BOX-1", "2026-11-13T08:00:00Z", 4.8, 0, 0))
    produce_clean(0, clean_record("BOX-2", "2026-11-13T08:05:00Z", 5.0, 0, 1,
                                  reading=41.0, scale="F"))
    produce_clean(1, clean_record("BOX-1", "2026-11-13T08:10:00Z", 6.2, 1, 0))

    report = _run(env.client)
    assert report["indexed_total"] == 3
    assert report["complete"] is True
    by_p = {p["partition"]: p for p in report["partitions"]}
    assert by_p[0]["indexed"] == 2 and by_p[0]["next_offset"] == 2
    assert by_p[1]["indexed"] == 1 and by_p[1]["next_offset"] == 1

    body = _query(env.client).json()
    assert body["status"] == "complete"
    assert [r["source_offset"] for r in body["readings"]] == [0, 0]
    # Ordering: sampled time, then source location.
    assert body["readings"][0]["sampled_utc"] == "2026-11-13T08:00:00Z"
    # Original scale correction retained alongside Celsius (BOX-2 is F).
    f_row = _query(env.client, box="BOX-2").json()["readings"][0]
    assert f_row["reading"] == 41.0 and f_row["celsius"] == 5.0
    # Both clean-topic and source locations are present.
    assert f_row["clean_partition"] == 0 and f_row["clean_offset"] == 1
    assert f_row["source_topic"] == "raw-readings"


def test_query_filters_by_box_and_utc_window_inclusive(env):
    produce_clean(0, clean_record("BOX-A", "2026-11-13T08:00:00Z", 1.0, 0, 0))
    produce_clean(0, clean_record("BOX-A", "2026-11-13T09:00:00Z", 2.0, 0, 1))
    produce_clean(0, clean_record("BOX-A", "2026-11-13T10:00:00Z", 3.0, 0, 2))
    produce_clean(1, clean_record("BOX-B", "2026-11-13T09:30:00Z", 9.0, 1, 0))
    _run(env.client)

    body = _query(env.client, box="BOX-A",
                  start="2026-11-13T09:00:00Z",
                  end="2026-11-13T10:00:00Z").json()
    assert body["status"] == "complete"
    assert [r["celsius"] for r in body["readings"]] == [2.0, 3.0]  # inclusive

    other = _query(env.client, box="BOX-B").json()
    assert [r["celsius"] for r in other["readings"]] == [9.0]

    none = _query(env.client, box="BOX-A",
                  start="2026-11-14T00:00:00Z",
                  end="2026-11-15T00:00:00Z").json()
    assert none["status"] == "complete" and none["count"] == 0


def test_query_accepts_offset_timezones_and_naive_is_422(env):
    produce_clean(0, clean_record("BOX-A", "2026-11-13T08:00:00Z", 1.0, 0, 0))
    _run(env.client)

    ok = _query(env.client, box="BOX-A",
                start="2026-11-13T09:00:00+01:00",  # 08:00Z
                end="2026-11-13T08:00:00+00:00").json()
    assert ok["count"] == 1

    bad = env.client.get("/readings/query", params={
        "box_id": "BOX-A", "start_utc": "2026-11-13T08:00:00",
        "end_utc": "2026-11-13T09:00:00Z"})
    assert bad.status_code == 422

    order = env.client.get("/readings/query", params={
        "box_id": "BOX-A", "start_utc": "2026-11-13T09:00:00Z",
        "end_utc": "2026-11-13T08:00:00Z"})
    assert order.status_code == 422


# ------------------------------------------------------------- incremental batching
def test_batch_caps_at_50_per_partition_and_later_runs_catch_up(env):
    for i in range(60):
        produce_clean(0, clean_record(
            "BOX-BATCH", f"2026-11-13T08:{i:02d}:00Z", float(i), 0, i))
    for i in range(30):
        produce_clean(1, clean_record(
            "BOX-BATCH", f"2026-11-13T09:{i:02d}:00Z", float(i), 1, i))

    first = _run(env.client)
    p0 = next(p for p in first["partitions"] if p["partition"] == 0)
    p1 = next(p for p in first["partitions"] if p["partition"] == 1)
    assert p0["indexed"] == 50 and p0["next_offset"] == 50
    assert p0["complete"] is False  # 10 committed records remain
    assert p1["indexed"] == 30 and p1["complete"] is True
    assert first["complete"] is False
    assert first["indexed_total"] == 80

    # A query while lagging must not pretend to be complete.
    laggy = _query(env.client, box="BOX-BATCH").json()
    assert laggy["status"] == "incomplete"
    lag = next(p for p in laggy["index_progress"] if p["partition"] == 0)
    assert lag["indexed_next_offset"] == 50
    assert lag["high_watermark_at_query_start"] == 60

    second = _run(env.client)
    p0b = next(p for p in second["partitions"] if p["partition"] == 0)
    assert p0b["indexed"] == 10 and p0b["next_offset"] == 60
    assert p0b["complete"] is True
    assert second["complete"] is True

    caught_up = _query(env.client, box="BOX-BATCH").json()
    assert caught_up["status"] == "complete"
    assert caught_up["count"] == 90


def test_repeated_trigger_is_idempotent_with_no_duplicates(env):
    for i in range(5):
        produce_clean(0, clean_record("BOX-DUP", "2026-11-13T08:00:00Z",
                                      float(i), 0, i))
    _run(env.client)
    again = _run(env.client)
    assert again["indexed_total"] == 0
    assert again["partitions"][0]["next_offset"] == 5

    progress = env.client.get("/index/progress").json()
    assert progress["indexed_total"] == 5
    body = _query(env.client, box="BOX-DUP").json()
    assert body["count"] == 5
    offsets = [r["clean_offset"] for r in body["readings"]]
    assert offsets == list(range(5)) and len(set(offsets)) == 5


def test_new_records_committed_after_index_are_picked_up_incrementally(env):
    produce_clean(0, clean_record("BOX-INC", "2026-11-13T08:00:00Z", 1.0, 0, 0))
    _run(env.client)
    assert env.client.get("/index/progress").json()["indexed_total"] == 1

    produce_clean(0, clean_record("BOX-INC", "2026-11-13T08:01:00Z", 2.0, 0, 1))
    produce_clean(1, clean_record("BOX-INC", "2026-11-13T08:02:00Z", 3.0, 1, 0))
    report = _run(env.client)
    assert report["indexed_total"] == 2
    body = _query(env.client, box="BOX-INC").json()
    assert body["status"] == "complete" and body["count"] == 3


# ------------------------------------------------------------- restart durability
def test_cursor_survives_restart_and_indexing_resumes_without_overlap(env):
    for i in range(10):
        produce_clean(0, clean_record("BOX-R", "2026-11-13T08:00:00Z",
                                      float(i), 0, i))
    first = _run(env.client)
    assert first["indexed_total"] == 10

    # Simulate process restart: brand-new AppState over the same SQLite file
    # and the same (in-memory, standing in for durable) broker.
    env.restart()
    for i in range(10, 15):
        produce_clean(0, clean_record("BOX-R", "2026-11-13T08:00:00Z",
                                      float(i), 0, i))
    report = _run(env.client)
    p0 = next(p for p in report["partitions"] if p["partition"] == 0)
    assert p0["start_offset"] == 10 and p0["indexed"] == 5
    assert report["indexed_total"] == 5  # only the new five

    body = _query(env.client, box="BOX-R").json()
    assert body["status"] == "complete" and body["count"] == 15
    assert len({r["clean_offset"] for r in body["readings"]}) == 15


def test_interrupted_run_resumes_without_loss_or_double(env, monkeypatch):
    """Crash after the 3rd row is durably committed: cursor == 3, so the
    restarted run re-reads only offsets 3+ and nothing is duplicated."""
    for i in range(6):
        produce_clean(0, clean_record("BOX-CRASH", "2026-11-13T08:00:00Z",
                                      float(i), 0, i))

    real_index_record = env.state.index_store.index_record
    calls = {"n": 0}

    def crash_after_three(*args, **kwargs):
        real_index_record(*args, **kwargs)
        calls["n"] += 1
        if calls["n"] == 3:
            raise SystemExit("simulated mid-run interruption")

    monkeypatch.setattr(env.state.index_store, "index_record", crash_after_three)
    with pytest.raises(SystemExit):
        env.state.indexer.run_once()

    # Durable state at interruption: exactly three rows, cursor at 3.
    assert env.state.index_store.count_readings(CLEAN) == 3
    cursor = env.state.index_store.get_cursor(CLEAN, 0)
    assert cursor["next_offset"] == 3

    # Restart (fresh state, same SQLite file) and finish the pass.
    env.restart()
    report = _run(env.client)
    p0 = next(p for p in report["partitions"] if p["partition"] == 0)
    assert p0["indexed"] == 3 and p0["next_offset"] == 6
    body = _query(env.client, box="BOX-CRASH").json()
    assert body["status"] == "complete" and body["count"] == 6
    assert sorted(r["clean_offset"] for r in body["readings"]) == list(range(6))


# ------------------------------------------------------------- read failures
def test_read_failure_keeps_cursor_and_makes_query_incomplete(env, monkeypatch):
    for i in range(4):
        produce_clean(0, clean_record("BOX-FAIL", "2026-11-13T08:00:00Z",
                                      float(i), 0, i))

    real_factory = env.state.indexer._consumer_factory

    class FailingConsumer:
        def __init__(self):
            self._inner = real_factory()
            self._polls = 0

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def poll(self, timeout=-1):
            self._polls += 1
            if self._polls == 1:
                class _Err:
                    code = KafkaError.UNKNOWN

                    def __str__(self):
                        return "simulated broker read failure"

                from confluent_kafka import _FakeMsg
                return _FakeMsg(None, _Err(), topic=CLEAN, partition=0, offset=0)
            return self._inner.poll(timeout)

    monkeypatch.setattr(env.state.indexer, "_consumer_factory",
                        lambda: FailingConsumer())

    report = env.state.indexer.run_once()
    p0 = next(p for p in report["partitions"] if p["partition"] == 0)
    assert p0["complete"] is False and p0["indexed"] == 0
    assert p0["next_offset"] == 0  # cursor never advanced
    assert any(i["kind"] == "read_failure" and i["clean_offset"] == 0
               for i in report["issues"])

    # The durable issue also forces the query API to answer incomplete, with
    # the source location of the failed read attached.
    body = _query(env.client, box="BOX-FAIL").json()
    assert body["status"] == "incomplete"
    issue = next(i for i in body["issues"] if i["kind"] == "read_failure")
    assert issue["partition"] == 0 and issue["clean_offset"] == 0

    # Once the broker recovers, the next run drains and clears the issue.
    env.restart()
    ok = _run(env.client)
    assert ok["complete"] is True and ok["indexed_total"] == 4
    assert env.client.get("/index/progress").json()["open_issues"] == []
    assert _query(env.client, box="BOX-FAIL").json()["status"] == "complete"


# ------------------------------------------------------------- unparseable time
def test_unparseable_sampled_utc_is_locatable_and_query_is_incomplete(env):
    produce_clean(0, clean_record("BOX-TIME", "2026-11-13T08:00:00Z", 1.0, 0, 0))
    bad = clean_record("BOX-TIME", "not-a-timestamp", 2.0, 0, 1)
    produce_clean(0, bad)
    produce_clean(0, clean_record("BOX-TIME", "2026-11-13T08:02:00Z", 3.0, 0, 2))

    report = _run(env.client)
    # The record itself is still indexed (no data loss), only time parsing fails.
    assert report["indexed_total"] == 3
    issue = next(i for i in report["issues"]
                 if i["kind"] == "unparseable_sampled_utc")
    assert issue["partition"] == 0 and issue["clean_offset"] == 1

    body = _query(env.client, box="BOX-TIME").json()
    assert body["status"] == "incomplete"
    assert body["count"] == 2  # the two parseable rows
    loc = next(i for i in body["issues"]
               if i["kind"] == "unparseable_sampled_utc")
    assert loc["box_id"] == "BOX-TIME" and loc["clean_offset"] == 1

    # An unrelated box is not burdened with this issue.
    other = _query(env.client, box="BOX-OTHER").json()
    assert other["status"] == "complete" and other["count"] == 0


# ------------------------------------------------------------- retention gaps
def test_retention_gap_reported_on_first_contact_and_not_crossed(env):
    # More records than the index has seen, then retention deletes the first
    # three before any indexing run.
    for i in range(6):
        produce_clean(0, clean_record("BOX-GAP", "2026-11-13T08:00:00Z",
                                      float(i), 0, i))
    BROKER.delete_records(CLEAN, 0, 3)

    report = _run(env.client)
    p0 = next(p for p in report["partitions"] if p["partition"] == 0)
    assert p0["complete"] is False
    gap = next(i for i in report["issues"] if i["kind"] == "retention_gap")
    assert gap["detail"]["missing_from"] == 0
    assert gap["detail"]["low_watermark"] == 3
    # Nothing was indexed past the gap.
    assert p0["indexed"] == 0 and p0["next_offset"] == 0

    body = _query(env.client, box="BOX-GAP").json()
    assert body["status"] == "incomplete"
    assert next(i for i in body["issues"]
                if i["kind"] == "retention_gap")["detail"]["missing_from"] == 0
    progress = next(p for p in body["index_progress"] if p["partition"] == 0)
    assert progress["status"] == "gap"

    # Re-triggering stays parked at the gap and never jumps over it.
    again = _run(env.client)
    p0b = next(p for p in again["partitions"] if p["partition"] == 0)
    assert p0b["indexed"] == 0 and p0b["next_offset"] == 0
    assert len([i for i in again["issues"] if i["kind"] == "retention_gap"]) == 1


def test_retention_gap_after_some_indexing_parks_and_other_partition_continues(env):
    for i in range(3):
        produce_clean(0, clean_record("BOX-G2", "2026-11-13T08:00:00Z",
                                      float(i), 0, i))
    produce_clean(1, clean_record("BOX-G2", "2026-11-13T08:00:00Z", 9.0, 1, 0))
    _run(env.client)  # fully indexes p0 offsets 0..2 and p1 offset 0

    for i in range(3, 6):
        produce_clean(0, clean_record("BOX-G2", "2026-11-13T08:00:00Z",
                                      float(i), 0, i))
    # Retention now deletes offsets 0..3, including not-yet-indexed offset 3.
    BROKER.delete_records(CLEAN, 0, 4)
    produce_clean(1, clean_record("BOX-G2", "2026-11-13T08:01:00Z", 8.0, 1, 1))

    report = _run(env.client)
    by_p = {p["partition"]: p for p in report["partitions"]}
    assert by_p[0]["complete"] is False
    assert any(i["kind"] == "retention_gap" for i in by_p[0]["issues"])
    # Partition 1 is unaffected and indexes its new record normally.
    assert by_p[1]["indexed"] == 1 and by_p[1]["complete"] is True

    # Already-indexed rows remain queryable, but the answer is incomplete.
    body = _query(env.client, box="BOX-G2").json()
    assert body["status"] == "incomplete"
    assert {r["clean_offset"] for r in body["readings"]
            if r["clean_partition"] == 0} == {0, 1, 2}


# ------------------------------------------------------------- concurrency
def test_concurrent_index_trigger_rejected_with_409(env, monkeypatch):
    produce_clean(0, clean_record("BOX-LOCK", "2026-11-13T08:00:00Z", 1.0, 0, 0))
    real_factory = env.state.indexer._consumer_factory
    entered = []

    class SlowConsumer:
        def __init__(self):
            self._inner = real_factory()

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def poll(self, timeout=-1):
            entered.append(1)
            import time as _t
            _t.sleep(0.2)
            return self._inner.poll(timeout)

    monkeypatch.setattr(env.state.indexer, "_consumer_factory",
                        lambda: SlowConsumer())
    import threading

    result = {}

    def worker():
        # Use the TestClient from a worker thread; the fake broker is
        # thread-safe and the indexer lock is the thing under test.
        result["code"] = env.client.post("/index/run").status_code

    t = threading.Thread(target=worker)
    t.start()
    env.wait_until(lambda: len(entered) == 1)
    rejected = env.client.post("/index/run")
    assert rejected.status_code == 409
    t.join()
    assert result["code"] == 200


# ------------------------------------------------------------- progress endpoint
def test_index_progress_reports_cursors_and_issues(env):
    produce_clean(0, clean_record("BOX-P", "2026-11-13T08:00:00Z", 1.0, 0, 0))
    produce_clean(1, clean_record("BOX-P", "2026-11-13T08:00:00Z", 2.0, 1, 0))
    _run(env.client)

    progress = env.client.get("/index/progress").json()
    assert progress["clean_topic"] == CLEAN
    assert progress["indexed_total"] == 2
    assert {p["partition"]: p["next_offset"] for p in progress["partitions"]} == {0: 1, 1: 1}
    assert all(p["status"] == "active" for p in progress["partitions"])
    assert progress["open_issues"] == []


def test_index_run_boundary_failure_returns_incomplete_not_500(env, monkeypatch):
    def broken_consumer():
        raise RuntimeError("broker unreachable")

    monkeypatch.setattr(env.state.indexer, "_consumer_factory", broken_consumer)
    r = env.client.post("/index/run")
    assert r.status_code == 200
    body = r.json()
    assert body["complete"] is False
    assert body["issues"][0]["kind"] == "boundary_unavailable"
    # The lock must have been released: a follow-up trigger is accepted.
    assert env.client.post("/index/run").status_code == 200


def test_uncommitted_records_are_not_indexed(env):
    """read_committed: records inside an open/aborted transaction stay out."""
    from confluent_kafka import Producer

    p = Producer({"bootstrap.servers": "fake", "transactional.id": "open-txn"})
    p.init_transactions()
    p.begin_transaction()
    p.produce(CLEAN, 0,
              json.dumps(clean_record("BOX-GHOST", "2026-11-13T08:00:00Z",
                                      1.0, 0, 0)).encode(), b"BOX-GHOST")
    p.abort_transaction()

    report = _run(env.client)
    assert report["indexed_total"] == 0
    assert report["complete"] is True
    assert _query(env.client, box="BOX-GHOST").json()["count"] == 0


def test_boundary_failure_makes_query_incomplete_with_progress(env, monkeypatch):
    produce_clean(0, clean_record("BOX-B", "2026-11-13T08:00:00Z", 1.0, 0, 0))
    _run(env.client)

    import app.query as query_mod

    def broken_boundary(settings, consumer_factory=None):
        return {"ok": False, "boundaries": [],
                "error": {"kind": "boundary_unavailable",
                          "detail": {"error": "broker down"}}}

    monkeypatch.setattr(query_mod, "snapshot_boundaries", broken_boundary)
    body = _query(env.client, box="BOX-B").json()
    assert body["status"] == "incomplete"
    assert body["count"] == 1  # local data still returned
    assert body["issues"][0]["kind"] == "boundary_unavailable"
    # Durable progress is locatable even without a live boundary.
    assert body["index_progress"][0]["status"] == "active"
    assert body["index_progress"][0]["indexed_next_offset"] == 1
    assert body["index_progress"][0]["high_watermark_at_query_start"] is None
