#!/usr/bin/env python3
"""Controlled transaction-crash demo against the docker-compose stack.

Two fresh environments are exercised:

* Scenario A (before_commit): a correction is durable in SQLite, then the
  process exits before the Kafka transaction commits.  Restart redelivers the
  source message and the clean record appears exactly once.
* Scenario B (after_commit): the process exits after the transaction commits
  but before SQLite bookkeeping.  Restart skips the already-committed offset:
  no duplicate, no reprocessing.

Run order:
    demo/reset.sh                 # pristine environment (scenario A)
    python demo/run_demo.py A     # before-commit crash demo
    demo/reset.sh                 # pristine environment (scenario B)
    python demo/run_demo.py B     # after-commit crash demo
    python demo/run_demo.py full  # both, with an automatic reset in between
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

BASE_URL = os.environ.get("BASE_URL", "http://localhost:8000")
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)


def call(method: str, path: str, payload: dict | None = None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE_URL + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"null")
    except urllib.error.URLError:
        return None, None


def banner(title: str) -> None:
    print("\n" + "=" * 74 + f"\n{title}\n" + "=" * 74)


def wait_health(timeout: float = 75.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, body = call("GET", "/health")
        if status == 200:
            return body
        time.sleep(1)
    raise SystemExit("API did not become healthy")


def wait_until(description: str, predicate, timeout: float = 25.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(0.5)
    raise SystemExit(f"timeout waiting for: {description} (last={last!r})")


def clean(partition: int | None = None) -> list[dict]:
    q = f"?partition={partition}" if partition is not None else ""
    return call("GET", f"/clean{q}")[1] or []


def reset_env() -> None:
    banner("RESET  pristine containers + volumes")
    subprocess.run(["sh", os.path.join(HERE, "reset.sh")], cwd=ROOT, check=True)


def seed_and_block() -> None:
    banner("SEED  8 messages across 2 partitions, one blocker each")
    subprocess.run(["docker", "compose", "run", "--rm", "seed"],
                   cwd=ROOT, check=True)
    wait_until("two blockers registered",
               lambda: len(call("GET", "/blocked")[1] or []) == 2)
    for row in call("GET", "/blocked")[1]:
        print(f"  BLOCKED p{row['partition']}@o{row['offset']} "
              f"reason={row['reason']} raw={json.dumps(row['raw_message'], ensure_ascii=False)}")
    print("  health:", call("GET", "/health")[1])
    # The other partition keeps moving while partition 0 is paused.
    wait_until("partition 1 produces 2 clean rows despite its own later blocker",
               lambda: len(clean(1)) >= 2)
    print(f"  partition 1 clean rows while p0 blocked: {len(clean(1))}")
    print(f"  partition 0 clean rows (stops before blocker o2): {len(clean(0))}")
    assert [r["source_offset"] for r in clean(0)] == [0, 1]


def crash_and_restart(crash_point: str, partition: int, offset: int,
                      scale: str) -> None:
    banner(f"CRASH  arm {crash_point} on p{partition}@o{offset} correction={scale}")
    status, body = call("POST", f"/blocked/{partition}/{offset}/correct",
                        {"scale": scale})
    print("  durable correction:", status, body)
    assert status == 200
    print(" ", call("POST", "/demo/crash", {"crash_point": crash_point}))
    print(" ", call("POST", f"/demo/resume/{partition}"))
    # Wait for the hard exit (code 42 inside the container).
    deadline = time.time() + 20
    while time.time() < deadline:
        status, _ = call("GET", "/health")
        if status is None:
            print("  process exited (health unreachable) as expected")
            break
        time.sleep(0.5)
    else:
        raise SystemExit("process did not exit at the crash point")


def verify_exactly_once(partition: int, expected_offsets: list[int]) -> None:
    wait_health()
    wait_until(f"partition {partition} drains after redelivery",
               lambda: clean(partition) if len(clean(partition)) == len(expected_offsets) else None)
    rows = clean(partition)
    offsets = [r["source_offset"] for r in rows]
    print("  clean order after restart:",
          [(r["source_offset"], r["box_id"], r["scale"], round(r["celsius"], 4))
           for r in rows])
    assert offsets == expected_offsets, offsets
    assert len(offsets) == len(set(offsets)), "duplicate clean record detected"


def scenario_before_commit() -> None:
    banner("SCENARIO A  exit BEFORE transaction commit")
    seed_and_block()
    visible_before = clean(0)
    crash_and_restart("before_commit", 0, 2, "F")
    # The blocker record must not be visible after the pre-commit crash.
    assert all(r["source_offset"] != 2 for r in clean(0))
    print(f"  after crash, p0 still has {len(clean(0))} rows; o2 invisible (txn aborted)")
    verify_exactly_once(0, [0, 1, 2, 3])
    row = next(r for r in clean(0) if r["source_offset"] == 2)
    assert row["reading"] == 39.2 and row["scale"] == "F"
    print(f"  original reading preserved: {row['reading']} F -> celsius={row['celsius']:.4f}")
    print("  no loss, no duplicate, blocker not skipped  -> PASS")


def scenario_after_commit() -> None:
    banner("SCENARIO B  exit AFTER transaction commit")
    seed_and_block()
    crash_and_restart("after_commit", 0, 2, "F")
    # Transaction committed before the exit: the row is already visible.
    wait_health()
    wait_until("committed blocker visible after restart",
               lambda: clean(0) if any(r["source_offset"] == 2 for r in clean(0)) else None)
    rows = clean(0)
    print("  clean order after restart:",
          [(r["source_offset"], r["box_id"], r["scale"]) for r in rows])
    assert [r["source_offset"] for r in rows] == [0, 1, 2, 3]
    assert len([r for r in rows if r["source_offset"] == 2]) == 1
    print("  committed offset not reprocessed; exactly one clean row for o2 -> PASS")

    banner("IDEMPOTENCY  repeated correction attempts")
    status, body = call("POST", "/blocked/0/2/correct", {"scale": "F"})
    print("  re-correct committed row ->", status, body)
    assert status == 409
    status, body = call("POST", "/blocked/1/2/correct", {"scale": "C"})
    print("  open blocker first correction ->", status)
    status2, body2 = call("POST", "/blocked/1/2/correct", {"scale": "C"})
    print("  same correction repeated ->", status2, body2["message"])
    assert status2 == 200 and "idempotent" in body2["message"]
    status3, body3 = call("POST", "/blocked/1/2/correct", {"scale": "F"})
    print("  conflicting correction ->", status3, body3["detail"])
    assert status3 == 409
    call("POST", "/demo/resume/1")
    wait_until("partition 1 drains", lambda: clean(1) if len(clean(1)) == 4 else None)
    assert len(clean(1)) == 4

    banner("FINAL STATE")
    print("  progress:", json.dumps(call("GET", "/progress")[1], ensure_ascii=False))
    all_rows = clean()
    counts = {0: 0, 1: 0}
    for r in all_rows:
        counts[r["source_partition"]] += 1
    print(f"  clean records total={len(all_rows)} per_partition={counts}")
    assert counts == {0: 4, 1: 4}
    print("\nALL DEMO CHECKS PASSED")


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "full"
    if mode == "A":
        wait_health()
        scenario_before_commit()
    elif mode == "B":
        wait_health()
        scenario_after_commit()
    elif mode == "full":
        reset_env()
        scenario_before_commit()
        reset_env()
        scenario_after_commit()
    else:
        raise SystemExit(f"unknown mode {mode!r}; use A, B or full")


if __name__ == "__main__":
    main()
