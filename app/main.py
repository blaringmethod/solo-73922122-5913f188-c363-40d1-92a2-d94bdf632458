"""FastAPI application: blocker inspection, correction, clean reads and index.

Endpoints (all visible in /docs):

* ``GET  /health``                       - liveness + worker pause state
* ``GET  /blocked``                      - list blocked messages (filter by status)
* ``POST /blocked/{partition}/{offset}/correct`` - durable C/F correction
* ``GET  /clean``                        - read committed clean records
* ``GET  /progress``                     - committed source offsets per partition
* ``POST /index/run``                    - manual incremental indexing pass
* ``GET  /index/progress``               - durable index cursors and open issues
* ``GET  /readings/query``               - box + UTC range query (completeness-checked)
* ``POST /demo/crash``                   - arm a controlled pre/post-commit exit
* ``GET  /demo/crash``                   - inspect the armed crash point
"""
from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query

from .clean_reader import read_clean
from .config import Settings, settings as default_settings
from .crash import CrashController
from .db import CorrectionStore
from .index_store import IndexStore
from .indexer import IncrementalIndexer, IndexerBusyError
from .models import (
    BlockedMessage,
    CleanReading,
    CorrectionRequest,
    CorrectionResponse,
    CrashRequest,
    CrashResponse,
    IndexProgressResponse,
    IndexRunResponse,
    QueryResponse,
)
from . import query as query_service
from .query import query_readings
from .timestamps import parse_query_instant
from .worker import Worker

logger = logging.getLogger("coldchain.api")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")


class AppState:
    """Wired together in the lifespan; tests can pre-populate it."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.store = CorrectionStore(settings.sqlite_path)
        self.index_store = IndexStore(settings.sqlite_path)
        self.crash = CrashController(settings.crash_on)
        self.worker = Worker(settings, self.store, self.crash)
        self.indexer = IncrementalIndexer(settings, self.index_store)


def build_app(state: AppState | None = None) -> FastAPI:
    settings = state.settings if state else default_settings

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        nonlocal state
        if state is None:
            state = AppState(settings)
        app.state.deps = state
        state.worker.start()
        logger.info("API ready; single worker running")
        try:
            yield
        finally:
            state.worker.stop()
            state.index_store.close()
            state.store.close()

    app = FastAPI(
        title="冷链温标补正入仓 API",
        description=(
            "Cold-chain intake with durable temperature-scale correction. "
            "A missing/unknown scale blocks exactly one partition; corrections "
            "are persisted and published with transactional exactly-once semantics."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )

    def deps() -> AppState:
        return app.state.deps

    @app.get("/health", tags=["ops"])
    def health(d: AppState = Depends(deps)) -> dict:
        return {
            "status": "ok",
            "raw_topic": d.settings.raw_topic,
            "clean_topic": d.settings.clean_topic,
            "partitions": d.settings.topic_partitions,
            "paused_partitions": sorted(d.worker.paused_partitions),
        }

    @app.get("/blocked", response_model=list[BlockedMessage], tags=["blockers"])
    def list_blocked(
        status: str | None = Query(
            None, pattern="^(blocked|corrected|committed)$",
            description="Optional lifecycle filter",
        ),
        d: AppState = Depends(deps),
    ) -> list[BlockedMessage]:
        rows = d.store.list_blocked(status)
        return [
            BlockedMessage(
                key=row["key"],
                partition=row["partition_no"],
                offset=row["offset_no"],
                raw_message=json.loads(row["raw_message"]),
                reason=row["reason"],
                status=row["status"],
                correction=row["correction"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
            for row in rows
        ]

    @app.post(
        "/blocked/{partition}/{offset}/correct",
        response_model=CorrectionResponse,
        tags=["blockers"],
    )
    def correct(
        partition: int,
        offset: int,
        body: CorrectionRequest,
        d: AppState = Depends(deps),
    ) -> CorrectionResponse:
        topic = d.settings.raw_topic
        if not (0 <= partition < d.settings.topic_partitions):
            raise HTTPException(400, "partition out of range")

        existing = d.store.get_blocked(topic, partition, offset)
        if existing is not None and existing["reason"].startswith("invalid_json"):
            raise HTTPException(
                422,
                "source payload is not valid JSON and cannot be corrected via "
                "scale override; fix the recorder and replay the message",
            )

        outcome, row = d.store.apply_correction(
            topic, partition, offset, body.scale
        )
        d.store.record_attempt(
            topic, partition, offset, body.scale, outcome,
            detail=(row["reason"] if row else "no blocked row"),
        )

        if outcome == "not_found":
            raise HTTPException(
                404,
                f"no blocked message at partition={partition} offset={offset}",
            )
        if outcome == "already_committed":
            raise HTTPException(
                409,
                "message already committed; correction is no longer applicable",
            )
        if outcome == "conflict":
            raise HTTPException(
                409,
                f"a different correction ({row['correction']}) is already durable; "
                f"repeating the same value is idempotent",
            )

        # Correction is now durable (WAL + synchronous=FULL). Safe to resume:
        # the paused partition redelivers the message and the worker picks up
        # the persisted scale.  Auto-resume can be disabled for the
        # controlled crash demo, which resumes via POST /demo/resume.
        if d.settings.auto_resume:
            d.worker.resume_due_to_correction(partition)

        if outcome == "duplicate_same":
            return CorrectionResponse(
                partition=partition, offset=offset, status=row["status"],
                correction=body.scale,
                message="duplicate correction ignored (idempotent); "
                        "durable value unchanged",
            )
        return CorrectionResponse(
            partition=partition, offset=offset, status=row["status"],
            correction=body.scale,
            message="correction persisted; partition resumed for redelivery",
        )

    @app.get("/clean", response_model=list[CleanReading], tags=["clean"])
    def get_clean(
        partition: int | None = Query(None, ge=0),
        limit: int | None = Query(None, ge=1),
        d: AppState = Depends(deps),
    ) -> list[CleanReading]:
        return [CleanReading(**r) for r in read_clean(d.settings, partition, limit)]

    @app.get("/progress", tags=["ops"])
    def progress(d: AppState = Depends(deps)) -> dict:
        """Committed source offsets - the worker's durable consume position."""
        from confluent_kafka import TopicPartition  # local import keeps tests light

        tps = [
            TopicPartition(d.settings.raw_topic, p)
            for p in range(d.settings.topic_partitions)
        ]
        committed = d.worker._consumer.committed(tps) if d.worker._consumer else []
        return {
            "group_id": d.settings.group_id,
            "positions": [
                {
                    "partition": tp.partition,
                    # OFFSET_INVALID (-1001) means nothing committed yet.
                    "committed_offset": tp.offset if tp.offset >= 0 else None,
                    "paused": tp.partition in d.worker.paused_partitions,
                }
                for tp in committed
            ],
        }

    # ------------------------------------------------------------- indexing
    @app.post("/index/run", response_model=IndexRunResponse, tags=["index"])
    def run_index(d: AppState = Depends(deps)) -> IndexRunResponse:
        """Run one bounded incremental indexing pass (max 50/partition).

        Reads only transaction-committed clean records (``read_committed``)
        and persists each reading together with the partition continuation
        position.  Safe to interrupt, restart and re-trigger: cursors only
        advance in the same transaction as the row insert, so nothing is
        skipped or recorded twice.
        """
        try:
            report = d.indexer.run_once()
        except IndexerBusyError as exc:
            raise HTTPException(409, str(exc))
        return IndexRunResponse(**report)

    @app.get("/index/progress", response_model=IndexProgressResponse,
             tags=["index"])
    def index_progress(d: AppState = Depends(deps)) -> IndexProgressResponse:
        """Durable index cursors, per partition, plus every open issue."""
        cursors = d.index_store.list_cursors(d.settings.clean_topic)
        return IndexProgressResponse(
            clean_topic=d.settings.clean_topic,
            indexed_total=d.index_store.count_readings(d.settings.clean_topic),
            partitions=[
                {
                    "partition": row["partition_no"],
                    "next_offset": row["next_offset"],
                    "status": row["status"],
                    "high_watermark": row["high_watermark"],
                }
                for row in cursors
            ],
            open_issues=d.index_store.open_issues(d.settings.clean_topic),
        )

    @app.get("/readings/query", response_model=QueryResponse, tags=["index"])
    def readings_query(
        box_id: str = Query(..., min_length=1, description="Exact box identifier"),
        start_utc: str = Query(
            ..., alias="start_utc",
            description="Inclusive UTC start, ISO-8601 with Z or offset",
        ),
        end_utc: str = Query(
            ..., alias="end_utc",
            description="Inclusive UTC end, ISO-8601 with Z or offset",
        ),
        limit: int | None = Query(None, ge=1, le=10_000),
        d: AppState = Depends(deps),
    ) -> QueryResponse:
        """Find indexed readings for one box inside a UTC window.

        The readable boundary of both partitions is fixed when the query
        starts.  ``status=incomplete`` (with locatable progress/issues) is
        returned whenever the index has not caught up, retention deleted
        unindexed records, a read failed, or a matching record's sample time
        could not be parsed - partial rows are still included but never
        reported as the complete answer.
        """
        try:
            start_dt = parse_query_instant(start_utc)
            end_dt = parse_query_instant(end_utc)
        except ValueError as exc:
            raise HTTPException(
                422,
                f"invalid UTC instant (use ISO-8601 with Z or offset): {exc}",
            )
        if end_dt < start_dt:
            raise HTTPException(422, "end_utc must not be earlier than start_utc")

        boundary = query_service.snapshot_boundaries(d.settings)
        result = query_readings(
            d.settings, d.index_store, box_id,
            start_dt.timestamp(), end_dt.timestamp(), limit,
            boundary=boundary,
        )
        return QueryResponse(**result)

    @app.post("/demo/resume/{partition}", tags=["demo"])
    def demo_resume(partition: int, d: AppState = Depends(deps)) -> dict:
        """Manually resume a paused partition (demo/test sequencing control).

        The correction endpoint normally resumes automatically; this lets a
        demo arm a crash point between the durable correction and redelivery.
        """
        if not (0 <= partition < d.settings.topic_partitions):
            raise HTTPException(400, "partition out of range")
        d.worker.resume_due_to_correction(partition)
        return {"partition": partition, "resumed": True,
                "paused_partitions": sorted(d.worker.paused_partitions)}

    @app.post("/demo/crash", response_model=CrashResponse, tags=["demo"])
    def arm_crash(body: CrashRequest, d: AppState = Depends(deps)) -> CrashResponse:
        d.crash.arm(body.crash_point)
        return CrashResponse(
            crash_point=body.crash_point,
            armed=True,
            message=f"process will exit (code 42) at crash point {body.crash_point}",
        )

    @app.get("/demo/crash", response_model=CrashResponse, tags=["demo"])
    def get_crash(d: AppState = Depends(deps)) -> CrashResponse:
        armed = d.crash.status()
        return CrashResponse(
            crash_point=armed or "",
            armed=bool(armed),
            message="one-shot crash flag armed" if armed else "no crash armed",
        )

    return app


app = build_app()
