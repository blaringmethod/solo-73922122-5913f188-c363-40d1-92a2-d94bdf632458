"""Pydantic request/response models for the correction API."""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

TemperatureScale = Literal["C", "F"]


class CorrectionRequest(BaseModel):
    """Correction payload: only the approved scale may be supplied.

    The original reading value is intentionally never part of the request, so
    the API cannot alter source data.
    """

    scale: TemperatureScale = Field(..., description="Approved temperature scale: C or F")


class BlockedMessage(BaseModel):
    key: str | None
    partition: int
    offset: int
    raw_message: Any
    reason: str
    status: str
    correction: TemperatureScale | None
    created_at: str
    updated_at: str


class CorrectionResponse(BaseModel):
    partition: int
    offset: int
    status: str
    correction: TemperatureScale
    message: str


class CleanReading(BaseModel):
    box_id: str
    sampled_utc: str
    reading: float
    scale: TemperatureScale
    celsius: float
    source_topic: str
    source_partition: int
    source_offset: int


class CrashRequest(BaseModel):
    crash_point: Literal["before_commit", "after_commit"]


class CrashResponse(BaseModel):
    crash_point: str
    armed: bool
    message: str


# ------------------------------------------------------------- index / query
class IndexedReading(BaseModel):
    """One indexed reading with both clean-topic and source locations."""

    box_id: str
    sampled_utc: str | None
    celsius: float
    scale: TemperatureScale
    reading: float
    source_topic: str
    source_partition: int
    source_offset: int
    clean_partition: int
    clean_offset: int


class IndexIssue(BaseModel):
    # id/created_at are present for durable issues but absent for transient
    # request-scoped problems (e.g. a boundary snapshot that failed).
    id: int | None = None
    kind: str
    partition: int | None = None
    clean_offset: int | None = None
    box_id: str | None = None
    detail: Any = None
    created_at: str | None = None
    resolved_at: str | None = None


class PartitionIndexResult(BaseModel):
    partition: int
    low_watermark: int
    high_watermark: int
    start_offset: int
    indexed: int
    next_offset: int
    complete: bool
    issues: list[IndexIssue] = Field(default_factory=list)


class IndexRunResponse(BaseModel):
    clean_topic: str
    batch_size: int
    indexed_total: int
    # True only when every partition reached the boundary fixed at run start.
    complete: bool
    partitions: list[PartitionIndexResult]
    issues: list[IndexIssue] = Field(default_factory=list)


class IndexProgressPartition(BaseModel):
    partition: int
    next_offset: int
    status: str
    high_watermark: int | None = None


class IndexProgressResponse(BaseModel):
    clean_topic: str
    indexed_total: int
    partitions: list[IndexProgressPartition]
    open_issues: list[IndexIssue] = Field(default_factory=list)


class QueryBoundary(BaseModel):
    partition: int
    low_watermark: int
    high_watermark_at_query_start: int


class QueryProgress(BaseModel):
    partition: int
    low_watermark: int | None
    high_watermark_at_query_start: int | None
    indexed_next_offset: int
    status: str


class QueryResponse(BaseModel):
    # "complete" only if the index covers the full readable boundary fixed at
    # query start and no open issue affects the answer.
    status: Literal["complete", "incomplete"]
    box_id: str
    count: int
    readings: list[IndexedReading]
    boundaries: list[QueryBoundary]
    index_progress: list[QueryProgress]
    issues: list[IndexIssue] = Field(default_factory=list)
