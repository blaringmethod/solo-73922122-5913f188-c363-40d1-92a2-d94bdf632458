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
