"""Pure validation and conversion logic for raw recorder messages.

A raw message is expected to look like::

    {
      "box_id": "BOX-001",
      "sampled_utc": "2026-11-13T08:00:00Z",
      "reading": 5.2,
      "scale": "C"
    }

``scale`` of "C" is taken directly as Celsius; "F" is converted with
``(reading - 32) * 5 / 9``.  Missing or unknown scales (and any other malformed
message) produce a blocker reason instead of a clean record.
"""
from __future__ import annotations

import math
from typing import Any

KNOWN_SCALES = ("C", "F")


def to_celsius(reading: float, scale: str) -> float:
    if scale == "C":
        return float(reading)
    return (float(reading) - 32.0) * 5.0 / 9.0


def validate_raw(value: Any) -> tuple[dict[str, Any] | None, str | None]:
    """Return ``(clean_fields, None)`` or ``(None, blocker_reason)``.

    ``clean_fields`` contains the validated source fields plus the approved
    scale and computed Celsius value, but no source location yet.
    """
    if not isinstance(value, dict):
        return None, "message_not_object"

    box_id = value.get("box_id")
    if not isinstance(box_id, str) or not box_id.strip():
        return None, "missing_or_invalid_box_id"

    sampled_utc = value.get("sampled_utc")
    if not isinstance(sampled_utc, str) or not sampled_utc.strip():
        return None, "missing_or_invalid_sampled_utc"

    if "reading" not in value:
        return None, "missing_reading"
    reading = value["reading"]
    # bool is a subclass of int; reject it explicitly.
    if isinstance(reading, bool) or not isinstance(reading, (int, float)):
        return None, "reading_not_number"
    reading = float(reading)
    if not math.isfinite(reading):
        return None, "reading_not_finite"

    if "scale" not in value or value["scale"] is None:
        return None, "missing_scale"
    scale = value["scale"]
    if not isinstance(scale, str) or scale not in KNOWN_SCALES:
        return None, f"unknown_scale:{scale!r}"

    return (
        {
            "box_id": box_id,
            "sampled_utc": sampled_utc,
            "reading": reading,
            "scale": scale,
            "celsius": to_celsius(reading, scale),
        },
        None,
    )


def build_clean_record(fields: dict[str, Any], source_topic: str,
                       source_partition: int, source_offset: int) -> dict[str, Any]:
    """Assemble the clean record persisted to the output topic."""
    return {
        "box_id": fields["box_id"],
        "sampled_utc": fields["sampled_utc"],
        "reading": fields["reading"],
        "scale": fields["scale"],
        "celsius": fields["celsius"],
        "source_topic": source_topic,
        "source_partition": source_partition,
        "source_offset": source_offset,
    }
