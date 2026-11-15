"""Strict UTC timestamp parsing for indexed clean records.

Clean records carry ``sampled_utc`` as an ISO-8601 string (the intake service
writes e.g. ``2026-11-13T08:00:20Z``).  The index must be able to answer UTC
range queries, so a record's timestamp is parsed once at index time and the
epoch is stored alongside the original string.

Parsing is deliberately strict: a value that cannot be interpreted is never
guessed.  The caller treats ``None`` as a locatable
``unparseable_sampled_utc`` issue rather than dropping or mis-indexing the
record.
"""
from __future__ import annotations

from datetime import datetime, timezone


def parse_utc_epoch(value: object) -> float | None:
    """Return the POSIX epoch for an ISO-8601 UTC string, else ``None``.

    Both the explicit ``Z`` suffix and explicit numeric UTC offsets are
    accepted; naive timestamps are rejected because their zone is ambiguous.
    """
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    try:
        # Python's fromisoformat understands offsets but not the trailing Z
        # until 3.11; normalise it explicitly for portable strictness.
        normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return None
    return dt.astimezone(timezone.utc).timestamp()


def parse_query_instant(value: str) -> datetime:
    """Parse a query-bound instant (HTTP layer), requiring an explicit zone.

    ``Z`` and numeric offsets are accepted; raises ``ValueError`` for naive or
    malformed input so the API can answer 422 instead of guessing the zone.
    """
    text = value.strip()
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        raise ValueError("timestamp must include a UTC designator (Z or offset)")
    return dt.astimezone(timezone.utc)
