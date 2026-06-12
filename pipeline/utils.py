"""Shared CLI utilities for pipeline entry points."""

from __future__ import annotations

from datetime import UTC, datetime


def parse_as_of(raw: str) -> datetime:
    """Parse an ISO-8601 timestamp string into an aware UTC datetime.

    Accepts a trailing 'Z' (Go/RFC-3339 style) as well as the standard
    '+00:00' offset. Naive inputs are assumed UTC.
    """
    raw = raw.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt
