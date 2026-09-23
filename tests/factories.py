"""Builders for the objects every test needs.

Kept as plain functions rather than fixtures because most tests need several
events with small differences, and a fixture per variation is unreadable.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from itertools import count
from typing import Any

from orders_stream.models import OrderEvent
from orders_stream.processor.pipeline import RawRecord

BASE_TIME = datetime(2026, 9, 14, 12, 0, 0, tzinfo=UTC)
_offsets = count()


def payload(**overrides: Any) -> dict[str, Any]:
    base = {
        "event_id": overrides.pop("event_id", f"evt-{next(_offsets):06d}"),
        "event_type": "order_paid",
        "occurred_at": BASE_TIME.isoformat(),
        "order_id": "ord-1",
        "merchant_id": "m_kioskone",
        "user_id": "u_0001",
        "amount_minor": 1000,
        "currency": "EUR",
        "schema_version": 1,
    }
    base.update(overrides)
    return base


def event(**overrides: Any) -> OrderEvent:
    data = payload(**overrides)
    return OrderEvent.model_validate(data)


def at(seconds: float) -> datetime:
    """A timestamp `seconds` after the base time. Negative means earlier."""
    return BASE_TIME + timedelta(seconds=seconds)


def record(value: dict[str, Any] | bytes | None = None, **kwargs: Any) -> RawRecord:
    if value is None:
        raw = json.dumps(payload(**kwargs)).encode("utf-8")
    elif isinstance(value, bytes):
        raw = value
    else:
        raw = json.dumps(value).encode("utf-8")

    return RawRecord(
        topic="orders.v1",
        partition=kwargs.get("partition", 0),
        offset=kwargs.get("offset", next(_offsets)),
        key=b"m_kioskone",
        value=raw,
    )
