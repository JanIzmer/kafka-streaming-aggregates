"""Domain models.

Two time concepts appear throughout and must never be confused:

* **event time** (`occurred_at`) - when the thing happened. Windows are built
  from this, always.
* **processing time** - when we read the record. Used only for the idle
  watermark and for measuring lateness.

Mixing them is the single most common bug in a streaming aggregate: a replay
after an outage then produces completely different numbers than the original
run, and neither is reproducible.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, field_validator


class EventType(StrEnum):
    ORDER_PLACED = "order_placed"
    ORDER_PAID = "order_paid"
    ORDER_CANCELLED = "order_cancelled"
    ORDER_REFUNDED = "order_refunded"


class OrderEvent(BaseModel):
    """One record off the topic, after validation."""

    event_id: str
    event_type: EventType
    occurred_at: datetime
    order_id: str
    merchant_id: str
    user_id: str | None = None
    amount_minor: int | None = Field(default=None, ge=0)
    currency: str | None = None
    payment_method: str | None = None
    schema_version: int = 1

    @field_validator("occurred_at")
    @classmethod
    def _must_be_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @property
    def amount(self) -> int:
        """Amount in minor units, or zero when the event carries none.

        Minor units are integers on purpose: summing floats is not associative,
        so replaying the same window in a different order would produce a
        different total.
        """
        return self.amount_minor or 0


class WindowKey(BaseModel, frozen=True):
    """Identity of one aggregate row: a time bucket plus the grouping key."""

    window_start: datetime
    merchant_id: str

    def window_end(self, window_size_seconds: int) -> datetime:
        return self.window_start + timedelta(seconds=window_size_seconds)

    def __str__(self) -> str:  # pragma: no cover - logging convenience
        return f"{self.merchant_id}@{self.window_start.isoformat()}"


def window_start_for(occurred_at: datetime, window_size_seconds: int) -> datetime:
    """Assign an event to a tumbling window.

    Buckets are aligned to the epoch, not to the first event seen. Two
    processors, a replay and the original run therefore all produce identical
    bucket boundaries - which is what makes the output reproducible.
    """
    if occurred_at.tzinfo is None:
        occurred_at = occurred_at.replace(tzinfo=timezone.utc)
    epoch_seconds = int(occurred_at.timestamp())
    bucket = epoch_seconds - (epoch_seconds % window_size_seconds)
    return datetime.fromtimestamp(bucket, tz=timezone.utc)


class Aggregate(BaseModel):
    """The accumulated state of one window, and the row that gets written."""

    window_start: datetime
    window_end: datetime
    merchant_id: str

    events_total: int = 0
    orders_placed: int = 0
    orders_paid: int = 0
    orders_cancelled: int = 0
    orders_refunded: int = 0

    gross_amount_minor: int = 0
    refunded_amount_minor: int = 0

    distinct_users: int = 0
    first_event_at: datetime | None = None
    last_event_at: datetime | None = None

    late_events_applied: int = 0
    revision: int = 1
    is_closed: bool = False

    @property
    def net_amount_minor(self) -> int:
        return self.gross_amount_minor - self.refunded_amount_minor

    def as_row(self) -> dict[str, Any]:
        return self.model_dump() | {"net_amount_minor": self.net_amount_minor}


class DeadLetter(BaseModel):
    """A record we refuse to process, kept with enough context to replay it."""

    reason: str
    detail: str
    topic: str
    partition: int
    offset: int
    key: str | None
    payload: str
    failed_at: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))

    def as_row(self) -> dict[str, Any]:
        return self.model_dump()
