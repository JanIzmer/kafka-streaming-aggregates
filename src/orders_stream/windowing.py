"""Tumbling windows, watermarks and late data.

This is where the correctness of the whole pipeline lives, so the rules are
written out explicitly rather than left implicit in the code:

* **Windows are tumbling and epoch-aligned.** `window_start_for` buckets on
  `floor(epoch / size)`, never on "the first event I happened to see". A replay
  therefore produces byte-identical bucket boundaries.

* **The watermark is `max_event_time_seen - allowed_lateness`.** It only ever
  moves forward. It is a statement about the input ("I do not expect anything
  older than this any more"), not about wall clock.

* **A window closes when the watermark passes its end.** Before that it is
  continuously upserted, so the aggregates table always shows the in-progress
  value rather than nothing at all.

* **An event is classified against the watermark as it was *before* that event
  advanced it.** Otherwise an event would be judged by a watermark it set
  itself, and nothing would ever be late.

* **An event whose window has already closed is never resurrected.** Re-opening
  a closed window would silently change a number a dashboard has already shown,
  with no record that it happened. It goes to the late table and the DLQ
  instead, where it is visible and replayable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import StrEnum

from orders_stream.logging_conf import get_logger
from orders_stream.models import Aggregate, EventType, OrderEvent, WindowKey, window_start_for

log = get_logger(__name__)


class Outcome(StrEnum):
    ON_TIME = "on_time"
    LATE_ACCEPTED = "late_accepted"
    TOO_LATE = "too_late"
    FUTURE_SKEW = "future_skew"


@dataclass
class WindowState:
    aggregate: Aggregate
    users: set[str] = field(default_factory=set)
    dirty: bool = True

    def touch(self) -> None:
        self.dirty = True


@dataclass
class WindowManager:
    """Owns the in-flight window state for the partitions this consumer holds."""

    window_size_seconds: int = 60
    allowed_lateness_seconds: int = 300
    # Guards against a producer with a broken clock. An event claiming to be an
    # hour in the future would drag the watermark forward with it and close -
    # permanently - every window in between. That is unrecoverable without a
    # replay, so such events are refused outright.
    max_future_skew_seconds: int = 60

    watermark: datetime | None = None
    windows: dict[WindowKey, WindowState] = field(default_factory=dict)

    on_time_count: int = 0
    late_count: int = 0
    too_late_count: int = 0
    future_skew_count: int = 0

    # -- watermark ---------------------------------------------------------

    def _advance_watermark(self, event_time: datetime) -> None:
        candidate = event_time - timedelta(seconds=self.allowed_lateness_seconds)
        if self.watermark is None or candidate > self.watermark:
            self.watermark = candidate

    def advance_on_idle(self, now: datetime) -> bool:
        """Move the watermark forward when the input has gone quiet.

        Without this a partition that stops receiving events freezes its
        watermark, and every open window stays open forever - the aggregates
        are never marked closed and state grows without bound. A quiet
        partition is normal (a merchant stops trading for the night), so this is
        not an edge case.

        Wall clock is used *only* here, and only to move the watermark to a
        point the event stream has effectively already reached.
        """
        candidate = now - timedelta(seconds=self.allowed_lateness_seconds)
        if self.watermark is None or candidate > self.watermark:
            previous = self.watermark
            self.watermark = candidate
            log.debug("watermark.idle_advance", from_=str(previous), to=str(candidate))
            return True
        return False

    def is_window_closed(self, window_start: datetime) -> bool:
        if self.watermark is None:
            return False
        return window_start + timedelta(seconds=self.window_size_seconds) <= self.watermark

    # -- ingestion ---------------------------------------------------------

    def add(self, event: OrderEvent, now: datetime | None = None) -> Outcome:
        """Apply one event and report how it was treated."""
        now = now or datetime.now(tz=timezone.utc)

        if event.occurred_at > now + timedelta(seconds=self.max_future_skew_seconds):
            self.future_skew_count += 1
            log.warning(
                "window.future_skew",
                event_id=event.event_id,
                occurred_at=event.occurred_at.isoformat(),
                skew_seconds=(event.occurred_at - now).total_seconds(),
            )
            return Outcome.FUTURE_SKEW

        window_start = window_start_for(event.occurred_at, self.window_size_seconds)

        if self.is_window_closed(window_start):
            self.too_late_count += 1
            log.info(
                "window.too_late",
                event_id=event.event_id,
                merchant_id=event.merchant_id,
                window_start=window_start.isoformat(),
                watermark=self.watermark.isoformat() if self.watermark else None,
                behind_seconds=(self.watermark - event.occurred_at).total_seconds()
                if self.watermark
                else None,
            )
            return Outcome.TOO_LATE

        # Classify before advancing: an event must not be judged against a
        # watermark it set itself.
        is_late = self.watermark is not None and event.occurred_at < self.watermark

        key = WindowKey(window_start=window_start, merchant_id=event.merchant_id)
        state = self.windows.get(key)
        if state is None:
            state = WindowState(
                aggregate=Aggregate(
                    window_start=window_start,
                    window_end=window_start + timedelta(seconds=self.window_size_seconds),
                    merchant_id=event.merchant_id,
                )
            )
            self.windows[key] = state

        self._apply(state, event, is_late=is_late)
        self._advance_watermark(event.occurred_at)

        if is_late:
            self.late_count += 1
            return Outcome.LATE_ACCEPTED
        self.on_time_count += 1
        return Outcome.ON_TIME

    def _apply(self, state: WindowState, event: OrderEvent, is_late: bool) -> None:
        aggregate = state.aggregate
        aggregate.events_total += 1

        match event.event_type:
            case EventType.ORDER_PLACED:
                aggregate.orders_placed += 1
            case EventType.ORDER_PAID:
                aggregate.orders_paid += 1
                aggregate.gross_amount_minor += event.amount
            case EventType.ORDER_CANCELLED:
                aggregate.orders_cancelled += 1
            case EventType.ORDER_REFUNDED:
                aggregate.orders_refunded += 1
                aggregate.refunded_amount_minor += event.amount

        if event.user_id:
            state.users.add(event.user_id)
            aggregate.distinct_users = len(state.users)

        if aggregate.first_event_at is None or event.occurred_at < aggregate.first_event_at:
            aggregate.first_event_at = event.occurred_at
        if aggregate.last_event_at is None or event.occurred_at > aggregate.last_event_at:
            aggregate.last_event_at = event.occurred_at

        if is_late:
            aggregate.late_events_applied += 1
            # The revision counter is what makes a restatement visible. A
            # consumer that sees revision jump from 3 to 4 knows the number it
            # read earlier has changed, which "the row was updated" alone does
            # not tell it.
            aggregate.revision += 1

        state.touch()

    # -- output ------------------------------------------------------------

    def dirty_aggregates(self) -> list[Aggregate]:
        """Windows changed since the last flush, open or closed.

        Open windows are published too: a dashboard showing the current minute
        filling up is far more useful than one that shows nothing until the
        minute is over.
        """
        return [state.aggregate for state in self.windows.values() if state.dirty]

    def mark_flushed(self, keys: list[WindowKey] | None = None) -> None:
        targets = keys if keys is not None else list(self.windows)
        for key in targets:
            state = self.windows.get(key)
            if state is not None:
                state.dirty = False

    def close_expired(self) -> list[Aggregate]:
        """Finalise and evict every window the watermark has passed.

        Eviction is what bounds memory. The number of live windows is roughly
        `merchants x (allowed_lateness / window_size)`, so with the defaults
        each merchant holds at most five one-minute windows.
        """
        if self.watermark is None:
            return []

        expired = [key for key in self.windows if self.is_window_closed(key.window_start)]
        closed: list[Aggregate] = []
        for key in expired:
            state = self.windows.pop(key)
            state.aggregate.is_closed = True
            closed.append(state.aggregate)

        if closed:
            log.info(
                "window.closed",
                count=len(closed),
                watermark=self.watermark.isoformat(),
                still_open=len(self.windows),
            )
        return closed

    @property
    def open_window_count(self) -> int:
        return len(self.windows)

    @property
    def tracked_user_count(self) -> int:
        """Size of the heaviest part of the state, worth exposing as a metric."""
        return sum(len(state.users) for state in self.windows.values())
