"""Per-batch processing, with no Kafka and no database in sight.

Everything that decides *what happens to a record* lives here, behind a plain
function that takes a list of raw records and returns what should be written.
That is deliberate: the interesting failure modes - a duplicate, an event that
is three minutes late, a payload that is not JSON - are then testable without a
broker, which means they are actually tested.

Routing table:

| Situation                        | Aggregated | Ledger | late_events | DLQ |
|----------------------------------|------------|--------|-------------|-----|
| valid, on time                   | yes        | yes    | no          | no  |
| valid, late but window still open| yes        | yes    | no          | no  |
| valid, window already closed     | no         | yes    | yes         | yes |
| event time far in the future     | no         | yes    | no          | yes |
| duplicate event_id               | no         | -      | no          | no  |
| fails the contract               | no         | yes    | no          | yes |
| undecodable bytes                | no         | no     | no          | yes |

The ledger entry on rejected records is the part that is easy to miss: without
it, a replay after a restart would write the same dead letter a second time.
Undecodable bytes are the exception - there is no id to record.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError

from orders_stream.codec import MalformedRecord, decode, decode_key, encode
from orders_stream.contract import Contract
from orders_stream.dedup import Deduplicator
from orders_stream.logging_conf import get_logger
from orders_stream.models import DeadLetter, OrderEvent, window_start_for
from orders_stream.windowing import Outcome, WindowManager

log = get_logger(__name__)


@dataclass(frozen=True)
class RawRecord:
    """A Kafka message, decoupled from the confluent_kafka Message type."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    timestamp_ms: int | None = None


@dataclass
class BatchResult:
    ledger_entries: list[dict[str, Any]] = field(default_factory=list)
    late_rows: list[dict[str, Any]] = field(default_factory=list)
    dead_letters: list[DeadLetter] = field(default_factory=list)
    offsets: dict[tuple[str, int], int] = field(default_factory=dict)

    on_time: int = 0
    late_accepted: int = 0
    too_late: int = 0
    future_skew: int = 0
    duplicates: int = 0
    malformed: int = 0
    contract_violations: int = 0

    @property
    def applied(self) -> int:
        return self.on_time + self.late_accepted

    @property
    def total(self) -> int:
        return (
            self.applied
            + self.too_late
            + self.future_skew
            + self.duplicates
            + self.malformed
            + self.contract_violations
        )

    def as_dict(self) -> dict[str, int]:
        return {
            "on_time": self.on_time,
            "late_accepted": self.late_accepted,
            "too_late": self.too_late,
            "future_skew": self.future_skew,
            "duplicates": self.duplicates,
            "malformed": self.malformed,
            "contract_violations": self.contract_violations,
        }


class BatchProcessor:
    def __init__(
        self,
        contract: Contract,
        deduplicator: Deduplicator,
        windows: WindowManager,
    ) -> None:
        self.contract = contract
        self.dedup = deduplicator
        self.windows = windows
        self._unknown_fields_seen: set[str] = set()

    def process(self, records: list[RawRecord], now: datetime | None = None) -> BatchResult:
        now = now or datetime.now(tz=UTC)
        result = BatchResult()
        events: list[tuple[OrderEvent, RawRecord]] = []

        for record in records:
            result.offsets[(record.topic, record.partition)] = max(
                record.offset, result.offsets.get((record.topic, record.partition), -1)
            )
            parsed = self._parse(record, result)
            if parsed is not None:
                events.append((parsed, record))

        # Dedup runs on the batch, not per record: one ledger query for the
        # whole poll instead of one per event.
        new_events, duplicates = self.dedup.filter_batch([event for event, _ in events])
        result.duplicates += len(duplicates)

        # Consume each id once rather than testing membership in a set: when a
        # producer retry puts the same event_id twice in one poll, both copies
        # match the set and both would be applied.
        pending_ids = {event.event_id for event in new_events}

        for event, record in events:
            if event.event_id not in pending_ids:
                continue
            pending_ids.discard(event.event_id)
            self._route(event, record, result, now)

        if result.total:
            log.info("batch.processed", **result.as_dict(), records=len(records))
        return result

    # -- parsing -----------------------------------------------------------

    def _parse(self, record: RawRecord, result: BatchResult) -> OrderEvent | None:
        try:
            payload = decode(record.value)
        except MalformedRecord as exc:
            result.malformed += 1
            result.dead_letters.append(
                self._dead_letter(record, "malformed", str(exc), raw_text=True)
            )
            return None

        problems = self.contract.violations(payload)
        if problems:
            result.contract_violations += 1
            result.dead_letters.append(
                self._dead_letter(record, "contract_violation", "; ".join(problems))
            )
            self._record_handled(result, payload)
            return None

        unknown = self.contract.unknown_fields(payload) - self._unknown_fields_seen
        if unknown:
            # Logged once per field, not once per record: an added field on a
            # 5k/s topic would otherwise drown every other log line.
            self._unknown_fields_seen |= unknown
            log.warning("contract.unknown_fields", fields=sorted(unknown))

        try:
            return OrderEvent.model_validate(payload)
        except ValidationError as exc:
            result.contract_violations += 1
            result.dead_letters.append(
                self._dead_letter(record, "validation_error", str(exc)[:1000])
            )
            self._record_handled(result, payload)
            return None

    # -- routing -----------------------------------------------------------

    def _route(
        self, event: OrderEvent, record: RawRecord, result: BatchResult, now: datetime
    ) -> None:
        watermark_before = self.windows.watermark
        outcome = self.windows.add(event, now=now)

        result.ledger_entries.append(
            {
                "event_id": event.event_id,
                "merchant_id": event.merchant_id,
                "window_start": window_start_for(
                    event.occurred_at, self.windows.window_size_seconds
                ),
            }
        )

        match outcome:
            case Outcome.ON_TIME:
                result.on_time += 1
            case Outcome.LATE_ACCEPTED:
                result.late_accepted += 1
            case Outcome.TOO_LATE:
                result.too_late += 1
                result.late_rows.append(self._late_row(event, watermark_before, now))
                result.dead_letters.append(
                    self._dead_letter(
                        record,
                        "too_late",
                        f"window closed; watermark was "
                        f"{watermark_before.isoformat() if watermark_before else 'unset'}",
                    )
                )
            case Outcome.FUTURE_SKEW:
                result.future_skew += 1
                result.dead_letters.append(
                    self._dead_letter(
                        record,
                        "future_skew",
                        f"occurred_at {event.occurred_at.isoformat()} is further ahead than "
                        f"{self.windows.max_future_skew_seconds}s",
                    )
                )

    def _late_row(
        self, event: OrderEvent, watermark: datetime | None, now: datetime
    ) -> dict[str, Any]:
        window_start = window_start_for(event.occurred_at, self.windows.window_size_seconds)
        reference = watermark or now
        return {
            "event_id": event.event_id,
            "merchant_id": event.merchant_id,
            "event_type": str(event.event_type),
            "occurred_at": event.occurred_at,
            "window_start": window_start,
            "watermark_at": reference,
            "lateness_seconds": (reference - event.occurred_at).total_seconds(),
            "amount_minor": event.amount_minor,
            "payload": encode(event.model_dump()).decode("utf-8"),
        }

    def _record_handled(self, result: BatchResult, payload: dict[str, Any]) -> None:
        """Ledger an event we refused, so a replay does not re-emit its dead letter."""
        event_id = payload.get("event_id")
        if not isinstance(event_id, str) or not event_id:
            return
        result.ledger_entries.append(
            {
                "event_id": event_id,
                "merchant_id": str(payload.get("merchant_id") or "unknown"),
                "window_start": datetime.now(tz=UTC),
            }
        )

    def _dead_letter(
        self, record: RawRecord, reason: str, detail: str, raw_text: bool = False
    ) -> DeadLetter:
        if raw_text and record.value is not None:
            payload = record.value.decode("utf-8", errors="replace")[:10_000]
        else:
            payload = (record.value or b"").decode("utf-8", errors="replace")[:10_000]
        return DeadLetter(
            reason=reason,
            detail=detail,
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            key=decode_key(record.key),
            payload=payload,
        )
