"""Event generator.

This is a test harness, not a toy. A stream of perfectly formed, perfectly
ordered events proves nothing about a processor whose entire job is handling
the opposite, so the generator deliberately produces the four things that break
naive consumers:

* **duplicates** - the same `event_id` twice, as an at-least-once producer does
  after an ambiguous ack;
* **late events** - a real `occurred_at` from a minute or two ago, as a mobile
  client does after reconnecting;
* **very late events** - past the allowed lateness, so the too-late path is
  exercised rather than just described;
* **poison records** - malformed JSON, unknown event types, clock skew.

Everything is seeded, so a failing run reproduces exactly.
"""

from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

MERCHANTS = (
    "m_kioskone",
    "m_nordbakery",
    "m_grocerylane",
    "m_technest",
    "m_bloomshop",
    "m_cyclecity",
)
EVENT_TYPES = ("order_placed", "order_paid", "order_cancelled", "order_refunded")
CURRENCIES = ("EUR", "PLN", "SEK")
PAYMENT_METHODS = ("card", "blik", "transfer", "wallet")


@dataclass
class FaultRates:
    """Probability of each kind of misbehaviour, per event."""

    duplicate: float = 0.05
    late: float = 0.08
    very_late: float = 0.01
    malformed: float = 0.005
    unknown_event_type: float = 0.003
    future_skew: float = 0.002
    unknown_field: float = 0.01

    def validate(self) -> None:
        total = sum(
            (
                self.duplicate,
                self.late,
                self.very_late,
                self.malformed,
                self.unknown_event_type,
                self.future_skew,
            )
        )
        if total > 1.0:
            raise ValueError(f"fault rates sum to {total:.2f}, which leaves no clean events")


@dataclass
class EventGenerator:
    seed: int = 42
    merchants: tuple[str, ...] = MERCHANTS
    user_pool: int = 500
    late_seconds: tuple[int, int] = (30, 180)
    very_late_seconds: tuple[int, int] = (400, 1800)
    faults: FaultRates = field(default_factory=FaultRates)

    _random: random.Random = field(init=False)
    _recent: list[dict] = field(init=False, default_factory=list)
    emitted: int = 0

    def __post_init__(self) -> None:
        self.faults.validate()
        self._random = random.Random(self.seed)

    # -- public ------------------------------------------------------------

    def next_record(self, now: datetime | None = None) -> tuple[str, bytes]:
        """Produce one (key, value) pair ready for Kafka."""
        now = now or datetime.now(tz=timezone.utc)
        roll = self._random.random()
        faults = self.faults

        threshold = faults.duplicate
        if roll < threshold and self._recent:
            return self._duplicate()

        threshold += faults.malformed
        if roll < threshold:
            return self._malformed()

        threshold += faults.unknown_event_type
        if roll < threshold:
            return self._event(now, event_type="order_partially_refunded")

        threshold += faults.future_skew
        if roll < threshold:
            return self._event(now + timedelta(seconds=self._random.randint(120, 900)))

        threshold += faults.very_late
        if roll < threshold:
            return self._event(now - timedelta(seconds=self._random.randint(*self.very_late_seconds)))

        threshold += faults.late
        if roll < threshold:
            return self._event(now - timedelta(seconds=self._random.randint(*self.late_seconds)))

        return self._event(now)

    def burst(self, count: int, now: datetime | None = None) -> list[tuple[str, bytes]]:
        return [self.next_record(now) for _ in range(count)]

    # -- internals ---------------------------------------------------------

    def _event(self, occurred_at: datetime, event_type: str | None = None) -> tuple[str, bytes]:
        merchant = self._random.choice(self.merchants)
        chosen_type = event_type or self._random.choices(
            EVENT_TYPES, weights=(40, 45, 10, 5), k=1
        )[0]

        payload = {
            "event_id": str(uuid.UUID(int=self._random.getrandbits(128))),
            "event_type": chosen_type,
            "occurred_at": occurred_at.isoformat(),
            "ingested_at": datetime.now(tz=timezone.utc).isoformat(),
            "order_id": f"ord_{self._random.getrandbits(40):x}",
            "merchant_id": merchant,
            "user_id": f"u_{self._random.randrange(self.user_pool):04d}",
            "amount_minor": self._random.randrange(199, 25_000),
            "currency": self._random.choice(CURRENCIES),
            "payment_method": self._random.choice(PAYMENT_METHODS),
            "schema_version": 1,
        }
        if chosen_type in ("order_placed", "order_cancelled"):
            # Nothing has been charged yet, so there is no amount to report.
            payload["amount_minor"] = None

        if self._random.random() < self.faults.unknown_field:
            # An additive schema change nobody told the consumer about. Must
            # pass straight through and be logged once, not rejected.
            payload["loyalty_tier"] = self._random.choice(("bronze", "silver", "gold"))

        self._remember(payload)
        self.emitted += 1
        return merchant, json.dumps(payload).encode("utf-8")

    def _duplicate(self) -> tuple[str, bytes]:
        payload = self._random.choice(self._recent)
        self.emitted += 1
        return payload["merchant_id"], json.dumps(payload).encode("utf-8")

    def _malformed(self) -> tuple[str, bytes]:
        merchant = self._random.choice(self.merchants)
        broken = self._random.choice(
            (
                b'{"event_id": "abc", "event_type": ',          # truncated JSON
                b"not json at all",
                b'{"event_id": "", "event_type": "order_paid"}',  # empty required field
                b'["order_paid"]',                               # JSON, but not an object
                b'{"event_id": "x1", "event_type": "order_paid", "occurred_at": "yesterday",'
                b' "order_id": "o1", "merchant_id": "m_kioskone"}',
            )
        )
        self.emitted += 1
        return merchant, broken

    def _remember(self, payload: dict) -> None:
        """Keep a small window of recent events so duplicates are realistic:
        a producer retry repeats something recent, not something from an hour ago."""
        self._recent.append(payload)
        if len(self._recent) > 200:
            self._recent.pop(0)
