"""Deduplication.

The producer is at-least-once, so the same `event_id` can and does arrive more
than once: a producer retry after an ambiguous ack, a consumer restart replaying
from the last committed offset, a rebalance re-delivering an in-flight batch.

Two tiers, because one is not enough:

1. **In-memory LRU.** Catches the overwhelming majority - duplicates almost
   always arrive close together. Costs one set lookup.
2. **Durable ledger in Postgres.** Catches the rest: a restart empties the
   memory tier, and a replay from a committed offset would otherwise re-apply
   every event in the uncommitted batch.

The ledger is written **in the same transaction as the aggregates**. That is the
whole trick: either both the aggregate update and the "I have seen this id"
record land, or neither does. A separate transaction would leave a window where
a crash double-counts, which is exactly the case dedup exists to prevent.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterable
from dataclasses import dataclass, field

from orders_stream.logging_conf import get_logger

log = get_logger(__name__)


class LruSet:
    """Fixed-size set with least-recently-added eviction."""

    def __init__(self, max_size: int) -> None:
        if max_size < 1:
            raise ValueError("max_size must be at least 1")
        self.max_size = max_size
        self._items: OrderedDict[str, None] = OrderedDict()

    def __contains__(self, item: str) -> bool:
        return item in self._items

    def __len__(self) -> int:
        return len(self._items)

    def add(self, item: str) -> None:
        if item in self._items:
            self._items.move_to_end(item)
            return
        self._items[item] = None
        if len(self._items) > self.max_size:
            self._items.popitem(last=False)

    def update(self, items: Iterable[str]) -> None:
        for item in items:
            self.add(item)


@dataclass
class DedupStats:
    seen: int = 0
    duplicates_memory: int = 0
    duplicates_ledger: int = 0
    duplicates_in_batch: int = 0

    @property
    def duplicates_total(self) -> int:
        return self.duplicates_memory + self.duplicates_ledger + self.duplicates_in_batch


@dataclass
class Deduplicator:
    """Two-tier dedup over `event_id`.

    `ledger_lookup` is injected so the hot path can be tested without a
    database, and so the ledger can be swapped for Redis without touching this
    logic.
    """

    cache_size: int = 200_000
    ledger_lookup: object | None = None
    _cache: LruSet = field(init=False)
    stats: DedupStats = field(default_factory=DedupStats)

    def __post_init__(self) -> None:
        self._cache = LruSet(self.cache_size)

    @property
    def cache_len(self) -> int:
        return len(self._cache)

    def filter_batch(self, events: list) -> tuple[list, list]:
        """Split a batch into (new, duplicate), preserving order.

        Duplicates *within* the batch are caught too - a producer retry inside
        one poll is common, and the memory tier alone would not see it because
        nothing has been committed yet.
        """
        if not events:
            return [], []

        ids = [event.event_id for event in events]
        self.stats.seen += len(ids)

        # One round trip for the whole batch rather than one per event: at
        # 5k events/s a per-event ledger query is the bottleneck, not the CPU.
        unknown_to_memory = [eid for eid in ids if eid not in self._cache]
        known_to_ledger: set[str] = set()
        if unknown_to_memory and self.ledger_lookup is not None:
            known_to_ledger = set(self.ledger_lookup(unknown_to_memory))  # type: ignore[operator]

        new_events, duplicates = [], []
        seen_in_batch: set[str] = set()

        for event in events:
            eid = event.event_id
            if eid in seen_in_batch:
                self.stats.duplicates_in_batch += 1
                duplicates.append(event)
            elif eid in self._cache:
                self.stats.duplicates_memory += 1
                duplicates.append(event)
            elif eid in known_to_ledger:
                self.stats.duplicates_ledger += 1
                duplicates.append(event)
            else:
                seen_in_batch.add(eid)
                new_events.append(event)

        if duplicates:
            log.debug(
                "dedup.filtered",
                batch=len(events),
                new=len(new_events),
                duplicates=len(duplicates),
            )
        return new_events, duplicates

    def commit(self, event_ids: Iterable[str]) -> None:
        """Mark ids as seen in the memory tier.

        Called only *after* the transaction that persisted both the aggregates
        and the ledger rows has committed. Marking them earlier would drop
        events on a rollback - they would look like duplicates on the retry.
        """
        self._cache.update(event_ids)
