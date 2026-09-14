from __future__ import annotations

import pytest

from orders_stream.dedup import Deduplicator, LruSet
from tests.factories import event


def test_lru_evicts_oldest_first():
    lru = LruSet(max_size=3)

    for item in ("a", "b", "c", "d"):
        lru.add(item)

    assert "a" not in lru
    assert {"b", "c", "d"} <= {"b", "c", "d"}
    assert len(lru) == 3


def test_lru_readding_refreshes_recency():
    lru = LruSet(max_size=3)
    lru.update(["a", "b", "c"])

    lru.add("a")  # a is now the most recent
    lru.add("d")  # evicts b, not a

    assert "a" in lru
    assert "b" not in lru


def test_lru_rejects_zero_size():
    with pytest.raises(ValueError, match="at least 1"):
        LruSet(0)


def test_first_sighting_passes_through(dedup: Deduplicator):
    events = [event(event_id="e1"), event(event_id="e2")]

    new, duplicates = dedup.filter_batch(events)

    assert [e.event_id for e in new] == ["e1", "e2"]
    assert duplicates == []


def test_duplicate_within_one_batch_is_caught(dedup: Deduplicator):
    """A producer retry inside a single poll: nothing has been committed yet,
    so only the in-batch check can see it."""
    events = [event(event_id="e1"), event(event_id="e1"), event(event_id="e2")]

    new, duplicates = dedup.filter_batch(events)

    assert [e.event_id for e in new] == ["e1", "e2"]
    assert len(duplicates) == 1
    assert dedup.stats.duplicates_in_batch == 1


def test_duplicate_across_batches_is_caught_after_commit(dedup: Deduplicator):
    first, _ = dedup.filter_batch([event(event_id="e1")])
    dedup.commit([e.event_id for e in first])

    new, duplicates = dedup.filter_batch([event(event_id="e1")])

    assert new == []
    assert len(duplicates) == 1
    assert dedup.stats.duplicates_memory == 1


def test_uncommitted_ids_are_not_treated_as_seen(dedup: Deduplicator):
    """A flush that failed must not make its events look like duplicates on the
    retry - that would drop them permanently."""
    dedup.filter_batch([event(event_id="e1")])  # no commit: the flush failed

    new, duplicates = dedup.filter_batch([event(event_id="e1")])

    assert [e.event_id for e in new] == ["e1"]
    assert duplicates == []


def test_ledger_catches_what_memory_has_forgotten():
    """After a restart the memory tier is empty; the durable ledger is what
    stops the replayed batch from being applied twice."""
    ledger = {"e1"}
    dedup = Deduplicator(cache_size=10, ledger_lookup=lambda ids: ledger & set(ids))

    new, duplicates = dedup.filter_batch([event(event_id="e1"), event(event_id="e2")])

    assert [e.event_id for e in new] == ["e2"]
    assert dedup.stats.duplicates_ledger == 1


def test_ledger_is_queried_once_per_batch():
    calls = []

    def lookup(ids):
        calls.append(list(ids))
        return set()

    dedup = Deduplicator(cache_size=10, ledger_lookup=lookup)
    dedup.filter_batch([event(event_id=f"e{i}") for i in range(50)])

    assert len(calls) == 1
    assert len(calls[0]) == 50


def test_ledger_is_not_queried_for_ids_already_in_memory():
    calls = []

    def lookup(ids):
        calls.append(list(ids))
        return set()

    dedup = Deduplicator(cache_size=10, ledger_lookup=lookup)
    new, _ = dedup.filter_batch([event(event_id="e1")])
    dedup.commit([e.event_id for e in new])
    dedup.filter_batch([event(event_id="e1"), event(event_id="e2")])

    assert calls[-1] == ["e2"]


def test_empty_batch_is_a_no_op(dedup: Deduplicator):
    assert dedup.filter_batch([]) == ([], [])


def test_stats_track_every_tier():
    dedup = Deduplicator(cache_size=10, ledger_lookup=lambda ids: {"e3"} & set(ids))
    new, _ = dedup.filter_batch([event(event_id="e1")])
    dedup.commit([e.event_id for e in new])

    dedup.filter_batch(
        [event(event_id="e1"), event(event_id="e2"), event(event_id="e2"), event(event_id="e3")]
    )

    assert dedup.stats.duplicates_memory == 1
    assert dedup.stats.duplicates_in_batch == 1
    assert dedup.stats.duplicates_ledger == 1
    assert dedup.stats.duplicates_total == 3
