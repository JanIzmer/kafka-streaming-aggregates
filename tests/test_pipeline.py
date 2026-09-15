from __future__ import annotations

import json

from orders_stream.processor.pipeline import BatchProcessor
from tests.factories import at, payload, record


def test_a_clean_batch_is_applied_and_ledgered(processor: BatchProcessor):
    result = processor.process([record(), record()], now=at(0))

    assert result.on_time == 2
    assert len(result.ledger_entries) == 2
    assert result.dead_letters == []


def test_a_duplicate_is_counted_and_not_applied_twice(processor: BatchProcessor):
    same = payload(event_id="dup-1")

    result = processor.process([record(same), record(same)], now=at(0))

    assert result.on_time == 1
    assert result.duplicates == 1
    assert processor.windows.dirty_aggregates()[0].events_total == 1


def test_undecodable_bytes_go_to_the_dlq_without_stopping_the_batch(processor: BatchProcessor):
    result = processor.process([record(b"{not json"), record()], now=at(0))

    assert result.malformed == 1
    assert result.on_time == 1
    assert result.dead_letters[0].reason == "malformed"


def test_a_poison_record_does_not_block_the_partition(processor: BatchProcessor):
    """The failure mode this avoids: retrying an unparseable record forever and
    never advancing the offset."""
    records = [record(b"garbage") for _ in range(5)] + [record()]

    result = processor.process(records, now=at(0))

    assert result.malformed == 5
    assert result.on_time == 1
    assert max(result.offsets.values()) == records[-1].offset


def test_a_missing_required_field_is_a_contract_violation(processor: BatchProcessor):
    broken = payload()
    del broken["merchant_id"]

    result = processor.process([record(broken)], now=at(0))

    assert result.contract_violations == 1
    assert "merchant_id" in result.dead_letters[0].detail


def test_an_unknown_event_type_is_dead_lettered_not_ignored(processor: BatchProcessor):
    """Silently dropping it would make revenue quietly wrong; silently counting
    it would change what revenue means. Neither is ours to decide."""
    result = processor.process([record(payload(event_type="order_disputed"))], now=at(0))

    assert result.contract_violations == 1
    assert "order_disputed" in result.dead_letters[0].detail


def test_a_rejected_record_is_still_ledgered(processor: BatchProcessor):
    """Otherwise a replay writes the same dead letter a second time."""
    result = processor.process([record(payload(event_id="bad-1", event_type="nope"))], now=at(0))

    assert [entry["event_id"] for entry in result.ledger_entries] == ["bad-1"]


def test_an_unknown_extra_field_passes_through(processor: BatchProcessor):
    result = processor.process([record(payload(loyalty_tier="gold"))], now=at(0))

    assert result.on_time == 1
    assert result.dead_letters == []


def test_a_too_late_event_is_recorded_in_both_places(processor: BatchProcessor):
    processor.process([record(payload(occurred_at=at(10).isoformat()))], now=at(10))
    processor.process([record(payload(occurred_at=at(400).isoformat()))], now=at(400))

    result = processor.process([record(payload(occurred_at=at(20).isoformat()))], now=at(400))

    assert result.too_late == 1
    assert len(result.late_rows) == 1
    assert result.dead_letters[0].reason == "too_late"
    assert result.late_rows[0]["lateness_seconds"] > 0


def test_clock_skew_is_dead_lettered(processor: BatchProcessor):
    result = processor.process([record(payload(occurred_at=at(7200).isoformat()))], now=at(0))

    assert result.future_skew == 1
    assert result.dead_letters[0].reason == "future_skew"


def test_offsets_track_the_highest_seen_per_partition(processor: BatchProcessor):
    records = [record(offset=5), record(offset=9), record(offset=7)]

    result = processor.process(records, now=at(0))

    assert result.offsets[("orders.v1", 0)] == max(r.offset for r in records)


def test_dead_letters_carry_enough_context_to_replay(processor: BatchProcessor):
    result = processor.process([record(b"\xff\xfe not utf8")], now=at(0))

    letter = result.dead_letters[0]
    assert (letter.topic, letter.partition) == ("orders.v1", 0)
    assert letter.offset is not None
    assert letter.payload


def test_a_json_array_is_rejected_as_malformed(processor: BatchProcessor):
    result = processor.process([record(json.dumps(["order_paid"]).encode())], now=at(0))

    assert result.malformed == 1
    assert "expected an object" in result.dead_letters[0].detail


def test_result_totals_account_for_every_record(processor: BatchProcessor):
    records = [record(), record(b"garbage"), record(payload(event_type="nope"))]

    result = processor.process(records, now=at(0))

    assert result.total == len(records)
