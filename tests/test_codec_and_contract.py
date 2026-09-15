from __future__ import annotations

import json

import pytest

from orders_stream.codec import MalformedRecord, decode, decode_key, encode
from orders_stream.contract import ContractError, load_contract
from tests.factories import payload


def test_decode_reads_a_json_object():
    assert decode(json.dumps({"a": 1}).encode())["a"] == 1


def test_decode_rejects_a_tombstone():
    with pytest.raises(MalformedRecord, match="null"):
        decode(None)


def test_decode_rejects_non_utf8():
    with pytest.raises(MalformedRecord, match="UTF-8"):
        decode(b"\xff\xfe\x00")


def test_decode_rejects_broken_json():
    with pytest.raises(MalformedRecord, match="valid JSON"):
        decode(b'{"a": ')


def test_decode_rejects_a_non_object():
    with pytest.raises(MalformedRecord, match="expected an object"):
        decode(b"[1, 2, 3]")


def test_decode_rejects_an_oversized_record():
    with pytest.raises(MalformedRecord, match="over the"):
        decode(b"x" * 2_000_000)


def test_encode_survives_values_json_cannot_serialise():
    """The DLQ write must never fail - its whole job is preserving a failure."""
    from datetime import datetime

    assert b"2026" in encode({"when": datetime(2026, 1, 1)})


def test_decode_key_returns_none_for_unreadable_keys():
    assert decode_key(b"\xff") is None
    assert decode_key(None) is None
    assert decode_key(b"m_kioskone") == "m_kioskone"


def test_contract_exposes_the_streaming_guarantees(contract):
    assert contract.topic == "orders.v1"
    assert contract.key_field == "merchant_id"
    assert contract.allowed_lateness_seconds == 300
    assert "order_paid" in contract.allowed_event_types


def test_a_valid_payload_has_no_violations(contract):
    assert contract.violations(payload()) == []


def test_missing_required_fields_are_listed(contract):
    broken = payload()
    del broken["order_id"]
    broken["merchant_id"] = ""

    problems = contract.violations(broken)

    assert any("order_id" in p for p in problems)
    assert any("merchant_id" in p for p in problems)


def test_a_bad_timestamp_is_a_violation(contract):
    assert any("ISO-8601" in p for p in contract.violations(payload(occurred_at="yesterday")))


def test_a_negative_amount_is_a_violation(contract):
    assert any("negative" in p for p in contract.violations(payload(amount_minor=-1)))


def test_a_float_amount_is_a_violation(contract):
    """Minor units must be integers: float addition is not associative, so a
    replay would produce a different total."""
    assert any("integer" in p for p in contract.violations(payload(amount_minor=19.99)))


def test_a_null_amount_is_allowed(contract):
    assert contract.violations(payload(amount_minor=None)) == []


def test_a_bad_currency_is_a_violation(contract):
    assert any("3-letter" in p for p in contract.violations(payload(currency="EURO")))


def test_unknown_fields_are_reported_but_not_violations(contract):
    assert contract.unknown_fields(payload(loyalty_tier="gold")) == {"loyalty_tier"}
    assert contract.violations(payload(loyalty_tier="gold")) == []


def test_a_missing_contract_file_raises():
    with pytest.raises(ContractError, match="not found"):
        load_contract("order_event", 99)
