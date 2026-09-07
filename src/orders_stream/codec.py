"""Decoding records off the topic.

Everything that can go wrong between "bytes on a partition" and "a dict" is
classified here, because each class needs a different response:

* `MalformedRecord` - it will never parse, however many times we retry. Straight
  to the DLQ, commit the offset, move on. Retrying it would block the partition
  forever, which is the classic poison-pill outage.
* Anything else propagates and stops the consumer, because a decode failure we
  did not anticipate is a bug, not bad data.
"""

from __future__ import annotations

import json
from typing import Any

MAX_PAYLOAD_BYTES = 1_000_000


class MalformedRecord(ValueError):
    """The record cannot be parsed and never will be."""


def decode(raw: bytes | None) -> dict[str, Any]:
    if raw is None:
        # A tombstone on a topic that is not compacted is a producer bug.
        raise MalformedRecord("record value is null (tombstone on a non-compacted topic)")
    if len(raw) > MAX_PAYLOAD_BYTES:
        raise MalformedRecord(f"record is {len(raw)} bytes, over the {MAX_PAYLOAD_BYTES} limit")

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise MalformedRecord(f"record is not valid UTF-8: {exc}") from exc

    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise MalformedRecord(f"record is not valid JSON: {exc}") from exc

    if not isinstance(payload, dict):
        raise MalformedRecord(f"record is a JSON {type(payload).__name__}, expected an object")

    return payload


def encode(payload: dict[str, Any]) -> bytes:
    """Serialise for the DLQ. `default=str` so a datetime never fails a write
    whose whole purpose is to preserve a failure."""
    return json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")


def decode_key(raw: bytes | None) -> str | None:
    if raw is None:
        return None
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return None
