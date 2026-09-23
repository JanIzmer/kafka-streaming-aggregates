"""Contract enforcement for incoming records.

The JSON Schema file is the source of truth. Rather than pulling in a full
JSON Schema engine for a handful of rules, the checks the contract actually
declares are applied directly - which keeps the failure messages specific
enough to be useful in a dead letter.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from orders_stream.logging_conf import get_logger

log = get_logger(__name__)


class ContractError(RuntimeError):
    """The contract file itself is missing or malformed."""


@dataclass(frozen=True)
class Contract:
    name: str
    version: int
    topic: str
    key_field: str
    required: tuple[str, ...]
    properties: dict[str, Any]
    allowed_event_types: tuple[str, ...]
    allowed_lateness_seconds: int
    raw: dict[str, Any]

    def violations(self, payload: dict[str, Any]) -> list[str]:
        """Return why this payload is unacceptable; empty means it is fine."""
        problems: list[str] = []

        for field in self.required:
            if payload.get(field) in (None, ""):
                problems.append(f"{field}: required field is missing or empty")

        event_type = payload.get("event_type")
        if event_type is not None and event_type not in self.allowed_event_types:
            # Deliberately not tolerated. See contracts/README.md: silently
            # ignoring or silently counting an unknown type both change what the
            # aggregate means without anyone deciding to.
            problems.append(
                f"event_type: '{event_type}' is not in contract v{self.version} "
                f"({', '.join(self.allowed_event_types)})"
            )

        occurred_at = payload.get("occurred_at")
        if occurred_at is not None:
            try:
                datetime.fromisoformat(str(occurred_at).replace("Z", "+00:00"))
            except ValueError:
                problems.append(f"occurred_at: '{occurred_at}' is not an ISO-8601 timestamp")

        amount = payload.get("amount_minor")
        if amount is not None:
            if isinstance(amount, bool) or not isinstance(amount, int):
                problems.append(
                    f"amount_minor: expected integer minor units, got {type(amount).__name__}"
                )
            elif amount < 0:
                problems.append(f"amount_minor: {amount} is negative")

        currency = payload.get("currency")
        if currency is not None and (not isinstance(currency, str) or len(currency) != 3):
            problems.append(f"currency: '{currency}' is not a 3-letter code")

        return problems

    def unknown_fields(self, payload: dict[str, Any]) -> set[str]:
        return set(payload) - set(self.properties)


def load_contract(
    name: str = "order_event", version: int = 1, contracts_dir: Path | str = "contracts"
) -> Contract:
    path = Path(contracts_dir) / f"{name}.v{version}.json"
    if not path.exists():
        raise ContractError(f"contract file not found: {path}")

    doc = json.loads(path.read_text(encoding="utf-8"))
    try:
        meta = doc["x-contract"]
        return Contract(
            name=doc["title"],
            version=int(doc["version"]),
            topic=meta["topic"],
            key_field=meta["key"],
            required=tuple(doc["required"]),
            properties=doc["properties"],
            allowed_event_types=tuple(doc["properties"]["event_type"]["enum"]),
            allowed_lateness_seconds=int(meta["allowed_lateness_seconds"]),
            raw=doc,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ContractError(f"malformed contract {path}: {exc}") from exc
