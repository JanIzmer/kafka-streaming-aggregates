from __future__ import annotations

from pathlib import Path

import pytest

from orders_stream.contract import load_contract
from orders_stream.dedup import Deduplicator
from orders_stream.processor.pipeline import BatchProcessor
from orders_stream.windowing import WindowManager

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def contract():
    return load_contract("order_event", 1, REPO_ROOT / "contracts")


@pytest.fixture
def windows() -> WindowManager:
    return WindowManager(
        window_size_seconds=60, allowed_lateness_seconds=120, max_future_skew_seconds=60
    )


@pytest.fixture
def dedup() -> Deduplicator:
    return Deduplicator(cache_size=1000)


@pytest.fixture
def processor(contract, dedup, windows) -> BatchProcessor:
    return BatchProcessor(contract=contract, deduplicator=dedup, windows=windows)
