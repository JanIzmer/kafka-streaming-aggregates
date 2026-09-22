from __future__ import annotations

from prometheus_client import CollectorRegistry

from orders_stream.metrics import Metrics, NullMetrics
from orders_stream.processor.pipeline import BatchResult
from orders_stream.windowing import WindowManager
from tests.factories import at, event


def test_two_collectors_can_coexist_in_one_process():
    """A second ProcessorService in the same interpreter must not die on
    DuplicateTimeseries - which is exactly what the integration tests build."""
    first = Metrics(registry=CollectorRegistry())
    second = Metrics(registry=CollectorRegistry())

    assert first is not second


def test_batch_outcomes_are_counted_by_label():
    registry = CollectorRegistry()
    metrics = Metrics(registry=registry)
    result = BatchResult(on_time=3, too_late=1, duplicates=2)

    metrics.observe_batch(result)

    assert registry.get_sample_value("orders_stream_events_total", {"outcome": "on_time"}) == 3
    assert registry.get_sample_value("orders_stream_events_total", {"outcome": "too_late"}) == 1
    assert registry.get_sample_value("orders_stream_events_total", {"outcome": "malformed"}) is None


def test_state_gauges_follow_the_window_manager():
    registry = CollectorRegistry()
    metrics = Metrics(registry=registry)
    windows = WindowManager(window_size_seconds=60, allowed_lateness_seconds=120)
    windows.add(event(user_id="u1", occurred_at=at(10)), now=at(10))

    from orders_stream.dedup import Deduplicator

    metrics.observe_state(windows, Deduplicator(cache_size=10))

    assert registry.get_sample_value("orders_stream_open_windows") == 1
    assert registry.get_sample_value("orders_stream_tracked_users") == 1


def test_null_metrics_absorbs_everything():
    null = NullMetrics()

    null.observe_batch(BatchResult(on_time=1))
    null.observe_state(WindowManager(), None)
    null.flush_failures.inc()
    null.flushes.inc()
