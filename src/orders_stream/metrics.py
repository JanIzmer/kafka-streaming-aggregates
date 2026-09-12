"""Prometheus metrics.

Only metrics somebody would actually alert on. The three that matter most:

* `orders_stream_events_total{outcome="too_late"}` rising - the allowed
  lateness no longer matches reality, and data is being dropped from the
  aggregates.
* `orders_stream_watermark_lag_seconds` rising - the processor is falling
  behind the stream. This is the leading indicator; consumer lag confirms it.
* `orders_stream_flush_failures_total` - Postgres is refusing writes, which
  means offsets have stopped moving and the backlog is growing.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from prometheus_client import Counter, Gauge, start_http_server

from orders_stream.logging_conf import get_logger

if TYPE_CHECKING:  # pragma: no cover
    from orders_stream.dedup import Deduplicator
    from orders_stream.processor.pipeline import BatchResult
    from orders_stream.windowing import WindowManager

log = get_logger(__name__)

NAMESPACE = "orders_stream"


class Metrics:
    def __init__(self) -> None:
        self.events = Counter(
            f"{NAMESPACE}_events_total",
            "Records handled, by how they were treated",
            ["outcome"],
        )
        self.flushes = Counter(f"{NAMESPACE}_flushes_total", "Successful flush + commit cycles")
        self.flush_failures = Counter(
            f"{NAMESPACE}_flush_failures_total", "Flushes that raised and were retried"
        )
        self.open_windows = Gauge(
            f"{NAMESPACE}_open_windows", "Windows held in memory - the state size"
        )
        self.tracked_users = Gauge(
            f"{NAMESPACE}_tracked_users", "User ids held across open windows"
        )
        self.dedup_cache = Gauge(
            f"{NAMESPACE}_dedup_cache_entries", "Event ids in the in-memory dedup tier"
        )
        self.watermark_lag = Gauge(
            f"{NAMESPACE}_watermark_lag_seconds",
            "Wall clock minus the watermark; how far behind event time we are",
        )

    def observe_batch(self, result: BatchResult) -> None:
        for outcome, count in result.as_dict().items():
            if count:
                self.events.labels(outcome=outcome).inc(count)

    def observe_state(self, windows: WindowManager, dedup: Deduplicator) -> None:
        self.open_windows.set(windows.open_window_count)
        self.tracked_users.set(windows.tracked_user_count)
        self.dedup_cache.set(dedup.cache_len)
        if windows.watermark is not None:
            lag = (datetime.now(tz=timezone.utc) - windows.watermark).total_seconds()
            self.watermark_lag.set(lag)

    @staticmethod
    def serve(port: int) -> None:
        start_http_server(port)
        log.info("metrics.serving", port=port)


class NullMetrics:
    """No-op implementation for tests and one-shot CLI runs."""

    def __getattr__(self, _name: str) -> Any:
        return self

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        return None

    def observe_batch(self, result: Any) -> None:
        return None

    def observe_state(self, windows: Any, dedup: Any) -> None:
        return None
