"""The run loop.

Ordering inside a flush is the whole delivery guarantee, so it is spelled out:

    1. write aggregates + ledger + late + DLQ rows   (one Postgres transaction)
    2. publish dead letters to the DLQ topic
    3. snapshot window state as flushed
    4. mark event ids as seen in the in-memory dedup tier
    5. commit Kafka offsets

A crash between any two steps is survivable:

* between 1 and 5, Kafka replays the batch. The ledger rows from step 1 are
  already durable, so dedup drops every replayed event and the aggregates do
  not move. That is the at-least-once delivery turned into exactly-once
  *application*.
* before 1, nothing was written and the replay applies the batch once.

Committing offsets first would invert this into data loss, and it is the
default behaviour of every Kafka client - which is why `enable.auto.commit` is
off.
"""

from __future__ import annotations

import signal
import time
from datetime import datetime, timezone
from types import FrameType
from typing import Any

from orders_stream.config import Settings
from orders_stream.contract import load_contract
from orders_stream.dedup import Deduplicator
from orders_stream.logging_conf import get_logger
from orders_stream.metrics import Metrics
from orders_stream.processor.kafka_io import (
    DlqPublisher,
    consumer_config,
    producer_config,
    to_raw_record,
)
from orders_stream.processor.pipeline import BatchProcessor, BatchResult, RawRecord
from orders_stream.store import Repository
from orders_stream.windowing import WindowManager

log = get_logger(__name__)

LEDGER_PURGE_INTERVAL_SECONDS = 900


class ProcessorService:
    def __init__(
        self,
        settings: Settings,
        repository: Repository,
        consumer: Any,
        dlq: Any,
        metrics: Metrics | None = None,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.consumer = consumer
        self.dlq = dlq
        self.metrics = metrics or Metrics()

        contract = load_contract("order_event", 1, settings.contracts_dir)
        self.windows = WindowManager(
            window_size_seconds=settings.window_size_seconds,
            allowed_lateness_seconds=settings.allowed_lateness_seconds,
            max_future_skew_seconds=settings.max_future_skew_seconds,
        )
        self.dedup = Deduplicator(
            cache_size=settings.dedup_cache_size,
            ledger_lookup=repository.known_event_ids,
        )
        self.processor = BatchProcessor(contract, self.dedup, self.windows)

        self._running = False
        self._pending: BatchResult | None = None
        self._pending_ids: list[str] = []
        self._last_flush = time.monotonic()
        self._last_purge = time.monotonic()
        self._last_record_at = time.monotonic()

    # -- lifecycle ---------------------------------------------------------

    def install_signal_handlers(self) -> None:
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._handle_signal)

    def _handle_signal(self, signum: int, frame: FrameType | None) -> None:
        # Do not flush from inside the handler: it can fire mid-transaction.
        # Setting the flag lets the loop finish its current batch and shut down
        # through the same path as a normal stop.
        log.info("service.signal", signal=signal.Signals(signum).name)
        self._running = False

    def run(self) -> None:
        self.consumer.subscribe(
            [self.settings.kafka_topic],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )
        self._running = True
        log.info(
            "service.started",
            topic=self.settings.kafka_topic,
            group=self.settings.kafka_consumer_group,
            window_seconds=self.settings.window_size_seconds,
            allowed_lateness_seconds=self.settings.allowed_lateness_seconds,
        )

        try:
            while self._running:
                self._tick()
        finally:
            log.info("service.stopping")
            self._flush(force=True)
            self.consumer.close()
            self.repository.close()
            log.info("service.stopped")

    # -- the loop ----------------------------------------------------------

    def _tick(self) -> None:
        messages = self.consumer.consume(
            num_messages=self.settings.max_batch_size,
            timeout=self.settings.poll_timeout_seconds,
        )
        records: list[RawRecord] = []
        for message in messages or []:
            if message.error():
                # Errors here are broker-level (not poison records) and the
                # client has already retried. Log and keep going; a fatal one
                # raises on the next consume.
                log.error("kafka.consume_error", error=str(message.error()))
                continue
            records.append(to_raw_record(message))

        if records:
            self._last_record_at = time.monotonic()
            result = self.processor.process(records)
            self._accumulate(result)
            self.metrics.observe_batch(result)
        else:
            self._advance_idle_watermark()

        if self._should_flush():
            self._flush()

        if time.monotonic() - self._last_purge > LEDGER_PURGE_INTERVAL_SECONDS:
            self.repository.purge_dedup_ledger(self.settings.dedup_ledger_retention_hours)
            self._last_purge = time.monotonic()

        self.metrics.observe_state(self.windows, self.dedup)

    def _advance_idle_watermark(self) -> None:
        """Keep windows closing when a partition goes quiet.

        Without this, a merchant that stops trading leaves its last window open
        forever: the aggregate is never marked closed and the state is never
        evicted.
        """
        idle_for = time.monotonic() - self._last_record_at
        if idle_for < self.settings.watermark_idle_seconds:
            return
        if self.windows.advance_on_idle(datetime.now(tz=timezone.utc)):
            self._last_record_at = time.monotonic()

    def _accumulate(self, result: BatchResult) -> None:
        if self._pending is None:
            self._pending = result
        else:
            pending = self._pending
            pending.ledger_entries.extend(result.ledger_entries)
            pending.late_rows.extend(result.late_rows)
            pending.dead_letters.extend(result.dead_letters)
            pending.offsets.update(result.offsets)
            for name, value in result.as_dict().items():
                setattr(pending, name, getattr(pending, name) + value)
        self._pending_ids.extend(entry["event_id"] for entry in result.ledger_entries)

    def _should_flush(self) -> bool:
        if self._pending is None and not self.windows.dirty_aggregates():
            return False
        if time.monotonic() - self._last_flush >= self.settings.flush_interval_seconds:
            return True
        return len(self._pending_ids) >= self.settings.max_batch_size

    # -- the flush ---------------------------------------------------------

    def _flush(self, force: bool = False) -> None:
        """Persist everything accumulated, then commit offsets. See module docstring."""
        closed = self.windows.close_expired()
        deltas = self.windows.pending_deltas() + closed
        pending = self._pending

        if not deltas and pending is None:
            self._last_flush = time.monotonic()
            return

        ledger = pending.ledger_entries if pending else []
        late_rows = pending.late_rows if pending else []
        dead_letters = pending.dead_letters if pending else []
        checkpoints = self._checkpoints(pending)

        try:
            # 1. one transaction: aggregates, ledger, late events, dead letters
            self.repository.flush(
                deltas=deltas,
                ledger_entries=ledger,
                late_events=late_rows,
                dead_letters=dead_letters,
                checkpoints=checkpoints,
            )
            # 2. DLQ topic, before offsets move
            if dead_letters:
                self.dlq.publish(dead_letters)
        except Exception:
            # Offsets are NOT committed and window state is NOT snapshotted, so
            # the next poll replays this batch and the ledger makes the replay
            # a no-op for anything that did land.
            log.exception("flush.failed", deltas=len(deltas), ledger=len(ledger))
            self.metrics.flush_failures.inc()
            raise

        # 3 + 4: only after the write succeeded
        self.windows.mark_flushed()
        self.dedup.commit(self._pending_ids)

        # 5: last, always
        self.consumer.commit(asynchronous=False)

        log.info(
            "flush.committed",
            aggregates=len(deltas),
            closed_windows=len(closed),
            events=len(self._pending_ids),
            forced=force,
        )
        self._pending = None
        self._pending_ids = []
        self._last_flush = time.monotonic()
        self.metrics.flushes.inc()

    def _checkpoints(self, pending: BatchResult | None) -> list[dict[str, Any]]:
        if pending is None:
            return []
        watermark = self.windows.watermark
        return [
            {
                "consumer_group": self.settings.kafka_consumer_group,
                "topic": topic,
                "partition": partition,
                "last_offset": offset,
                "watermark_at": watermark,
            }
            for (topic, partition), offset in pending.offsets.items()
        ]

    # -- rebalancing -------------------------------------------------------

    def _on_assign(self, consumer: Any, partitions: list[Any]) -> None:
        log.info("kafka.assigned", partitions=[p.partition for p in partitions])
        # Cooperative protocol: incremental_assign, not assign, so partitions we
        # already own keep their in-memory window state.
        consumer.incremental_assign(partitions)

    def _on_revoke(self, consumer: Any, partitions: list[Any]) -> None:
        """Flush before losing partitions.

        The window state for a revoked partition is about to be thrown away.
        Whatever it accumulated has to reach Postgres first, or those events are
        lost: their offsets were never committed, but the replay will land on a
        different consumer whose dedup ledger *does* contain them.
        """
        log.info("kafka.revoking", partitions=[p.partition for p in partitions])
        try:
            self._flush(force=True)
        except Exception:
            log.exception("kafka.revoke_flush_failed")
        consumer.incremental_unassign(partitions)
