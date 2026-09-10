"""The Postgres sink.

One public method matters: `flush`. It writes the aggregate deltas, the dedup
ledger entries, the late-event records and the dead letters **in a single
transaction**.

That is the core of the delivery guarantee. Offsets are committed to Kafka only
after this transaction returns, so the two possible crash points are:

* crash before the commit -> nothing was written, Kafka replays the batch, the
  ledger has no record of those ids, the batch is applied once;
* crash after the commit but before the offset commit -> everything was
  written, Kafka replays the batch, the ledger *does* have those ids, dedup
  drops them all, and the aggregates are untouched.

Either way each event is applied exactly once. Splitting the ledger write into
its own transaction would open a gap where the second case double-counts.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from orders_stream.logging_conf import get_logger
from orders_stream.models import DeadLetter
from orders_stream.windowing import WindowDelta

log = get_logger(__name__)

SQL_DIR = Path(__file__).parent / "sql"

UPSERT_AGGREGATE = """
INSERT INTO agg_orders_minute (
    window_start, window_end, merchant_id,
    events_total, orders_placed, orders_paid, orders_cancelled, orders_refunded,
    gross_amount_minor, refunded_amount_minor, net_amount_minor,
    distinct_users, first_event_at, last_event_at,
    late_events_applied, revision, is_closed, updated_at
)
VALUES (
    %(window_start)s, %(window_end)s, %(merchant_id)s,
    %(events_total)s, %(orders_placed)s, %(orders_paid)s, %(orders_cancelled)s, %(orders_refunded)s,
    %(gross_amount_minor)s, %(refunded_amount_minor)s,
    %(gross_amount_minor)s - %(refunded_amount_minor)s,
    %(distinct_users)s, %(first_event_at)s, %(last_event_at)s,
    %(late_events_applied)s, 1, %(is_closed)s, now()
)
ON CONFLICT (window_start, merchant_id) DO UPDATE SET
    events_total          = agg_orders_minute.events_total          + EXCLUDED.events_total,
    orders_placed         = agg_orders_minute.orders_placed         + EXCLUDED.orders_placed,
    orders_paid           = agg_orders_minute.orders_paid           + EXCLUDED.orders_paid,
    orders_cancelled      = agg_orders_minute.orders_cancelled      + EXCLUDED.orders_cancelled,
    orders_refunded       = agg_orders_minute.orders_refunded       + EXCLUDED.orders_refunded,
    gross_amount_minor    = agg_orders_minute.gross_amount_minor    + EXCLUDED.gross_amount_minor,
    refunded_amount_minor = agg_orders_minute.refunded_amount_minor + EXCLUDED.refunded_amount_minor,
    net_amount_minor      = (agg_orders_minute.gross_amount_minor + EXCLUDED.gross_amount_minor)
                          - (agg_orders_minute.refunded_amount_minor + EXCLUDED.refunded_amount_minor),
    distinct_users        = GREATEST(agg_orders_minute.distinct_users, EXCLUDED.distinct_users),
    first_event_at        = LEAST(
                                COALESCE(agg_orders_minute.first_event_at, EXCLUDED.first_event_at),
                                COALESCE(EXCLUDED.first_event_at, agg_orders_minute.first_event_at)),
    last_event_at         = GREATEST(
                                COALESCE(agg_orders_minute.last_event_at, EXCLUDED.last_event_at),
                                COALESCE(EXCLUDED.last_event_at, agg_orders_minute.last_event_at)),
    late_events_applied   = agg_orders_minute.late_events_applied   + EXCLUDED.late_events_applied,
    -- Only a restatement bumps the revision. A normal in-progress update of an
    -- open window is not a correction, and treating it as one would make the
    -- counter meaningless.
    revision              = agg_orders_minute.revision
                          + CASE WHEN EXCLUDED.late_events_applied > 0 THEN 1 ELSE 0 END,
    is_closed             = agg_orders_minute.is_closed OR EXCLUDED.is_closed,
    updated_at            = now()
"""

INSERT_LEDGER = """
INSERT INTO dedup_ledger (event_id, merchant_id, window_start)
VALUES (%(event_id)s, %(merchant_id)s, %(window_start)s)
ON CONFLICT (event_id) DO NOTHING
"""

INSERT_LATE = """
INSERT INTO late_events (
    event_id, merchant_id, event_type, occurred_at, window_start,
    watermark_at, lateness_seconds, amount_minor, payload
)
VALUES (
    %(event_id)s, %(merchant_id)s, %(event_type)s, %(occurred_at)s, %(window_start)s,
    %(watermark_at)s, %(lateness_seconds)s, %(amount_minor)s, %(payload)s
)
"""

INSERT_DLQ = """
INSERT INTO dlq_events (reason, detail, topic, partition, "offset", key, payload, failed_at)
VALUES (%(reason)s, %(detail)s, %(topic)s, %(partition)s, %(offset)s, %(key)s, %(payload)s, %(failed_at)s)
ON CONFLICT (topic, partition, "offset") DO NOTHING
"""

UPSERT_CHECKPOINT = """
INSERT INTO consumer_checkpoint (consumer_group, topic, partition, last_offset, watermark_at)
VALUES (%(consumer_group)s, %(topic)s, %(partition)s, %(last_offset)s, %(watermark_at)s)
ON CONFLICT (consumer_group, topic, partition) DO UPDATE SET
    last_offset  = EXCLUDED.last_offset,
    watermark_at = EXCLUDED.watermark_at,
    updated_at   = now()
"""


class Repository:
    def __init__(self, dsn: str, min_size: int = 1, max_size: int = 4) -> None:
        from psycopg_pool import ConnectionPool

        self._pool = ConnectionPool(dsn, min_size=min_size, max_size=max_size, open=True)

    def close(self) -> None:
        self._pool.close()

    # -- schema ------------------------------------------------------------

    def apply_schema(self) -> None:
        script = (SQL_DIR / "001_schema.sql").read_text(encoding="utf-8")
        with self._pool.connection() as connection:
            connection.execute(script)
        log.info("store.schema_applied")

    # -- dedup -------------------------------------------------------------

    def known_event_ids(self, event_ids: Sequence[str]) -> set[str]:
        """Which of these ids have already been applied.

        One query for the whole batch. A per-event lookup is the first thing to
        become the bottleneck at a few thousand events per second.
        """
        if not event_ids:
            return set()
        with self._pool.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT event_id FROM dedup_ledger WHERE event_id = ANY(%s)",
                (list(event_ids),),
            )
            return {row[0] for row in cursor.fetchall()}

    def purge_dedup_ledger(self, retention_hours: int, batch_size: int = 50_000) -> int:
        """Delete ledger rows older than the retention window.

        Deleted in bounded batches rather than one statement: a single DELETE
        over millions of rows takes a long lock and bloats the WAL, and this
        runs while the processor is consuming.
        """
        cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=retention_hours)
        removed = 0
        with self._pool.connection() as connection:
            while True:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        DELETE FROM dedup_ledger
                        WHERE event_id IN (
                            SELECT event_id FROM dedup_ledger WHERE seen_at < %s LIMIT %s
                        )
                        """,
                        (cutoff, batch_size),
                    )
                    deleted = cursor.rowcount
                connection.commit()
                removed += deleted
                if deleted < batch_size:
                    break
        if removed:
            log.info("store.ledger_purged", rows=removed, older_than=cutoff.isoformat())
        return removed

    # -- the write path ----------------------------------------------------

    def flush(
        self,
        deltas: Iterable[WindowDelta],
        ledger_entries: Iterable[dict[str, Any]],
        late_events: Iterable[dict[str, Any]] = (),
        dead_letters: Iterable[DeadLetter] = (),
        checkpoints: Iterable[dict[str, Any]] = (),
    ) -> int:
        """Persist one batch atomically. Returns the number of aggregate rows touched."""
        aggregate_rows = [
            {
                "window_start": delta.key.window_start,
                "window_end": delta.window_end,
                "merchant_id": delta.key.merchant_id,
                "events_total": delta.events_total,
                "orders_placed": delta.orders_placed,
                "orders_paid": delta.orders_paid,
                "orders_cancelled": delta.orders_cancelled,
                "orders_refunded": delta.orders_refunded,
                "gross_amount_minor": delta.gross_amount_minor,
                "refunded_amount_minor": delta.refunded_amount_minor,
                "distinct_users": delta.distinct_users,
                "first_event_at": delta.first_event_at,
                "last_event_at": delta.last_event_at,
                "late_events_applied": delta.late_events_applied,
                "is_closed": delta.is_closed,
            }
            for delta in deltas
        ]
        ledger_rows = list(ledger_entries)
        late_rows = list(late_events)
        dlq_rows = [letter.as_row() for letter in dead_letters]
        checkpoint_rows = list(checkpoints)

        with self._pool.connection() as connection:
            # psycopg opens a transaction implicitly and commits on a clean
            # exit; any exception rolls the whole thing back, which is exactly
            # the all-or-nothing property this method promises.
            with connection.cursor() as cursor:
                if aggregate_rows:
                    cursor.executemany(UPSERT_AGGREGATE, aggregate_rows)
                if ledger_rows:
                    cursor.executemany(INSERT_LEDGER, ledger_rows)
                if late_rows:
                    cursor.executemany(INSERT_LATE, late_rows)
                if dlq_rows:
                    cursor.executemany(INSERT_DLQ, dlq_rows)
                if checkpoint_rows:
                    cursor.executemany(UPSERT_CHECKPOINT, checkpoint_rows)

        log.info(
            "store.flushed",
            aggregates=len(aggregate_rows),
            ledger=len(ledger_rows),
            late=len(late_rows),
            dlq=len(dlq_rows),
        )
        return len(aggregate_rows)

    # -- observability -----------------------------------------------------

    def health(self) -> dict[str, Any]:
        with self._pool.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT * FROM v_pipeline_health")
            row = cursor.fetchone()
            columns = [description[0] for description in cursor.description or []]
        return dict(zip(columns, row, strict=False)) if row else {}
