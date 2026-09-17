"""End to end through the real stack.

Marked `integration` and skipped unless `RUN_INTEGRATION=1`, because the unit
suite must stay runnable with nothing installed but Python.

What this proves that the unit tests cannot: that the flush-then-commit
ordering, the Postgres upsert and the durable ledger actually compose. The unit
tests verify each of them in isolation with a fake on the other side, and a
wrong SQL keyword would pass every one of them.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("RUN_INTEGRATION") != "1",
        reason="needs Kafka and Postgres; set RUN_INTEGRATION=1",
    ),
]

WINDOW_SECONDS = 60


@pytest.fixture(scope="module")
def settings():
    from orders_stream.config import Settings

    # A unique consumer group per run: a leftover group from a previous run
    # would start at its committed offset and see nothing.
    return Settings(
        kafka_bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
        kafka_topic=f"orders.it.{uuid.uuid4().hex[:8]}",
        kafka_dlq_topic=f"orders.it.dlq.{uuid.uuid4().hex[:8]}",
        kafka_consumer_group=f"it-{uuid.uuid4().hex[:8]}",
        postgres_host=os.environ.get("POSTGRES_HOST", "localhost"),
        postgres_port=int(os.environ.get("POSTGRES_PORT", "5433")),
        window_size_seconds=WINDOW_SECONDS,
        allowed_lateness_seconds=120,
        flush_interval_seconds=1.0,
        max_batch_size=100,
        poll_timeout_seconds=0.5,
    )


@pytest.fixture(scope="module")
def repository(settings):
    from orders_stream.store import Repository

    repo = Repository(settings.postgres_dsn)
    repo.apply_schema()
    yield repo
    repo.close()


def produce(settings, payloads):
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers, "acks": "all"})
    for item in payloads:
        producer.produce(
            topic=settings.kafka_topic,
            key=item["merchant_id"].encode(),
            value=json.dumps(item).encode(),
        )
    producer.flush(30.0)


def drain(settings, repository, seconds: float = 12.0):
    """Run the service until it has been idle for a moment, then stop it."""
    from confluent_kafka import Consumer

    from orders_stream.processor.kafka_io import NullDlqPublisher, consumer_config
    from orders_stream.processor.service import ProcessorService

    service = ProcessorService(
        settings=settings,
        repository=repository,
        consumer=Consumer(consumer_config(settings)),
        dlq=NullDlqPublisher(),
    )

    deadline = time.monotonic() + seconds
    service.consumer.subscribe([settings.kafka_topic])
    service._running = True
    try:
        while time.monotonic() < deadline:
            service._tick()
    finally:
        service._flush(force=True)
        service.consumer.close()
    return service


def base_payload(**overrides):
    occurred = overrides.pop("occurred_at", datetime.now(tz=timezone.utc))
    payload = {
        "event_id": str(uuid.uuid4()),
        "event_type": "order_paid",
        "occurred_at": occurred.isoformat(),
        "order_id": str(uuid.uuid4()),
        "merchant_id": "m_it",
        "user_id": "u_1",
        "amount_minor": 1000,
        "currency": "EUR",
        "schema_version": 1,
    }
    payload.update(overrides)
    return payload


def test_duplicates_are_applied_once_end_to_end(settings, repository):
    merchant = f"m_dup_{uuid.uuid4().hex[:6]}"
    duplicated = base_payload(merchant_id=merchant)

    produce(settings, [duplicated, duplicated, duplicated, base_payload(merchant_id=merchant)])
    drain(settings, repository)

    with repository._pool.connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT sum(events_total), sum(gross_amount_minor) FROM agg_orders_minute "
            "WHERE merchant_id = %s",
            (merchant,),
        )
        events, gross = cursor.fetchone()

    assert events == 2
    assert gross == 2000


def test_a_restart_does_not_double_apply(settings, repository):
    """The ledger is written in the flush transaction, so a consumer group that
    re-reads the same offsets must not move the aggregates."""
    merchant = f"m_restart_{uuid.uuid4().hex[:6]}"
    produce(settings, [base_payload(merchant_id=merchant) for _ in range(5)])
    drain(settings, repository)

    with repository._pool.connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT sum(events_total) FROM agg_orders_minute WHERE merchant_id = %s", (merchant,)
        )
        (before,) = cursor.fetchone()

    # A fresh group replays the topic from the beginning.
    settings.kafka_consumer_group = f"it-replay-{uuid.uuid4().hex[:8]}"
    drain(settings, repository)

    with repository._pool.connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT sum(events_total) FROM agg_orders_minute WHERE merchant_id = %s", (merchant,)
        )
        (after,) = cursor.fetchone()

    assert before == after == 5


def test_a_too_late_event_lands_in_late_events(settings, repository):
    merchant = f"m_late_{uuid.uuid4().hex[:6]}"
    now = datetime.now(tz=timezone.utc)

    produce(
        settings,
        [
            base_payload(merchant_id=merchant, occurred_at=now - timedelta(seconds=600)),
            base_payload(merchant_id=merchant, occurred_at=now),
            base_payload(merchant_id=merchant, occurred_at=now - timedelta(seconds=900)),
        ],
    )
    drain(settings, repository)

    with repository._pool.connection() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM late_events WHERE merchant_id = %s", (merchant,))
        (late_count,) = cursor.fetchone()

    assert late_count >= 1


def test_a_poison_record_does_not_stop_the_stream(settings, repository):
    from confluent_kafka import Producer

    merchant = f"m_poison_{uuid.uuid4().hex[:6]}"
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers, "acks": "all"})
    producer.produce(settings.kafka_topic, key=merchant.encode(), value=b"{not json")
    producer.produce(
        settings.kafka_topic,
        key=merchant.encode(),
        value=json.dumps(base_payload(merchant_id=merchant)).encode(),
    )
    producer.flush(30.0)

    drain(settings, repository)

    with repository._pool.connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT sum(events_total) FROM agg_orders_minute WHERE merchant_id = %s", (merchant,)
        )
        (events,) = cursor.fetchone()
        cursor.execute("SELECT count(*) FROM dlq_events WHERE reason = 'malformed'")
        (dlq,) = cursor.fetchone()

    assert events == 1
    assert dlq >= 1
