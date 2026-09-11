"""Kafka wiring.

The settings here are the ones that change behaviour under failure, so each
non-default is justified in place rather than in a wiki nobody opens during an
incident.
"""

from __future__ import annotations

from typing import Any

from orders_stream.codec import encode
from orders_stream.config import Settings
from orders_stream.logging_conf import get_logger
from orders_stream.models import DeadLetter
from orders_stream.processor.pipeline import RawRecord

log = get_logger(__name__)


def consumer_config(settings: Settings) -> dict[str, Any]:
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": settings.kafka_consumer_group,
        # Offsets are committed by hand, after the database transaction has
        # returned. Auto-commit would acknowledge records the sink has not
        # persisted yet, which turns any crash into silent data loss.
        "enable.auto.commit": False,
        "auto.offset.reset": settings.kafka_auto_offset_reset,
        # Cooperative rebalancing: adding a replica moves a few partitions
        # instead of stopping every consumer in the group ("stop the world"),
        # which with a stateful processor means throwing away every open window.
        "partition.assignment.strategy": "cooperative-sticky",
        "session.timeout.ms": settings.kafka_session_timeout_ms,
        # Must exceed the worst-case time between polls. A long flush to a
        # struggling Postgres is the realistic cause of exceeding it, and the
        # symptom is an endless rebalance loop.
        "max.poll.interval.ms": settings.kafka_max_poll_interval_ms,
        # Only read committed records, so a transactional producer's aborted
        # batch is never aggregated.
        "isolation.level": "read_committed",
        "enable.partition.eof": False,
        "client.id": f"{settings.kafka_consumer_group}-consumer",
    }


def producer_config(settings: Settings) -> dict[str, Any]:
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        # The DLQ is the record of what we could not process. Losing it to an
        # in-flight buffer on a crash would defeat the point, so: full acks,
        # idempotent producer, and no reordering.
        "acks": "all",
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 5,
        "retries": 10,
        "compression.type": "zstd",
        "linger.ms": 20,
        "client.id": f"{settings.kafka_consumer_group}-dlq-producer",
    }


def to_raw_record(message: Any) -> RawRecord:
    """Adapt a confluent_kafka Message so nothing downstream imports the driver."""
    return RawRecord(
        topic=message.topic(),
        partition=message.partition(),
        offset=message.offset(),
        key=message.key(),
        value=message.value(),
        timestamp_ms=(message.timestamp() or (None, None))[1],
    )


class DlqPublisher:
    """Publishes dead letters to the DLQ topic.

    Keyed by the original partition so the DLQ preserves the same ordering
    relationship as the source topic - useful when replaying a poison range.
    """

    def __init__(self, producer: Any, topic: str) -> None:
        self._producer = producer
        self._topic = topic
        self.published = 0

    def publish(self, dead_letters: list[DeadLetter]) -> None:
        for letter in dead_letters:
            self._producer.produce(
                topic=self._topic,
                key=(letter.key or str(letter.partition)).encode("utf-8"),
                value=encode(letter.as_row()),
                headers=[
                    ("reason", letter.reason.encode("utf-8")),
                    ("source_topic", letter.topic.encode("utf-8")),
                    ("source_offset", str(letter.offset).encode("utf-8")),
                ],
            )
            self.published += 1
        if dead_letters:
            # Block until the broker has them. The DLQ write must land before
            # the source offset is committed, or the record is lost entirely.
            self._producer.flush(10.0)
            log.warning(
                "dlq.published",
                count=len(dead_letters),
                reasons=sorted({letter.reason for letter in dead_letters}),
            )


class NullDlqPublisher:
    """Used when only the Postgres DLQ table is wanted (tests, local runs)."""

    published = 0

    def publish(self, dead_letters: list[DeadLetter]) -> None:
        if dead_letters:
            log.warning("dlq.not_published", count=len(dead_letters), reason="no producer")
