"""Producer CLI.

    orders-producer create-topics
    orders-producer run --rate 200 --duration 120
    orders-producer run --rate 500 --duplicate-rate 0.2   # stress the dedup path
    orders-producer scenario late-burst                   # reproducible demo
"""

from __future__ import annotations

import time

import typer

from orders_stream.config import get_settings
from orders_stream.logging_conf import configure_logging, get_logger
from orders_stream.producer.generator import EventGenerator, FaultRates

app = typer.Typer(add_completion=False, help="Synthetic order event producer")
log = get_logger(__name__)


@app.callback()
def _configure(json_logs: bool = typer.Option(False, "--json-logs", envvar="JSON_LOGS")) -> None:
    configure_logging(level=get_settings().log_level, json_logs=json_logs)


@app.command("create-topics")
def create_topics(partitions: int = 6, replication: int = 1) -> None:
    """Create the source and DLQ topics.

    Six partitions is not arbitrary: it is the ceiling on consumer parallelism,
    it must divide evenly among however many replicas run, and it cannot be
    lowered later without re-keying the topic. See docs/cost_and_sizing.md.
    """
    from confluent_kafka.admin import AdminClient, NewTopic

    settings = get_settings()
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    topics = [
        NewTopic(
            settings.kafka_topic,
            num_partitions=partitions,
            replication_factor=replication,
            config={"retention.ms": str(7 * 24 * 3600 * 1000), "compression.type": "producer"},
        ),
        NewTopic(
            settings.kafka_dlq_topic,
            num_partitions=partitions,
            replication_factor=replication,
            # The DLQ is kept four times longer than the source: a poison
            # record is usually found days after it was written, and the
            # original is long gone by then.
            config={"retention.ms": str(28 * 24 * 3600 * 1000)},
        ),
    ]
    for name, future in admin.create_topics(topics).items():
        try:
            future.result()
            typer.echo(f"created {name}")
        except Exception as exc:  # noqa: BLE001 - "already exists" is the common case
            typer.echo(f"{name}: {exc}")


@app.command()
def run(
    rate: int = typer.Option(100, help="Events per second."),
    duration: int = typer.Option(60, help="Seconds to run; 0 means forever."),
    seed: int = typer.Option(42, help="Same seed, same stream."),
    duplicate_rate: float = typer.Option(0.05),
    late_rate: float = typer.Option(0.08),
    very_late_rate: float = typer.Option(0.01),
    malformed_rate: float = typer.Option(0.005),
) -> None:
    """Produce events at a fixed rate with the configured fault mix."""
    from confluent_kafka import Producer

    settings = get_settings()
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "zstd",
            "linger.ms": 10,
            "client.id": "orders-producer",
        }
    )
    generator = EventGenerator(
        seed=seed,
        faults=FaultRates(
            duplicate=duplicate_rate,
            late=late_rate,
            very_late=very_late_rate,
            malformed=malformed_rate,
        ),
    )

    interval = 1.0 / rate if rate > 0 else 0.0
    deadline = time.monotonic() + duration if duration else None
    produced = 0

    log.info("producer.started", topic=settings.kafka_topic, rate=rate, duration=duration)
    try:
        while deadline is None or time.monotonic() < deadline:
            key, value = generator.next_record()
            # Keying by merchant_id is what makes single-consumer windowing
            # possible: every event for one aggregation key lands on one
            # partition, so no two consumers ever hold the same window.
            producer.produce(topic=settings.kafka_topic, key=key.encode("utf-8"), value=value)
            produced += 1

            if produced % 500 == 0:
                producer.poll(0)
                log.info("producer.progress", produced=produced)
            if interval:
                time.sleep(interval)
    except KeyboardInterrupt:
        log.info("producer.interrupted")
    finally:
        producer.flush(30.0)
        log.info("producer.stopped", produced=produced)

    typer.echo(f"produced {produced} events")


@app.command()
def scenario(
    name: str = typer.Argument(..., help="clean | duplicates | late-burst | poison"),
    count: int = typer.Option(2000),
) -> None:
    """Produce a fixed, reproducible scenario. Used for demos and for the README."""
    from confluent_kafka import Producer

    profiles = {
        "clean": FaultRates(duplicate=0, late=0, very_late=0, malformed=0, unknown_event_type=0, future_skew=0),
        "duplicates": FaultRates(duplicate=0.35, late=0.02, very_late=0, malformed=0),
        "late-burst": FaultRates(duplicate=0.02, late=0.35, very_late=0.15, malformed=0),
        "poison": FaultRates(duplicate=0.02, late=0.05, malformed=0.15, unknown_event_type=0.08, future_skew=0.05),
    }
    if name not in profiles:
        raise typer.BadParameter(f"unknown scenario '{name}'; pick one of {sorted(profiles)}")

    settings = get_settings()
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers, "acks": "all"})
    generator = EventGenerator(seed=7, faults=profiles[name])

    for key, value in generator.burst(count):
        producer.produce(topic=settings.kafka_topic, key=key.encode("utf-8"), value=value)
    producer.flush(30.0)

    typer.echo(f"scenario '{name}': produced {count} events")


if __name__ == "__main__":  # pragma: no cover
    app()
