"""Processor CLI.

    orders-processor run               # consume forever
    orders-processor init-db           # apply the schema
    orders-processor health            # one-shot health read
    orders-processor replay-dlq --reason too_late --limit 100
"""

from __future__ import annotations

import json

import typer

from orders_stream.config import get_settings
from orders_stream.logging_conf import configure_logging, get_logger

app = typer.Typer(add_completion=False, help="Kafka -> Postgres aggregate processor")
log = get_logger(__name__)


@app.callback()
def _configure(
    log_level: str = typer.Option(None, envvar="LOG_LEVEL"),
    json_logs: bool = typer.Option(False, "--json-logs", envvar="JSON_LOGS"),
) -> None:
    settings = get_settings()
    configure_logging(level=log_level or settings.log_level, json_logs=json_logs)


@app.command("init-db")
def init_db() -> None:
    """Create the tables, indexes and views. Safe to re-run."""
    from orders_stream.store import Repository

    settings = get_settings()
    repository = Repository(settings.postgres_dsn)
    try:
        repository.apply_schema()
    finally:
        repository.close()
    typer.echo("schema applied")


@app.command()
def run(
    metrics: bool = typer.Option(True, help="Serve /metrics on METRICS_PORT."),
    init: bool = typer.Option(True, help="Apply the schema before consuming."),
) -> None:
    """Consume the topic and maintain the aggregates until stopped."""
    from confluent_kafka import Consumer, Producer

    from orders_stream.metrics import Metrics
    from orders_stream.processor.kafka_io import DlqPublisher, consumer_config, producer_config
    from orders_stream.processor.service import ProcessorService
    from orders_stream.store import Repository

    settings = get_settings()
    repository = Repository(
        settings.postgres_dsn,
        min_size=settings.postgres_pool_min,
        max_size=settings.postgres_pool_max,
    )
    if init:
        repository.apply_schema()

    collector = Metrics()
    if metrics:
        Metrics.serve(settings.metrics_port)

    service = ProcessorService(
        settings=settings,
        repository=repository,
        consumer=Consumer(consumer_config(settings)),
        dlq=DlqPublisher(Producer(producer_config(settings)), settings.kafka_dlq_topic),
        metrics=collector,
    )
    service.install_signal_handlers()
    service.run()


@app.command()
def health() -> None:
    """Print the pipeline health view. Used by the runbook and by docker healthchecks."""
    from orders_stream.store import Repository

    settings = get_settings()
    repository = Repository(settings.postgres_dsn)
    try:
        typer.echo(json.dumps(repository.health(), indent=2, default=str))
    finally:
        repository.close()


@app.command("replay-dlq")
def replay_dlq(
    reason: str = typer.Option(None, help="Only replay this failure reason."),
    limit: int = typer.Option(100, help="Maximum records to republish."),
    dry_run: bool = typer.Option(True, help="Print what would be replayed without producing."),
) -> None:
    """Republish dead letters to the source topic.

    Only makes sense once the cause is fixed - a malformed record replayed
    unchanged fails identically. `too_late` records cannot be replayed into
    their original window at all; they are re-emitted for a separate
    reconciliation job rather than for the live aggregate.
    """
    from confluent_kafka import Producer

    from orders_stream.store import Repository

    settings = get_settings()
    repository = Repository(settings.postgres_dsn)
    producer = None if dry_run else Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})

    try:
        with repository._pool.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, reason, key, payload
                FROM dlq_events
                WHERE replayed_at IS NULL
                  AND (%s IS NULL OR reason = %s)
                ORDER BY failed_at
                LIMIT %s
                """,
                (reason, reason, limit),
            )
            rows = cursor.fetchall()

            for row_id, row_reason, key, payload in rows:
                typer.echo(f"{'[dry-run] ' if dry_run else ''}{row_id} {row_reason}")
                if dry_run or producer is None:
                    continue
                producer.produce(
                    topic=settings.kafka_topic,
                    key=(key or "").encode("utf-8"),
                    value=payload.encode("utf-8"),
                )
                cursor.execute(
                    "UPDATE dlq_events SET replayed_at = now() WHERE id = %s", (row_id,)
                )
            if producer is not None:
                producer.flush(30.0)
    finally:
        repository.close()

    typer.echo(f"{len(rows)} record(s) {'listed' if dry_run else 'replayed'}")


if __name__ == "__main__":  # pragma: no cover
    app()
