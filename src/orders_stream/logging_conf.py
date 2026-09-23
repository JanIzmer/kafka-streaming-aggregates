"""Structured logging.

Every log line a streaming processor emits is either a per-event line (far too
many) or an operational line (few, and they must carry partition/offset so they
can be correlated with Kafka). This configures the latter shape.
"""

from __future__ import annotations

import logging
import sys

import structlog


def configure_logging(level: str = "INFO", json_logs: bool = False) -> None:
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level.upper())

    # An if/else rather than a ternary: the two renderer classes have no
    # common base, so a conditional expression widens to `object` and mypy
    # rejects the assignment. SIM108 wants the ternary back, and this is the
    # one place the two tools genuinely disagree - the type checker wins.
    renderer: structlog.typing.Processor
    if json_logs:  # noqa: SIM108
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level.upper())),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)
