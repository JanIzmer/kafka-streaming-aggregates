"""Runtime configuration.

Every knob that changes correctness - window size, allowed lateness, flush
cadence - is here rather than scattered through the processor, because these
are the numbers that get tuned during an incident and they need to be readable
in one place.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # --- Kafka ------------------------------------------------------------
    kafka_bootstrap_servers: str = "localhost:19092"
    kafka_topic: str = "orders.v1"
    kafka_dlq_topic: str = "orders.v1.dlq"
    kafka_consumer_group: str = "orders-aggregator-v1"
    # earliest, not latest: a new deployment must not silently skip the backlog.
    kafka_auto_offset_reset: str = "earliest"
    kafka_session_timeout_ms: int = 30_000
    kafka_max_poll_interval_ms: int = 300_000

    # --- windowing --------------------------------------------------------
    window_size_seconds: int = Field(default=60, ge=1)
    # How long after a window closes we still accept events for it. Directly
    # sets how much state is held: windows_in_memory ~ lateness / window_size.
    allowed_lateness_seconds: int = Field(default=300, ge=0)
    # If a partition goes quiet, its watermark would freeze and windows would
    # never close. After this long with no events, the watermark advances on
    # wall clock instead.
    watermark_idle_seconds: int = Field(default=30, ge=1)
    # An event claiming to be further in the future than this has a broken
    # producer clock. Accepting it would drag the watermark forward and close
    # every window in between, permanently.
    max_future_skew_seconds: int = Field(default=60, ge=0)

    # --- throughput / durability tradeoff ---------------------------------
    flush_interval_seconds: float = Field(default=5.0, gt=0)
    max_batch_size: int = Field(default=500, ge=1)
    poll_timeout_seconds: float = 1.0

    # --- deduplication ----------------------------------------------------
    dedup_cache_size: int = Field(default=200_000, ge=1)
    dedup_ledger_retention_hours: int = Field(default=48, ge=1)

    # --- Postgres ---------------------------------------------------------
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "orders"
    postgres_user: str = "orders"
    postgres_password: SecretStr = SecretStr("orders")
    postgres_pool_min: int = 1
    postgres_pool_max: int = 4

    # --- observability ----------------------------------------------------
    metrics_port: int = 9108
    log_level: str = "INFO"
    json_logs: bool = False
    contracts_dir: Path = Path("contracts")

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password.get_secret_value()}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @model_validator(mode="after")
    def _lateness_must_be_usable(self) -> Settings:
        if self.allowed_lateness_seconds and self.allowed_lateness_seconds < self.window_size_seconds:
            # Not fatal, but it means a window can close before the next window's
            # first event arrives, which surprises people. Say so at startup.
            import warnings

            warnings.warn(
                f"allowed_lateness_seconds ({self.allowed_lateness_seconds}) is smaller than "
                f"window_size_seconds ({self.window_size_seconds}); windows will close "
                "aggressively and late events will be rejected sooner than expected",
                stacklevel=2,
            )
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
