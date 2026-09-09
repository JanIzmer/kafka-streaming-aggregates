-- ---------------------------------------------------------------------------
-- Everything the processor writes. Applied on startup; every statement is
-- IF NOT EXISTS so it is safe on every deploy and every restart.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS agg_orders_minute (
    window_start            TIMESTAMPTZ  NOT NULL,
    window_end              TIMESTAMPTZ  NOT NULL,
    merchant_id             TEXT         NOT NULL,

    events_total            BIGINT       NOT NULL DEFAULT 0,
    orders_placed           BIGINT       NOT NULL DEFAULT 0,
    orders_paid             BIGINT       NOT NULL DEFAULT 0,
    orders_cancelled        BIGINT       NOT NULL DEFAULT 0,
    orders_refunded         BIGINT       NOT NULL DEFAULT 0,

    -- Minor units as BIGINT, never NUMERIC/FLOAT: integer addition is
    -- associative, so replaying a window in a different order gives the same
    -- total. Float addition does not, and the difference shows up as a few
    -- cents that nobody can explain.
    gross_amount_minor      BIGINT       NOT NULL DEFAULT 0,
    refunded_amount_minor   BIGINT       NOT NULL DEFAULT 0,
    net_amount_minor        BIGINT       NOT NULL DEFAULT 0,

    distinct_users          BIGINT       NOT NULL DEFAULT 0,
    first_event_at          TIMESTAMPTZ,
    last_event_at           TIMESTAMPTZ,

    late_events_applied     BIGINT       NOT NULL DEFAULT 0,
    -- Bumped on every restatement. A consumer that sees this change knows the
    -- number it read before is no longer the number: "row was updated" alone
    -- does not distinguish a correction from normal in-progress accumulation.
    revision                INTEGER      NOT NULL DEFAULT 1,
    is_closed               BOOLEAN      NOT NULL DEFAULT FALSE,
    updated_at              TIMESTAMPTZ  NOT NULL DEFAULT now(),

    -- The whole idempotency story in one line: the processor upserts on this
    -- key, so replaying the same events converges instead of accumulating.
    PRIMARY KEY (window_start, merchant_id)
);

-- Dashboards read "the last N minutes across merchants"; the PK is
-- (window_start, merchant_id) so a scan by merchant alone would not use it.
CREATE INDEX IF NOT EXISTS ix_agg_orders_minute_merchant
    ON agg_orders_minute (merchant_id, window_start DESC);

-- Partial index: "which windows are still open?" is asked on every health
-- check, and open windows are a tiny fraction of the table.
CREATE INDEX IF NOT EXISTS ix_agg_orders_minute_open
    ON agg_orders_minute (window_start DESC)
    WHERE NOT is_closed;

-- ---------------------------------------------------------------------------
-- Durable half of deduplication. Written in the SAME transaction as the
-- aggregates, which is what makes "applied exactly once" true across a crash.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dedup_ledger (
    event_id        TEXT         PRIMARY KEY,
    merchant_id     TEXT         NOT NULL,
    window_start    TIMESTAMPTZ  NOT NULL,
    seen_at         TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Retention is enforced by a periodic delete, and this index is what makes
-- that delete cheap. Without it the purge degenerates into a full scan that
-- gets slower exactly as the table gets bigger.
CREATE INDEX IF NOT EXISTS ix_dedup_ledger_seen_at ON dedup_ledger (seen_at);

-- ---------------------------------------------------------------------------
-- Events that arrived after their window had closed. Not dropped: dropping
-- data silently is how you end up unable to explain a number.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS late_events (
    id                  BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id            TEXT         NOT NULL,
    merchant_id         TEXT         NOT NULL,
    event_type          TEXT,
    occurred_at         TIMESTAMPTZ  NOT NULL,
    window_start        TIMESTAMPTZ  NOT NULL,
    watermark_at        TIMESTAMPTZ  NOT NULL,
    lateness_seconds    DOUBLE PRECISION NOT NULL,
    amount_minor        BIGINT,
    payload             JSONB        NOT NULL,
    recorded_at         TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_late_events_window ON late_events (window_start, merchant_id);
CREATE INDEX IF NOT EXISTS ix_late_events_recorded ON late_events (recorded_at DESC);

-- ---------------------------------------------------------------------------
-- Records the processor refuses. Mirrored to the DLQ topic; the table exists
-- because "how many poison records today, and why" is a SQL question.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dlq_events (
    id              BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    reason          TEXT         NOT NULL,
    detail          TEXT         NOT NULL,
    topic           TEXT         NOT NULL,
    partition       INTEGER      NOT NULL,
    "offset"        BIGINT       NOT NULL,
    key             TEXT,
    payload         TEXT         NOT NULL,
    failed_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    replayed_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_dlq_events_reason ON dlq_events (reason, failed_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dlq_events_record
    ON dlq_events (topic, partition, "offset");

-- ---------------------------------------------------------------------------
-- Offsets live in Kafka - this is not a second source of truth, it is an
-- observability record so "where was the consumer when it died" survives the
-- consumer group being reset.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS consumer_checkpoint (
    consumer_group  TEXT         NOT NULL,
    topic           TEXT         NOT NULL,
    partition       INTEGER      NOT NULL,
    last_offset     BIGINT       NOT NULL,
    watermark_at    TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, topic, partition)
);

-- ---------------------------------------------------------------------------
-- Convenience views for dashboards and for the runbook.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_agg_orders_hourly AS
SELECT
    date_trunc('hour', window_start)  AS hour_start,
    merchant_id,
    sum(orders_paid)                  AS orders_paid,
    sum(gross_amount_minor)           AS gross_amount_minor,
    sum(refunded_amount_minor)        AS refunded_amount_minor,
    sum(net_amount_minor)             AS net_amount_minor,
    sum(late_events_applied)          AS late_events_applied,
    count(*) FILTER (WHERE NOT is_closed) AS open_windows
FROM agg_orders_minute
GROUP BY 1, 2;

CREATE OR REPLACE VIEW v_pipeline_health AS
SELECT
    max(window_start)                                        AS newest_window,
    max(updated_at)                                          AS last_write,
    EXTRACT(EPOCH FROM (now() - max(updated_at)))            AS seconds_since_write,
    count(*) FILTER (WHERE NOT is_closed)                    AS open_windows,
    sum(late_events_applied)                                 AS late_events_applied,
    (SELECT count(*) FROM late_events
      WHERE recorded_at > now() - interval '1 hour')         AS too_late_last_hour,
    (SELECT count(*) FROM dlq_events
      WHERE failed_at > now() - interval '1 hour')           AS dlq_last_hour
FROM agg_orders_minute;
