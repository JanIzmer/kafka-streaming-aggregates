# Setup

## What you need

**Docker, and nothing else.** There is no external account anywhere in this
project — Redpanda and Postgres both run locally, and the producer generates
its own traffic.

| Check | Command | Needs |
|---|---|---|
| Unit tests (76) | `make install && make test` | Python 3.11+ |
| Full stack | `make up` | Docker |
| Integration tests | see below | Docker |
| CI | push | **no secrets** |

## First run

```bash
cp .env.example .env     # the defaults already match docker-compose
make up                  # redpanda, postgres, setup, processor
make demo                # clean, duplicates, late-burst, poison scenarios
```

`make up` returns before Redpanda is fully ready; the `setup` service waits on
its healthcheck, creates both topics and applies the Postgres schema, and only
then does the processor start.

Then look at what happened:

```bash
make aggregates    # the rows that were built
make lateness      # events that missed their window
make dlq           # what was refused, grouped by reason
make dedup-proof   # every record accounted for, exactly once
make lag           # consumer group lag
```

`make dedup-proof` is the one that demonstrates the central claim. It prints the
per-outcome counters next to the rows that actually reached the windows. Two
things must hold: the counters sum to exactly the number of records the
producer sent, and `on_time + late_accepted` equals `sum(events_total)`. If the
second is short, events were lost; if it is over, something was applied twice.

Redpanda Console is on <http://localhost:8090> (topics, consumer lag, raw DLQ
messages). Prometheus metrics are on <http://localhost:9108/metrics>.

## Integration tests

They are skipped unless explicitly enabled, so the unit suite stays runnable
with nothing installed:

```bash
docker compose up -d redpanda postgres

RUN_INTEGRATION=1 \
KAFKA_BOOTSTRAP_SERVERS=localhost:19092 \
POSTGRES_HOST=localhost POSTGRES_PORT=5433 \
./.venv/bin/pytest -m integration -v
```

Note the ports: services inside the compose network talk to `redpanda:9092` and
`postgres:5432`, while anything on the host uses `localhost:19092` and
`localhost:5433`. `.env.example` is written for the in-network case, which is
why the host-side run overrides them.

## Ports

| Port | Service |
|---|---|
| 19092 | Kafka API (from the host) |
| 9644 | Redpanda admin API |
| 8090 | Redpanda Console |
| 5433 | Postgres (from the host) |
| 9108 | Prometheus metrics |

5433 rather than 5432 so it does not collide with a Postgres already running
locally.

## Knobs worth knowing before you change them

All in `.env`; the consequences are in [late_data.md](late_data.md) and
[cost_and_sizing.md](cost_and_sizing.md).

| Variable | Default | Raising it costs |
|---|---|---|
| `WINDOW_SIZE_SECONDS` | 60 | nothing directly, but it changes the output grain |
| `ALLOWED_LATENESS_SECONDS` | 300 | memory, proportionally — it is the only way to accept older stragglers |
| `FLUSH_INTERVAL_SECONDS` | 5 | nothing but staleness; lowering it costs transactions |
| `MAX_BATCH_SIZE` | 500 | must stay flushable inside `max.poll.interval.ms` (5 min) |
| `DEDUP_LEDGER_RETENTION_HOURS` | 48 | disk — this is already the largest table |

## Known first-run friction

* **Apple Silicon**: the Redpanda and Postgres images are multi-arch and fine.
  `confluent-kafka` ships arm64 wheels for 2.5.x.
* **Port 5433 or 19092 already taken** — change the host side of the mapping in
  `docker-compose.yml`; the in-network ports are what the services use.
* **`make demo` right after `make up`** can produce into a topic the processor
  has not yet been assigned. Nothing is lost — `auto.offset.reset=earliest`
  means it reads the backlog — but the aggregates appear a few seconds later.
* **Replaying more than 48 hours of history** re-applies events, because the
  dedup ledger has forgotten them. This is deliberate and documented in the
  [runbook](runbook.md#replaying); it is the sharpest edge in the design.
