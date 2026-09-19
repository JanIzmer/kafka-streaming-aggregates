# Runbook

> The scenario: **the processor died at 03:00, or the numbers are wrong.**

## 1. How you find out

| Signal | Source | Fires when |
|---|---|---|
| `orders_stream_flush_failures_total` > 0 | Prometheus | Postgres is refusing writes. Offsets have stopped moving. |
| `orders_stream_watermark_lag_seconds` rising | Prometheus | Falling behind the stream. The **leading** indicator. |
| Consumer group lag rising | `rpk group describe` | Confirms the above with real offsets. |
| `orders_stream_events_total{outcome="too_late"}` rising | Prometheus | Allowed lateness no longer matches reality; data is being excluded. |
| `orders_stream_events_total{outcome="malformed"}` spike | Prometheus | A producer started emitting something new. |
| Container restart loop | orchestrator | Usually an unhandled flush error. |
| `v_pipeline_health.seconds_since_write` large | SQL | Works even when metrics scraping is what broke. |

Suggested alert thresholds:

```
watermark_lag_seconds       > 300 for 5m   -> page
flush_failures_total        increase > 0   -> page
events_total{outcome="too_late"} rate      > 1/s for 10m -> notify
events_total{outcome="malformed"} rate     > 0.5/s for 5m -> notify
consumer lag                > 100k for 10m -> page
```

The split is the same as in the batch project: page only for things a human
must act on tonight. `too_late` and `malformed` need a decision, not a
firefight.

## 2. Triage

```
Is the processor running?
├─ no  -> why did it exit? §3.1
└─ yes
   ├─ lag rising, flush failures > 0   -> Postgres. §3.2
   ├─ lag rising, no flush failures    -> throughput. §3.3
   ├─ lag flat, numbers wrong          -> §4
   └─ restart loop                     -> §3.1
```

One query answers most of it:

```sql
SELECT * FROM v_pipeline_health;
```

`seconds_since_write` is the fastest signal that the pipeline has stopped, and
it does not depend on the metrics pipeline being healthy.

## 3. Common causes

### 3.1 The processor keeps exiting

```bash
docker compose logs --tail 200 processor | grep -E "flush.failed|Traceback"
```

Nothing was lost: offsets are committed last, so everything not flushed is
replayed. The question is only *why* it exits.

* `flush.failed` with a Postgres error → §3.2.
* `max.poll.interval.ms` exceeded → the flush is taking longer than 5 minutes.
  Reduce `MAX_BATCH_SIZE`, or fix whatever is making Postgres slow.
* Unhandled exception in decoding → a bug. The record that caused it is in the
  log with its partition and offset; reproduce it as a unit test first.

### 3.2 Postgres is the problem

```bash
docker compose exec postgres psql -U orders -d orders -c "\
SELECT count(*) FROM dedup_ledger;"
```

* **Ledger has grown huge** — the retention purge has not been running (it runs
  every 15 minutes inside the processor, so a crash loop also stops the purge).
  Purge manually:
  ```sql
  DELETE FROM dedup_ledger WHERE seen_at < now() - interval '48 hours';
  DELETE FROM window_user  WHERE window_start < now() - interval '48 hours';
  ```
* **Connections exhausted** — `POSTGRES_POOL_MAX` per replica; count the
  replicas.
* **Disk full** — the ledger and `window_user` are the two tables that grow.

Once Postgres is healthy, the processor catches up on its own. Lag drains at
whatever rate the sink can absorb; do not reset the group.

### 3.3 Lag without errors

In order of likelihood:

1. **Flush too frequent.** A commit every 5 seconds at high volume means a lot
   of tiny transactions. Raise `FLUSH_INTERVAL_SECONDS` — this trades latency
   for throughput, and the aggregates simply update slightly less often.
2. **Not enough partitions.** Consumer parallelism is capped by partition
   count. Six partitions means at most six useful replicas. Adding partitions
   changes key distribution, so it is not free — see
   [cost_and_sizing.md](cost_and_sizing.md).
3. **One hot merchant.** Keying by `merchant_id` means one very large merchant
   is one partition. Check the skew:
   ```bash
   docker compose exec redpanda rpk topic describe orders.v1 -p
   ```
   The fix is a composite key (`merchant_id:bucket`) plus a second aggregation
   step — a design change, not a config change.

### 3.4 Too many `too_late` events

```sql
SELECT merchant_id,
       count(*)                       AS events,
       round(avg(lateness_seconds))   AS avg_late,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY lateness_seconds)) AS p99_late
FROM late_events
WHERE recorded_at > now() - interval '6 hours'
GROUP BY 1 ORDER BY 2 DESC;
```

If `p99_late` is above `ALLOWED_LATENESS_SECONDS`, the setting is wrong for the
traffic. Raise it, and update `x-contract.expected_lateness_p99_seconds` in the
contract so the next person sees the real number. Cost: more open windows held
in memory, proportional to the increase.

**Do not** try to re-apply the already-excluded events into their old windows.
They are in `late_events` with their payload; a separate reconciliation job can
correct history if it matters. Reopening closed windows in the live path
changes published numbers with no audit trail.

## 4. The numbers look wrong

Check in this order — each is cheaper than the next:

1. **Excluded by lateness** (§3.4). By far the most common cause of an
   under-count.
2. **Rejected by the contract:**
   ```sql
   SELECT reason, count(*) FROM dlq_events
   WHERE failed_at > now() - interval '1 day' GROUP BY 1 ORDER BY 2 DESC;
   ```
3. **Restated after someone read it:**
   ```sql
   SELECT window_start, merchant_id, revision, late_events_applied
   FROM agg_orders_minute WHERE revision > 1 ORDER BY updated_at DESC LIMIT 50;
   ```
4. **Double-counted** — should be impossible; prove it:
   ```sql
   SELECT count(*) AS rows, count(DISTINCT event_id) AS ids FROM dedup_ledger;
   ```
   If those differ, the ledger's primary key is not doing its job and that is a
   serious bug, not an operational issue.

## 5. Replaying

### Replaying dead letters

Only after the cause is fixed — a malformed record replayed unchanged fails
identically.

```bash
docker compose run --rm setup orders-processor replay-dlq --reason contract_violation --limit 100
# inspect the list, then:
docker compose run --rm setup orders-processor replay-dlq --reason contract_violation --limit 100 --no-dry-run
```

`too_late` records cannot go back into their original window — the watermark is
long past. Replaying them produces new `too_late` entries. They exist for a
reconciliation job, not for the live path.

### Replaying the topic

Resetting the consumer group re-reads everything within the topic's 7-day
retention:

```bash
docker compose exec redpanda rpk group seek orders-aggregator-v1 --to start
```

This is safe — the ledger makes it converge, which the integration test
`test_a_restart_does_not_double_apply` proves — but two caveats:

* events older than the **48-hour ledger retention** are no longer in the
  ledger, so a replay of older history *will* re-apply them. Replaying more
  than two days back means truncating `agg_orders_minute` for that range first
  and letting it rebuild.
* every replayed event whose window is long closed becomes `too_late`. A full
  replay for correctness therefore needs `ALLOWED_LATENESS_SECONDS` raised for
  the duration of the replay, or the rebuild produces almost nothing.

That second point is the sharpest edge in this design and is worth knowing
before an incident rather than during one.

## Quick reference

```bash
make health         # v_pipeline_health
make lag            # consumer group lag
make aggregates     # newest aggregate rows
make lateness       # events that missed their window
make dlq            # dead letters by reason
make dedup-proof    # ledger rows vs distinct ids
docker compose logs -f processor
```
