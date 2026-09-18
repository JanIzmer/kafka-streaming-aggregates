# Delivery semantics

## The claim

The producer is **at-least-once**. The processor turns that into **effectively
exactly-once application**: every event moves the aggregates exactly once, no
matter how many times Kafka delivers it, how many times the consumer restarts,
or where a crash lands.

Note the wording. Kafka still *delivers* a record more than once — that is not
something a consumer can prevent. What is guaranteed is that the second
delivery does not change the result.

## Where duplicates come from

Three sources, and they need different defences:

| Source | Mechanism | Caught by |
|---|---|---|
| Producer retry after an ambiguous ack | the same `event_id` twice, seconds apart | in-memory LRU |
| Consumer restart replaying from the last committed offset | a whole batch again, after the state is gone | durable ledger |
| Rebalance re-delivering in-flight records | a batch again, on a different instance | durable ledger |
| Producer retry inside one poll | the same id twice in one batch | in-batch check |

All four are real; the in-batch case is the one usually forgotten, because it
is invisible to both tiers — nothing has been committed yet.

## The two tiers

```
event_id ──> LRU set (200k ids, in memory) ──miss──> dedup_ledger (Postgres)
                    │                                        │
                    └── hit: drop ──────────────────┬─────────┘
                                                    └─ hit: drop
```

The memory tier catches almost everything, because duplicates cluster in time.
The ledger exists for the cases the memory tier structurally cannot see: it is
empty after a restart, and it is per-instance.

The ledger is queried **once per batch**, not once per event. At a few thousand
events per second a per-event round trip is the first thing to saturate.

## Why the ledger write is in the aggregate transaction

This is the whole guarantee, so it is worth being precise.

```
1. BEGIN
     upsert aggregate deltas
     insert dedup_ledger rows
     insert late_events rows
     insert dlq_events rows
   COMMIT                       <-- Postgres
2. publish dead letters          <-- Kafka DLQ topic
3. snapshot window state as flushed
4. mark ids seen in the memory tier
5. commit consumer offsets       <-- Kafka
```

Crash points:

| Crash between | What is durable | What happens on restart |
|---|---|---|
| before 1 | nothing | Kafka replays, batch applied once |
| 1 and 5 | aggregates **and** ledger | Kafka replays, ledger says "seen", dedup drops all of it, aggregates unchanged |
| after 5 | everything | nothing to replay |

The second row is the one that matters. If the ledger were written in its own
transaction — or worse, only kept in memory — that window would double-count
every event in the batch.

The order is also why **`enable.auto.commit` is off**. The default behaviour
acknowledges records before the sink has them, which converts every crash into
silent data loss.

## Why the aggregates are upserted as deltas

`UPDATE ... SET total = total + EXCLUDED.total`, not `SET total = EXCLUDED.total`.

Absolute totals would be correct only if the processor always held the complete
window in memory. It does not: after a restart the state of a still-open window
is gone, and writing the absolute value from a fresh, partial state would
*overwrite* what is already in Postgres with a smaller number.

Deltas are safe precisely because deduplication guarantees each event
contributes once. The two mechanisms only work together — deltas without dedup
would double-count on every replay, and absolute writes with dedup would still
lose data after a restart.

## Why `distinct_users` is handled differently

Distinct counts are not additive, so the delta trick does not apply. Three
options:

1. keep the set in memory and write the count — wrong after a restart;
2. HyperLogLog — approximate, and the error is not explainable to a merchant;
3. store the membership.

This project stores the membership, in `window_user (window_start, merchant_id,
user_id)`. The insert is `ON CONFLICT DO NOTHING`, so it is idempotent, and the
count is *derived* rather than accumulated, so a replay converges. The rows are
purged on the same retention schedule as the ledger — once no event can still
be applied to a window, the set is dead weight.

## Ordering

Kafka orders records **per partition**. Events are keyed by `merchant_id`, so
all events for one merchant land on one partition and are consumed by one
instance. That is what makes windowing possible without cross-instance
coordination.

Nothing in this system assumes global ordering, and it must stay that way:
changing the key would silently break the aggregate, which is why the PR
template asks about it explicitly. Changing the key requires a new topic, not
a redeploy — the existing partitions would keep the old key's distribution.

## What is *not* guaranteed

* **Exactly-once across the DLQ.** The DLQ publish (step 2) is outside the
  Postgres transaction. A crash between 1 and 2 leaves the dead letter in the
  table but not on the topic; a crash between 2 and 5 can publish it twice. The
  DLQ topic is therefore at-least-once, and the `dlq_events` table has a unique
  index on `(topic, partition, offset)` so the table stays exact.
* **Ordered output.** Aggregates are upserted, so a consumer reading
  `agg_orders_minute` sees the current value, not a change log.
* **Infinite dedup.** The ledger has a 48-hour retention. A duplicate arriving
  after that would be counted again. 48 hours is far beyond any observed retry
  window, and unbounded retention would mean an unbounded table.
