# Cost and sizing

Baseline assumption: **200 events/s average, 1 000/s peak**, ~350 bytes of JSON
per event (~120 bytes after zstd).

## Volume

| | Per day | Per month |
|---|---|---|
| Events | 17.3 M | 518 M |
| Raw bytes | ~6.0 GB | ~180 GB |
| On-broker (zstd) | ~2.1 GB | ~62 GB |
| On-broker, 7-day retention, RF=3 | **~44 GB** | steady state |

## Why six partitions

Partition count is the one number that is genuinely hard to change later, so
it is chosen deliberately:

* **It caps consumer parallelism.** Six partitions means at most six useful
  replicas. A seventh sits idle.
* **Replicas should divide evenly into it.** Six allows 1, 2, 3 or 6 replicas
  with balanced assignment. Five would force skew at every count but 1 and 5.
* **Headroom without waste.** One instance handles ~2 000 events/s, so peak
  traffic needs one. Six leaves room for roughly a 10x growth in traffic before
  the topic has to be touched at all.
* **Raising it is not free.** Partition count is part of the key→partition
  function, so adding partitions re-routes existing keys. Events for one
  merchant would land on two partitions, two consumers would hold the same
  window, and the aggregate would split. Growing past six therefore means a new
  topic and a migration, not an `alter`.

Per-partition state is small (a few windows per merchant), so the usual
counter-pressure against many partitions — broker memory and rebalance time —
does not bite at this size.

## Why keyed by `merchant_id`

The aggregation key and the partition key are the same on purpose. That single
choice is what removes the need for any cross-instance coordination: all events
for one merchant land on one partition, one consumer owns that partition, so
one consumer owns the whole window.

The cost is **skew**. One merchant ten times larger than the rest makes one
partition ten times hotter, and no amount of scaling fixes it. The fix is a
composite key (`merchant_id:bucket`) and a second aggregation step — real work,
which is why the PR template asks about key changes explicitly.

## Why 7 days of retention (and 28 for the DLQ)

Retention is a **replay budget**. Seven days means an incident discovered on a
Friday can still be repaired from the topic. Below that, a long weekend is
enough to lose the ability to rebuild.

The DLQ is kept four times longer, because a poison record is normally found
days after it was written — by which time the original is gone from the source
topic.

Note the interaction with the 48-hour dedup ledger: a replay reaching back
further than 48 hours will **re-apply** events, because the ledger no longer
remembers them. Replaying more than two days of history means truncating the
affected aggregate range first. This is documented in the
[runbook](runbook.md#replaying) because it is the sharpest edge in the design.

## Postgres sizing

| Table | Rows/day @ 6 merchants | Rows/day @ 5 000 merchants |
|---|---|---|
| `agg_orders_minute` | 8.6 k | 7.2 M |
| `dedup_ledger` (48 h) | 17.3 M/day, ~35 M resident | same |
| `window_user` (48 h) | bounded by distinct users x windows | grows with users |
| `late_events` | ~0.5 % of events | same |

**The ledger is the biggest table, by an order of magnitude.** ~35 M rows at
~100 bytes including the index is roughly 3.5 GB resident. That is the reason
retention exists at all, and the reason the purge deletes in bounded batches
rather than one statement — a single `DELETE` over millions of rows takes a
long lock and bloats the WAL while the processor is still writing.

At 5 000 merchants the aggregate table also needs monthly partitioning on
`window_start` and its own retention. It does not today, and adding it before
it is needed would be complexity with no payoff.

## Throughput and the flush trade-off

`FLUSH_INTERVAL_SECONDS` is the one knob that trades latency for cost:

| Interval | Aggregate freshness | Transactions/hour | Notes |
|---|---|---|---|
| 1 s | ~1 s | 3 600 | Visibly wasteful; most flushes write a handful of rows. |
| **5 s (default)** | ~5 s | 720 | Fresh enough for a live dashboard. |
| 30 s | ~30 s | 120 | Sensible if Postgres is the bottleneck. |

Longer intervals do **not** risk data: offsets are committed after the flush,
so a crash replays whatever was not written. The only cost of a longer interval
is staleness, and the only cost of a shorter one is transaction overhead.

`MAX_BATCH_SIZE` (500) interacts with `max.poll.interval.ms` (5 min): a batch
that takes longer than the poll interval to flush triggers a rebalance loop.
Raising the batch means checking that ceiling too.

## Rough monthly cost

Self-managed on AWS, 200 events/s:

| Component | Spec | USD/month |
|---|---|---|
| Kafka brokers | 3 × m7g.large | ~$220 |
| Broker storage | 44 GB gp3 | ~$4 |
| Postgres | db.m7g.large + 100 GB | ~$135 |
| Processor | 2 × 0.5 vCPU / 1 GB container | ~$30 |
| **Total** | | **≈ $390** |

≈ **$0.75 per million events**. The brokers dominate, and they are sized for
availability (RF=3), not for throughput — 200 events/s would fit on one.

The honest version: at this volume a single Postgres instance consuming from a
queue would cost a fifth as much. Kafka earns its price when there are multiple
independent consumers of the same stream, when replay is a requirement, or when
the peak is an order of magnitude above the average. If none of those is true,
that is worth saying out loud before provisioning three brokers.

## Where this design stops scaling

In the order the limits arrive:

1. **~2 000 events/s per instance**, single-threaded Python. Six partitions
   carries it to ~12 000/s. Past that: more partitions (new topic) or a JVM
   consumer.
2. **The dedup ledger's write rate.** Every event is an insert. At 10 000/s
   that is 10 000 inserts/s on top of the aggregate upserts; the ledger moves
   to Redis with a TTL, keeping Postgres for the aggregates only.
3. **Hot-key skew**, which arrives whenever one merchant grows large and is not
   solved by any amount of hardware.
4. **`agg_orders_minute` row count** at high merchant cardinality — partitioning
   and retention, in that order.
