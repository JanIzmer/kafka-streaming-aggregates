# Failure modes

How each known failure is detected, what happens without a human, and what a
human has to do. The short version is in the README.

## Input

### A record is not valid JSON, not UTF-8, or not an object

* **Detection:** `MalformedRecord` in the codec.
* **Automatic:** dead letter (table + topic), offset committed, processing
  continues.
* **Why it must not retry:** this is the poison-pill outage. A record that will
  never parse, retried forever, stops the partition advancing and the backlog
  grows behind one bad byte. The test
  `test_a_poison_record_does_not_block_the_partition` exists for exactly this.

### A record violates the contract

* **Detection:** `Contract.violations` — missing required field, bad timestamp,
  negative or non-integer amount, malformed currency.
* **Automatic:** dead letter with the specific reason; **a ledger row is still
  written**, so a replay does not emit the same dead letter twice.
* **Manual:** the reason is in `dlq_events.detail`; group by reason to see
  whether it is one broken producer or a schema change.

### An unknown `event_type` appears

* **Automatic:** dead-lettered, not ignored and not counted.
* **Why:** the aggregate's meaning depends on which types are included.
  Silently ignoring `order_partially_refunded` makes revenue quietly wrong;
  silently counting it changes what revenue means. Neither is the consumer's
  decision to make.

### An unknown *field* appears

* **Automatic:** passes through, logged once per field name — not once per
  record, which on a 5k/s topic would drown every other log line.
* **Manual:** adopt it in a new contract version if it is wanted.

### The producer's clock is wrong

* **Detection:** `occurred_at > now + 60s`.
* **Automatic:** refused, dead-lettered, **watermark untouched**.
* **Why it is a hard guard:** the watermark follows the maximum event time. One
  event an hour ahead would close every window in between, permanently, and
  only a topic replay would recover it.

## Lateness

### An event arrives behind the watermark, window still open

* **Automatic:** applied; `late_events_applied` and `revision` both increment.
* **Blast radius:** one window's published value changes.

### An event arrives after its window closed

* **Automatic:** `late_events` row + DLQ. The window is **not** reopened.
* **Manual:** if this is frequent, `allowed_lateness_seconds` no longer matches
  reality. Raising it costs state proportionally, so it is a decision, not a
  default.
* **Alert:** `orders_stream_events_total{outcome="too_late"}` rising.

### A partition goes quiet and windows never close

* **Automatic:** after `watermark_idle_seconds` the watermark advances on wall
  clock, windows close, state is evicted.
* **Why it is not an edge case:** a merchant closing for the night is normal.
  Without this, memory grows for every quiet key.

## Processing

### Postgres is down or refusing writes

* **Detection:** exception in `flush`, `orders_stream_flush_failures_total`
  increments.
* **Automatic:** offsets are **not** committed and window state is **not**
  snapshotted, so nothing is lost. The container restarts and replays.
* **Manual:** fix Postgres. Consumer lag grows meanwhile — that is the intended
  tradeoff, backpressure rather than data loss.
* **Second-order risk:** a long flush can exceed `max.poll.interval.ms` and
  trigger an endless rebalance loop. That is why the timeout is 5 minutes, not
  the default.

### The flush partially succeeded

* **Cannot happen.** Aggregates, ledger, late events and dead letters are one
  transaction. Either all of it is durable or none of it is.

### The consumer is killed mid-batch

* **Automatic:** whatever was not flushed is replayed; anything already flushed
  is in the ledger and is dropped on replay.
* **Residual risk:** in-memory window state for open windows is lost. Because
  the sink stores **deltas**, the already-written part stays correct — the
  restarted processor simply continues adding. This is the specific reason the
  upsert is additive rather than absolute.

### A rebalance moves partitions away

* **Automatic:** `on_revoke` flushes before the partitions are released;
  cooperative-sticky assignment means only the moving partitions are affected,
  not every consumer in the group.
* **Why it matters:** the revoked partition's window state is about to be
  discarded. Without the flush those events are lost — their offsets were never
  committed, but the replay lands on a different instance whose ledger *does*
  contain them.

### Two instances process the same key

* **Cannot happen while the key is the partition key.** Kafka assigns a
  partition to one consumer in a group. This is the property that makes
  windowing possible without coordination, and it is why changing the key needs
  a new topic.

### The DLQ topic is unavailable

* **Automatic:** the producer retries (idempotent, `acks=all`); if it still
  fails, the flush raises and the batch is replayed.
* **Consequence:** dead letters are at-least-once on the topic. The
  `dlq_events` table has a unique index on `(topic, partition, offset)`, so the
  table stays exact even when the topic has a duplicate.

## Consumer group

### A new deployment starts from the wrong offset

* **Automatic:** `auto.offset.reset=earliest`. A brand new group reads the
  backlog rather than silently skipping it.
* **Consequence:** resetting a group replays everything. That is safe — the
  ledger makes it a no-op — but it is not free; the replay still reads the
  topic. The integration test `test_a_restart_does_not_double_apply` is the
  proof that it converges.

### Lag grows steadily

* **Detection:** `orders_stream_watermark_lag_seconds` is the leading
  indicator; `rpk group describe` confirms with real offset lag.
* **Cause, in order of likelihood:** Postgres slow, flush interval too short
  (a commit per 5 s at high volume), too few partitions to parallelise over.
* **Manual:** see the [runbook](runbook.md).

## Data quality

### The numbers do not match the source system

* **First check:** `late_events` — events excluded because they missed their
  window are the single most likely explanation for an under-count.
* **Second:** `dlq_events` grouped by reason.
* **Third:** `revision > 1` rows, which are restatements the downstream may
  have read before they were corrected.

### A unit changes silently (minor units become major)

* **Detection:** **weak.** Nothing in this design catches an amount that is
  still a plausible integer. Only a distribution check would.
* **Honest position:** this is the failure mode the system is worst at, in both
  this project and the batch one. Mitigation is the producer's changelog and
  watching the shape of `gross_amount_minor`, not a schema rule.
