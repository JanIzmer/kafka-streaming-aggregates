# Contracts

`order_event.v1.json` is the agreement between the checkout service and this
consumer. It is read at runtime by `orders_stream.contract`, so the validation
the processor performs is exactly what the file says.

## What is in the contract that is *not* in the schema

The `x-contract` block holds the things that matter in a streaming system and
that a plain field schema cannot express:

* **key and why** — `merchant_id`. Partitioning by the aggregation key is what
  makes windowing possible without cross-partition coordination.
* **ordering** — per partition only. Nothing in this system may assume global
  ordering, and the watermark logic is built on that assumption.
* **delivery** — at-least-once. Duplicates are *expected*, not exceptional, and
  removing them is this consumer's job.
* **lateness** — p99 of 120 s observed, 300 s allowed. The difference between
  those two numbers is the safety margin, and it is the single number that
  decides how much state the processor holds.

## Changing the schema

Compatibility is **backward**: a consumer written against v1 must keep working
against a newer producer.

| Change | Breaking? | What happens |
|---|---|---|
| New optional field | no | `additionalProperties: true` means it passes straight through; logged once per field |
| New required field | yes | new topic `orders.v2`, both run in parallel |
| Field removed | yes | new topic version |
| Enum value added to `event_type` | **yes in effect** | unknown types are routed to the DLQ, not silently counted — see below |
| Type widened | no | accepted |
| `amount_minor` changing units | yes, and invisible | nothing catches it; only a distribution check would |

### Why a new enum value is treated as breaking

An unknown `event_type` is not dropped and not counted. It goes to the DLQ,
because the aggregate's meaning depends on which types are included: silently
ignoring `order_partially_refunded` would make revenue figures quietly wrong,
and silently counting it would change what "revenue" means without anyone
deciding to.

### Rolling out v2

1. Producer starts writing `orders.v2` **in addition to** `orders.v1`.
2. This consumer keeps reading v1. A second consumer group reads v2 and writes
   to a parallel aggregate table.
3. Compare the two tables over a few days.
4. Cut the dashboard over, stop the v1 producer, retire the topic after its
   retention window.

Never rewrite a topic in place: replaying it later would then produce different
numbers than the ones already published, and no audit trail would explain why.
