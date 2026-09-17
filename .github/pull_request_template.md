## What changed

## Streaming impact

- [ ] Does this change the event contract? Link the version.
- [ ] Does it change window size, allowed lateness or the flush cadence?
      If yes, state the new state-size and latency implications.
- [ ] Does it change the order of flush / DLQ publish / offset commit?
      If yes, explain which crash point is now survivable and which is not.
- [ ] Does it change the aggregation key or the partition key?
      Either one requires a new topic, not a redeploy.

## Replay safety

- [ ] Can this change be deployed without replaying the topic?
- [ ] If the consumer group is reset, do the aggregates converge to the same
      values? Which test proves it?

## Verification

- [ ] `make test`
- [ ] `make up && make demo` then `make aggregates` / `make dlq`
