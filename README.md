# Rill subscriptions

Exact incremental subscriptions in Rust. Records and queries change in atomic batches; the engine emits complete `Added`, `Removed` and `Updated` events. Identical predicates share matching and membership storage while every subscriber receives its notifications.

**Performance acceptance is OPEN.** The target is 20,000 active **in-process subscriptions**. No claim of outperforming the whole inventory, sustained server capacity or 20,000 network clients is made. See [the preregistered protocol](research/protocol.json), [source notes](research/source-notes.md) and raw [results](results).

The core has no dependencies and forbids unsafe code. The benchmark workspace calls actual upstream libraries behind optional features; it is not part of the published library.

```rust
use rill_subscriptions::*;

let mut subscriptions = Engine::default();
let events = subscriptions.apply(&Batch {
    records: vec![RecordChange::Put(Record::new(42, [10, 0, 20]))],
    subscriptions: vec![SubscriptionChange::Put(
        7, Query::bounds([0, -10, 0], [100, 10, 100]),
    )],
})?;
assert_eq!(events[0].kind, EventKind::Added);
# Ok::<(), Error>(())
```

`Engine` is a ready-to-use specialization for exact integer 3-D bounds, tag masks and numeric ranges. The same kernel supports application-defined records, predicates and conservative partition routing:

```rust
use rill_subscriptions::*;

#[derive(Clone, Copy, PartialEq, Eq)]
struct Sample { id: u64, channel: u16, reading: i64, revision: u64 }
impl Projection for Sample {
    fn record_id(&self) -> u64 { self.id }
    fn revision(&self) -> u64 { self.revision }
}
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct Filter { channel: u16, minimum: i64 }
impl Predicate<Sample> for Filter {
    fn matches(&self, r: &Sample) -> bool {
        r.channel == self.channel && r.reading >= self.minimum
    }
}
struct Channels;
impl Router<Sample, Filter> for Channels {
    type Key = u16;
    fn record_key(&self, r: &Sample) -> u16 { r.channel }
    fn query_keys(&self, q: Filter) -> Option<Vec<u16>> {
        Some(vec![q.channel])
    }
}
let mut engine = SubscriptionEngine::with_router(Channels);
engine.apply(&Batch {
    records: vec![RecordChange::Put(Sample {
        id: 42, channel: 3, reading: 17, revision: 1,
    })],
    subscriptions: vec![SubscriptionChange::Put(7, Filter {
        channel: 3, minimum: 10,
    })],
})?;
assert_eq!(engine.matching_records(7).len(), 1);
# Ok::<(), Error>(())
```

Use `Unpartitioned` for any predicate when no routing scheme is available. Partition keys are an optimization: a query's keys must conservatively cover every partition that can contain a match. Returning `None` safely considers every partition. Duplicate query keys are tolerated. Records and predicates are compact `Copy` projections; store large payloads outside the engine and identify their changes with a revision. Stable IDs and equality must reflect the observable projection.

Batches commit final before/after views. Repeated operations on an ID use the last operation. Equal records/queries and removal of missing IDs are no-ops. A changed record that remains matched emits `Updated`; moving a query alone does not mark its unchanged intersection updated. Removed events identify the old revision. Invalid predicates are rejected before state or caller output changes, including invalid queries earlier in a batch. Events and snapshot rows have unspecified order and are never capped.

`apply_into` reuses an output buffer and returns work counters. `record`, `query`, `matching_records` and `snapshot` inspect committed state. Removing the last subscriber releases its predicate and dependency references. The engine has one writer per batch, ordinary Rust ownership and no mandatory async runtime; applications own scheduling, persistence and notification transport. Full fanout costs at least the number of events delivered.

Run core validation with `tools/cargo.sh test -p rill-subscriptions --locked`. Benchmark builds use exact versions in `Cargo.lock` and the isolated compiler chosen by `RILL_RUST_TOOLCHAIN`. Screens, exact executable-bound proofs, serial paired runs and separate allocation-audit builds are implemented under `tools/` and `benchmarks/`. A configuration or component variant is not counted as another independent upstream subscription engine.
