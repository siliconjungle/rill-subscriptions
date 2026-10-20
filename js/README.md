# Rill subscriptions for Node.js

A dependency-free JavaScript port of the generic Rust kernel. Apply atomic record/query batches and consume complete `Added`, `Removed` and `Updated` events. Identical predicates share matching and membership storage. No transport or database is required.

```js
import {Engine} from '@siliconjungle/rill-subscriptions';
const engine = new Engine();
const events = engine.apply({
  records: [{type: 'put', record: {id: 42n, point: [10, 0, 20]}}],
  subscriptions: [{type: 'put', id: 7n, query: {
    lower: [0, -10, 0], upper: [100, 10, 100]
  }}]
});
```

The built-in `Engine` uses exact i32 coordinates, u64 BigInt IDs/masks/revisions and i64 BigInt values. Missing tags, values and revisions default to zero. Query tag/range defaults are unconstrained. Inputs are captured; public default projections and coordinates are frozen. IDs never pass through Number. Output order is unspecified and fanout is never capped.

`SubscriptionEngine(adapter)` supports any compact projected record, predicate and primitive partition key. Supply `id`, `revision`, `captureRecord`, `captureQuery`, `equalRecord`, `equalQuery`, `queryKey`, `validateQuery`, `matches`, `recordKey` and `queryKeys`. An optional `validateId` checks operation IDs. Capture functions must own the logical projection; external mutable payloads must not affect equality, routing or matching. Query keys must be stable; hash collisions are resolved with exact query equality. Routing must conservatively cover every possible match. Return `null` for an exact unpartitioned scan; duplicate keys are tolerated.

Capture and query validation run before mutation, including overwritten operations. Custom adapter equality, routing, matching and visits must be pure and must not throw after validation. Adapters are trusted code, like Rust implementations of the corresponding traits; exceptions from those callbacks do not provide transaction rollback.

`applyInto(batch, output)` reuses an event array and returns counters. Final repeated operations win; equal projections and missing-ID removals are no-ops. Mixed query/data changes compare complete before/after membership. `record`, `query`, `matchingRecords` and `snapshot` inspect committed state. `select(query)` evaluates a transient query without retaining a subscription. `forEachMatch(query, visit)` visits borrowed projections without output copies; callers must not mutate them or reenter the engine during a visit. Removing a final subscriber releases its predicate and reverse dependency references.

One writer executes each batch synchronously. Applications own persistence, scheduling and networking. Twenty thousand subscriptions do not establish capacity for twenty thousand network connections. Run `npm test` in this directory. No npm publication has been made yet.
