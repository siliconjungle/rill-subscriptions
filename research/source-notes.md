# Source survey and comparison boundaries

The inventory is discovery, not a claim that 57 independent subscription engines have been measured. Versions/checksums come from the primary crates.io sparse index. Native adapters call upstream code. Index/bitmap primitives, supporting runtimes, framework integrations and algorithm configurations are labelled separately.

- [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow), pinned 0.25.1 with Timely 0.31.0. Maintained signed equijoins. Compare both a join followed by filtering and predicate fusion inside `join_core`; both are genuine upstream operators. [Shared arrangements](https://timelydataflow.github.io/differential-dataflow/chapter_5/chapter_5_3.html) motivated explicit canonical-query controls. Joining 20,000 identical subscriptions separately is not the strongest available plan.
- [DBSP tutorial](https://docs.rs/dbsp/0.367.0/dbsp/tutorial/index.html) describes signed indexed Z-set input and incremental joins. Prepared native typed circuit adapter. The pinned current release requires Rust 1.96.1; that exact isolated toolchain was installed without changing the user's default. Build/validation pending until the active timing screen finishes.
- [Drasi Core](https://github.com/drasi-project/drasi-core), pinned 0.5.10. Prepared one continuous Cypher query over cell-joined subscription/record nodes, full u64 IDs encoded as strings and exact mask postfilter. Native per-change results are coalesced against complete before/after batch views. Build/validation pending.
- [Datafrog](https://github.com/rust-lang/datafrog), Ascent and Crepe: native relation evaluation with a cell equality join and exact postfilter. Their current comparison adapters recompute relations for retractions; they are not presented as native general incremental deletion engines.
- [Salsa](https://salsa-rs.github.io/salsa/), Incremental, Adapton and related memo engines: partition inputs by cell, create dependencies for empty cells too, retain one memo per unique predicate. Whole-world inputs would invalidate every subscription and would be an inadequate sole comparison.
- [Reactive Graph](https://docs.rs/reactive_graph/0.2.16/reactive_graph/), Floem, Sycamore, Leptos, Futures Signals and Dioxus: compare cached pull evaluation and, where native notifications exist, a notification-driven result-delta adapter. Include native batching/task draining and complete notification fanout. Pool scope-owned inputs where explicit deletion is unavailable; dispose owned scopes otherwise.
- Rstar: compare both query re-evaluation and genuinely change-driven trees in both directions. New/removed records query the subscription-box tree; subscription movement queries the record tree. Explicit sharing controls preserve all subscriber notifications.
- Roaring, Bitvec, FixedBitSet, Bit-set, Hibitset, Rangemap and Intervaltree are component comparisons, not separate full subscription engines. Dense set storage has different memory costs from sparse storage. Full stable u64 IDs are mapped to internal slots and restored exactly.
- Existing Rill Spatial: only source/Cargo/licenses from full commit `f160fb893b58291c66329826bca1701aad6bfa09` materialized. Its live repository, experiments and workers are untouched. Conservative radius candidate lookup is followed by exact integer predicates; no neighbor cap.

Two discovered names were unrelated: `reflex` 0.1.2 is a lexer; `frp` 0.1.0 is the network proxy associated with gofrp. Neither is evidence for an incremental-computation comparator. The inventory will retain those corrections rather than silently inflate the count.

The public kernel is being generalized around compact application projections, exact predicates and conservative partition routing. Spatial/tag/numeric queries are one ready-to-use specialization. This does not implement a SQL parser, recursive Datalog, persistence, network connections or application transaction durability. The 20,000 target here means active exact in-process subscriptions.

Acceptance remains OPEN. Small screens select experiments; they do not establish sustained p99, allocations, isolated-host results, or a universal speed ranking.

## Publication checkpoint

The generic core passes eight behavioral tests and two doctests. The expanded
native adapter sweep is development work: the reactive-graph and futures-signals
push variants currently miss notifications; their results are ineligible for
performance claims. The sweep also exposed upstream stdout before JSON in Drasi;
the driver now preserves those lines separately. No claim of beating every
engine is made. The DBSP adapter has not yet been built.
