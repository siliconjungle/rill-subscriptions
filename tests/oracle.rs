use rill_subscriptions::*;
use std::collections::BTreeMap;
#[derive(Default)]
struct Oracle {
    records: BTreeMap<u64, Record>,
    queries: BTreeMap<u64, Query>,
}
impl Oracle {
    fn snapshot(&self) -> BTreeMap<(u64, u64), Record> {
        let mut out = BTreeMap::new();
        for (&q, query) in &self.queries {
            for (&r, row) in &self.records {
                let inside = (0..3)
                    .all(|d| query.lower[d] <= row.point[d] && row.point[d] <= query.upper[d]);
                if inside
                    && row.tags & query.required_tags == query.required_tags
                    && row.value >= query.min_value
                    && row.value <= query.max_value
                {
                    out.insert((q, r), *row);
                }
            }
        }
        out
    }
    fn apply(&mut self, b: &Batch) -> Vec<Event> {
        let old = self.snapshot();
        for op in &b.records {
            match *op {
                RecordChange::Put(r) => {
                    self.records.insert(r.id, r);
                }
                RecordChange::Remove(id) => {
                    self.records.remove(&id);
                }
            }
        }
        for op in &b.subscriptions {
            match *op {
                SubscriptionChange::Put(id, q) => {
                    self.queries.insert(id, q);
                }
                SubscriptionChange::Remove(id) => {
                    self.queries.remove(&id);
                }
            }
        }
        let new = self.snapshot();
        let mut out = Vec::new();
        for (&(q, r), row) in &old {
            match new.get(&(q, r)) {
                None => out.push(Event {
                    subscription: q,
                    record: r,
                    kind: EventKind::Removed,
                    revision: row.revision,
                }),
                Some(current) if current != row => out.push(Event {
                    subscription: q,
                    record: r,
                    kind: EventKind::Updated,
                    revision: current.revision,
                }),
                _ => {}
            }
        }
        for (&(q, r), row) in &new {
            if !old.contains_key(&(q, r)) {
                out.push(Event {
                    subscription: q,
                    record: r,
                    kind: EventKind::Added,
                    revision: row.revision,
                });
            }
        }
        out.sort_unstable();
        out
    }
}
fn check(engine: &mut Engine, oracle: &mut Oracle, b: &Batch) {
    let mut got = engine.apply(b).unwrap();
    got.sort_unstable();
    assert_eq!(got, oracle.apply(b));
    let current: BTreeMap<_, _> = engine
        .snapshot()
        .into_iter()
        .map(|(q, r)| ((q, r.id), r))
        .collect();
    assert_eq!(current, oracle.snapshot());
    assert_eq!(engine.record_count(), oracle.records.len());
    assert_eq!(engine.subscription_count(), oracle.queries.len());
}
#[test]
fn boundaries_negative_cells_tags_numeric_extremes_and_wide_ids() {
    for size in [1, 3, 64, i32::MAX] {
        let mut e = Engine::new(Options {
            cell_size: size,
            max_query_cells: 8,
        })
        .unwrap();
        let mut o = Oracle::default();
        let rows = [
            Record {
                id: u64::MAX,
                point: [i32::MIN, -64, 0],
                tags: u64::MAX,
                value: i64::MIN,
                revision: u64::MAX,
            },
            Record {
                id: 1 << 63,
                point: [i32::MAX, 64, 0],
                tags: 3,
                value: i64::MAX,
                revision: 1,
            },
            Record::new(13, [-1, 0, 1]),
        ];
        let b = Batch {
            records: rows.into_iter().map(RecordChange::Put).collect(),
            subscriptions: vec![
                SubscriptionChange::Put(u64::MAX, Query::all()),
                SubscriptionChange::Put(2, Query::bounds([-64, -64, -64], [64, 64, 64])),
                SubscriptionChange::Put(
                    3,
                    Query {
                        required_tags: 3,
                        ..Query::all()
                    },
                ),
                SubscriptionChange::Put(4, Query::bounds([i32::MAX, 64, 0], [i32::MAX, 64, 0])),
            ],
        };
        check(&mut e, &mut o, &b);
        for r in rows {
            check(
                &mut e,
                &mut o,
                &Batch {
                    records: vec![RecordChange::Remove(r.id)],
                    ..Batch::default()
                },
            );
        }
    }
}
#[test]
fn mixed_batches_repeated_operations_id_reuse_and_revision_only_updates() {
    let mut e = Engine::default();
    let mut o = Oracle::default();
    let r = Record::new(9, [0, 0, 0]);
    let batches = [
        Batch {
            records: vec![RecordChange::Put(r)],
            subscriptions: vec![
                SubscriptionChange::Put(1, Query::all()),
                SubscriptionChange::Put(2, Query::all()),
            ],
        },
        Batch {
            records: vec![
                RecordChange::Remove(9),
                RecordChange::Put(Record { revision: 1, ..r }),
            ],
            subscriptions: vec![
                SubscriptionChange::Put(1, Query::bounds([-1; 3], [1; 3])),
                SubscriptionChange::Remove(2),
            ],
        },
        Batch {
            records: vec![RecordChange::Put(Record {
                point: [4, 0, 0],
                revision: 2,
                ..r
            })],
            subscriptions: vec![
                SubscriptionChange::Put(1, Query::bounds([3, -1, -1], [5, 1, 1])),
                SubscriptionChange::Put(2, Query::all()),
            ],
        },
        Batch {
            records: vec![RecordChange::Remove(9)],
            subscriptions: vec![SubscriptionChange::Remove(1)],
        },
        Batch {
            records: vec![RecordChange::Put(r)],
            subscriptions: vec![SubscriptionChange::Put(1, Query::all())],
        },
        Batch {
            records: vec![RecordChange::Put(r)],
            subscriptions: vec![SubscriptionChange::Put(1, Query::all())],
        },
    ];
    for b in batches {
        check(&mut e, &mut o, &b);
    }
}
#[test]
fn groups_share_membership_but_deliver_every_logical_event_and_release_on_last_unsubscribe() {
    let mut e = Engine::default();
    let b = Batch {
        records: vec![RecordChange::Put(Record::new(1, [0; 3]))],
        subscriptions: (0..20000)
            .map(|id| SubscriptionChange::Put(id, Query::all()))
            .collect(),
    };
    let events = e.apply(&b).unwrap();
    assert_eq!(events.len(), 20000);
    assert_eq!(e.unique_query_count(), 1);
    let events = e
        .apply(&Batch {
            records: vec![RecordChange::Put(Record {
                revision: 1,
                ..Record::new(1, [0; 3])
            })],
            ..Batch::default()
        })
        .unwrap();
    assert_eq!(events.len(), 20000);
    assert!(events.iter().all(|e| e.kind == EventKind::Updated));
    e.apply(&Batch {
        subscriptions: (0..20000).map(SubscriptionChange::Remove).collect(),
        ..Batch::default()
    })
    .unwrap();
    assert_eq!(e.subscription_count(), 0);
    assert_eq!(e.unique_query_count(), 0);
    assert!(
        e.apply(&Batch {
            records: vec![RecordChange::Put(Record::new(2, [0; 3]))],
            ..Batch::default()
        })
        .unwrap()
        .is_empty()
    );
    e.apply(&Batch {
        subscriptions: vec![SubscriptionChange::Put(0, Query::all())],
        ..Batch::default()
    })
    .unwrap();
    assert_eq!(e.matching_records(0).len(), 2);
}
#[test]
fn invalid_batch_leaves_state_and_output_unchanged() {
    let mut e = Engine::default();
    e.apply(&Batch {
        records: vec![RecordChange::Put(Record::new(1, [0; 3]))],
        subscriptions: vec![SubscriptionChange::Put(1, Query::all())],
    })
    .unwrap();
    let before = e.snapshot();
    let mut events = vec![Event {
        subscription: 999,
        record: 888,
        kind: EventKind::Added,
        revision: 7,
    }];
    let original = events.clone();
    let bad = Batch {
        records: vec![RecordChange::Remove(1)],
        subscriptions: vec![SubscriptionChange::Put(2, Query::bounds([1; 3], [-1; 3]))],
    };
    assert_eq!(e.apply_into(&bad, &mut events), Err(Error::Bounds));
    assert_eq!(events, original);
    assert_eq!(e.snapshot(), before);
    assert!(matches!(
        Engine::new(Options {
            cell_size: 0,
            max_query_cells: 1
        }),
        Err(Error::Configuration)
    ));
}
#[test]
fn randomized_mixed_trace_matches_independent_full_recomputation() {
    for seed in [3_u64, 71923, 61937, 99177] {
        for options in [
            Options::default(),
            Options {
                cell_size: 7,
                max_query_cells: 2,
            },
        ] {
            let mut e = Engine::new(options).unwrap();
            let mut o = Oracle::default();
            let mut state = seed;
            let mut rnd = || {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                state >> 32
            };
            for step in 0..180 {
                let mut b = Batch::default();
                for _ in 0..1 + rnd() % 8 {
                    let id = (rnd() % 65).wrapping_add(1 << 63);
                    if rnd() % 5 == 0 {
                        b.records.push(RecordChange::Remove(id));
                    } else {
                        b.records.push(RecordChange::Put(Record {
                            id,
                            point: [rnd() as i32 % 150, rnd() as i32 % 20, rnd() as i32 % 150],
                            tags: rnd() % 16,
                            value: rnd() as i64 % 50,
                            revision: step,
                        }));
                    }
                }
                for _ in 0..rnd() % 5 {
                    let id = rnd() % 35;
                    if rnd() % 4 == 0 {
                        b.subscriptions.push(SubscriptionChange::Remove(id));
                    } else {
                        let p = [rnd() as i32 % 150, rnd() as i32 % 20, rnd() as i32 % 150];
                        let radius = (rnd() % 100) as i32;
                        let q = if rnd() % 8 == 0 {
                            Query::all()
                        } else {
                            Query {
                                required_tags: rnd() % 8,
                                min_value: -30,
                                max_value: 30,
                                ..Query::bounds(p.map(|v| v - radius), p.map(|v| v + radius))
                            }
                        };
                        b.subscriptions.push(SubscriptionChange::Put(id, q));
                    }
                }
                check(&mut e, &mut o, &b);
            }
        }
    }
}
#[test]
fn packed_membership_promotes_demotes_and_updates_packed_record_offsets() {
    let mut e = Engine::new(Options {
        cell_size: 64,
        max_query_cells: 512,
    })
    .unwrap();
    let mut o = Oracle::default();
    let initial = Batch {
        records: (0..1000)
            .map(|id| RecordChange::Put(Record::new(id, [id as i32 % 20, 0, 0])))
            .collect(),
        subscriptions: vec![
            SubscriptionChange::Put(1, Query::all()),
            SubscriptionChange::Put(2, Query::bounds([0, -1, -1], [12, 1, 1])),
        ],
    };
    check(&mut e, &mut o, &initial);
    check(
        &mut e,
        &mut o,
        &Batch {
            records: (0..900).map(RecordChange::Remove).collect(),
            ..Batch::default()
        },
    );
    check(
        &mut e,
        &mut o,
        &Batch {
            records: (0..700)
                .rev()
                .map(|id| {
                    RecordChange::Put(Record {
                        id,
                        point: [id as i32 % 30, 0, 0],
                        tags: 0,
                        value: 0,
                        revision: 1,
                    })
                })
                .collect(),
            ..Batch::default()
        },
    );
    let b = Batch {
        records: (0..700)
            .step_by(3)
            .map(|id| {
                RecordChange::Put(Record {
                    id,
                    point: [300, 0, 0],
                    tags: 0,
                    value: 0,
                    revision: 2,
                })
            })
            .collect(),
        subscriptions: vec![SubscriptionChange::Put(
            2,
            Query::bounds([10, -1, -1], [301, 1, 1]),
        )],
    };
    check(&mut e, &mut o, &b);
    check(
        &mut e,
        &mut o,
        &Batch {
            records: (0..700).step_by(2).map(RecordChange::Remove).collect(),
            ..Batch::default()
        },
    );
    let mut unchanged = Vec::new();
    let stats = e.apply_into(&Batch::default(), &mut unchanged).unwrap();
    assert_eq!(stats.changed_records, 0);
    assert_eq!(stats.changed_subscriptions, 0);
    assert_eq!(stats.unique_queries, 2);
}
