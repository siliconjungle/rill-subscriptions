use rill_subscriptions::*;
use std::collections::{BTreeMap, BTreeSet};
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct Sample {
    id: u64,
    channel: u16,
    reading: i64,
    revision: u64,
}
impl Projection for Sample {
    fn record_id(&self) -> u64 {
        self.id
    }
    fn revision(&self) -> u64 {
        self.revision
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
struct Filter {
    channel: Option<u16>,
    low: i64,
    high: i64,
}
impl Predicate<Sample> for Filter {
    fn validate(&self) -> Result<(), Error> {
        if self.low > self.high {
            Err(Error::Bounds)
        } else {
            Ok(())
        }
    }
    fn matches(&self, r: &Sample) -> bool {
        self.channel.is_none_or(|c| c == r.channel)
            && r.reading >= self.low
            && r.reading <= self.high
    }
}
struct Channels;
impl Router<Sample, Filter> for Channels {
    type Key = u16;
    fn record_key(&self, r: &Sample) -> u16 {
        r.channel
    }
    fn query_keys(&self, q: Filter) -> Option<Vec<u16>> {
        q.channel.map(|c| vec![c])
    }
}
fn relation(
    rs: &BTreeMap<u64, Sample>,
    qs: &BTreeMap<u64, Filter>,
) -> BTreeMap<(u64, u64), Sample> {
    let mut out = BTreeMap::new();
    for (&s, q) in qs {
        for (&id, r) in rs {
            if (q.channel.is_none() || q.channel == Some(r.channel))
                && r.reading >= q.low
                && r.reading <= q.high
            {
                out.insert((s, id), *r);
            }
        }
    }
    out
}
#[test]
fn arbitrary_categorical_projection_matches_full_relation_with_and_without_routing() {
    for partitioned in [false, true] {
        fn run<R: Router<Sample, Filter>>(router: R) {
            let mut engine = SubscriptionEngine::with_router(router);
            let mut rs = BTreeMap::new();
            let mut qs = BTreeMap::new();
            let mut rng = 98345u64;
            for tick in 0..300 {
                let old = relation(&rs, &qs);
                let mut batch = Batch::<Sample, Filter>::default();
                for _ in 0..12 {
                    rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let id = (rng >> 32) % 51;
                    let r = Sample {
                        id,
                        channel: ((rng >> 20) % 5) as u16,
                        reading: ((rng >> 24) % 81) as i64 - 40,
                        revision: tick,
                    };
                    if rng % 7 == 0 {
                        batch.records.push(RecordChange::Remove(id));
                        rs.remove(&id);
                    } else {
                        batch.records.push(RecordChange::Put(r));
                        rs.insert(id, r);
                    }
                }
                for _ in 0..4 {
                    rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let id = (rng >> 32) % 27;
                    let q = Filter {
                        channel: if rng % 3 == 0 {
                            None
                        } else {
                            Some(((rng >> 20) % 5) as u16)
                        },
                        low: -20,
                        high: (rng % 41) as i64,
                    };
                    if rng % 5 == 0 {
                        batch.subscriptions.push(SubscriptionChange::Remove(id));
                        qs.remove(&id);
                    } else {
                        batch.subscriptions.push(SubscriptionChange::Put(id, q));
                        qs.insert(id, q);
                    }
                }
                let new = relation(&rs, &qs);
                let keys: BTreeSet<_> = old.keys().chain(new.keys()).copied().collect();
                let mut expected = Vec::new();
                for (subscription, record) in keys {
                    match (
                        old.get(&(subscription, record)),
                        new.get(&(subscription, record)),
                    ) {
                        (None, Some(r)) => expected.push(Event {
                            subscription,
                            record,
                            kind: EventKind::Added,
                            revision: r.revision,
                        }),
                        (Some(r), None) => expected.push(Event {
                            subscription,
                            record,
                            kind: EventKind::Removed,
                            revision: r.revision,
                        }),
                        (Some(a), Some(b)) if a != b => expected.push(Event {
                            subscription,
                            record,
                            kind: EventKind::Updated,
                            revision: b.revision,
                        }),
                        _ => {}
                    }
                }
                let mut got = engine.apply(&batch).unwrap();
                got.sort_unstable();
                expected.sort_unstable();
                assert_eq!(got, expected, "tick{tick}");
                let actual: BTreeMap<_, _> = engine
                    .snapshot()
                    .into_iter()
                    .map(|(s, r)| ((s, r.id), r))
                    .collect();
                assert_eq!(actual, new);
            }
        }
        if partitioned {
            run(Channels)
        } else {
            run(Unpartitioned)
        }
    }
}
#[test]
fn generic_invalid_batch_is_atomic() {
    let mut engine = SubscriptionEngine::<Sample, Filter, _>::with_router(Channels);
    let q = Filter {
        channel: Some(2),
        low: -10,
        high: 10,
    };
    let r = Sample {
        id: 8,
        channel: 2,
        reading: 5,
        revision: 1,
    };
    engine
        .apply(&Batch {
            records: vec![RecordChange::Put(r)],
            subscriptions: vec![SubscriptionChange::Put(1, q)],
        })
        .unwrap();
    let old = engine.snapshot();
    let bad = Batch {
        records: vec![RecordChange::Remove(8)],
        subscriptions: vec![SubscriptionChange::Put(1, Filter { low: 12, ..q })],
    };
    assert_eq!(engine.apply(&bad), Err(Error::Bounds));
    assert_eq!(engine.snapshot(), old);
}
