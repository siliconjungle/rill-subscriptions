//! Actual DBSP maintained indexed Z-set join. Both direct and shared-query
//! wrappers retain signed relations; no full relation recompute per batch.
use crate::model::*;
use dbsp::{
    CircuitHandle, IndexedZSetHandle, IndexedZSetReader, OrdIndexedZSet, OutputHandle, RootCircuit,
    utils::{Tup2, Tup3, Tup5},
};
use rill_subscriptions::*;
use std::collections::HashMap;
type C = Tup3<i32, i32, i32>;
type R = Tup5<u64, C, u64, i64, u64>;
type Q = Tup5<C, C, u64, i64, i64>;
type O = Tup2<u64, R>;
fn coord(p: [i32; 3]) -> C {
    Tup3(p[0], p[1], p[2])
}
fn point(p: C) -> [i32; 3] {
    [p.0, p.1, p.2]
}
fn row(r: Record) -> R {
    Tup5(r.id, coord(r.point), r.tags, r.value, r.revision)
}
fn query(q: Query) -> Q {
    Tup5(
        coord(q.lower),
        coord(q.upper),
        q.required_tags,
        q.min_value,
        q.max_value,
    )
}
fn matches(q: Q, r: R) -> bool {
    Query {
        lower: point(q.0),
        upper: point(q.1),
        required_tags: q.2,
        min_value: q.3,
        max_value: q.4,
    }
    .matches(&Record {
        id: r.0,
        point: point(r.1),
        tags: r.2,
        value: r.3,
        revision: r.4,
    })
}
const GLOBAL: C = Tup3(i32::MIN, i32::MIN, i32::MIN);
pub struct Native {
    state: State,
    circuit: CircuitHandle,
    records: IndexedZSetHandle<C, R>,
    queries: IndexedZSetHandle<C, Tup2<u64, Q>>,
    output: OutputHandle<OrdIndexedZSet<O, ()>>,
}
impl Native {
    pub fn new() -> Self {
        let (circuit, (records, queries, output)) = RootCircuit::build(|c| {
            let (rs, records) = c.add_input_indexed_zset::<C, R>();
            let (qs, queries) = c.add_input_indexed_zset::<C, Tup2<u64, Q>>();
            let joined = qs.join_index(&rs, |_, q, r| {
                matches(q.1, *r).then_some((Tup2(q.0, *r), ()))
            });
            Ok((records, queries, joined.output()))
        })
        .unwrap();
        Self {
            state: State::default(),
            circuit,
            records,
            queries,
            output,
        }
    }
}
impl Backend for Native {
    fn apply(&mut self, b: &Batch) -> Result<Vec<Event>, Error> {
        let p = self.state.prepare(b)?;
        if p.records.is_empty() && p.queries.is_empty() {
            return Ok(Vec::new());
        }
        for &(_, old, new) in &p.records {
            for (r, w) in [(old, -1), (new, 1)] {
                if let Some(r) = r {
                    self.records.push(coord(cell(r.point, 64)), (row(r), w));
                    self.records.push(GLOBAL, (row(r), w));
                }
            }
        }
        for &(id, old, new) in &p.queries {
            for (q, w) in [(old, -1), (new, 1)] {
                if let Some(q) = q {
                    for k in cells(q, 64, 512)
                        .map(|ks| ks.into_iter().map(coord).collect())
                        .unwrap_or_else(|| vec![GLOBAL])
                    {
                        self.queries.push(k, (Tup2(id, query(q)), w));
                    }
                }
            }
        }
        self.circuit.step().unwrap();
        let mut changes: HashMap<(u64, u64), HashMap<R, i64>> = HashMap::new();
        let output = self.output.consolidate();
        for (Tup2(id, r), _, w) in output.iter() {
            *changes.entry((id, r.0)).or_default().entry(r).or_default() += w;
        }
        let mut out = Vec::new();
        for ((subscription, record), rows) in changes {
            let (mut old, mut new) = (None, None);
            for (r, w) in rows {
                match w {
                    0 => {}
                    -1 => assert!(old.replace(r).is_none()),
                    1 => assert!(new.replace(r).is_none()),
                    _ => panic!("DBSP set weight {w}"),
                }
            }
            match (old, new) {
                (None, None) => {}
                (Some(r), None) => out.push(Event {
                    subscription,
                    record,
                    kind: EventKind::Removed,
                    revision: r.4,
                }),
                (None, Some(r)) => out.push(Event {
                    subscription,
                    record,
                    kind: EventKind::Added,
                    revision: r.4,
                }),
                (Some(a), Some(r)) if a != r => out.push(Event {
                    subscription,
                    record,
                    kind: EventKind::Updated,
                    revision: r.4,
                }),
                _ => {}
            }
        }
        self.state.commit(&p);
        Ok(out)
    }
}
