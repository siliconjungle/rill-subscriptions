use rill_subscriptions::*;
use std::collections::{BTreeMap, HashMap, HashSet};
pub trait Backend {
    fn apply(&mut self, batch: &Batch) -> Result<Vec<Event>, Error>;
}
pub struct Library(pub Engine);
impl Backend for Library {
    fn apply(&mut self, b: &Batch) -> Result<Vec<Event>, Error> {
        self.0.apply(b)
    }
}
#[derive(Default)]
pub struct State {
    pub records: HashMap<u64, Record>,
    pub queries: HashMap<u64, Query>,
}
pub struct Prepared {
    pub records: Vec<(u64, Option<Record>, Option<Record>)>,
    pub queries: Vec<(u64, Option<Query>, Option<Query>)>,
}
impl State {
    pub fn prepare(&self, b: &Batch) -> Result<Prepared, Error> {
        b.validate()?;
        if b.records.iter().all(|op| match op {
            RecordChange::Put(r) => self.records.get(&r.id) == Some(r),
            RecordChange::Remove(id) => !self.records.contains_key(id),
        }) && b.subscriptions.iter().all(|op| match op {
            SubscriptionChange::Put(id, q) => self.queries.get(id) == Some(q),
            SubscriptionChange::Remove(id) => !self.queries.contains_key(id),
        }) {
            return Ok(Prepared {
                records: Vec::new(),
                queries: Vec::new(),
            });
        }
        let mut rs = BTreeMap::new();
        let mut qs = BTreeMap::new();
        for c in &b.records {
            match *c {
                RecordChange::Put(r) => {
                    rs.insert(r.id, Some(r));
                }
                RecordChange::Remove(id) => {
                    rs.insert(id, None);
                }
            }
        }
        for c in &b.subscriptions {
            match *c {
                SubscriptionChange::Put(id, q) => {
                    qs.insert(id, Some(q));
                }
                SubscriptionChange::Remove(id) => {
                    qs.insert(id, None);
                }
            }
        }
        Ok(Prepared {
            records: rs
                .into_iter()
                .filter_map(|(id, new)| {
                    let old = self.records.get(&id).copied();
                    (old != new).then_some((id, old, new))
                })
                .collect(),
            queries: qs
                .into_iter()
                .filter_map(|(id, new)| {
                    let old = self.queries.get(&id).copied();
                    (old != new).then_some((id, old, new))
                })
                .collect(),
        })
    }
    pub fn commit(&mut self, p: &Prepared) {
        for &(id, _, r) in &p.records {
            if let Some(r) = r {
                self.records.insert(id, r);
            } else {
                self.records.remove(&id);
            }
        }
        for &(id, _, q) in &p.queries {
            if let Some(q) = q {
                self.queries.insert(id, q);
            } else {
                self.queries.remove(&id);
            }
        }
    }
}
pub(crate) fn emit(
    subscription: u64,
    old: Option<Record>,
    new: Option<Record>,
    out: &mut Vec<Event>,
) {
    if old == new {
        return;
    }
    match (old, new) {
        (None, Some(r)) => out.push(Event {
            subscription,
            record: r.id,
            kind: EventKind::Added,
            revision: r.revision,
        }),
        (Some(r), None) => out.push(Event {
            subscription,
            record: r.id,
            kind: EventKind::Removed,
            revision: r.revision,
        }),
        (Some(_), Some(r)) => out.push(Event {
            subscription,
            record: r.id,
            kind: EventKind::Updated,
            revision: r.revision,
        }),
        _ => {}
    }
}
#[derive(Default)]
pub struct DeltaScan {
    state: State,
    pub share: bool,
}
impl DeltaScan {
    pub fn new(share: bool) -> Self {
        Self {
            state: State::default(),
            share,
        }
    }
}
impl Backend for DeltaScan {
    fn apply(&mut self, b: &Batch) -> Result<Vec<Event>, Error> {
        let p = self.state.prepare(b)?;
        let mut out = Vec::new();
        if p.records.is_empty() && p.queries.is_empty() {
            return Ok(out);
        }
        let changing: HashSet<_> = p.queries.iter().map(|v| v.0).collect();
        let old_queries: Vec<_> = p
            .queries
            .iter()
            .map(|&(id, old, _)| {
                (
                    id,
                    old.map(|q| {
                        self.state
                            .records
                            .values()
                            .filter(|r| q.matches(r))
                            .map(|r| (r.id, *r))
                            .collect::<HashMap<_, _>>()
                    })
                    .unwrap_or_default(),
                )
            })
            .collect();
        if self.share {
            let mut groups: HashMap<Query, Vec<u64>> = HashMap::new();
            for (&id, &q) in &self.state.queries {
                if !changing.contains(&id) {
                    groups.entry(q).or_default().push(id);
                }
            }
            for (_, old, new) in &p.records {
                for (q, subs) in &groups {
                    let old = old.filter(|r| q.matches(r));
                    let new = new.filter(|r| q.matches(r));
                    if old != new {
                        for &id in subs {
                            emit(id, old, new, &mut out);
                        }
                    }
                }
            }
        } else {
            for (_, old, new) in &p.records {
                for (&id, q) in &self.state.queries {
                    if !changing.contains(&id) {
                        emit(
                            id,
                            old.filter(|r| q.matches(r)),
                            new.filter(|r| q.matches(r)),
                            &mut out,
                        );
                    }
                }
            }
        }
        self.state.commit(&p);
        for (id, mut old) in old_queries {
            if let Some(q) = self.state.queries.get(&id) {
                for row in self.state.records.values().filter(|r| q.matches(r)) {
                    emit(id, old.remove(&row.id), Some(*row), &mut out);
                }
            }
            for row in old.into_values() {
                emit(id, Some(row), None, &mut out);
            }
        }
        Ok(out)
    }
}
pub trait Evaluator {
    fn update(&mut self, state: &State, p: &Prepared);
    fn query(&mut self, state: &State, query: Query, output: &mut Vec<Record>);
}
#[derive(Default)]
pub struct Scan;
impl Evaluator for Scan {
    fn update(&mut self, _: &State, _: &Prepared) {}
    fn query(&mut self, s: &State, q: Query, out: &mut Vec<Record>) {
        out.extend(s.records.values().filter(|r| q.matches(r)).copied());
    }
}
pub struct Recompute<E> {
    pub state: State,
    evaluator: E,
    members: HashMap<(u64, u64), Record>,
    shared: bool,
}
impl<E: Evaluator> Recompute<E> {
    pub fn new(evaluator: E, shared: bool) -> Self {
        Self {
            state: State::default(),
            evaluator,
            members: HashMap::new(),
            shared,
        }
    }
}
impl<E: Evaluator> Backend for Recompute<E> {
    fn apply(&mut self, b: &Batch) -> Result<Vec<Event>, Error> {
        let p = self.state.prepare(b)?;
        if p.records.is_empty() && p.queries.is_empty() {
            return Ok(Vec::new());
        }
        self.state.commit(&p);
        self.evaluator.update(&self.state, &p);
        let mut out = Vec::new();
        let mut next = HashMap::with_capacity(self.members.len());
        let mut rows = Vec::new();
        let mut cache: HashMap<Query, Vec<Record>> = HashMap::new();
        for (&id, &q) in &self.state.queries {
            let records = if self.shared {
                cache.entry(q).or_insert_with(|| {
                    let mut rs = Vec::new();
                    self.evaluator.query(&self.state, q, &mut rs);
                    rs
                })
            } else {
                rows.clear();
                self.evaluator.query(&self.state, q, &mut rows);
                &rows
            };
            for &r in records {
                let old = self.members.remove(&(id, r.id));
                emit(id, old, Some(r), &mut out);
                assert!(
                    next.insert((id, r.id), r).is_none(),
                    "duplicate native query row"
                );
            }
        }
        for ((id, _), r) in self.members.drain() {
            emit(id, Some(r), None, &mut out);
        }
        self.members = next;
        Ok(out)
    }
}
pub type Cell = [i32; 3];
pub fn cell(p: [i32; 3], size: i32) -> Cell {
    p.map(|x| x.div_euclid(size))
}
pub fn cells(q: Query, size: i32, budget: usize) -> Option<Vec<Cell>> {
    let lo = cell(q.lower, size);
    let hi = cell(q.upper, size);
    let n: i128 = (0..3)
        .map(|d| i128::from(hi[d]) - i128::from(lo[d]) + 1)
        .product();
    if n > budget as i128 {
        return None;
    }
    let mut out = Vec::with_capacity(n as usize);
    for x in lo[0]..=hi[0] {
        for y in lo[1]..=hi[1] {
            for z in lo[2]..=hi[2] {
                out.push([x, y, z]);
            }
        }
    }
    Some(out)
}
pub struct Grid {
    pub size: i32,
    pub buckets: HashMap<Cell, HashSet<u64>>,
}
impl Grid {
    pub fn new(size: i32) -> Self {
        Self {
            size,
            buckets: HashMap::new(),
        }
    }
}
impl Evaluator for Grid {
    fn update(&mut self, _: &State, p: &Prepared) {
        for &(id, old, new) in &p.records {
            let a = old.map(|r| cell(r.point, self.size));
            let b = new.map(|r| cell(r.point, self.size));
            if a == b {
                continue;
            }
            if let Some(k) = a {
                let ids = self.buckets.get_mut(&k).unwrap();
                ids.remove(&id);
                if ids.is_empty() {
                    self.buckets.remove(&k);
                }
            }
            if let Some(k) = b {
                self.buckets.entry(k).or_default().insert(id);
            }
        }
    }
    fn query(&mut self, s: &State, q: Query, out: &mut Vec<Record>) {
        if let Some(keys) = cells(q, self.size, 512) {
            for k in keys {
                if let Some(ids) = self.buckets.get(&k) {
                    out.extend(ids.iter().map(|id| s.records[id]).filter(|r| q.matches(r)));
                }
            }
        } else {
            out.extend(s.records.values().filter(|r| q.matches(r)).copied());
        }
    }
}
