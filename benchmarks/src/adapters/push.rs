//! Shared result-delta adapter driven by native memo/observer notifications.
//! No polling of unaffected queries or rebuilding of unchanged subscriber rows.
use super::reactive::{Reactive, Runtime};
use crate::model::*;
use rill_subscriptions::*;
use std::collections::{HashMap, HashSet};
struct Group {
    subs: HashSet<u64>,
    members: HashMap<u64, Record>,
}
pub struct Push<R: Runtime> {
    state: State,
    graph: Reactive<R>,
    groups: HashMap<Query, Group>,
}
impl<R: Runtime> Push<R> {
    pub fn new(runtime: R) -> Self {
        Self {
            state: State::default(),
            graph: Reactive::new(runtime),
            groups: HashMap::new(),
        }
    }
}
impl<R: Runtime> Backend for Push<R> {
    fn apply(&mut self, b: &Batch) -> Result<Vec<Event>, Error> {
        let p = self.state.prepare(b)?;
        if p.records.is_empty() && p.queries.is_empty() {
            return Ok(Vec::new());
        }
        let changing: HashSet<_> = p.queries.iter().map(|x| x.0).collect();
        let previous: Vec<_> = p
            .queries
            .iter()
            .map(|&(id, old, _)| {
                (
                    id,
                    old.map(|q| self.groups[&q].members.clone())
                        .unwrap_or_default(),
                )
            })
            .collect();
        for &(id, old, new) in &p.queries {
            if let Some(q) = old {
                self.groups.get_mut(&q).unwrap().subs.remove(&id);
            }
            if let Some(q) = new {
                self.groups
                    .entry(q)
                    .or_insert_with(|| Group {
                        subs: HashSet::new(),
                        members: HashMap::new(),
                    })
                    .subs
                    .insert(id);
            }
        }
        self.groups.retain(|_, g| !g.subs.is_empty());
        self.state.commit(&p);
        self.graph.update(&self.state, &p);
        let mut dirty: HashSet<_> = self
            .graph
            .runtime
            .changed()
            .unwrap_or_else(|| self.groups.keys().copied().collect())
            .into_iter()
            .collect();
        for &(_, _, new) in &p.queries {
            if let Some(q) = new {
                dirty.insert(q);
            }
        }
        let mut out = Vec::new();
        let mut rows = Vec::new();
        for q in dirty {
            if let Some(g) = self.groups.get_mut(&q) {
                rows.clear();
                self.graph.query(&self.state, q, &mut rows);
                let mut next = HashMap::with_capacity(rows.len());
                for &r in &rows {
                    let old = g.members.remove(&r.id);
                    for &id in &g.subs {
                        if !changing.contains(&id) {
                            emit(id, old, Some(r), &mut out);
                        }
                    }
                    assert!(next.insert(r.id, r).is_none());
                }
                for r in g.members.drain().map(|(_, r)| r) {
                    for &id in &g.subs {
                        if !changing.contains(&id) {
                            emit(id, Some(r), None, &mut out);
                        }
                    }
                }
                g.members = next;
            }
        }
        for (id, mut old) in previous {
            if let Some(q) = self.state.queries.get(&id) {
                for &r in self.groups[q].members.values() {
                    emit(id, old.remove(&r.id), Some(r), &mut out);
                }
            }
            for r in old.into_values() {
                emit(id, Some(r), None, &mut out);
            }
        }
        Ok(out)
    }
}
