//! Canonical-query sharing wrapper for backends that otherwise maintain one
//! query identity per subscriber. Group membership is retained once; output is
//! still fully expanded to every live subscriber, including atomic reconfiguration.
use crate::model::*;
use rill_subscriptions::*;
use std::collections::{HashMap, HashSet};
struct Group {
    id: u64,
    subs: HashSet<u64>,
    members: HashMap<u64, Record>,
}
pub struct Shared {
    state: State,
    inner: Box<dyn Backend>,
    groups: HashMap<Query, Group>,
    keys: HashMap<u64, Query>,
    next: u64,
}
impl Shared {
    pub fn new(inner: Box<dyn Backend>) -> Self {
        Self {
            state: State::default(),
            inner,
            groups: HashMap::new(),
            keys: HashMap::new(),
            next: 0,
        }
    }
}
impl Backend for Shared {
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
                if !self.groups.contains_key(&q) {
                    let gid = self.next;
                    self.next += 1;
                    self.groups.insert(
                        q,
                        Group {
                            id: gid,
                            subs: HashSet::new(),
                            members: HashMap::new(),
                        },
                    );
                }
                self.groups.get_mut(&q).unwrap().subs.insert(id);
            }
        }
        let mut inner = Batch {
            records: p
                .records
                .iter()
                .map(|&(id, _, new)| {
                    new.map(RecordChange::Put)
                        .unwrap_or(RecordChange::Remove(id))
                })
                .collect(),
            subscriptions: Vec::new(),
        };
        let gone: Vec<_> = self
            .groups
            .iter()
            .filter_map(|(q, g)| g.subs.is_empty().then_some((*q, g.id)))
            .collect();
        for (q, id) in gone {
            self.groups.remove(&q);
            if self.keys.remove(&id).is_some() {
                inner.subscriptions.push(SubscriptionChange::Remove(id));
            }
        }
        for (&q, g) in &self.groups {
            if let std::collections::hash_map::Entry::Vacant(e) = self.keys.entry(g.id) {
                e.insert(q);
                inner.subscriptions.push(SubscriptionChange::Put(g.id, q));
            }
        }
        self.state.commit(&p);
        let events = self.inner.apply(&inner)?;
        let mut out = Vec::new();
        for e in events {
            let Some(q) = self.keys.get(&e.subscription) else {
                continue;
            };
            let g = self.groups.get_mut(q).unwrap();
            match e.kind {
                EventKind::Removed => {
                    g.members.remove(&e.record);
                }
                _ => {
                    g.members.insert(e.record, self.state.records[&e.record]);
                }
            }
            out.extend(
                g.subs
                    .iter()
                    .filter(|id| !changing.contains(id))
                    .map(|&subscription| Event { subscription, ..e }),
            );
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
