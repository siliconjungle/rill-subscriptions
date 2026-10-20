//! Exact incremental subscriptions over projected records.
//!
//! The kernel has no game, database, transport or async-runtime dependency.
//! Application payloads stay with the caller: stable IDs and `revision` identify
//! the projection to deliver. Integer bounds are inclusive and exact. A batch
//! commits its final before/after relation; repeated operations use the last value.
//! Events are unordered and never capped. Invalid queries leave state and output
//! unchanged. Only live subscribers retain a shared query group.
#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap, HashSet};

pub type RecordId = u64;
pub type SubscriptionId = u64;
type Cell = [i32; 3];

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Record {
    pub id: RecordId,
    pub point: [i32; 3],
    pub tags: u64,
    pub value: i64,
    /// Change this when application payload outside the indexed projection changes.
    pub revision: u64,
}
impl Record {
    pub fn new(id: RecordId, point: [i32; 3]) -> Self {
        Self { id, point, tags: 0, value: 0, revision: 0 }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Query {
    pub lower: [i32; 3],
    pub upper: [i32; 3],
    pub required_tags: u64,
    pub min_value: i64,
    pub max_value: i64,
}
impl Query {
    pub const fn all() -> Self {
        Self { lower: [i32::MIN; 3], upper: [i32::MAX; 3], required_tags: 0,
            min_value: i64::MIN, max_value: i64::MAX }
    }
    pub const fn bounds(lower: [i32; 3], upper: [i32; 3]) -> Self {
        Self { lower, upper, ..Self::all() }
    }
    pub fn validate(&self) -> Result<(), Error> {
        if self.min_value > self.max_value || (0..3).any(|d| self.lower[d] > self.upper[d]) {
            Err(Error::Bounds)
        } else { Ok(()) }
    }
    #[inline]
    pub fn matches(&self, row: &Record) -> bool {
        row.tags & self.required_tags == self.required_tags
            && row.value >= self.min_value && row.value <= self.max_value
            && (0..3).all(|d| row.point[d] >= self.lower[d] && row.point[d] <= self.upper[d])
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordChange { Put(Record), Remove(RecordId) }
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubscriptionChange { Put(SubscriptionId, Query), Remove(SubscriptionId) }
#[derive(Clone, Debug, Default)]
pub struct Batch {
    pub records: Vec<RecordChange>,
    pub subscriptions: Vec<SubscriptionChange>,
}
impl Batch {
    pub fn validate(&self) -> Result<(), Error> {
        for change in &self.subscriptions {
            if let SubscriptionChange::Put(_, query) = change { query.validate()?; }
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum EventKind { Added, Removed, Updated }
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Event {
    pub subscription: SubscriptionId,
    pub record: RecordId,
    pub kind: EventKind,
    /// The removed projection's revision, or the current added/updated revision.
    pub revision: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error { Bounds, Configuration }
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self { Self::Bounds => "inverted query bounds", Self::Configuration => "cell size and cell budget must be positive" })
    }
}
impl std::error::Error for Error {}
#[derive(Clone, Copy, Debug)]
pub struct Options { pub cell_size: i32, pub max_query_cells: usize }
impl Default for Options {
    fn default() -> Self { Self { cell_size: 64, max_query_cells: 512 } }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Stats {
    pub changed_records: usize,
    pub changed_subscriptions: usize,
    pub unique_queries: usize,
    pub predicate_tests: usize,
    pub emitted: usize,
}
struct Group {
    query: Query,
    subscribers: HashSet<SubscriptionId>,
    members: HashSet<RecordId>,
    cells: Option<Vec<Cell>>,
}

/// Shared query groups and a reverse spatial dependency index. Single-writer
/// batches use ordinary Rust ownership; concurrent readers may borrow `&Engine`.
/// Broad queries fall back to exact scanning rather than expanding unbounded cells.
/// Identical subscriptions share matching and membership storage, not notifications.
/// Every subscriber still receives every required logical event.
 pub struct Engine {
    options: Options,
    records: HashMap<RecordId, Record>,
    record_cells: HashMap<Cell, HashSet<RecordId>>,
    subscriptions: HashMap<SubscriptionId, usize>,
    group_ids: HashMap<Query, usize>,
    groups: Vec<Option<Group>>,
    free: Vec<usize>,
    cell_groups: HashMap<Cell, HashSet<usize>>,
    broad_groups: HashSet<usize>,
    marks: Vec<u64>,
    epoch: u64,
    candidates: Vec<usize>,
}
impl Default for Engine {
    fn default() -> Self { Self::new(Options::default()).expect("valid defaults") }
}
impl Engine {
    pub fn new(options: Options) -> Result<Self, Error> {
        if options.cell_size <= 0 || options.max_query_cells == 0 { return Err(Error::Configuration); }
        Ok(Self { options, records: HashMap::new(), record_cells: HashMap::new(),
            subscriptions: HashMap::new(), group_ids: HashMap::new(), groups: Vec::new(), free: Vec::new(),
            cell_groups: HashMap::new(), broad_groups: HashSet::new(), marks: Vec::new(), epoch: 0, candidates: Vec::new() })
    }
    pub fn record_count(&self) -> usize { self.records.len() }
    pub fn subscription_count(&self) -> usize { self.subscriptions.len() }
    pub fn unique_query_count(&self) -> usize { self.group_ids.len() }
    pub fn record(&self, id: RecordId) -> Option<&Record> { self.records.get(&id) }
    pub fn query(&self, id: SubscriptionId) -> Option<Query> {
        self.subscriptions.get(&id).map(|g| self.groups[*g].as_ref().unwrap().query)
    }
    pub fn matching_records(&self, id: SubscriptionId) -> Vec<Record> {
        self.subscriptions.get(&id).map(|g| self.groups[*g].as_ref().unwrap().members.iter()
            .map(|id| self.records[id]).collect()).unwrap_or_default()
    }
    pub fn snapshot(&self) -> Vec<(SubscriptionId, Record)> {
        let mut rows = Vec::new();
        for group in self.groups.iter().flatten() {
            for &sub in &group.subscribers {
                rows.extend(group.members.iter().map(|id| (sub, self.records[id])));
            }
        }
        rows
    }
    pub fn apply(&mut self, batch: &Batch) -> Result<Vec<Event>, Error> {
        let mut events = Vec::new(); self.apply_into(batch, &mut events)?; Ok(events)
    }
    pub fn apply_into(&mut self, batch: &Batch, events: &mut Vec<Event>) -> Result<Stats, Error> {
        batch.validate()?;
        let mut record_ops = BTreeMap::new();
        for op in &batch.records {
            match *op { RecordChange::Put(r) => { record_ops.insert(r.id, Some(r)); }, RecordChange::Remove(id) => { record_ops.insert(id, None); } }
        }
        record_ops.retain(|id, value| self.records.get(id).copied() != *value);
        let mut query_ops = BTreeMap::new();
        for op in &batch.subscriptions {
            match *op { SubscriptionChange::Put(id, q) => { query_ops.insert(id, Some(q)); }, SubscriptionChange::Remove(id) => { query_ops.insert(id, None); } }
        }
        query_ops.retain(|id, value| self.query(*id) != *value);
        let changing: HashSet<_> = query_ops.keys().copied().collect();
        // Capture old query membership before changing any record. This is needed
        // for atomic mixed query/data batches, including deletes and ID reuse.
        let old_queries: Vec<_> = query_ops.keys().map(|id| (*id, self.matching_records(*id))).collect();
        events.clear();
        let mut stats = Stats { changed_records: record_ops.len(), changed_subscriptions: query_ops.len(), ..Stats::default() };
        for (id, new) in record_ops {
            let old = self.records.get(&id).copied();
            self.collect_candidates(old.as_ref().map(|r| r.point), new.as_ref().map(|r| r.point));
            for &g in &self.candidates {
                let group = self.groups[g].as_mut().unwrap();
                let was = old.as_ref().is_some_and(|r| group.query.matches(r));
                let now = new.as_ref().is_some_and(|r| group.query.matches(r));
                stats.predicate_tests += usize::from(old.is_some()) + usize::from(new.is_some());
                if was != now { if now { group.members.insert(id); } else { group.members.remove(&id); } }
                if was || now {
                    let kind = if !was { EventKind::Added } else if !now { EventKind::Removed } else { EventKind::Updated };
                    let revision = if now { new.unwrap().revision } else { old.unwrap().revision };
                    events.extend(group.subscribers.iter().filter(|s| !changing.contains(s))
                        .map(|&subscription| Event { subscription, record: id, kind, revision }));
                }
            }
            let old_cell = old.map(|r| self.cell(r.point));
            let new_cell = new.map(|r| self.cell(r.point));
            if old_cell != new_cell {
                if let Some(cell) = old_cell {
                    let bucket = self.record_cells.get_mut(&cell).unwrap(); bucket.remove(&id);
                    if bucket.is_empty() { self.record_cells.remove(&cell); }
                }
                if let Some(cell) = new_cell { self.record_cells.entry(cell).or_default().insert(id); }
            }
            if let Some(row) = new { self.records.insert(id, row); } else { self.records.remove(&id); }
        }
        for (&id, &query) in &query_ops {
            self.detach(id);
            if let Some(query) = query { self.attach(id, query); }
        }
        for (subscription, old) in old_queries {
            let new_group = self.subscriptions.get(&subscription).map(|g| self.groups[*g].as_ref().unwrap());
            let mut old_ids = HashSet::with_capacity(old.len());
            for row in old {
                old_ids.insert(row.id);
                if new_group.is_some_and(|g| g.members.contains(&row.id)) {
                    let new = self.records[&row.id];
                    if new != row { events.push(Event { subscription, record: row.id, kind: EventKind::Updated, revision: new.revision }); }
                } else { events.push(Event { subscription, record: row.id, kind: EventKind::Removed, revision: row.revision }); }
            }
            if let Some(group) = new_group {
                events.extend(group.members.iter().filter(|id| !old_ids.contains(id)).map(|&record| Event {
                    subscription, record, kind: EventKind::Added, revision: self.records[&record].revision }));
            }
        }
        stats.unique_queries = self.unique_query_count(); stats.emitted = events.len(); Ok(stats)
    }
    fn cell(&self, point: [i32; 3]) -> Cell { point.map(|v| v.div_euclid(self.options.cell_size)) }
    fn cells(&self, query: Query) -> Option<Vec<Cell>> {
        let lo = self.cell(query.lower); let hi = self.cell(query.upper);
        let count: i128 = (0..3).map(|d| i128::from(hi[d]) - i128::from(lo[d]) + 1).product();
        if count > self.options.max_query_cells as i128 { return None; }
        let mut keys = Vec::with_capacity(count as usize);
        for x in lo[0]..=hi[0] { for y in lo[1]..=hi[1] { for z in lo[2]..=hi[2] { keys.push([x,y,z]); } } }
        Some(keys)
    }
    fn collect_candidates(&mut self, old: Option<[i32; 3]>, new: Option<[i32; 3]>) {
        self.candidates.clear(); self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 { self.marks.fill(0); self.epoch = 1; }
        let cells = [old.map(|p| self.cell(p)), new.map(|p| self.cell(p))];
        for cell in cells.into_iter().flatten() {
            if let Some(groups) = self.cell_groups.get(&cell) {
                for &g in groups { if self.marks[g] != self.epoch { self.marks[g] = self.epoch; self.candidates.push(g); } }
            }
        }
        for &g in &self.broad_groups { if self.marks[g] != self.epoch { self.marks[g] = self.epoch; self.candidates.push(g); } }
    }
    fn attach(&mut self, subscriber: SubscriptionId, query: Query) {
        if let Some(&g) = self.group_ids.get(&query) {
            self.groups[g].as_mut().unwrap().subscribers.insert(subscriber); self.subscriptions.insert(subscriber,g); return;
        }
        let cells = self.cells(query);
        let mut members = HashSet::new();
        if let Some(cells) = &cells {
            for cell in cells { if let Some(ids) = self.record_cells.get(cell) {
                members.extend(ids.iter().filter(|id| query.matches(&self.records[id])).copied());
            } }
        } else { members.extend(self.records.values().filter(|r| query.matches(r)).map(|r| r.id)); }
        let g = self.free.pop().unwrap_or_else(|| { self.groups.push(None); self.marks.push(0); self.groups.len()-1 });
        if let Some(cells) = &cells { for &cell in cells { self.cell_groups.entry(cell).or_default().insert(g); } }
        else { self.broad_groups.insert(g); }
        self.groups[g] = Some(Group { query, subscribers: HashSet::from([subscriber]), members, cells });
        self.group_ids.insert(query,g); self.subscriptions.insert(subscriber,g);
    }
    fn detach(&mut self, subscriber: SubscriptionId) {
        let Some(g) = self.subscriptions.remove(&subscriber) else { return; };
        let group = self.groups[g].as_mut().unwrap(); group.subscribers.remove(&subscriber);
        if !group.subscribers.is_empty() { return; }
        let group = self.groups[g].take().unwrap(); self.group_ids.remove(&group.query);
        if let Some(cells) = group.cells {
            for cell in cells {
                let bucket = self.cell_groups.get_mut(&cell).unwrap(); bucket.remove(&g);
                if bucket.is_empty() { self.cell_groups.remove(&cell); }
            }
        } else { self.broad_groups.remove(&g); }
        self.free.push(g); self.marks[g] = 0;
    }
}
