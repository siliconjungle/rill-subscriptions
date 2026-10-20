//! Exact incremental subscriptions over projected records.
//!
//! The kernel has no game, database, transport or async-runtime dependency.
//! Application payloads stay with the caller: stable IDs and `revision` identify
//! the projection to deliver. Integer bounds are inclusive and exact. A batch
//! commits its final before/after relation; repeated operations use the last value.
//! Events are unordered and never capped. Invalid queries leave state and output
//! unchanged. Only live subscribers retain a shared query group.
#![doc = include_str!("../README.md")]
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
        Self {
            id,
            point,
            tags: 0,
            value: 0,
            revision: 0,
        }
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
        Self {
            lower: [i32::MIN; 3],
            upper: [i32::MAX; 3],
            required_tags: 0,
            min_value: i64::MIN,
            max_value: i64::MAX,
        }
    }
    pub const fn bounds(lower: [i32; 3], upper: [i32; 3]) -> Self {
        Self {
            lower,
            upper,
            ..Self::all()
        }
    }
    pub fn validate(&self) -> Result<(), Error> {
        if self.min_value > self.max_value || (0..3).any(|d| self.lower[d] > self.upper[d]) {
            Err(Error::Bounds)
        } else {
            Ok(())
        }
    }
    #[inline]
    pub fn matches(&self, row: &Record) -> bool {
        row.tags & self.required_tags == self.required_tags
            && row.value >= self.min_value
            && row.value <= self.max_value
            && (0..3).all(|d| row.point[d] >= self.lower[d] && row.point[d] <= self.upper[d])
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordChange<P = Record> {
    Put(P),
    Remove(RecordId),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubscriptionChange<Q = Query> {
    Put(SubscriptionId, Q),
    Remove(SubscriptionId),
}
#[derive(Clone, Debug)]
pub struct Batch<P = Record, Q = Query> {
    pub records: Vec<RecordChange<P>>,
    pub subscriptions: Vec<SubscriptionChange<Q>>,
}
impl<P, Q> Default for Batch<P, Q> {
    fn default() -> Self {
        Self {
            records: Vec::new(),
            subscriptions: Vec::new(),
        }
    }
}
impl<P: Projection, Q: Predicate<P>> Batch<P, Q> {
    pub fn validate(&self) -> Result<(), Error> {
        for change in &self.subscriptions {
            if let SubscriptionChange::Put(_, query) = change {
                query.validate()?;
            }
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum EventKind {
    Added,
    Removed,
    Updated,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Event {
    pub subscription: SubscriptionId,
    pub record: RecordId,
    pub kind: EventKind,
    /// The removed projection's revision, or the current added/updated revision.
    pub revision: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Bounds,
    Configuration,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Bounds => "inverted query bounds",
            Self::Configuration => "cell size and cell budget must be positive",
        })
    }
}
impl std::error::Error for Error {}
#[derive(Clone, Copy, Debug)]
pub struct Options {
    pub cell_size: i32,
    pub max_query_cells: usize,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            cell_size: 64,
            max_query_cells: 512,
        }
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Stats {
    pub changed_records: usize,
    pub changed_subscriptions: usize,
    pub unique_queries: usize,
    pub predicate_tests: usize,
    pub emitted: usize,
}
/// Compact application projection. Keep large payloads outside the kernel and
/// use a revision to identify changes to that payload.
pub trait Projection: Copy + Eq {
    fn record_id(&self) -> RecordId;
    fn revision(&self) -> u64;
}
/// Exact application predicate. Validation runs before a batch mutates state.
pub trait Predicate<P: Projection>: Copy + Eq + std::hash::Hash {
    fn validate(&self) -> Result<(), Error> {
        Ok(())
    }
    fn matches(&self, row: &P) -> bool;
}
/// Conservative dependency routing. Every matching record's partition must be
/// among its query keys. Return `None` to match across every partition safely.
/// Keys are an optimization; predicates always decide exact membership.
pub trait Router<P: Projection, Q: Predicate<P>> {
    type Key: Copy + Eq + std::hash::Hash;
    fn record_key(&self, row: &P) -> Self::Key;
    fn query_keys(&self, query: Q) -> Option<Vec<Self::Key>>;
}
#[derive(Clone, Copy, Debug, Default)]
pub struct Unpartitioned;
impl<P: Projection, Q: Predicate<P>> Router<P, Q> for Unpartitioned {
    type Key = ();
    fn record_key(&self, _: &P) {}
    fn query_keys(&self, _: Q) -> Option<Vec<()>> {
        None
    }
}
impl Projection for Record {
    fn record_id(&self) -> RecordId {
        self.id
    }
    fn revision(&self) -> u64 {
        self.revision
    }
}
impl Predicate<Record> for Query {
    fn validate(&self) -> Result<(), Error> {
        Query::validate(self)
    }
    fn matches(&self, row: &Record) -> bool {
        Query::matches(self, row)
    }
}
#[derive(Clone, Copy, Debug)]
pub struct Grid3 {
    options: Options,
}
impl Grid3 {
    pub fn new(options: Options) -> Result<Self, Error> {
        if options.cell_size <= 0 || options.max_query_cells == 0 {
            Err(Error::Configuration)
        } else {
            Ok(Self { options })
        }
    }
    fn cell(&self, p: [i32; 3]) -> Cell {
        p.map(|v| v.div_euclid(self.options.cell_size))
    }
}
impl Router<Record, Query> for Grid3 {
    type Key = Cell;
    fn record_key(&self, row: &Record) -> Cell {
        self.cell(row.point)
    }
    fn query_keys(&self, query: Query) -> Option<Vec<Cell>> {
        let lo = self.cell(query.lower);
        let hi = self.cell(query.upper);
        let count: i128 = (0..3)
            .map(|d| i128::from(hi[d]) - i128::from(lo[d]) + 1)
            .product();
        if count > self.options.max_query_cells as i128 {
            return None;
        }
        let mut keys = Vec::with_capacity(count as usize);
        for x in lo[0]..=hi[0] {
            for y in lo[1]..=hi[1] {
                for z in lo[2]..=hi[2] {
                    keys.push([x, y, z]);
                }
            }
        }
        Some(keys)
    }
}
/// Ready-to-use integer spatial/tag/range projection. Custom records and queries
/// use `SubscriptionEngine::with_router` with any conservative partition key.
pub type Engine = SubscriptionEngine<Record, Query, Grid3>;
// Most distinct predicates have exactly one subscriber. Avoid a separate hash
// table allocation for that common case, retaining hashed membership for fanout.
enum Subscribers {
    One(SubscriptionId),
    Many(HashSet<SubscriptionId>),
}
impl Subscribers {
    fn new(id: SubscriptionId) -> Self {
        Self::One(id)
    }
    fn insert(&mut self, id: SubscriptionId) {
        match self {
            Self::One(old) if *old != id => {
                *self = Self::Many(HashSet::from([*old, id]));
            }
            Self::Many(ids) => {
                ids.insert(id);
            }
            _ => {}
        }
    }
    fn remove(&mut self, id: SubscriptionId) -> bool {
        match self {
            Self::One(old) => *old == id,
            Self::Many(ids) => {
                ids.remove(&id);
                if ids.len() == 1 {
                    *self = Self::One(*ids.iter().next().unwrap());
                    false
                } else {
                    ids.is_empty()
                }
            }
        }
    }
    fn iter(&self) -> impl Iterator<Item = &SubscriptionId> {
        let (one, many) = match self {
            Self::One(id) => (Some(id), None),
            Self::Many(ids) => (None, Some(ids)),
        };
        one.into_iter()
            .chain(many.into_iter().flat_map(|ids| ids.iter()))
    }
}
enum Members {
    Packed(Vec<RecordId>),
    Hashed(HashSet<RecordId>),
}
impl Members {
    fn from_vec(mut ids: Vec<RecordId>) -> Self {
        ids.sort_unstable();
        ids.dedup();
        if ids.len() > 256 {
            Self::Hashed(ids.into_iter().collect())
        } else {
            Self::Packed(ids)
        }
    }
    fn insert(&mut self, id: RecordId) {
        match self {
            Self::Packed(ids) if ids.len() < 256 => ids.push(id),
            Self::Packed(ids) => {
                let mut set: HashSet<_> = std::mem::take(ids).into_iter().collect();
                set.insert(id);
                *self = Self::Hashed(set);
            }
            Self::Hashed(ids) => {
                ids.insert(id);
            }
        }
    }
    fn remove(&mut self, id: &RecordId) {
        match self {
            Self::Packed(ids) => {
                let i = ids
                    .iter()
                    .position(|x| x == id)
                    .expect("matching record membership");
                ids.swap_remove(i);
            }
            Self::Hashed(ids) => {
                ids.remove(id);
                if ids.len() < 128 {
                    *self = Self::Packed(ids.drain().collect());
                }
            }
        }
    }
    fn iter(&self) -> impl Iterator<Item = &RecordId> {
        let (packed, hashed) = match self {
            Self::Packed(ids) => (Some(ids), None),
            Self::Hashed(ids) => (None, Some(ids)),
        };
        packed
            .into_iter()
            .flat_map(|ids| ids.iter())
            .chain(hashed.into_iter().flat_map(|ids| ids.iter()))
    }
}
struct Entry<P> {
    value: P,
    offset: usize,
}
struct Group<Q, K> {
    query: Q,
    subscribers: Subscribers,
    members: Members,
    cells: Option<Vec<K>>,
}

/// Shared query groups and a reverse dependency index. Single-writer
/// batches use ordinary Rust ownership; concurrent readers may borrow `&Engine`.
/// Broad queries fall back to exact scanning rather than expanding unbounded cells.
/// Identical subscriptions share matching and membership storage, not notifications.
/// Every subscriber still receives every required logical event.
pub struct SubscriptionEngine<P: Projection, Q: Predicate<P>, R: Router<P, Q>> {
    routing: R,
    records: HashMap<RecordId, Entry<P>>,
    record_cells: HashMap<R::Key, Vec<P>>,
    subscriptions: HashMap<SubscriptionId, usize>,
    group_ids: HashMap<Q, usize>,
    groups: Vec<Option<Group<Q, R::Key>>>,
    free: Vec<usize>,
    cell_groups: HashMap<R::Key, Vec<usize>>,
    broad_groups: HashSet<usize>,
    marks: Vec<u64>,
    epoch: u64,
    candidates: Vec<usize>,
}
impl Default for Engine {
    fn default() -> Self {
        Self::new(Options::default()).expect("valid defaults")
    }
}
impl SubscriptionEngine<Record, Query, Grid3> {
    pub fn new(options: Options) -> Result<Self, Error> {
        Ok(Self::with_router(Grid3::new(options)?))
    }
}
impl<P: Projection, Q: Predicate<P>, R: Router<P, Q>> SubscriptionEngine<P, Q, R> {
    /// Construct a generic engine. The router's query keys must cover every
    /// partition where a matching record can occur; `None` safely scans all.
    pub fn with_router(routing: R) -> Self {
        Self {
            routing,
            records: HashMap::new(),
            record_cells: HashMap::new(),
            subscriptions: HashMap::new(),
            group_ids: HashMap::new(),
            groups: Vec::new(),
            free: Vec::new(),
            cell_groups: HashMap::new(),
            broad_groups: HashSet::new(),
            marks: Vec::new(),
            epoch: 0,
            candidates: Vec::new(),
        }
    }
    pub fn record_count(&self) -> usize {
        self.records.len()
    }
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }
    pub fn unique_query_count(&self) -> usize {
        self.group_ids.len()
    }
    pub fn record(&self, id: RecordId) -> Option<&P> {
        self.records.get(&id).map(|e| &e.value)
    }
    pub fn query(&self, id: SubscriptionId) -> Option<Q> {
        self.subscriptions
            .get(&id)
            .map(|g| self.groups[*g].as_ref().unwrap().query)
    }
    /// Evaluate an ephemeral query against committed records without installing
    /// a subscription. Routing is conservative; exact matching decides results.
    pub fn select(&self, query: Q) -> Result<Vec<P>, Error> {
        query.validate()?;
        let mut rows = Vec::new();
        match self.routing.query_keys(query) {
            None => rows.extend(
                self.records
                    .values()
                    .filter(|e| query.matches(&e.value))
                    .map(|e| e.value),
            ),
            Some(keys) => {
                for key in keys.into_iter().collect::<HashSet<_>>() {
                    if let Some(bucket) = self.record_cells.get(&key) {
                        rows.extend(bucket.iter().copied().filter(|r| query.matches(r)));
                    }
                }
            }
        }
        Ok(rows)
    }
    pub fn matching_records(&self, id: SubscriptionId) -> Vec<P> {
        self.subscriptions
            .get(&id)
            .map(|g| {
                self.groups[*g]
                    .as_ref()
                    .unwrap()
                    .members
                    .iter()
                    .map(|id| self.records[id].value)
                    .collect()
            })
            .unwrap_or_default()
    }
    pub fn snapshot(&self) -> Vec<(SubscriptionId, P)> {
        let mut rows = Vec::new();
        for group in self.groups.iter().flatten() {
            for &sub in group.subscribers.iter() {
                rows.extend(group.members.iter().map(|id| (sub, self.records[id].value)));
            }
        }
        rows
    }
    pub fn apply(&mut self, batch: &Batch<P, Q>) -> Result<Vec<Event>, Error> {
        let mut events = Vec::new();
        self.apply_into(batch, &mut events)?;
        Ok(events)
    }
    pub fn apply_into(
        &mut self,
        batch: &Batch<P, Q>,
        events: &mut Vec<Event>,
    ) -> Result<Stats, Error> {
        batch.validate()?;
        // If every supplied operation already agrees with the committed state,
        // last-write normalization cannot change the result. Validate first so
        // even a no-op batch preserves the documented failure atomicity.
        if batch.records.iter().all(|op| match op {
            RecordChange::Put(r) => self.record(r.record_id()) == Some(r),
            RecordChange::Remove(id) => !self.records.contains_key(id),
        }) && batch.subscriptions.iter().all(|op| match op {
            SubscriptionChange::Put(id, q) => self.query(*id) == Some(*q),
            SubscriptionChange::Remove(id) => !self.subscriptions.contains_key(id),
        }) {
            events.clear();
            return Ok(Stats {
                unique_queries: self.unique_query_count(),
                ..Stats::default()
            });
        }
        let mut record_ops = BTreeMap::new();
        for op in &batch.records {
            match *op {
                RecordChange::Put(r) => {
                    record_ops.insert(r.record_id(), Some(r));
                }
                RecordChange::Remove(id) => {
                    record_ops.insert(id, None);
                }
            }
        }
        record_ops.retain(|id, value| self.record(*id).copied() != *value);
        let mut query_ops = BTreeMap::new();
        for op in &batch.subscriptions {
            match *op {
                SubscriptionChange::Put(id, q) => {
                    query_ops.insert(id, Some(q));
                }
                SubscriptionChange::Remove(id) => {
                    query_ops.insert(id, None);
                }
            }
        }
        query_ops.retain(|id, value| self.query(*id) != *value);
        if self.records.is_empty() {
            self.records.reserve(record_ops.len());
        }
        if self.subscriptions.is_empty() {
            self.subscriptions.reserve(query_ops.len());
        }
        let changing: HashSet<_> = query_ops.keys().copied().collect();
        // Capture old query membership before changing any record. This is needed
        // for atomic mixed query/data batches, including deletes and ID reuse.
        let old_queries: Vec<_> = query_ops
            .keys()
            .map(|id| (*id, self.matching_records(*id)))
            .collect();
        events.clear();
        let mut stats = Stats {
            changed_records: record_ops.len(),
            changed_subscriptions: query_ops.len(),
            ..Stats::default()
        };
        for (id, new) in record_ops {
            let old = self.record(id).copied();
            self.collect_candidates(
                old.as_ref().map(|r| self.routing.record_key(r)),
                new.as_ref().map(|r| self.routing.record_key(r)),
            );
            for &g in &self.candidates {
                let group = self.groups[g].as_mut().unwrap();
                let was = old.as_ref().is_some_and(|r| group.query.matches(r));
                let now = new.as_ref().is_some_and(|r| group.query.matches(r));
                stats.predicate_tests += usize::from(old.is_some()) + usize::from(new.is_some());
                if was != now {
                    if now {
                        group.members.insert(id);
                    } else {
                        group.members.remove(&id);
                    }
                }
                if was || now {
                    let kind = if !was {
                        EventKind::Added
                    } else if !now {
                        EventKind::Removed
                    } else {
                        EventKind::Updated
                    };
                    let revision = if now {
                        new.unwrap().revision()
                    } else {
                        old.unwrap().revision()
                    };
                    events.extend(
                        group
                            .subscribers
                            .iter()
                            .filter(|s| !changing.contains(s))
                            .map(|&subscription| Event {
                                subscription,
                                record: id,
                                kind,
                                revision,
                            }),
                    );
                }
            }
            let old_cell = old.as_ref().map(|r| self.routing.record_key(r));
            let new_cell = new.as_ref().map(|r| self.routing.record_key(r));
            if old_cell == new_cell {
                if let Some(row) = new {
                    let entry = self.records.get_mut(&id).unwrap();
                    entry.value = row;
                    self.record_cells.get_mut(&new_cell.unwrap()).unwrap()[entry.offset] = row;
                }
            } else {
                if let Some(cell) = old_cell {
                    let entry = self.records.remove(&id).unwrap();
                    let bucket = self.record_cells.get_mut(&cell).unwrap();
                    bucket.swap_remove(entry.offset);
                    if let Some(swapped) = bucket.get(entry.offset) {
                        self.records.get_mut(&swapped.record_id()).unwrap().offset = entry.offset;
                    }
                    if bucket.is_empty() {
                        self.record_cells.remove(&cell);
                    }
                }
                if let Some(row) = new {
                    let bucket = self.record_cells.entry(new_cell.unwrap()).or_default();
                    let offset = bucket.len();
                    bucket.push(row);
                    self.records.insert(id, Entry { value: row, offset });
                }
            }
        }
        for (&id, &query) in &query_ops {
            self.detach(id);
            if let Some(query) = query {
                self.attach(id, query);
            }
        }
        for (subscription, old) in old_queries {
            let new_group = self
                .subscriptions
                .get(&subscription)
                .map(|g| self.groups[*g].as_ref().unwrap());
            let mut old_ids = HashSet::with_capacity(old.len());
            for row in old {
                old_ids.insert(row.record_id());
                if let Some(new) = self
                    .record(row.record_id())
                    .filter(|new| new_group.is_some_and(|g| g.query.matches(new)))
                    .copied()
                {
                    if new != row {
                        events.push(Event {
                            subscription,
                            record: row.record_id(),
                            kind: EventKind::Updated,
                            revision: new.revision(),
                        });
                    }
                } else {
                    events.push(Event {
                        subscription,
                        record: row.record_id(),
                        kind: EventKind::Removed,
                        revision: row.revision(),
                    });
                }
            }
            if let Some(group) = new_group {
                events.extend(group.members.iter().filter(|id| !old_ids.contains(id)).map(
                    |&record| Event {
                        subscription,
                        record,
                        kind: EventKind::Added,
                        revision: self.records[&record].value.revision(),
                    },
                ));
            }
        }
        stats.unique_queries = self.unique_query_count();
        stats.emitted = events.len();
        Ok(stats)
    }
    fn collect_candidates(&mut self, old: Option<R::Key>, new: Option<R::Key>) {
        self.candidates.clear();
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            self.marks.fill(0);
            self.epoch = 1;
        }
        let cells = [old, new];
        for cell in cells.into_iter().flatten() {
            if let Some(groups) = self.cell_groups.get(&cell) {
                for &g in groups {
                    if self.marks[g] != self.epoch {
                        self.marks[g] = self.epoch;
                        self.candidates.push(g);
                    }
                }
            }
        }
        for &g in &self.broad_groups {
            if self.marks[g] != self.epoch {
                self.marks[g] = self.epoch;
                self.candidates.push(g);
            }
        }
    }
    fn attach(&mut self, subscriber: SubscriptionId, query: Q) {
        if let Some(&g) = self.group_ids.get(&query) {
            self.groups[g]
                .as_mut()
                .unwrap()
                .subscribers
                .insert(subscriber);
            self.subscriptions.insert(subscriber, g);
            return;
        }
        let cells = self.routing.query_keys(query);
        let mut members = Vec::new();
        if let Some(cells) = &cells {
            for cell in cells {
                if let Some(ids) = self.record_cells.get(cell) {
                    members.extend(
                        ids.iter()
                            .filter(|r| query.matches(r))
                            .map(|r| r.record_id()),
                    );
                }
            }
        } else {
            members.extend(
                self.records
                    .values()
                    .filter(|e| query.matches(&e.value))
                    .map(|e| e.value.record_id()),
            );
        }
        let g = self.free.pop().unwrap_or_else(|| {
            self.groups.push(None);
            self.marks.push(0);
            self.groups.len() - 1
        });
        if let Some(cells) = &cells {
            for &cell in cells {
                self.cell_groups.entry(cell).or_default().push(g);
            }
        } else {
            self.broad_groups.insert(g);
        }
        self.groups[g] = Some(Group {
            query,
            subscribers: Subscribers::new(subscriber),
            members: Members::from_vec(members),
            cells,
        });
        self.group_ids.insert(query, g);
        self.subscriptions.insert(subscriber, g);
    }
    fn detach(&mut self, subscriber: SubscriptionId) {
        let Some(g) = self.subscriptions.remove(&subscriber) else {
            return;
        };
        let group = self.groups[g].as_mut().unwrap();
        if !group.subscribers.remove(subscriber) {
            return;
        }
        let group = self.groups[g].take().unwrap();
        self.group_ids.remove(&group.query);
        if let Some(cells) = group.cells {
            for cell in cells {
                let bucket = self.cell_groups.get_mut(&cell).unwrap();
                let offset = bucket
                    .iter()
                    .position(|&id| id == g)
                    .expect("registered query cell");
                bucket.swap_remove(offset);
                if bucket.is_empty() {
                    self.cell_groups.remove(&cell);
                }
            }
        } else {
            self.broad_groups.remove(&g);
        }
        self.free.push(g);
        self.marks[g] = 0;
    }
}
