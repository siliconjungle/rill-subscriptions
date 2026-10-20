//! Exact dynamic radius reporting over stable IDs in two or three dimensions.
//!
//! The index is independent of game engines, rendering, databases and runtimes.
//! Results are unordered and include points at the query centre. Reuse an output
//! vector with [`SpatialIndex::within_into`] to avoid repeated result allocation.
//! Finite coordinates and nonnegative finite radii are required. Very large or
//! very small coordinate/cell ratios safely fall back to a contiguous scan.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
mod batch;
mod hierarchy;
pub use batch::{BatchOptions, BatchStats};

/// A validation failure. Invalid operations leave the index unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// Only dimensions 2 and 3 are supported.
    Dimension,
    /// Batch workers must be in 1..=64 and queries_per_group must be positive.
    BatchConfiguration,
    /// A cell size must be finite and strictly positive.
    CellSize,
    /// Every coordinate must be finite.
    Coordinate,
    /// A query radius must be finite and nonnegative.
    Radius,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::BatchConfiguration => {
                "batch workers must be in 1..=64 and group size must be positive"
            }
            Self::Dimension => "only 2D and 3D indexes are supported",
            Self::CellSize => "cell size must be finite and positive",
            Self::Coordinate => "coordinates must be finite",
            Self::Radius => "radius must be finite and nonnegative",
        })
    }
}
impl std::error::Error for Error {}

#[derive(Clone, Default)]
struct CellHasher(u64);
impl Hasher for CellHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut word = [0; 8];
            word[..chunk.len()].copy_from_slice(chunk);
            self.write_u64(u64::from_le_bytes(word));
        }
    }
    fn write_u64(&mut self, word: u64) {
        self.0 = (self.0 ^ word).wrapping_mul(0x9e3779b97f4a7c15);
        self.0 ^= self.0 >> 29;
    }
    fn write_i64(&mut self, value: i64) {
        self.write_u64(value as u64);
    }
}

#[derive(Clone)]
struct Entry<const D: usize> {
    id: u64,
    point: [f64; D],
    cell: [i64; D],
    bucket_offset: usize,
}

/// A mutable point index with stable `u64` identities.
///
/// `D` is 2 or 3. Cell size is a performance choice, not a query restriction:
/// every nonnegative finite radius is accepted. Updates are immediately visible.
/// Cloning produces an independently mutable index. Concurrent read-only queries
/// may share an index through ordinary Rust references.
#[derive(Clone)]
pub struct SpatialIndex<const D: usize> {
    cell_size: f64,
    inverse_cell: f64,
    entries: Vec<Entry<D>>,
    ids: HashMap<u64, usize>,
    cells: HashMap<[i64; D], Vec<usize>, BuildHasherDefault<CellHasher>>,
    lower: [f64; D],
    upper: [f64; D],
    exceptional_cells: bool,
    prepared: Option<hierarchy::Hierarchy<D>>,
    direct_ids: bool,
    linear: bool,
    grid_stale: bool,
}

/// A two-dimensional point index.
pub type Index2 = SpatialIndex<2>;
/// A three-dimensional point index.
pub type Index3 = SpatialIndex<3>;

impl<const D: usize> SpatialIndex<D> {
    /// Create an empty index. A cell size near a common query radius is a useful
    /// starting point, but the best size depends on density and query workload.
    pub fn new(cell_size: f64) -> Result<Self, Error> {
        if D != 2 && D != 3 {
            return Err(Error::Dimension);
        }
        if !cell_size.is_finite() || cell_size <= 0.0 {
            return Err(Error::CellSize);
        }
        Ok(Self {
            cell_size,
            inverse_cell: cell_size.recip(),
            entries: Vec::new(),
            ids: HashMap::new(),
            cells: HashMap::default(),
            lower: [f64::INFINITY; D],
            upper: [f64::NEG_INFINITY; D],
            exceptional_cells: !cell_size.recip().is_finite(),
            prepared: None,
            direct_ids: true,
            linear: true,
            grid_stale: false,
        })
    }

    /// Bulk-load a point collection for packed query phases, avoiding temporary
    /// grid construction. Duplicate IDs use the last supplied position. Queries
    /// are immediately correct through a scan; call `prepare()` to build/refit
    /// the packed hierarchy before a query-heavy phase.
    pub fn from_points(
        cell_size: f64,
        points: impl IntoIterator<Item = (u64, [f64; D])>,
    ) -> Result<Self, Error> {
        let mut index = Self::new(cell_size)?;
        index.grid_stale = true;
        let points = points.into_iter();
        index.entries.reserve(points.size_hint().0);
        for (id, point) in points {
            index.upsert(id, point)?;
        }
        Ok(index)
    }

    /// Number of currently indexed points.
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    /// Whether the index contains no points.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    /// Configured cell width, in the same units as the coordinates.
    pub fn cell_size(&self) -> f64 {
        self.cell_size
    }
    /// Reserve point and lookup capacity before an anticipated insertion burst.
    pub fn reserve(&mut self, additional: usize) {
        self.entries.reserve(additional);
        if !self.direct_ids {
            self.ids.reserve(additional);
        }
        if !self.grid_stale && additional + self.len() > 128 {
            self.cells.reserve(additional);
        }
    }

    fn cell_for(&self, point: &[f64; D]) -> ([i64; D], bool) {
        let mut cell = [0; D];
        let mut exceptional = false;
        for i in 0..D {
            let x = (point[i] * self.inverse_cell).floor();
            if !x.is_finite() || x.abs() > (1u64 << 48) as f64 {
                exceptional = true;
            }
            cell[i] = x as i64;
        }
        (cell, exceptional)
    }

    fn locate(&self, id: u64) -> Option<usize> {
        if self.direct_ids {
            let at = usize::try_from(id).ok()?;
            if at < self.entries.len() {
                Some(at)
            } else {
                None
            }
        } else {
            self.ids.get(&id).copied()
        }
    }
    fn materialize_ids(&mut self) {
        if self.direct_ids {
            self.ids
                .extend(self.entries.iter().enumerate().map(|(i, e)| (e.id, i)));
            self.direct_ids = false;
        }
    }
    fn materialize_cells(&mut self) {
        if self.linear {
            for i in 0..self.entries.len() {
                let cell = self.entries[i].cell;
                let bucket = self.cells.entry(cell).or_default();
                self.entries[i].bucket_offset = bucket.len();
                bucket.push(i);
            }
            self.linear = false;
        }
    }

    /// Insert a stable ID or replace its location. Returns its old location.
    pub fn upsert(&mut self, id: u64, point: [f64; D]) -> Result<Option<[f64; D]>, Error> {
        if point.iter().any(|x| !x.is_finite()) {
            return Err(Error::Coordinate);
        }
        if self.grid_stale {
            if let Some(index) = self.locate(id) {
                let old = std::mem::replace(&mut self.entries[index].point, point);
                if let Some(view) = &mut self.prepared {
                    view.update(index, point);
                }
                return Ok(Some(old));
            }
            self.prepared = None;
            let index = self.len();
            if self.direct_ids && id != index as u64 {
                self.materialize_ids();
            }
            self.entries.push(Entry {
                id,
                point,
                cell: [0; D],
                bucket_offset: 0,
            });
            if !self.direct_ids {
                self.ids.insert(id, index);
            }
            return Ok(None);
        }
        let (cell, exceptional) = self.cell_for(&point);
        self.exceptional_cells |= exceptional;
        for (i, &coordinate) in point.iter().enumerate() {
            self.lower[i] = self.lower[i].min(coordinate);
            self.upper[i] = self.upper[i].max(coordinate);
        }
        if let Some(index) = self.locate(id) {
            let old = self.entries[index].point;
            if !self.linear && self.entries[index].cell != cell {
                self.detach(index);
                let bucket = self.cells.entry(cell).or_default();
                let offset = bucket.len();
                bucket.push(index);
                self.entries[index].cell = cell;
                self.entries[index].bucket_offset = offset;
            }
            self.entries[index].cell = cell;
            self.entries[index].point = point;
            if let Some(view) = &mut self.prepared {
                view.update(index, point);
            }
            Ok(Some(old))
        } else {
            self.prepared = None;
            let index = self.entries.len();
            if self.direct_ids && id != index as u64 {
                self.materialize_ids();
            }
            if self.linear && index >= 128 {
                self.materialize_cells();
            }
            let offset = if self.linear {
                0
            } else {
                let bucket = self.cells.entry(cell).or_default();
                let offset = bucket.len();
                bucket.push(index);
                offset
            };
            self.entries.push(Entry {
                id,
                point,
                cell,
                bucket_offset: offset,
            });
            if !self.direct_ids {
                self.ids.insert(id, index);
            }
            Ok(None)
        }
    }

    fn detach(&mut self, index: usize) {
        let e = &self.entries[index];
        let cell = e.cell;
        let offset = e.bucket_offset;
        let bucket = self.cells.get_mut(&cell).expect("internal cell invariant");
        bucket.swap_remove(offset);
        if offset < bucket.len() {
            self.entries[bucket[offset]].bucket_offset = offset;
        }
        if bucket.is_empty() {
            self.cells.remove(&cell);
        }
    }

    /// Remove an ID. Removal is immediately visible to subsequent queries.
    pub fn remove(&mut self, id: u64) -> Option<[f64; D]> {
        let index = self.locate(id)?;
        self.prepared = None;
        if self.direct_ids && index + 1 != self.len() {
            self.materialize_ids();
        }
        if !self.direct_ids {
            self.ids.remove(&id);
        }
        if !self.grid_stale && !self.linear {
            self.detach(index);
        }
        let old = self.entries.swap_remove(index);
        if index < self.entries.len() {
            let moved = &self.entries[index];
            if !self.direct_ids {
                self.ids.insert(moved.id, index);
            }
            if !self.grid_stale && !self.linear {
                self.cells
                    .get_mut(&moved.cell)
                    .expect("internal cell invariant")[moved.bucket_offset] = index;
            }
        }
        if self.entries.is_empty() {
            self.lower = [f64::INFINITY; D];
            self.upper = [f64::NEG_INFINITY; D];
        }
        Some(old.point)
    }

    /// Find a stored point by its stable ID.
    pub fn get(&self, id: u64) -> Option<&[f64; D]> {
        self.locate(id).map(|i| &self.entries[i].point)
    }
    /// Iterate over current IDs and locations, in unspecified order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (u64, &[f64; D])> {
        self.entries.iter().map(|e| (e.id, &e.point))
    }
    /// Remove all points while retaining allocated storage where possible.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.ids.clear();
        self.cells.clear();
        self.prepared = None;
        self.direct_ids = true;
        self.linear = true;
        self.grid_stale = false;
        self.lower = [f64::INFINITY; D];
        self.upper = [f64::NEG_INFINITY; D];
        self.exceptional_cells = !self.inverse_cell.is_finite();
    }
    /// Tighten conservative bounds after long-running churn. This changes only
    /// performance and takes linear time; queries never require it for correctness.
    pub fn refresh_bounds(&mut self) {
        self.lower = [f64::INFINITY; D];
        self.upper = [f64::NEG_INFINITY; D];
        self.exceptional_cells = !self.inverse_cell.is_finite();
        for e in &self.entries {
            for i in 0..D {
                self.lower[i] = self.lower[i].min(e.point[i]);
                self.upper[i] = self.upper[i].max(e.point[i]);
            }
            self.exceptional_cells |= self.cell_for(&e.point).1;
        }
    }

    /// Prepare a packed read view for a query-heavy phase. Its construction cost
    /// is explicit. Inserts and removals invalidate the view; point updates are refitted at preparation. Unprepared queries remain
    /// immediately correct through a scan. Preparation selects packed mode until clear();
    /// the dynamic grid is released. Small indexes use a scan.
    pub fn prepare(&mut self) {
        if self.entries.len() > 16 && self.prepared.as_mut().is_none_or(|view| !view.refit()) {
            self.cells.clear();
            self.grid_stale = true;
            self.prepared = Some(hierarchy::Hierarchy::new(
                self.entries.iter().map(|e| (e.id, e.point)),
            ));
        }
    }

    /// Report all IDs within a closed radius, overwriting a reusable output.
    /// Invalid queries leave the output unchanged. Valid empty results clear it.
    pub fn within_into(
        &self,
        centre: [f64; D],
        radius: f64,
        output: &mut Vec<u64>,
    ) -> Result<(), Error> {
        if centre.iter().any(|x| !x.is_finite()) {
            return Err(Error::Coordinate);
        }
        if !radius.is_finite() || radius < 0.0 {
            return Err(Error::Radius);
        }
        output.clear();
        if self.entries.is_empty() {
            return Ok(());
        }
        if (1e-150..=1e150).contains(&radius) {
            if let Some(view) = &self.prepared {
                if view.is_ready() {
                    view.query(&centre, radius, output);
                    return Ok(());
                }
            }
        }
        if self.entries.len() <= 128 {
            self.scan(&centre, radius, output);
            return Ok(());
        }
        if self.grid_stale {
            self.scan(&centre, radius, output);
            return Ok(());
        }
        let mut far = [0.0; D];
        for i in 0..D {
            far[i] = (centre[i] - self.lower[i])
                .abs()
                .max((centre[i] - self.upper[i]).abs());
        }
        if in_radius(&far, &[0.0; D], radius) {
            output.extend(self.entries.iter().map(|e| e.id));
            return Ok(());
        }
        let mut low = [0; D];
        let mut high = [0; D];
        let mut visits = 1usize;
        for i in 0..D {
            // Conservative arithmetic guard, including cells on an exact radius
            // boundary. Extreme coordinate/cell ratios take the scan path.
            let guard = 32.0 * f64::EPSILON * (centre[i].abs() + radius + self.cell_size);
            let a = ((centre[i] - radius - guard) * self.inverse_cell).floor();
            let b = ((centre[i] + radius + guard) * self.inverse_cell).floor();
            if !a.is_finite() || !b.is_finite() || a.abs().max(b.abs()) > (1u64 << 48) as f64 {
                self.scan(&centre, radius, output);
                return Ok(());
            }
            low[i] = a as i64;
            high[i] = b as i64;
            visits = visits.saturating_mul((high[i] - low[i] + 1) as usize);
        }
        if self.exceptional_cells
            || radius < 1e-150
            || self.entries.len() < 32
            || visits > self.cells.len().saturating_mul(4).max(8)
        {
            self.scan(&centre, radius, output);
            return Ok(());
        }
        let mut cell = low;
        loop {
            if let Some(bucket) = self.cells.get(&cell) {
                for &index in bucket {
                    let e = &self.entries[index];
                    if in_radius(&e.point, &centre, radius) {
                        output.push(e.id);
                    }
                }
            }
            let mut dimension = 0;
            loop {
                cell[dimension] += 1;
                if cell[dimension] <= high[dimension] {
                    break;
                }
                cell[dimension] = low[dimension];
                dimension += 1;
                if dimension == D {
                    return Ok(());
                }
            }
        }
    }

    fn scan(&self, centre: &[f64; D], radius: f64, output: &mut Vec<u64>) {
        for e in &self.entries {
            if in_radius(&e.point, centre, radius) {
                output.push(e.id);
            }
        }
    }

    /// Convenience allocating query. Prefer `within_into` for repeated queries.
    pub fn within(&self, centre: [f64; D], radius: f64) -> Result<Vec<u64>, Error> {
        let mut result = Vec::new();
        self.within_into(centre, radius, &mut result)?;
        Ok(result)
    }
}

#[inline]
fn in_radius<const D: usize>(point: &[f64; D], centre: &[f64; D], radius: f64) -> bool {
    if radius == 0.0 {
        return point.iter().zip(centre).all(|(a, b)| a == b);
    }
    if !(1e-150..=1e150).contains(&radius) {
        let mut distance = 0.0_f64;
        for i in 0..D {
            distance = distance.hypot(point[i] - centre[i]);
        }
        return distance <= radius;
    }
    let mut squared = 0.0;
    for i in 0..D {
        let delta = point[i] - centre[i];
        squared += delta * delta;
    }
    squared <= radius * radius
}
