//! Bounded exact batches over an immutable index.
//! With `adaptive-batches`, four exact probes may reduce the configured maximum
//! group size toward a soft 1 MiB output target per worker. Samples can miss dense
//! queries, so this does not impose a hard byte bound or change any result.
use crate::{Error, SpatialIndex};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

/// Resource choices for [`SpatialIndex::within_batch`] and
/// [`SpatialIndex::within_batch_parallel`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchOptions {
    /// Maximum scoped query workers, from 1 through 64. Default: 1.
    pub workers: usize,
    /// Maximum complete result rows retained by each worker. Must be positive.
    /// Default: 64. Smaller groups reduce dense-output memory and may lose locality.
    /// The opt-in `adaptive-batches` feature can choose fewer rows using four
    /// exact density probes and a soft 1 MiB output target per worker. Samples
    /// can miss hotspots; this is not a hard byte bound.
    pub queries_per_group: usize,
}
impl Default for BatchOptions {
    fn default() -> Self {
        Self {
            workers: 1,
            queries_per_group: 64,
        }
    }
}
/// Work completed by a successful batch. Counts include empty query rows.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BatchStats {
    pub queries: usize,
    pub groups: usize,
    /// Workers actually used, capped by the number of groups; zero for no queries.
    pub workers: usize,
    pub matches: u128,
}
type Query<const D: usize> = ([f64; D], f64);
#[cfg(feature = "adaptive-batches")]
const SOFT_OUTPUT_TARGET_BYTES: usize = 1024 * 1024;
fn partition<const D: usize>(
    queries: &[Query<D>],
    order: &mut [usize],
    base: usize,
    limit: usize,
    groups: &mut Vec<std::ops::Range<usize>>,
) {
    if order.len() <= limit {
        if !order.is_empty() {
            groups.push(base..base + order.len());
        }
        return;
    }
    let mut lo = [f64::INFINITY; D];
    let mut hi = [f64::NEG_INFINITY; D];
    for &i in order.iter() {
        for d in 0..D {
            lo[d] = lo[d].min(queries[i].0[d]);
            hi[d] = hi[d].max(queries[i].0[d]);
        }
    }
    let axis = (0..D)
        .max_by(|&a, &b| (hi[a] - lo[a]).total_cmp(&(hi[b] - lo[b])))
        .unwrap();
    let mid = order.len() / 2;
    order.select_nth_unstable_by(mid, |&a, &b| {
        queries[a].0[axis]
            .total_cmp(&queries[b].0[axis])
            .then(a.cmp(&b))
    });
    let (left, right) = order.split_at_mut(mid);
    partition(queries, left, base, limit, groups);
    partition(queries, right, base + mid, limit, groups);
}
impl<const D: usize> SpatialIndex<D> {
    /// Report an exact batch in spatially coherent groups with bounded output storage.
    ///
    /// Each input is `(centre, radius)`. `visit(indices, rows)` receives aligned
    /// original query indices and complete unordered ID vectors. Every query is
    /// visited exactly once, including empty results. Group/row order is unspecified.
    /// Rows are borrowed and reused after the callback; copy only what you retain.
    /// Callbacks are serialized, even when query work runs on several threads.
    ///
    /// All query/configuration validation completes before the first callback.
    /// Updates are excluded by this method's immutable borrow; unprepared/dirty
    /// indexes remain correct using the same fallback as `within_into`.
    ///
    /// This creates scoped threads per call, not a persistent pool. One worker
    /// runs on the caller's thread. At most `workers * queries_per_group` result
    /// vectors are live, plus O(query count) ordering/scheduling storage. Vector
    /// capacity may exceed length; each exact row may contain the entire index.
    /// The caller chooses a suitable worker count and memory budget. A callback
    /// panic propagates after scoped workers join.
    ///
    /// With `adaptive-batches`, large batches may use four exact density probes
    /// to reduce group size. Probes add call cost but emit no callbacks or stats;
    /// every original query is still reported once. The output target is soft.
    pub fn within_batch<F>(
        &self,
        queries: &[Query<D>],
        options: BatchOptions,
        visit: F,
    ) -> Result<BatchStats, Error>
    where
        F: FnMut(&[usize], &[Vec<u64>]) + Send,
    {
        let consumer = Mutex::new(visit);
        self.run_batch(queries, options, |indices, rows| {
            let mut consumer = consumer.lock().expect("batch callback panicked");
            consumer(indices, rows);
        })
    }

    /// Report complete exact query groups to a concurrently invoked callback.
    ///
    /// Inputs, original query indices, complete unordered rows and validation
    /// semantics are the same as [`Self::within_batch`]. Each query is visited
    /// exactly once, including empty results. Different workers can call `visit`
    /// simultaneously on disjoint groups, so the callback must synchronize any
    /// shared mutable state. There is no consumer mutex in this method.
    /// Borrowed result vectors belong to that worker and are reused only after
    /// its callback returns; copy only what you retain.
    ///
    /// All query/configuration validation completes before any callback. The
    /// immutable index borrow excludes updates, and dirty/unprepared indexes use
    /// the same exact fallback as [`Self::within_into`]. One worker participates
    /// on the caller's thread; at most `workers - 1` scoped threads are created.
    /// At most `workers * queries_per_group` output vectors are live, plus
    /// O(query count) ordering/scheduling storage. Each row may contain the
    /// entire index, and vector capacity may exceed length.
    ///
    /// With `adaptive-batches`, four exact density probes may reduce group size
    /// toward a soft cache target. Their work is charged to the call; only the
    /// original queries contribute callbacks and statistics.
    ///
    /// A callback panic propagates after scoped workers join. Other callbacks
    /// may already be running and can continue before the call unwinds. The
    /// index remains unchanged and can be queried again after catching a panic.
    pub fn within_batch_parallel<F>(
        &self,
        queries: &[Query<D>],
        options: BatchOptions,
        visit: F,
    ) -> Result<BatchStats, Error>
    where
        F: Fn(&[usize], &[Vec<u64>]) + Sync,
    {
        self.run_batch(queries, options, visit)
    }

    // Keep exact density probes out of the callback-specific scheduler code.
    // This is intentionally not cold: dense query phases use it routinely.
    #[cfg(feature = "adaptive-batches")]
    #[inline(never)]
    fn adaptive_group_limit(&self, queries: &[Query<D>], configured_limit: usize) -> usize {
        let mut effective_limit = configured_limit.min(queries.len());
        let worst_case_bytes = self
            .len()
            .saturating_mul(effective_limit)
            .saturating_mul(std::mem::size_of::<u64>());
        if queries.len() >= 1024 && worst_case_bytes > SOFT_OUTPUT_TARGET_BYTES {
            let mut scratch = Vec::new();
            let mut max_matches = 0usize;
            for q in [
                0,
                queries.len() / 4,
                queries.len() / 2,
                3 * queries.len() / 4,
            ] {
                self.within_into(queries[q].0, queries[q].1, &mut scratch)
                    .expect("prevalidated probe query");
                max_matches = max_matches.max(scratch.len());
            }
            if max_matches > 0 {
                let sampled_row_bytes = max_matches.saturating_mul(std::mem::size_of::<u64>());
                effective_limit =
                    effective_limit.min((SOFT_OUTPUT_TARGET_BYTES / sampled_row_bytes).max(1));
            }
            // Probe results never reach consumers/stats and do not overlap the
            // worker result allocations. Probe CPU/allocation remains call cost.
            drop(scratch);
        }
        effective_limit
    }

    fn run_batch<F>(
        &self,
        queries: &[Query<D>],
        options: BatchOptions,
        visit: F,
    ) -> Result<BatchStats, Error>
    where
        F: Fn(&[usize], &[Vec<u64>]) + Sync,
    {
        if !(1..=64).contains(&options.workers) || options.queries_per_group == 0 {
            return Err(Error::BatchConfiguration);
        }
        for (centre, radius) in queries {
            if centre.iter().any(|x| !x.is_finite()) {
                return Err(Error::Coordinate);
            }
            if !radius.is_finite() || *radius < 0.0 {
                return Err(Error::Radius);
            }
        }
        if queries.is_empty() {
            return Ok(BatchStats::default());
        }
        #[cfg(feature = "adaptive-batches")]
        let effective_limit = self.adaptive_group_limit(queries, options.queries_per_group);
        #[cfg(not(feature = "adaptive-batches"))]
        let effective_limit = options.queries_per_group.min(queries.len());
        let mut order: Vec<_> = (0..queries.len()).collect();
        let mut groups = Vec::new();
        partition(queries, &mut order, 0, effective_limit, &mut groups);
        let workers = options.workers.min(groups.len());
        let next = AtomicUsize::new(0);
        let work = || {
            let mut matches = 0u128;
            let mut outputs: Vec<Vec<u64>> = (0..effective_limit).map(|_| Vec::new()).collect();
            loop {
                let at = next.fetch_add(1, Ordering::Relaxed);
                let Some(range) = groups.get(at) else {
                    break;
                };
                let indices = &order[range.clone()];
                let rows = &mut outputs[..indices.len()];
                for (&q, out) in indices.iter().zip(rows.iter_mut()) {
                    self.within_into(queries[q].0, queries[q].1, out)
                        .expect("prevalidated query");
                }
                matches += rows.iter().map(|r| r.len() as u128).sum::<u128>();
                visit(indices, rows);
            }
            matches
        };
        let matches = if workers == 1 {
            work()
        } else {
            std::thread::scope(|scope| {
                let mut handles = Vec::with_capacity(workers - 1);
                for _ in 1..workers {
                    handles.push(scope.spawn(work));
                }
                let mut matches = work();
                for handle in handles {
                    matches += handle.join().expect("batch worker panicked");
                }
                matches
            })
        };
        Ok(BatchStats {
            queries: queries.len(),
            groups: groups.len(),
            workers,
            matches,
        })
    }
}
