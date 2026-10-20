//! Prepared packed hierarchy. Spatial order makes accepted subtrees contiguous.
#[inline]
fn inside<const D: usize>(point: &[f64; D], centre: &[f64; D], r2: f64) -> bool {
    let mut squared = 0.0;
    for d in 0..D {
        let delta = point[d] - centre[d];
        squared += delta * delta;
        if squared > r2 {
            return false;
        }
    }
    true
}

#[cfg(feature = "simd")]
use wide::{CmpLe, f64x4};
#[derive(Clone)]
struct Block<const D: usize> {
    axes: [[f64; 4]; D],
    ids: [u64; 4],
    len: usize,
}
#[derive(Clone)]
struct Node<const D: usize> {
    lo: [f64; D],
    hi: [f64; D],
    start: usize,
    end: usize,
    left: usize,
    right: usize,
    half_diagonal_sq: f64,
}
#[derive(Clone)]
pub(crate) struct Hierarchy<const D: usize> {
    points: Vec<[f64; D]>,
    ids: Vec<u64>,
    nodes: Vec<Node<D>>,
    positions: Vec<usize>,
    dirty: bool,
    baseline_span: f64,
    blocks: Vec<Block<D>>,
}

impl<const D: usize> Hierarchy<D> {
    pub(crate) fn new(mut rows: impl ExactSizeIterator<Item = (u64, [f64; D])>) -> Self {
        if rows.len() <= 128 {
            let mut blocks = Vec::with_capacity(rows.len().div_ceil(4));
            while rows.len() > 0 {
                let len = 4.min(rows.len());
                let mut axes = [[0.0; 4]; D];
                let mut ids = [0; 4];
                for lane in 0..len {
                    let (id, point) = rows.next().unwrap();
                    ids[lane] = id;
                    for d in 0..D {
                        axes[d][lane] = point[d];
                    }
                }
                blocks.push(Block { axes, ids, len });
            }
            return Self {
                points: Vec::new(),
                ids: Vec::new(),
                nodes: Vec::new(),
                positions: Vec::new(),
                dirty: false,
                baseline_span: 0.0,
                blocks,
            };
        }
        let mut rows: Vec<_> = rows
            .enumerate()
            .map(|(position, (id, p))| (id, p, position))
            .collect();
        let mut nodes = Vec::new();
        if rows.len() > 128 {
            Self::build(&mut rows, 0, &mut nodes);
        }
        let points = rows.iter().map(|x| x.1).collect();
        let ids = rows.iter().map(|x| x.0).collect();
        let mut positions = vec![0; rows.len()];
        for (i, row) in rows.iter().enumerate() {
            positions[row.2] = i;
        }
        let baseline_span = nodes
            .iter()
            .map(|n| (0..D).map(|d| n.hi[d] - n.lo[d]).sum::<f64>())
            .sum();
        let mut result = Self {
            points,
            ids,
            nodes,
            positions,
            dirty: false,
            baseline_span,
            blocks: Vec::new(),
        };
        result.refresh_blocks();
        result
    }
    fn refresh_blocks(&mut self) {
        if self.nodes.is_empty() {
            return;
        }
        self.blocks.clear();
        for node in &mut self.nodes {
            if node.left != usize::MAX {
                continue;
            }
            // A leaf uses right for its first packed block; an internal node
            // uses right for its child. left distinguishes the two node kinds.
            node.right = self.blocks.len();
            for base in (node.start..node.end).step_by(4) {
                let len = 4.min(node.end - base);
                let axes = std::array::from_fn(|d| {
                    std::array::from_fn(|lane| {
                        if lane < len {
                            self.points[base + lane][d]
                        } else {
                            0.0
                        }
                    })
                });
                let ids =
                    std::array::from_fn(|lane| if lane < len { self.ids[base + lane] } else { 0 });
                self.blocks.push(Block { axes, ids, len });
            }
        }
    }
    fn build(rows: &mut [(u64, [f64; D], usize)], start: usize, nodes: &mut Vec<Node<D>>) -> usize {
        let mut lo = [f64::INFINITY; D];
        let mut hi = [f64::NEG_INFINITY; D];
        for (_, p, _) in rows.iter() {
            for d in 0..D {
                lo[d] = lo[d].min(p[d]);
                hi[d] = hi[d].max(p[d]);
            }
        }
        let mut axis = 0;
        for d in 1..D {
            if hi[d] - lo[d] > hi[axis] - lo[axis] {
                axis = d;
            }
        }
        let at = nodes.len();
        let half_diagonal_sq = (0..D)
            .map(|d| {
                let x = (hi[d] - lo[d]) * 0.5;
                x * x
            })
            .sum();
        nodes.push(Node {
            lo,
            hi,
            start,
            end: start + rows.len(),
            left: usize::MAX,
            right: usize::MAX,
            half_diagonal_sq,
        });
        if rows.len() > 24 {
            let middle = rows.len() / 2;
            rows.select_nth_unstable_by(middle, |a, b| a.1[axis].total_cmp(&b.1[axis]));
            let (a, b) = rows.split_at_mut(middle);
            let left = Self::build(a, start, nodes);
            let right = Self::build(b, start + middle, nodes);
            nodes[at].left = left;
            nodes[at].right = right;
        }
        at
    }
    pub(crate) fn update(&mut self, entry: usize, point: [f64; D]) {
        if self.nodes.is_empty() {
            let block = &mut self.blocks[entry / 4];
            for (d, &coordinate) in point.iter().enumerate() {
                block.axes[d][entry % 4] = coordinate;
            }
        } else {
            self.points[self.positions[entry]] = point;
            self.dirty = true;
        }
    }
    pub(crate) fn is_ready(&self) -> bool {
        !self.dirty
    }
    // Refit leaves and then parents. The permutation remains valid after movement;
    // overlap growth affects speed, never membership. Return false when rebuilding
    // would restore a substantially tighter hierarchy.
    pub(crate) fn refit(&mut self) -> bool {
        if !self.dirty {
            return true;
        }
        if self.nodes.is_empty() {
            self.dirty = false;
            return true;
        }
        let mut span = 0.0;
        for i in (0..self.nodes.len()).rev() {
            let mut lo = [f64::INFINITY; D];
            let mut hi = [f64::NEG_INFINITY; D];
            let n = &self.nodes[i];
            if n.left == usize::MAX {
                for p in &self.points[n.start..n.end] {
                    for d in 0..D {
                        lo[d] = lo[d].min(p[d]);
                        hi[d] = hi[d].max(p[d]);
                    }
                }
            } else {
                for d in 0..D {
                    lo[d] = self.nodes[n.left].lo[d].min(self.nodes[n.right].lo[d]);
                    hi[d] = self.nodes[n.left].hi[d].max(self.nodes[n.right].hi[d]);
                }
            }
            let n = &mut self.nodes[i];
            n.lo = lo;
            n.hi = hi;
            n.half_diagonal_sq = (0..D)
                .map(|d| {
                    let x = (hi[d] - lo[d]) * 0.5;
                    x * x
                })
                .sum();
            span += (0..D).map(|d| hi[d] - lo[d]).sum::<f64>();
        }
        self.dirty = false;
        let keep = span <= self.baseline_span * 1.5;
        if keep {
            self.refresh_blocks();
        }
        keep
    }
    #[inline]
    pub(crate) fn query(&self, p: &[f64; D], r: f64, out: &mut Vec<u64>) {
        if self.nodes.is_empty() && !self.blocks.is_empty() {
            Self::scan_blocks::<true>(&self.blocks, p, r * r, out);
            return;
        }
        if self.nodes.is_empty() {
            return;
        }
        self.visit(0, p, r * r, out);
    }
    #[inline(always)]
    fn scan_blocks<const EARLY: bool>(
        blocks: &[Block<D>],
        p: &[f64; D],
        r2: f64,
        out: &mut Vec<u64>,
    ) {
        for block in blocks {
            #[cfg(feature = "simd")]
            let mut mask = {
                let first = f64x4::from(block.axes[0]) - f64x4::splat(p[0]);
                let mut squared = first * first;
                if EARLY && !squared.cmp_le(f64x4::splat(r2)).any() {
                    continue;
                }
                for (d, &coordinate) in p.iter().enumerate().skip(1) {
                    let delta = f64x4::from(block.axes[d]) - f64x4::splat(coordinate);
                    squared += delta * delta;
                }
                squared.cmp_le(f64x4::splat(r2)).move_mask() as u32
            };
            #[cfg(not(feature = "simd"))]
            let mut mask = {
                let mut squared = [0.0; 4];
                for d in 0..D {
                    for lane in 0..4 {
                        let delta = block.axes[d][lane] - p[d];
                        squared[lane] += delta * delta;
                    }
                }
                (squared[0] <= r2) as u32
                    | (((squared[1] <= r2) as u32) << 1)
                    | (((squared[2] <= r2) as u32) << 2)
                    | (((squared[3] <= r2) as u32) << 3)
            };
            mask &= (1 << block.len) - 1;
            while mask != 0 {
                let lane = mask.trailing_zeros() as usize;
                out.push(block.ids[lane]);
                mask &= mask - 1;
            }
        }
    }
    fn visit(&self, index: usize, p: &[f64; D], r2: f64, out: &mut Vec<u64>) {
        let n = &self.nodes[index];
        let mut near = 0.0;
        for (d, &coordinate) in p.iter().enumerate() {
            let delta = if coordinate < n.lo[d] {
                n.lo[d] - coordinate
            } else if coordinate > n.hi[d] {
                coordinate - n.hi[d]
            } else {
                0.0
            };
            near += delta * delta;
            if near > r2 {
                return;
            }
        }
        if near > r2 {
            return;
        }
        if r2 >= n.half_diagonal_sq {
            let far = std::array::from_fn(|d| (p[d] - n.lo[d]).abs().max((p[d] - n.hi[d]).abs()));
            if inside(&far, &[0.0; D], r2) {
                out.extend_from_slice(&self.ids[n.start..n.end]);
                return;
            }
        }
        if n.left == usize::MAX {
            let count = (n.end - n.start).div_ceil(4);
            Self::scan_blocks::<false>(&self.blocks[n.right..n.right + count], p, r2, out);
        } else {
            self.visit(n.left, p, r2, out);
            self.visit(n.right, p, r2, out);
        }
    }
}
