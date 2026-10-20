use crate::model::*;
use rill_subscriptions::*;
#[cfg(feature = "intervaltree")]
pub struct Tree(intervaltree::IntervalTree<i64, Record>);
#[cfg(feature = "intervaltree")]
impl Default for Tree {
    fn default() -> Self {
        Self(std::iter::empty::<(std::ops::Range<i64>, Record)>().collect())
    }
}
#[cfg(feature = "intervaltree")]
impl Evaluator for Tree {
    fn update(&mut self, s: &State, p: &Prepared) {
        if !p.records.is_empty() {
            self.0 = s
                .records
                .values()
                .map(|r| (i64::from(r.point[0])..i64::from(r.point[0]) + 1, *r))
                .collect();
        }
    }
    fn query(&mut self, _: &State, q: Query, out: &mut Vec<Record>) {
        out.extend(
            self.0
                .query(i64::from(q.lower[0])..i64::from(q.upper[0]) + 1)
                .map(|e| e.value)
                .filter(|r| q.matches(r)),
        );
    }
}
#[cfg(feature = "rangemap")]
pub struct Map {
    ranges: rangemap::RangeMap<i64, Vec<Record>>,
    by_x: std::collections::BTreeMap<i32, std::collections::HashSet<u64>>,
}
#[cfg(feature = "rangemap")]
impl Default for Map {
    fn default() -> Self {
        Self {
            ranges: rangemap::RangeMap::new(),
            by_x: Default::default(),
        }
    }
}
#[cfg(feature = "rangemap")]
impl Evaluator for Map {
    fn update(&mut self, s: &State, p: &Prepared) {
        let mut dirty = std::collections::HashSet::new();
        for &(id, old, new) in &p.records {
            if let Some(r) = old {
                dirty.insert(r.point[0]);
                let ids = self.by_x.get_mut(&r.point[0]).unwrap();
                ids.remove(&id);
                if ids.is_empty() {
                    self.by_x.remove(&r.point[0]);
                }
            }
            if let Some(r) = new {
                dirty.insert(r.point[0]);
                self.by_x.entry(r.point[0]).or_default().insert(id);
            }
        }
        for x in dirty {
            let k = i64::from(x);
            if let Some(ids) = self.by_x.get(&x) {
                let mut rows: Vec<_> = ids.iter().map(|id| s.records[id]).collect();
                rows.sort_unstable();
                self.ranges.insert(k..k + 1, rows);
            } else {
                self.ranges.remove(k..k + 1);
            }
        }
    }
    fn query(&mut self, _: &State, q: Query, out: &mut Vec<Record>) {
        for (_, rs) in self
            .ranges
            .overlapping(i64::from(q.lower[0])..i64::from(q.upper[0]) + 1)
        {
            out.extend(rs.iter().filter(|r| q.matches(r)).copied());
        }
    }
}
