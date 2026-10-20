//! Pinned upstream Rill radius index, querying a conservative circumsphere and
//! applying exact integer-box/tag/value postfilters. Full IDs, no neighbor cap.
use crate::model::*;
use rill_spatial::SpatialIndex;
use rill_subscriptions::*;
pub struct Native {
    index: SpatialIndex<3>,
    ids: Vec<u64>,
}
impl Native {
    pub fn new() -> Self {
        Self {
            index: SpatialIndex::new(64.).unwrap(),
            ids: Vec::new(),
        }
    }
}
impl Evaluator for Native {
    fn update(&mut self, _: &State, p: &Prepared) {
        for &(id, old, new) in &p.records {
            if let Some(r) = new {
                if old.is_none_or(|a| a.point != r.point) {
                    self.index.upsert(id, r.point.map(f64::from)).unwrap();
                }
            } else {
                assert!(self.index.remove(id).is_some());
            }
        }
        self.index.prepare();
    }
    fn query(&mut self, s: &State, q: Query, out: &mut Vec<Record>) {
        let centre = std::array::from_fn(|d| (f64::from(q.lower[d]) + f64::from(q.upper[d])) * 0.5);
        let squared: f64 = (0..3)
            .map(|d| {
                let half = (f64::from(q.upper[d]) - f64::from(q.lower[d])) * 0.5;
                half * half
            })
            .sum();
        let radius = squared.sqrt() * (1. + 128. * f64::EPSILON) + f64::EPSILON;
        self.index
            .within_into(centre, radius, &mut self.ids)
            .unwrap();
        out.extend(
            self.ids
                .iter()
                .map(|id| s.records[id])
                .filter(|r| q.matches(r)),
        );
    }
}
