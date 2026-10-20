//! Actual upstream set implementations as interchangeable cell membership stores.
//! These are data-structure comparisons, not independent subscription engines.
use crate::model::*;
use rill_subscriptions::*;
use std::collections::HashMap;
pub trait Bits: Default {
    fn add(&mut self, i: u32);
    fn remove(&mut self, i: u32);
    fn each(&self, f: impl FnMut(u32));
}
struct Bucket<B> {
    bits: B,
    count: usize,
}
impl<B: Default> Default for Bucket<B> {
    fn default() -> Self {
        Self {
            bits: B::default(),
            count: 0,
        }
    }
}
pub struct Index<B: Bits> {
    slots: Vec<Option<Record>>,
    ids: HashMap<u64, u32>,
    free: Vec<u32>,
    cells: HashMap<Cell, Bucket<B>>,
    all: B,
}
impl<B: Bits> Default for Index<B> {
    fn default() -> Self {
        Self {
            slots: Vec::new(),
            ids: HashMap::new(),
            free: Vec::new(),
            cells: HashMap::new(),
            all: B::default(),
        }
    }
}
impl<B: Bits> Evaluator for Index<B> {
    fn update(&mut self, _: &State, p: &Prepared) {
        for &(id, old, new) in &p.records {
            let slot = if old.is_some() {
                self.ids[&id]
            } else {
                let slot = self.free.pop().unwrap_or_else(|| {
                    let slot = u32::try_from(self.slots.len()).expect("bitmap slot capacity");
                    self.slots.push(None);
                    slot
                });
                self.ids.insert(id, slot);
                self.all.add(slot);
                slot
            };
            let a = old.map(|r| cell(r.point, 64));
            let b = new.map(|r| cell(r.point, 64));
            if a != b {
                if let Some(k) = a {
                    let bucket = self.cells.get_mut(&k).unwrap();
                    bucket.bits.remove(slot);
                    bucket.count -= 1;
                    if bucket.count == 0 {
                        self.cells.remove(&k);
                    }
                }
                if let Some(k) = b {
                    let bucket = self.cells.entry(k).or_default();
                    bucket.bits.add(slot);
                    bucket.count += 1;
                }
            }
            self.slots[slot as usize] = new;
            if new.is_none() {
                self.ids.remove(&id);
                self.free.push(slot);
                self.all.remove(slot);
            }
        }
    }
    fn query(&mut self, _: &State, q: Query, out: &mut Vec<Record>) {
        let mut visit = |slot| {
            let r = self.slots[slot as usize].unwrap();
            if q.matches(&r) {
                out.push(r);
            }
        };
        if let Some(keys) = cells(q, 64, 512) {
            for k in keys {
                if let Some(bucket) = self.cells.get(&k) {
                    bucket.bits.each(&mut visit);
                }
            }
        } else {
            self.all.each(visit);
        }
    }
}
#[cfg(feature = "roaring")]
#[derive(Default)]
pub struct Roaring(roaring::RoaringBitmap);
#[cfg(feature = "roaring")]
impl Bits for Roaring {
    fn add(&mut self, i: u32) {
        self.0.insert(i);
    }
    fn remove(&mut self, i: u32) {
        self.0.remove(i);
    }
    fn each(&self, mut f: impl FnMut(u32)) {
        for i in self.0.iter() {
            f(i);
        }
    }
}
#[cfg(feature = "bitvec")]
#[derive(Default)]
pub struct Bitvec(bitvec::vec::BitVec);
#[cfg(feature = "bitvec")]
impl Bits for Bitvec {
    fn add(&mut self, i: u32) {
        if self.0.len() <= i as usize {
            self.0.resize(i as usize + 1, false);
        }
        self.0.set(i as usize, true);
    }
    fn remove(&mut self, i: u32) {
        self.0.set(i as usize, false);
    }
    fn each(&self, mut f: impl FnMut(u32)) {
        for i in self.0.iter_ones() {
            f(i as u32);
        }
    }
}
#[cfg(feature = "fixedbitset")]
#[derive(Default)]
pub struct Fixed(fixedbitset::FixedBitSet);
#[cfg(feature = "fixedbitset")]
impl Bits for Fixed {
    fn add(&mut self, i: u32) {
        self.0.grow(i as usize + 1);
        self.0.insert(i as usize);
    }
    fn remove(&mut self, i: u32) {
        self.0.set(i as usize, false);
    }
    fn each(&self, mut f: impl FnMut(u32)) {
        for i in self.0.ones() {
            f(i as u32);
        }
    }
}
#[cfg(feature = "bit_set")]
#[derive(Default)]
pub struct Bitset(bit_set::BitSet);
#[cfg(feature = "bit_set")]
impl Bits for Bitset {
    fn add(&mut self, i: u32) {
        self.0.insert(i as usize);
    }
    fn remove(&mut self, i: u32) {
        self.0.remove(i as usize);
    }
    fn each(&self, mut f: impl FnMut(u32)) {
        for i in self.0.iter() {
            f(i as u32);
        }
    }
}
#[cfg(feature = "hibitset")]
#[derive(Default)]
pub struct Hibi(hibitset::BitSet);
#[cfg(feature = "hibitset")]
impl Bits for Hibi {
    fn add(&mut self, i: u32) {
        self.0.add(i);
    }
    fn remove(&mut self, i: u32) {
        self.0.remove(i);
    }
    fn each(&self, mut f: impl FnMut(u32)) {
        use hibitset::BitSetLike;
        for i in (&self.0).iter() {
            f(i);
        }
    }
}
