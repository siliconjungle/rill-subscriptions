use crate::model::*;
#[cfg(any(
    feature = "roaring",
    feature = "bitvec",
    feature = "fixedbitset",
    feature = "bit_set",
    feature = "hibitset"
))]
mod bitmap;
#[cfg(feature = "dbsp")]
mod dbsp_native;
#[cfg(feature = "differential_dataflow")]
mod differential;
#[cfg(feature = "dioxus_signals")]
mod dioxus_native;
#[cfg(feature = "drasi")]
mod drasi_native;
#[cfg(any(feature = "intervaltree", feature = "rangemap"))]
mod intervals;
#[cfg(any(
    feature = "incremental",
    feature = "reactive_graph",
    feature = "floem_reactive",
    feature = "sycamore_reactive",
    feature = "futures_signals",
    feature = "carboxyl",
    feature = "frappe",
    feature = "rxrust",
    feature = "signals",
    feature = "dioxus_signals"
))]
mod push;
#[cfg(any(
    feature = "incremental",
    feature = "reactive_graph",
    feature = "floem_reactive",
    feature = "sycamore_reactive",
    feature = "salsa",
    feature = "futures_signals",
    feature = "incr_compute",
    feature = "adapton",
    feature = "carboxyl",
    feature = "frappe",
    feature = "sodium_rust",
    feature = "leptos_reactive",
    feature = "rxrust",
    feature = "depends",
    feature = "incr_concurrent",
    feature = "signals",
    feature = "dioxus_signals"
))]
mod reactive;
#[cfg(any(
    feature = "incr_compute",
    feature = "adapton",
    feature = "carboxyl",
    feature = "frappe",
    feature = "sodium_rust",
    feature = "leptos_reactive",
    feature = "rxrust",
    feature = "depends",
    feature = "incr_concurrent"
))]
mod reactive_more;
#[cfg(any(feature = "datafrog", feature = "ascent", feature = "crepe"))]
mod relations;
#[cfg(feature = "rstar")]
mod reverse_rstar;
#[cfg(feature = "rill_spatial")]
mod rill_spatial_native;
#[cfg(feature = "signals")]
mod signals_native;
pub fn names() -> Vec<String> {
    let mut out = Vec::new();
    #[cfg(feature = "rstar")]
    {
        out.extend(
            [
                "native-rstar",
                "native-rstar-shared",
                "native-rstar-delta",
                "native-rstar-delta-shared",
            ]
            .map(String::from),
        );
    }
    #[cfg(feature = "datafrog")]
    out.push("native-datafrog".into());
    #[cfg(feature = "ascent")]
    out.push("native-ascent".into());
    #[cfg(feature = "crepe")]
    out.push("native-crepe".into());
    #[cfg(feature = "differential_dataflow")]
    out.push("native-differential".into());
    #[cfg(feature = "incremental")]
    out.push("native-incremental-cells".into());
    #[cfg(feature = "reactive_graph")]
    out.push("native-reactive-graph-cells".into());
    #[cfg(feature = "floem_reactive")]
    out.push("native-floem-cells".into());
    #[cfg(feature = "sycamore_reactive")]
    out.push("native-sycamore-cells".into());
    #[cfg(feature = "salsa")]
    out.push("native-salsa-cells".into());
    #[cfg(feature = "futures_signals")]
    out.push("native-futures-signals-cells".into());
    #[cfg(feature = "incr_compute")]
    out.push("native-incr-compute-cells".into());
    #[cfg(feature = "adapton")]
    out.push("native-adapton-cells".into());
    #[cfg(feature = "carboxyl")]
    out.push("native-carboxyl-cells".into());
    #[cfg(feature = "frappe")]
    out.push("native-frappe-cells".into());
    #[cfg(feature = "sodium_rust")]
    out.push("native-sodium-rust-cells".into());
    #[cfg(feature = "leptos_reactive")]
    out.push("native-leptos-reactive-cells".into());
    #[cfg(feature = "rxrust")]
    out.push("native-rxrust-cells".into());
    #[cfg(feature = "depends")]
    out.push("native-depends-cells".into());
    #[cfg(feature = "incremental")]
    out.push("native-incremental-push".into());
    #[cfg(feature = "reactive_graph")]
    out.push("native-reactive-graph-push".into());
    #[cfg(feature = "floem_reactive")]
    out.push("native-floem-reactive-push".into());
    #[cfg(feature = "sycamore_reactive")]
    out.push("native-sycamore-reactive-push".into());
    #[cfg(feature = "futures_signals")]
    out.push("native-futures-signals-push".into());
    #[cfg(feature = "roaring")]
    out.push("native-roaring-grid".into());
    #[cfg(feature = "roaring")]
    out.push("native-roaring-grid-shared".into());
    #[cfg(feature = "bitvec")]
    out.push("native-bitvec-grid".into());
    #[cfg(feature = "bitvec")]
    out.push("native-bitvec-grid-shared".into());
    #[cfg(feature = "fixedbitset")]
    out.push("native-fixedbitset-grid".into());
    #[cfg(feature = "fixedbitset")]
    out.push("native-fixedbitset-grid-shared".into());
    #[cfg(feature = "bit_set")]
    out.push("native-bit-set-grid".into());
    #[cfg(feature = "bit_set")]
    out.push("native-bit-set-grid-shared".into());
    #[cfg(feature = "hibitset")]
    out.push("native-hibitset-grid".into());
    #[cfg(feature = "hibitset")]
    out.push("native-hibitset-grid-shared".into());
    #[cfg(feature = "dbsp")]
    out.push("native-dbsp".into());
    #[cfg(feature = "dbsp")]
    out.push("native-dbsp-shared".into());
    #[cfg(feature = "drasi")]
    out.push("native-drasi".into());
    #[cfg(feature = "drasi")]
    out.push("native-drasi-shared".into());
    #[cfg(feature = "intervaltree")]
    out.push("native-intervaltree".into());
    #[cfg(feature = "intervaltree")]
    out.push("native-intervaltree-shared".into());
    #[cfg(feature = "rangemap")]
    out.push("native-rangemap".into());
    #[cfg(feature = "rangemap")]
    out.push("native-rangemap-shared".into());
    #[cfg(feature = "rill_spatial")]
    out.push("native-rill-spatial".into());
    #[cfg(feature = "rill_spatial")]
    out.push("native-rill-spatial-shared".into());
    #[cfg(feature = "carboxyl")]
    out.push("native-carboxyl-push".into());
    #[cfg(feature = "frappe")]
    out.push("native-frappe-push".into());
    #[cfg(feature = "rxrust")]
    out.push("native-rxrust-push".into());
    #[cfg(feature = "signals")]
    out.push("native-signals-cells".into());
    #[cfg(feature = "signals")]
    out.push("native-signals-push".into());
    #[cfg(feature = "incr_concurrent")]
    out.push("native-incr-concurrent-cells".into());
    #[cfg(feature = "dioxus_signals")]
    out.push("native-dioxus-signals-cells".into());
    #[cfg(feature = "dioxus_signals")]
    out.push("native-dioxus-signals-push".into());
    out
}
pub fn make(name: &str) -> Box<dyn Backend> {
    match name {
        #[cfg(feature = "rstar")]
        "native-rstar-delta" => Box::new(reverse_rstar::Native::default()),
        #[cfg(feature = "rstar")]
        "native-rstar-delta-shared" => Box::new(crate::shared::Shared::new(Box::new(
            reverse_rstar::Native::default(),
        ))),
        #[cfg(feature = "rstar")]
        "native-rstar" => Box::new(Recompute::new(rstar_adapter::Index::default(), false)),
        #[cfg(feature = "rstar")]
        "native-rstar-shared" => Box::new(Recompute::new(rstar_adapter::Index::default(), true)),
        #[cfg(feature = "datafrog")]
        "native-datafrog" => Box::new(Recompute::new(relations::Relations::new(0), true)),
        #[cfg(feature = "ascent")]
        "native-ascent" => Box::new(Recompute::new(relations::Relations::new(1), true)),
        #[cfg(feature = "crepe")]
        "native-crepe" => Box::new(Recompute::new(relations::Relations::new(2), true)),
        #[cfg(feature = "differential_dataflow")]
        "native-differential" => Box::new(differential::Differential::new()),
        #[cfg(feature = "incremental")]
        "native-incremental-cells" => Box::new(Recompute::new(
            reactive::Reactive::new(reactive::incr::Native::new()),
            true,
        )),
        #[cfg(feature = "reactive_graph")]
        "native-reactive-graph-cells" => Box::new(Recompute::new(
            reactive::Reactive::new(reactive::graph::Native::new()),
            true,
        )),
        #[cfg(feature = "floem_reactive")]
        "native-floem-cells" => Box::new(Recompute::new(
            reactive::Reactive::new(reactive::floem::Native::new()),
            true,
        )),
        #[cfg(feature = "sycamore_reactive")]
        "native-sycamore-cells" => Box::new(Recompute::new(
            reactive::Reactive::new(reactive::sycamore::Native::new()),
            true,
        )),
        #[cfg(feature = "salsa")]
        "native-salsa-cells" => Box::new(Recompute::new(
            reactive::Reactive::new(reactive::salsa_native::Native::new()),
            true,
        )),
        #[cfg(feature = "futures_signals")]
        "native-futures-signals-cells" => Box::new(Recompute::new(
            reactive::Reactive::new(reactive::futures::Native::new()),
            true,
        )),
        #[cfg(feature = "incr_compute")]
        "native-incr-compute-cells" => Box::new(Recompute::new(
            reactive::Reactive::new(reactive_more::incr::Native::new()),
            true,
        )),
        #[cfg(feature = "adapton")]
        "native-adapton-cells" => Box::new(Recompute::new(
            reactive::Reactive::new(reactive_more::adapton_native::Native::new()),
            true,
        )),
        #[cfg(feature = "carboxyl")]
        "native-carboxyl-cells" => Box::new(Recompute::new(
            reactive::Reactive::new(reactive_more::carboxyl_native::Native::new()),
            true,
        )),
        #[cfg(feature = "frappe")]
        "native-frappe-cells" => Box::new(Recompute::new(
            reactive::Reactive::new(reactive_more::frappe_native::Native::new()),
            true,
        )),
        #[cfg(feature = "sodium_rust")]
        "native-sodium-rust-cells" => Box::new(Recompute::new(
            reactive::Reactive::new(reactive_more::sodium::Native::new()),
            true,
        )),
        #[cfg(feature = "leptos_reactive")]
        "native-leptos-reactive-cells" => Box::new(Recompute::new(
            reactive::Reactive::new(reactive_more::leptos::Native::new()),
            true,
        )),
        #[cfg(feature = "rxrust")]
        "native-rxrust-cells" => Box::new(Recompute::new(
            reactive::Reactive::new(reactive_more::rx::Native::new()),
            true,
        )),
        #[cfg(feature = "depends")]
        "native-depends-cells" => Box::new(Recompute::new(
            reactive::Reactive::new(reactive_more::depends_native::Native::new()),
            true,
        )),
        #[cfg(feature = "incremental")]
        "native-incremental-push" => Box::new(push::Push::new(reactive::incr::Native::pushed())),
        #[cfg(feature = "reactive_graph")]
        "native-reactive-graph-push" => {
            Box::new(push::Push::new(reactive::graph::Native::pushed()))
        }
        #[cfg(feature = "floem_reactive")]
        "native-floem-reactive-push" => {
            Box::new(push::Push::new(reactive::floem::Native::pushed()))
        }
        #[cfg(feature = "sycamore_reactive")]
        "native-sycamore-reactive-push" => {
            Box::new(push::Push::new(reactive::sycamore::Native::pushed()))
        }
        #[cfg(feature = "futures_signals")]
        "native-futures-signals-push" => {
            Box::new(push::Push::new(reactive::futures::Native::pushed()))
        }
        #[cfg(feature = "roaring")]
        "native-roaring-grid" => Box::new(Recompute::new(
            bitmap::Index::<bitmap::Roaring>::default(),
            false,
        )),
        #[cfg(feature = "roaring")]
        "native-roaring-grid-shared" => Box::new(Recompute::new(
            bitmap::Index::<bitmap::Roaring>::default(),
            true,
        )),
        #[cfg(feature = "bitvec")]
        "native-bitvec-grid" => Box::new(Recompute::new(
            bitmap::Index::<bitmap::Bitvec>::default(),
            false,
        )),
        #[cfg(feature = "bitvec")]
        "native-bitvec-grid-shared" => Box::new(Recompute::new(
            bitmap::Index::<bitmap::Bitvec>::default(),
            true,
        )),
        #[cfg(feature = "fixedbitset")]
        "native-fixedbitset-grid" => Box::new(Recompute::new(
            bitmap::Index::<bitmap::Fixed>::default(),
            false,
        )),
        #[cfg(feature = "fixedbitset")]
        "native-fixedbitset-grid-shared" => Box::new(Recompute::new(
            bitmap::Index::<bitmap::Fixed>::default(),
            true,
        )),
        #[cfg(feature = "bit_set")]
        "native-bit-set-grid" => Box::new(Recompute::new(
            bitmap::Index::<bitmap::Bitset>::default(),
            false,
        )),
        #[cfg(feature = "bit_set")]
        "native-bit-set-grid-shared" => Box::new(Recompute::new(
            bitmap::Index::<bitmap::Bitset>::default(),
            true,
        )),
        #[cfg(feature = "hibitset")]
        "native-hibitset-grid" => Box::new(Recompute::new(
            bitmap::Index::<bitmap::Hibi>::default(),
            false,
        )),
        #[cfg(feature = "hibitset")]
        "native-hibitset-grid-shared" => Box::new(Recompute::new(
            bitmap::Index::<bitmap::Hibi>::default(),
            true,
        )),
        #[cfg(feature = "dbsp")]
        "native-dbsp" => Box::new(dbsp_native::Native::new()),
        #[cfg(feature = "dbsp")]
        "native-dbsp-shared" => Box::new(crate::shared::Shared::new(Box::new(
            dbsp_native::Native::new(),
        ))),
        #[cfg(feature = "drasi")]
        "native-drasi" => Box::new(drasi_native::Native::new()),
        #[cfg(feature = "drasi")]
        "native-drasi-shared" => Box::new(crate::shared::Shared::new(Box::new(
            drasi_native::Native::new(),
        ))),
        #[cfg(feature = "intervaltree")]
        "native-intervaltree" => Box::new(Recompute::new(intervals::Tree::default(), false)),
        #[cfg(feature = "intervaltree")]
        "native-intervaltree-shared" => Box::new(Recompute::new(intervals::Tree::default(), true)),
        #[cfg(feature = "rangemap")]
        "native-rangemap" => Box::new(Recompute::new(intervals::Map::default(), false)),
        #[cfg(feature = "rangemap")]
        "native-rangemap-shared" => Box::new(Recompute::new(intervals::Map::default(), true)),
        #[cfg(feature = "rill_spatial")]
        "native-rill-spatial" => {
            Box::new(Recompute::new(rill_spatial_native::Native::new(), false))
        }
        #[cfg(feature = "rill_spatial")]
        "native-rill-spatial-shared" => {
            Box::new(Recompute::new(rill_spatial_native::Native::new(), true))
        }
        #[cfg(feature = "carboxyl")]
        "native-carboxyl-push" => Box::new(push::Push::new(
            reactive_more::carboxyl_native::Native::pushed(),
        )),
        #[cfg(feature = "frappe")]
        "native-frappe-push" => Box::new(push::Push::new(
            reactive_more::frappe_native::Native::pushed(),
        )),
        #[cfg(feature = "rxrust")]
        "native-rxrust-push" => Box::new(push::Push::new(reactive_more::rx::Native::pushed())),
        #[cfg(feature = "signals")]
        "native-signals-cells" => Box::new(Recompute::new(
            reactive::Reactive::new(signals_native::Native::new()),
            true,
        )),
        #[cfg(feature = "signals")]
        "native-signals-push" => Box::new(push::Push::new(signals_native::Native::pushed())),
        #[cfg(feature = "incr_concurrent")]
        "native-incr-concurrent-cells" => Box::new(Recompute::new(
            reactive::Reactive::new(reactive_more::concurrent::Native::new()),
            true,
        )),
        #[cfg(feature = "dioxus_signals")]
        "native-dioxus-signals-cells" => Box::new(Recompute::new(
            reactive::Reactive::new(dioxus_native::Native::new()),
            true,
        )),
        #[cfg(feature = "dioxus_signals")]
        "native-dioxus-signals-push" => Box::new(push::Push::new(dioxus_native::Native::pushed())),
        _ => panic!("Unknown or feature-disabled backend {name}"),
    }
}
#[cfg(feature = "rstar")]
mod rstar_adapter {
    use crate::model::*;
    use rill_subscriptions::*;
    use rstar::{AABB, RTree, RTreeObject};
    #[derive(Clone, Copy, PartialEq)]
    struct Row(Record);
    impl RTreeObject for Row {
        type Envelope = AABB<[i32; 3]>;
        fn envelope(&self) -> Self::Envelope {
            AABB::from_point(self.0.point)
        }
    }
    #[derive(Default)]
    pub struct Index(RTree<Row>);
    impl Evaluator for Index {
        fn update(&mut self, s: &State, p: &Prepared) {
            if p.records.len() > s.records.len() / 2 {
                self.0 = RTree::bulk_load(s.records.values().map(|r| Row(*r)).collect());
            } else {
                for (_, old, new) in &p.records {
                    if let Some(r) = old {
                        assert!(self.0.remove(&Row(*r)).is_some());
                    }
                    if let Some(r) = new {
                        self.0.insert(Row(*r));
                    }
                }
            }
        }
        fn query(&mut self, _: &State, q: Query, out: &mut Vec<Record>) {
            out.extend(
                self.0
                    .locate_in_envelope_intersecting(AABB::from_corners(q.lower, q.upper))
                    .map(|r| r.0)
                    .filter(|r| q.matches(r)),
            );
        }
    }
}
