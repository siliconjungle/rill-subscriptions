use super::reactive::Runtime;
use rill_subscriptions::*;
#[cfg(feature = "incr_compute")]
pub mod incr {
    use super::*;
    use incr_compute::{Incr, Runtime as Graph};
    type Input = Incr<Vec<Record>>;
    #[derive(Clone)]
    struct Spec(Query, Vec<Input>);
    impl PartialEq for Spec {
        fn eq(&self, other: &Self) -> bool {
            self.0 == other.0
                && self.1.len() == other.1.len()
                && self
                    .1
                    .iter()
                    .zip(&other.1)
                    .all(|(a, b)| a.node_id() == b.node_id())
        }
    }
    type Config = Incr<Spec>;
    #[derive(Clone, Copy)]
    pub struct View {
        config: Config,
        result: Incr<Vec<Record>>,
    }
    pub struct Native {
        rt: Graph,
        inputs: Vec<Input>,
        views: Vec<View>,
    }
    impl Native {
        pub fn new() -> Self {
            Self {
                rt: Graph::new(),
                inputs: Vec::new(),
                views: Vec::new(),
            }
        }
    }
    impl Runtime for Native {
        type Context = ();
        fn context(&self) {}
        type Input = Input;
        type Memo = View;
        fn input(&mut self, r: Vec<Record>) -> Input {
            if let Some(i) = self.inputs.pop() {
                self.rt.set(i, r);
                i
            } else {
                self.rt.create_input(r)
            }
        }
        fn set(&mut self, i: &Input, r: Vec<Record>) {
            self.rt.set(*i, r);
        }
        fn memo(&mut self, q: Query, inputs: Vec<Input>) -> View {
            if let Some(v) = self.views.pop() {
                self.rt.set(v.config, Spec(q, inputs));
                v
            } else {
                let config = self.rt.create_input(Spec(q, inputs));
                let result = self.rt.create_query(move |rt| {
                    let Spec(q, inputs) = rt.get(config);
                    let mut out = Vec::new();
                    for i in inputs {
                        out.extend(rt.get(i).into_iter().filter(|r| q.matches(r)));
                    }
                    out.sort_unstable();
                    out
                });
                View { config, result }
            }
        }
        fn get(&mut self, v: &View) -> Vec<Record> {
            self.rt.get(v.result)
        }
        fn release_input(&mut self, i: Input) {
            self.rt.set(i, Vec::new());
            self.inputs.push(i);
        }
        fn release_memo(&mut self, v: View) {
            self.rt.set(v.config, Spec(Query::all(), Vec::new()));
            let _: Vec<Record> = self.rt.get(v.result);
            self.views.push(v);
        }
    }
}
#[cfg(feature = "adapton")]
pub mod adapton_native {
    use super::*;
    use adapton::{
        engine::{self, Art, NameChoice, manage, name_of_usize},
        macros::ProgPt,
    };
    use std::rc::Rc;
    #[derive(Clone)]
    pub struct View {
        id: usize,
        art: Art<Vec<Record>>,
    }
    pub struct Native {
        next: usize,
        inputs: Vec<Art<Vec<Record>>>,
        views: Vec<usize>,
    }
    impl Native {
        pub fn new() -> Self {
            manage::init_dcg();
            Self {
                next: 0,
                inputs: Vec::new(),
                views: Vec::new(),
            }
        }
        fn id(&mut self) -> usize {
            let id = self.next;
            self.next += 1;
            id
        }
    }
    impl Drop for Native {
        fn drop(&mut self) {
            manage::init_naive();
        }
    }
    fn view(id: usize, q: Query, inputs: Vec<Art<Vec<Record>>>) -> View {
        let art = engine::thunk(
            NameChoice::Nominal(name_of_usize(id)),
            ProgPt {
                symbol: "rill-subscription-adapter",
            },
            Rc::new(Box::new(
                |(q, inputs): (Query, Vec<Art<Vec<Record>>>), ()| {
                    let mut out = Vec::new();
                    for i in inputs {
                        out.extend(engine::force(&i).into_iter().filter(|r| q.matches(r)));
                    }
                    out.sort_unstable();
                    out
                },
            )),
            (q, inputs),
            (),
        );
        View { id, art }
    }
    impl Runtime for Native {
        type Context = ();
        fn context(&self) {}
        type Input = Art<Vec<Record>>;
        type Memo = View;
        fn input(&mut self, r: Vec<Record>) -> Self::Input {
            if let Some(i) = self.inputs.pop() {
                engine::set(&i, r);
                i
            } else {
                engine::cell(name_of_usize(self.id()), r)
            }
        }
        fn set(&mut self, i: &Self::Input, r: Vec<Record>) {
            engine::set(i, r);
        }
        fn memo(&mut self, q: Query, inputs: Vec<Self::Input>) -> View {
            let id = if let Some(id) = self.views.pop() {
                id
            } else {
                self.id()
            };
            view(id, q, inputs)
        }
        fn get(&mut self, v: &View) -> Vec<Record> {
            engine::force(&v.art)
        }
        fn release_input(&mut self, i: Self::Input) {
            engine::set(&i, Vec::new());
            self.inputs.push(i);
        }
        fn release_memo(&mut self, v: View) {
            let empty = view(v.id, Query::all(), Vec::new());
            let _ = engine::force(&empty.art);
            self.views.push(v.id);
        }
    }
}
#[cfg(feature = "carboxyl")]
pub mod carboxyl_native {
    use super::*;
    use carboxyl::{Signal, Sink};
    #[derive(Clone)]
    pub struct Input {
        sink: Sink<Vec<Record>>,
        signal: Signal<Vec<Record>>,
    }
    pub struct Native {
        pending: Option<std::sync::Arc<std::sync::Mutex<std::collections::HashSet<Query>>>>,
    }
    impl Native {
        pub fn new() -> Self {
            Self { pending: None }
        }
        pub fn pushed() -> Self {
            Self {
                pending: Some(Default::default()),
            }
        }
    }
    impl Runtime for Native {
        type Context = ();
        fn context(&self) {}
        type Input = Input;
        type Memo = Vec<Signal<Vec<Record>>>;
        fn input(&mut self, r: Vec<Record>) -> Input {
            let sink = Sink::new();
            let signal = sink.stream().hold(r);
            Input { sink, signal }
        }
        fn set(&mut self, i: &Input, r: Vec<Record>) {
            i.sink.send(r);
        }
        fn memo(&mut self, q: Query, inputs: Vec<Input>) -> Self::Memo {
            let pending = self.pending.clone();
            inputs
                .into_iter()
                .map(|i| {
                    let pending = pending.clone();
                    let initial = i
                        .signal
                        .sample()
                        .into_iter()
                        .filter(|r| q.matches(r))
                        .collect();
                    i.sink
                        .stream()
                        .map(move |rs| {
                            if let Some(p) = &pending {
                                p.lock().unwrap().insert(q);
                            }
                            rs.into_iter().filter(|r| q.matches(r)).collect()
                        })
                        .hold(initial)
                })
                .collect()
        }
        fn get(&mut self, m: &Self::Memo) -> Vec<Record> {
            let mut out = Vec::new();
            for s in m {
                out.extend(s.sample());
            }
            out.sort_unstable();
            out
        }
        fn changed(&mut self) -> Option<Vec<Query>> {
            self.pending
                .as_ref()
                .map(|p| p.lock().unwrap().drain().collect())
        }
    }
}
#[cfg(feature = "frappe")]
pub mod frappe_native {
    use super::*;
    use frappe::{Signal, Sink};
    #[derive(Clone)]
    pub struct Input {
        sink: Sink<Vec<Record>>,
        signal: Signal<Vec<Record>>,
    }
    pub struct Native {
        pending: Option<std::sync::Arc<std::sync::Mutex<std::collections::HashSet<Query>>>>,
    }
    impl Native {
        pub fn new() -> Self {
            Self { pending: None }
        }
        pub fn pushed() -> Self {
            Self {
                pending: Some(Default::default()),
            }
        }
    }
    impl Runtime for Native {
        type Context = ();
        fn context(&self) {}
        type Input = Input;
        type Memo = Vec<Signal<Vec<Record>>>;
        fn input(&mut self, r: Vec<Record>) -> Input {
            let sink = Sink::new();
            let signal = sink.stream().hold(r);
            Input { sink, signal }
        }
        fn set(&mut self, i: &Input, r: Vec<Record>) {
            i.sink.send(r);
        }
        fn memo(&mut self, q: Query, inputs: Vec<Input>) -> Self::Memo {
            let pending = self.pending.clone();
            inputs
                .into_iter()
                .map(|i| {
                    let pending = pending.clone();
                    let initial = i
                        .signal
                        .sample()
                        .into_iter()
                        .filter(|r| q.matches(r))
                        .collect();
                    i.sink
                        .stream()
                        .map(move |rs| {
                            if let Some(p) = &pending {
                                p.lock().unwrap().insert(q);
                            }
                            rs.iter().filter(|r| q.matches(r)).copied().collect()
                        })
                        .hold(initial)
                })
                .collect()
        }
        fn get(&mut self, m: &Self::Memo) -> Vec<Record> {
            let mut out = Vec::new();
            for s in m {
                out.extend(s.sample());
            }
            out.sort_unstable();
            out
        }
        fn changed(&mut self) -> Option<Vec<Query>> {
            self.pending
                .as_ref()
                .map(|p| p.lock().unwrap().drain().collect())
        }
    }
}
#[cfg(feature = "sodium_rust")]
pub mod sodium {
    use super::*;
    use sodium_rust::{Cell, CellSink, SodiumCtx};
    pub struct Native(SodiumCtx);
    impl Native {
        pub fn new() -> Self {
            Self(SodiumCtx::new())
        }
    }
    impl Runtime for Native {
        type Context = SodiumCtx;
        fn context(&self) -> SodiumCtx {
            self.0.clone()
        }
        type Input = CellSink<Vec<Record>>;
        type Memo = Cell<Vec<Record>>;
        fn input(&mut self, r: Vec<Record>) -> Self::Input {
            self.0.new_cell_sink(r)
        }
        fn set(&mut self, i: &Self::Input, r: Vec<Record>) {
            i.send(r);
        }
        fn memo(&mut self, q: Query, inputs: Vec<Self::Input>) -> Self::Memo {
            let mut layer: Vec<Cell<Vec<Record>>> = inputs
                .into_iter()
                .map(|i| {
                    i.cell().map(move |rs: &Vec<Record>| {
                        rs.iter().filter(|r| q.matches(r)).copied().collect()
                    })
                })
                .collect();
            if layer.is_empty() {
                return self.0.new_cell(Vec::new());
            }
            while layer.len() > 1 {
                layer = layer
                    .chunks(2)
                    .map(|p| {
                        if p.len() == 1 {
                            p[0].clone()
                        } else {
                            p[0].lift2(&p[1], |a: &Vec<Record>, b: &Vec<Record>| {
                                a.iter().chain(b).copied().collect()
                            })
                        }
                    })
                    .collect();
            }
            layer.pop().unwrap().map(|rs: &Vec<Record>| {
                let mut rs = rs.clone();
                rs.sort_unstable();
                rs
            })
        }
        fn get(&mut self, m: &Self::Memo) -> Vec<Record> {
            m.sample()
        }
        fn batch<T>(ctx: SodiumCtx, f: impl FnOnce() -> T) -> T {
            ctx.transaction(f)
        }
    }
}
#[cfg(feature = "leptos_reactive")]
pub mod leptos {
    use super::*;
    use leptos_reactive::{
        Memo, RuntimeId, RwSignal, SignalDispose, SignalGet, SignalSet, SignalWith, create_memo,
        create_runtime, create_rw_signal,
    };
    pub struct Native(RuntimeId);
    impl Native {
        pub fn new() -> Self {
            Self(create_runtime())
        }
    }
    impl Drop for Native {
        fn drop(&mut self) {
            self.0.dispose();
        }
    }
    impl Runtime for Native {
        type Context = ();
        fn context(&self) {}
        type Input = RwSignal<Vec<Record>>;
        type Memo = Memo<Vec<Record>>;
        fn input(&mut self, r: Vec<Record>) -> Self::Input {
            create_rw_signal(r)
        }
        fn set(&mut self, i: &Self::Input, r: Vec<Record>) {
            i.set(r);
        }
        fn memo(&mut self, q: Query, inputs: Vec<Self::Input>) -> Self::Memo {
            create_memo(move |_| {
                let mut out = Vec::new();
                for i in &inputs {
                    i.with(|rs| out.extend(rs.iter().filter(|r| q.matches(r)).copied()));
                }
                out.sort_unstable();
                out
            })
        }
        fn get(&mut self, m: &Self::Memo) -> Vec<Record> {
            m.get()
        }
        fn release_input(&mut self, i: Self::Input) {
            i.dispose();
        }
        fn release_memo(&mut self, m: Self::Memo) {
            m.dispose();
        }
        fn batch<T>(_: (), f: impl FnOnce() -> T) -> T {
            leptos_reactive::batch(f)
        }
    }
}
#[cfg(feature = "rxrust")]
pub mod rx {
    use super::*;
    use rxrust::prelude::*;
    use std::{cell::RefCell, rc::Rc};
    #[derive(Clone)]
    pub struct Input {
        subject: LocalSubject<'static, Vec<Record>, ()>,
        rows: Rc<RefCell<Vec<Record>>>,
    }
    pub struct View {
        signals: Vec<Rc<RefCell<Vec<Record>>>>,
        subscriptions: Vec<LocalSubscription>,
        inputs: Vec<Input>,
    }
    impl Drop for View {
        fn drop(&mut self) {
            for s in &mut self.subscriptions {
                s.unsubscribe();
            }
            for i in &self.inputs {
                i.subject.clone().next(i.rows.borrow().clone());
            }
        }
    }
    pub struct Native {
        pending: Option<Rc<RefCell<std::collections::HashSet<Query>>>>,
    }
    impl Native {
        pub fn new() -> Self {
            Self { pending: None }
        }
        pub fn pushed() -> Self {
            Self {
                pending: Some(Rc::default()),
            }
        }
    }
    impl Runtime for Native {
        type Context = ();
        fn context(&self) {}
        type Input = Input;
        type Memo = View;
        fn input(&mut self, r: Vec<Record>) -> Input {
            Input {
                subject: LocalSubject::new(),
                rows: Rc::new(RefCell::new(r)),
            }
        }
        fn set(&mut self, i: &Input, r: Vec<Record>) {
            *i.rows.borrow_mut() = r.clone();
            i.subject.clone().next(r);
        }
        fn memo(&mut self, q: Query, inputs: Vec<Input>) -> View {
            let mut signals = Vec::new();
            let mut subscriptions = Vec::new();
            let keep = inputs.clone();
            for i in inputs {
                let pending = self.pending.clone();
                let rows = Rc::new(RefCell::new(
                    i.rows
                        .borrow()
                        .iter()
                        .filter(|r| q.matches(r))
                        .copied()
                        .collect(),
                ));
                let into = rows.clone();
                let subscription = i
                    .subject
                    .map(move |rs| {
                        if let Some(p) = &pending {
                            p.borrow_mut().insert(q);
                        }
                        rs.into_iter().filter(|r| q.matches(r)).collect::<Vec<_>>()
                    })
                    .subscribe(move |rs| *into.borrow_mut() = rs)
                    .into_inner();
                signals.push(rows);
                subscriptions.push(subscription);
            }
            View {
                signals,
                subscriptions,
                inputs: keep,
            }
        }
        fn get(&mut self, m: &View) -> Vec<Record> {
            let mut out = Vec::new();
            for s in &m.signals {
                out.extend(s.borrow().iter().copied());
            }
            out.sort_unstable();
            out
        }
        fn changed(&mut self) -> Option<Vec<Query>> {
            self.pending
                .as_ref()
                .map(|p| p.borrow_mut().drain().collect())
        }
    }
}
#[cfg(feature = "depends")]
pub mod depends_native {
    use super::*;
    use depends::{
        DepRef, Dependency, DerivedNode, HashSetVisitor, InputNode, Resolve, UpdateDerived,
        UpdateInput,
        derives::{Operation, Value},
        error::EarlyExit,
    };
    use std::rc::Rc;
    #[derive(Value, Hash)]
    pub struct Rows {
        q: Query,
        rows: Vec<Record>,
    }
    impl UpdateInput for Rows {
        type Update = Vec<Record>;
        fn update_mut(&mut self, r: Vec<Record>) {
            self.rows = r;
        }
    }
    #[derive(Operation)]
    pub struct Filter;
    impl UpdateDerived<DepRef<'_, Rows>, Filter> for Rows {
        fn update(&mut self, d: DepRef<'_, Rows>) -> Result<(), EarlyExit> {
            self.rows = d
                .data()
                .value()
                .rows
                .iter()
                .filter(|r| self.q.matches(r))
                .copied()
                .collect();
            Ok(())
        }
    }
    type Input = Rc<InputNode<Rows>>;
    type View = Rc<DerivedNode<Dependency<Input>, Rows, Filter>>;
    pub struct Native {
        visitor: HashSetVisitor,
    }
    impl Native {
        pub fn new() -> Self {
            Self {
                visitor: HashSetVisitor::new(),
            }
        }
    }
    impl Runtime for Native {
        type Context = ();
        fn context(&self) {}
        type Input = Input;
        type Memo = Vec<View>;
        fn input(&mut self, r: Vec<Record>) -> Input {
            InputNode::new(Rows {
                q: Query::all(),
                rows: r,
            })
        }
        fn set(&mut self, i: &Input, r: Vec<Record>) {
            i.update(r).unwrap();
        }
        fn memo(&mut self, q: Query, inputs: Vec<Input>) -> Self::Memo {
            inputs
                .into_iter()
                .map(|i| {
                    DerivedNode::new(
                        Dependency::new(i),
                        Filter,
                        Rows {
                            q,
                            rows: Vec::new(),
                        },
                    )
                })
                .collect()
        }
        fn get(&mut self, m: &Self::Memo) -> Vec<Record> {
            let mut out = Vec::new();
            for v in m {
                let r = v.resolve_root(&mut self.visitor).unwrap();
                out.extend(r.value().rows.iter().copied());
            }
            out.sort_unstable();
            out
        }
    }
}

#[cfg(feature = "incr_concurrent")]
pub mod concurrent {
    use super::*;
    use incr_concurrent::{Incr, Runtime as Graph};
    #[derive(Clone, PartialEq)]
    pub struct Data(Vec<Record>);
    incr_concurrent::impl_value!(Data);
    type Input = Incr<Data>;
    #[derive(Clone, PartialEq)]
    struct Spec(Query, Vec<Input>);
    incr_concurrent::impl_value!(Spec);
    type Config = Incr<Spec>;
    #[derive(Clone, Copy)]
    pub struct View {
        config: Config,
        result: Incr<Data>,
    }
    pub struct Native {
        rt: Graph,
        inputs: Vec<Input>,
        views: Vec<View>,
    }
    impl Native {
        pub fn new() -> Self {
            Self {
                rt: Graph::new(),
                inputs: Vec::new(),
                views: Vec::new(),
            }
        }
    }
    impl Runtime for Native {
        type Context = ();
        fn context(&self) {}
        type Input = Input;
        type Memo = View;
        fn input(&mut self, r: Vec<Record>) -> Input {
            if let Some(i) = self.inputs.pop() {
                self.rt.set(i, Data(r));
                i
            } else {
                self.rt.create_input(Data(r))
            }
        }
        fn set(&mut self, i: &Input, r: Vec<Record>) {
            self.rt.set(*i, Data(r));
        }
        fn memo(&mut self, q: Query, inputs: Vec<Input>) -> View {
            if let Some(v) = self.views.pop() {
                self.rt.set(v.config, Spec(q, inputs));
                v
            } else {
                let config = self.rt.create_input(Spec(q, inputs));
                let result = self.rt.create_query(move |rt| {
                    let Spec(q, inputs) = rt.get(config);
                    let mut out = Vec::new();
                    for i in inputs {
                        out.extend(rt.get(i).0.into_iter().filter(|r| q.matches(r)));
                    }
                    out.sort_unstable();
                    Data(out)
                });
                View { config, result }
            }
        }
        fn get(&mut self, v: &View) -> Vec<Record> {
            self.rt.get(v.result).0
        }
        fn release_input(&mut self, i: Input) {
            self.rt.set(i, Data(Vec::new()));
            self.inputs.push(i);
        }
        fn release_memo(&mut self, v: View) {
            self.rt.set(v.config, Spec(Query::all(), Vec::new()));
            let _: Data = self.rt.get(v.result);
            self.views.push(v);
        }
    }
}
