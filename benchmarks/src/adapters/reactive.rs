//! Native dependency graphs with shared, partitioned inputs. Each query observes
//! only intersected cells, including currently empty cells. Broad queries observe
//! an all-record input. Runtime invalidation/memo evaluation is upstream code;
//! the common wrapper materializes and diffs complete subscriber results.
use crate::model::*;
use rill_subscriptions::*;
use std::collections::{HashMap, HashSet};
pub trait Runtime {
    type Input: Clone;
    type Memo;
    type Context;
    fn context(&self) -> Self::Context;
    fn input(&mut self, rows: Vec<Record>) -> Self::Input;
    fn set(&mut self, input: &Self::Input, rows: Vec<Record>);
    fn memo(&mut self, q: Query, inputs: Vec<Self::Input>) -> Self::Memo;
    fn get(&mut self, memo: &Self::Memo) -> Vec<Record>;
    fn release_input(&mut self, _: Self::Input) {}
    fn release_memo(&mut self, _: Self::Memo) {}
    fn settle(&mut self) {}
    fn changed(&mut self) -> Option<Vec<Query>> {
        None
    }
    fn batch<T>(_: Self::Context, f: impl FnOnce() -> T) -> T {
        f()
    }
}
// None is the broad-query input; cannot collide with any integer cell.
type Key = Option<Cell>;
struct Input<I> {
    handle: I,
    refs: usize,
}
pub struct Reactive<R: Runtime> {
    queries: HashMap<Query, (R::Memo, Vec<Key>)>,
    inputs: HashMap<Key, Input<R::Input>>,
    grid: Grid,
    refs: HashMap<Query, usize>,
    pub(super) runtime: R,
}
impl<R: Runtime> Reactive<R> {
    pub fn new(runtime: R) -> Self {
        Self {
            queries: HashMap::new(),
            inputs: HashMap::new(),
            grid: Grid::new(64),
            refs: HashMap::new(),
            runtime,
        }
    }
    fn rows(&self, s: &State, key: Key) -> Vec<Record> {
        let mut rs: Vec<Record> = match key {
            None => s.records.values().copied().collect(),
            Some(k) => self
                .grid
                .buckets
                .get(&k)
                .map(|ids| ids.iter().map(|id| s.records[id]).collect())
                .unwrap_or_default(),
        };
        rs.sort_unstable();
        rs
    }
    fn update_inner(&mut self, s: &State, p: &Prepared) {
        self.grid.update(s, p);
        let mut dirty = HashSet::new();
        for (_, old, new) in &p.records {
            if let Some(r) = old {
                dirty.insert(Some(cell(r.point, 64)));
            }
            if let Some(r) = new {
                dirty.insert(Some(cell(r.point, 64)));
            }
        }
        if !p.records.is_empty() {
            dirty.insert(None);
        }
        // Aggregate reference changes first: swapping query identities does not destroy
        // a memo that still has subscribers in the final committed view.
        for (_, old, new) in &p.queries {
            if let Some(q) = old {
                *self.refs.get_mut(q).unwrap() -= 1;
            }
            if let Some(q) = new {
                *self.refs.entry(*q).or_default() += 1;
            }
        }
        let dead: Vec<_> = self
            .refs
            .iter()
            .filter_map(|(q, n)| (*n == 0).then_some(*q))
            .collect();
        for q in dead {
            self.refs.remove(&q);
            if let Some((memo, keys)) = self.queries.remove(&q) {
                self.runtime.release_memo(memo);
                for k in keys {
                    let i = self.inputs.get_mut(&k).unwrap();
                    i.refs -= 1;
                    if i.refs == 0 {
                        let input = self.inputs.remove(&k).unwrap();
                        self.runtime.release_input(input.handle);
                    }
                }
            }
        }
        for k in dirty {
            if self.inputs.contains_key(&k) {
                let rows = self.rows(s, k);
                self.runtime.set(&self.inputs[&k].handle, rows);
            }
        }
        let fresh: Vec<_> = self
            .refs
            .keys()
            .filter(|q| !self.queries.contains_key(q))
            .copied()
            .collect();
        for q in fresh {
            let keys: Vec<Key> = cells(q, 64, 512)
                .map(|v| v.into_iter().map(Some).collect())
                .unwrap_or_else(|| vec![None]);
            let mut handles = Vec::new();
            for &k in &keys {
                if !self.inputs.contains_key(&k) {
                    let rows = self.rows(s, k);
                    let handle = self.runtime.input(rows);
                    self.inputs.insert(k, Input { handle, refs: 0 });
                }
                let i = self.inputs.get_mut(&k).unwrap();
                i.refs += 1;
                handles.push(i.handle.clone());
            }
            let memo = self.runtime.memo(q, handles);
            self.queries.insert(q, (memo, keys));
        }
        self.runtime.settle();
    }
}
impl<R: Runtime> Evaluator for Reactive<R> {
    fn update(&mut self, s: &State, p: &Prepared) {
        R::batch(self.runtime.context(), || self.update_inner(s, p));
    }
    fn query(&mut self, _: &State, q: Query, out: &mut Vec<Record>) {
        out.extend(self.runtime.get(&self.queries[&q].0));
    }
}
impl<R: Runtime> Drop for Reactive<R> {
    fn drop(&mut self) {
        for (_, (m, _)) in self.queries.drain() {
            self.runtime.release_memo(m);
        }
        for (_, i) in self.inputs.drain() {
            self.runtime.release_input(i.handle);
        }
    }
}

#[cfg(feature = "incremental")]
pub mod incr {
    use super::*;
    use incremental::{IncrState, Observer, SubscriptionToken, Var};
    use std::{cell::RefCell, rc::Rc};
    pub struct View {
        observer: Observer<Vec<Record>>,
        token: Option<SubscriptionToken>,
    }
    impl Drop for View {
        fn drop(&mut self) {
            if let Some(t) = self.token.take() {
                self.observer.unsubscribe(t).unwrap();
            }
        }
    }
    pub struct Native(IncrState, Option<Rc<RefCell<HashSet<Query>>>>);
    impl Native {
        pub fn new() -> Self {
            Self(IncrState::new(), None)
        }
        pub fn pushed() -> Self {
            Self(IncrState::new(), Some(Rc::default()))
        }
    }
    impl Runtime for Native {
        type Context = ();
        fn context(&self) {}
        type Input = Var<Vec<Record>>;
        type Memo = View;
        fn input(&mut self, r: Vec<Record>) -> Self::Input {
            self.0.var(r)
        }
        fn set(&mut self, i: &Self::Input, r: Vec<Record>) {
            i.set(r);
        }
        fn memo(&mut self, q: Query, inputs: Vec<Self::Input>) -> Self::Memo {
            let observer = self
                .0
                .fold(
                    inputs.iter().map(|i| i.watch()).collect(),
                    Vec::new(),
                    move |mut out: Vec<Record>, rows: &Vec<Record>| {
                        out.extend(rows.iter().filter(|r| q.matches(r)).copied());
                        out
                    },
                )
                .observe();
            let token = self.1.as_ref().map(|pending| {
                let pending = pending.clone();
                observer.subscribe(move |_| {
                    pending.borrow_mut().insert(q);
                })
            });
            View { observer, token }
        }
        fn get(&mut self, m: &Self::Memo) -> Vec<Record> {
            m.observer.value()
        }
        fn settle(&mut self) {
            self.0.stabilise();
        }
        fn changed(&mut self) -> Option<Vec<Query>> {
            self.1.as_ref().map(|p| p.borrow_mut().drain().collect())
        }
    }
}
#[cfg(feature = "reactive_graph")]
pub mod graph {
    use super::*;
    use reactive_graph::{
        computed::ArcMemo, effect::ImmediateEffect, prelude::*, signal::ArcRwSignal,
    };
    use std::sync::{Arc, Mutex};
    pub struct View {
        memo: ArcMemo<Vec<Record>>,
        _effect: Option<ImmediateEffect>,
    }
    pub struct Native {
        pending: Option<Arc<Mutex<HashSet<Query>>>>,
    }
    impl Native {
        pub fn new() -> Self {
            Self { pending: None }
        }
        pub fn pushed() -> Self {
            Self {
                pending: Some(Arc::default()),
            }
        }
    }
    impl Runtime for Native {
        type Context = ();
        fn context(&self) {}
        type Input = ArcRwSignal<Vec<Record>>;
        type Memo = View;
        fn input(&mut self, r: Vec<Record>) -> Self::Input {
            ArcRwSignal::new(r)
        }
        fn set(&mut self, i: &Self::Input, r: Vec<Record>) {
            i.set(r);
        }
        fn memo(&mut self, q: Query, inputs: Vec<Self::Input>) -> Self::Memo {
            let memo = ArcMemo::new(move |_| {
                let mut out = Vec::new();
                for i in &inputs {
                    out.extend(i.read().iter().filter(|r| q.matches(r)).copied());
                }
                out.sort_unstable();
                out
            });
            let effect = self.pending.as_ref().map(|p| {
                let p = p.clone();
                let memo = memo.clone();
                ImmediateEffect::new_isomorphic(move || {
                    let _rows = memo.read();
                    p.lock().unwrap().insert(q);
                })
            });
            View {
                memo,
                _effect: effect,
            }
        }
        fn get(&mut self, m: &Self::Memo) -> Vec<Record> {
            m.memo.read().clone()
        }
        fn changed(&mut self) -> Option<Vec<Query>> {
            self.pending
                .as_ref()
                .map(|p| p.lock().unwrap().drain().collect())
        }
        fn batch<T>(_: (), f: impl FnOnce() -> T) -> T {
            reactive_graph::effect::batch(f)
        }
    }
}
#[cfg(feature = "floem_reactive")]
pub mod floem {
    use super::*;
    use floem_reactive::{Memo, RwSignal, Scope, SignalGet, SignalUpdate, SignalWith};
    use std::rc::Rc;
    pub struct Cell {
        scope: Scope,
        signal: RwSignal<Vec<Record>>,
    }
    impl Drop for Cell {
        fn drop(&mut self) {
            self.scope.dispose();
        }
    }
    pub struct View {
        scope: Scope,
        memo: Memo<Vec<Record>>,
    }
    impl Drop for View {
        fn drop(&mut self) {
            self.scope.dispose();
        }
    }
    pub struct Native {
        pending: Option<Rc<std::cell::RefCell<HashSet<Query>>>>,
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
        type Input = Rc<Cell>;
        type Memo = View;
        fn input(&mut self, r: Vec<Record>) -> Self::Input {
            let scope = Scope::new();
            Rc::new(Cell {
                scope,
                signal: scope.create_rw_signal(r),
            })
        }
        fn set(&mut self, i: &Self::Input, r: Vec<Record>) {
            i.signal.set(r);
        }
        fn memo(&mut self, q: Query, inputs: Vec<Self::Input>) -> Self::Memo {
            let scope = Scope::new();
            let memo = scope.create_memo(move |_| {
                let mut out = Vec::new();
                for i in &inputs {
                    i.signal
                        .with(|rs| out.extend(rs.iter().filter(|r| q.matches(r)).copied()));
                }
                out.sort_unstable();
                out
            });
            if let Some(p) = &self.pending {
                let p = p.clone();
                scope.create_effect(move |_| {
                    memo.with(|_| {
                        p.borrow_mut().insert(q);
                    })
                });
            }
            View { scope, memo }
        }
        fn get(&mut self, m: &Self::Memo) -> Vec<Record> {
            m.memo.get()
        }
        fn changed(&mut self) -> Option<Vec<Query>> {
            self.pending
                .as_ref()
                .map(|p| p.borrow_mut().drain().collect())
        }
        fn batch<T>(_: (), f: impl FnOnce() -> T) -> T {
            floem_reactive::batch(f)
        }
    }
}
#[cfg(feature = "sycamore_reactive")]
pub mod sycamore {
    use super::*;
    use std::rc::Rc;
    use sycamore_reactive::{
        NodeHandle, ReadSignal, RootHandle, Signal, create_child_scope, create_memo, create_root,
        create_signal,
    };
    pub struct Cell {
        scope: NodeHandle,
        signal: Signal<Vec<Record>>,
    }
    impl Drop for Cell {
        fn drop(&mut self) {
            self.scope.dispose();
        }
    }
    pub struct View {
        scope: NodeHandle,
        memo: ReadSignal<Vec<Record>>,
    }
    impl Drop for View {
        fn drop(&mut self) {
            self.scope.dispose();
        }
    }
    pub struct Native(RootHandle, Option<Rc<std::cell::RefCell<HashSet<Query>>>>);
    impl Native {
        pub fn new() -> Self {
            Self(create_root(|| {}), None)
        }
        pub fn pushed() -> Self {
            Self(create_root(|| {}), Some(Rc::default()))
        }
    }
    impl Drop for Native {
        fn drop(&mut self) {
            self.0.dispose();
        }
    }
    impl Runtime for Native {
        type Context = RootHandle;
        fn context(&self) -> RootHandle {
            self.0
        }
        type Input = Rc<Cell>;
        type Memo = View;
        fn input(&mut self, r: Vec<Record>) -> Self::Input {
            self.0.run_in(|| {
                let mut signal = None;
                let scope = create_child_scope(|| signal = Some(create_signal(r)));
                Rc::new(Cell {
                    scope,
                    signal: signal.unwrap(),
                })
            })
        }
        fn set(&mut self, i: &Self::Input, r: Vec<Record>) {
            self.0.run_in(|| i.signal.set(r));
        }
        fn memo(&mut self, q: Query, inputs: Vec<Self::Input>) -> Self::Memo {
            let pending = self.1.clone();
            self.0.run_in(|| {
                let mut memo = None;
                let scope = create_child_scope(|| {
                    let result = create_memo(move || {
                        let mut out = Vec::new();
                        for i in &inputs {
                            i.signal
                                .with(|rs| out.extend(rs.iter().filter(|r| q.matches(r)).copied()));
                        }
                        out.sort_unstable();
                        out
                    });
                    if let Some(p) = pending {
                        sycamore_reactive::create_effect(move || {
                            result.with(|_| {
                                p.borrow_mut().insert(q);
                            })
                        });
                    }
                    memo = Some(result);
                });
                View {
                    scope,
                    memo: memo.unwrap(),
                }
            })
        }
        fn get(&mut self, m: &Self::Memo) -> Vec<Record> {
            self.0.run_in(|| m.memo.get_clone())
        }
        fn changed(&mut self) -> Option<Vec<Query>> {
            self.1.as_ref().map(|p| p.borrow_mut().drain().collect())
        }
        fn batch<T>(root: RootHandle, f: impl FnOnce() -> T) -> T {
            root.run_in(|| sycamore_reactive::batch(f))
        }
    }
}
#[cfg(feature = "salsa")]
pub mod salsa_native {
    use super::*;
    use salsa::Setter;
    #[salsa::input]
    pub struct CellInput {
        #[returns(ref)]
        rows: Vec<Record>,
    }
    #[salsa::input]
    pub struct QueryInput {
        query: Query,
        #[returns(ref)]
        cells: Vec<CellInput>,
    }
    #[salsa::db]
    #[derive(Default, Clone)]
    pub struct Database {
        storage: salsa::Storage<Self>,
    }
    #[salsa::db]
    impl salsa::Database for Database {}
    #[salsa::tracked(returns(ref))]
    fn evaluate(db: &dyn salsa::Database, input: QueryInput) -> Vec<Record> {
        let q = input.query(db);
        let mut out = Vec::new();
        for i in input.cells(db) {
            out.extend(i.rows(db).iter().filter(|r| q.matches(r)).copied());
        }
        out.sort_unstable();
        out
    }
    // Salsa owns input IDs; bounded pools reuse retired inputs rather than retaining
    // one new input identity per moving query/cell forever.
    pub struct Native {
        db: Database,
        free_cells: Vec<CellInput>,
        free_queries: Vec<QueryInput>,
    }
    impl Native {
        pub fn new() -> Self {
            Self {
                db: Database::default(),
                free_cells: Vec::new(),
                free_queries: Vec::new(),
            }
        }
    }
    impl Runtime for Native {
        type Context = ();
        fn context(&self) {}
        type Input = CellInput;
        type Memo = QueryInput;
        fn input(&mut self, r: Vec<Record>) -> CellInput {
            if let Some(i) = self.free_cells.pop() {
                i.set_rows(&mut self.db).to(r);
                i
            } else {
                CellInput::new(&self.db, r)
            }
        }
        fn set(&mut self, i: &CellInput, r: Vec<Record>) {
            i.set_rows(&mut self.db).to(r);
        }
        fn memo(&mut self, q: Query, inputs: Vec<CellInput>) -> QueryInput {
            if let Some(i) = self.free_queries.pop() {
                i.set_query(&mut self.db).to(q);
                i.set_cells(&mut self.db).to(inputs);
                i
            } else {
                QueryInput::new(&self.db, q, inputs)
            }
        }
        fn get(&mut self, m: &QueryInput) -> Vec<Record> {
            evaluate(&self.db, *m).clone()
        }
        fn release_input(&mut self, i: CellInput) {
            i.set_rows(&mut self.db).to(Vec::new());
            self.free_cells.push(i);
        }
        fn release_memo(&mut self, m: QueryInput) {
            m.set_cells(&mut self.db).to(Vec::new());
            self.free_queries.push(m);
        }
    }
}
#[cfg(feature = "futures_signals")]
pub mod futures {
    use super::*;
    use futures_signals::signal::{Mutable, Signal, SignalExt};
    use std::{
        cell::RefCell,
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll, Wake, Waker},
    };
    struct Notify {
        q: Query,
        pending: Arc<Mutex<HashSet<Query>>>,
    }
    impl Wake for Notify {
        fn wake(self: Arc<Self>) {
            self.pending.lock().unwrap().insert(self.q);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.pending.lock().unwrap().insert(self.q);
        }
    }
    struct ViewState {
        signals: Vec<Pin<Box<dyn Signal<Item = Vec<Record>>>>>,
        latest: Vec<Vec<Record>>,
        cached: Vec<Record>,
        waker: Waker,
    }
    pub struct View(RefCell<ViewState>);
    pub struct Native {
        pending: Option<Arc<Mutex<HashSet<Query>>>>,
    }
    impl Native {
        pub fn new() -> Self {
            Self { pending: None }
        }
        pub fn pushed() -> Self {
            Self {
                pending: Some(Arc::default()),
            }
        }
    }
    impl Runtime for Native {
        type Context = ();
        fn context(&self) {}
        type Input = Mutable<Vec<Record>>;
        type Memo = View;
        fn input(&mut self, r: Vec<Record>) -> Self::Input {
            Mutable::new(r)
        }
        fn set(&mut self, i: &Self::Input, r: Vec<Record>) {
            i.set(r);
        }
        fn memo(&mut self, q: Query, inputs: Vec<Self::Input>) -> View {
            let count = inputs.len();
            let waker = self
                .pending
                .as_ref()
                .map(|p| {
                    Waker::from(Arc::new(Notify {
                        q,
                        pending: p.clone(),
                    }))
                })
                .unwrap_or_else(|| Waker::noop().clone());
            View(RefCell::new(ViewState {
                signals: inputs
                    .into_iter()
                    .map(|i| {
                        Box::pin(
                            i.signal_cloned().map(move |rows| {
                                rows.into_iter().filter(|r| q.matches(r)).collect()
                            }),
                        ) as Pin<Box<dyn Signal<Item = Vec<Record>>>>
                    })
                    .collect(),
                latest: vec![Vec::new(); count],
                cached: Vec::new(),
                waker,
            }))
        }
        fn get(&mut self, m: &View) -> Vec<Record> {
            let mut v = m.0.borrow_mut();
            let waker = v.waker.clone();
            let mut cx = Context::from_waker(&waker);
            let mut dirty = false;
            for i in 0..v.signals.len() {
                if let Poll::Ready(Some(rows)) = v.signals[i].as_mut().poll_change(&mut cx) {
                    v.latest[i] = rows;
                    dirty = true;
                }
            }
            if dirty {
                v.cached = v.latest.iter().flatten().copied().collect();
                v.cached.sort_unstable();
            }
            v.cached.clone()
        }
        fn changed(&mut self) -> Option<Vec<Query>> {
            self.pending
                .as_ref()
                .map(|p| p.lock().unwrap().drain().collect())
        }
    }
}
