//! Native Dioxus signal/memo runtime with bounded pools for scope-owned inputs.
//! Runtime::process_events drains native memo tasks after each committed batch;
//! no renderer or DOM is attached. Native ReactiveContext callbacks drive Push.
use super::reactive::Runtime;
use dioxus_core::{ReactiveContext, ScopeId, VNode, VirtualDom};
use dioxus_signals::{Memo, ReadableExt, Signal, WritableExt};
use rill_subscriptions::*;
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};
type Input = Signal<Vec<Record>>;
type Spec = Signal<(Query, Vec<Input>)>;
#[derive(Clone)]
pub struct View {
    spec: Spec,
    memo: Memo<Vec<Record>>,
    notify: Option<(ReactiveContext, Arc<Mutex<Query>>)>,
}
pub struct Native {
    dom: VirtualDom,
    inputs: Vec<Input>,
    views: Vec<View>,
    pending: Option<Arc<Mutex<HashSet<Query>>>>,
}
impl Native {
    pub fn new() -> Self {
        let mut dom = VirtualDom::new(VNode::empty);
        dom.rebuild_in_place();
        Self {
            dom,
            inputs: Vec::new(),
            views: Vec::new(),
            pending: None,
        }
    }
    pub fn pushed() -> Self {
        let mut this = Self::new();
        this.pending = Some(Arc::default());
        this
    }
}
impl Runtime for Native {
    type Context = ();
    fn context(&self) {}
    type Input = Input;
    type Memo = View;
    fn input(&mut self, r: Vec<Record>) -> Input {
        self.dom.in_scope(ScopeId::ROOT, || {
            if let Some(mut i) = self.inputs.pop() {
                i.set(r);
                i
            } else {
                Signal::new(r)
            }
        })
    }
    fn set(&mut self, i: &Input, r: Vec<Record>) {
        self.dom.in_scope(ScopeId::ROOT, || {
            let mut i = *i;
            i.set(r);
        });
    }
    fn memo(&mut self, q: Query, inputs: Vec<Input>) -> View {
        self.dom.in_scope(ScopeId::ROOT, || {
            if let Some(mut v) = self.views.pop() {
                if let Some((_, name)) = &v.notify {
                    *name.lock().unwrap() = q;
                }
                v.spec.set((q, inputs));
                v
            } else {
                let spec = Signal::new((q, inputs));
                let memo = Memo::new(move || {
                    let (q, inputs) = spec.read().clone();
                    let mut out = Vec::new();
                    for i in inputs {
                        out.extend(i.read().iter().filter(|r| q.matches(r)).copied());
                    }
                    out.sort_unstable();
                    out
                });
                let notify = self.pending.as_ref().map(|p| {
                    let p = p.clone();
                    let name = Arc::new(Mutex::new(q));
                    let key = name.clone();
                    let context = ReactiveContext::new_with_callback(
                        move || {
                            p.lock().unwrap().insert(*key.lock().unwrap());
                        },
                        ScopeId::ROOT,
                        std::panic::Location::caller(),
                    );
                    (context, name)
                });
                View { spec, memo, notify }
            }
        })
    }
    fn get(&mut self, v: &View) -> Vec<Record> {
        self.dom.in_scope(ScopeId::ROOT, || {
            if let Some((ctx, _)) = &v.notify {
                ctx.reset_and_run_in(|| v.memo.read().clone())
            } else {
                v.memo.read().clone()
            }
        })
    }
    fn settle(&mut self) {
        self.dom.process_events();
    }
    fn release_input(&mut self, mut i: Input) {
        self.dom.in_scope(ScopeId::ROOT, || i.set(Vec::new()));
        self.inputs.push(i);
    }
    fn release_memo(&mut self, mut v: View) {
        self.dom.in_scope(ScopeId::ROOT, || {
            if let Some((ctx, _)) = &v.notify {
                ctx.clear_subscribers();
            }
            v.spec.set((Query::all(), Vec::new()));
            let _ = v.memo.read();
        });
        self.views.push(v);
    }
    fn changed(&mut self) -> Option<Vec<Query>> {
        self.pending
            .as_ref()
            .map(|p| p.lock().unwrap().drain().collect())
    }
}
