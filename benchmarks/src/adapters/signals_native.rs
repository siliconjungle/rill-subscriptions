//! Native synchronous listeners from signals 0.0.5. No threads are spawned by
//! this adapter; weakly held listeners live exactly as long as their query view.
use super::reactive::Runtime;
use rill_subscriptions::*;
use signals::{Am, Emitter, SigFn, Signal};
use std::{
    any::Any,
    cell::RefCell,
    collections::HashSet,
    rc::Rc,
    sync::{Arc, Mutex},
};
type Rows = Vec<Record>;
type Pending = Option<Arc<Mutex<HashSet<Query>>>>;
trait Source {
    fn set(&self, r: Rows);
    fn listen(&self, q: Query, rows: Arc<Mutex<Rows>>, pending: Pending) -> Box<dyn Any>;
}
impl<F: SigFn<Rows, Rows> + Send + Sync + 'static> Source for Am<Signal<Rows, Rows, F>> {
    fn set(&self, r: Rows) {
        self.lock().emit(r);
    }
    fn listen(&self, q: Query, rows: Arc<Mutex<Rows>>, pending: Pending) -> Box<dyn Any> {
        Box::new(self.lock().create_listener(move |r: Rows| {
            *rows.lock().unwrap() = r.into_iter().filter(|r| q.matches(r)).collect();
            if let Some(p) = &pending {
                p.lock().unwrap().insert(q);
            }
            Ok(())
        }))
    }
}
#[derive(Clone)]
pub struct Input {
    source: Rc<dyn Source>,
    rows: Rc<RefCell<Rows>>,
}
pub struct View {
    rows: Vec<Arc<Mutex<Rows>>>,
    _listeners: Vec<Box<dyn Any>>,
}
pub struct Native {
    pending: Pending,
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
    type Memo = View;
    fn input(&mut self, r: Rows) -> Input {
        Input {
            source: Rc::new(Signal::new_arc_mutex(|r: Rows| Ok(r))),
            rows: Rc::new(RefCell::new(r)),
        }
    }
    fn set(&mut self, i: &Input, r: Rows) {
        *i.rows.borrow_mut() = r.clone();
        i.source.set(r);
    }
    fn memo(&mut self, q: Query, inputs: Vec<Input>) -> View {
        let mut rows = Vec::new();
        let mut listeners = Vec::new();
        for i in inputs {
            let cache = Arc::new(Mutex::new(
                i.rows
                    .borrow()
                    .iter()
                    .filter(|r| q.matches(r))
                    .copied()
                    .collect(),
            ));
            listeners.push(i.source.listen(q, cache.clone(), self.pending.clone()));
            rows.push(cache);
        }
        View {
            rows,
            _listeners: listeners,
        }
    }
    fn get(&mut self, m: &View) -> Rows {
        let mut out = Vec::new();
        for r in &m.rows {
            out.extend(r.lock().unwrap().iter().copied());
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
