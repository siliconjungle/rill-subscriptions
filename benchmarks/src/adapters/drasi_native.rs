//! One native Drasi continuous query over cell-joined records and query nodes.
//! Strings preserve full u64 identifiers/masks/revisions. Native Cypher executes
//! spatial/numeric conditions; exact Rust mask postfilter uses the same predicate
//! as other adapters. All native diffs are consumed; atomic output is consolidated
//! against pre/post batch projections, never capped or truncated.
use crate::model::*;
use drasi_core::{
    evaluation::{
        context::{QueryPartEvaluationContext, QueryVariables},
        functions::FunctionRegistry,
        variable_value::VariableValue,
    },
    models::{
        Element, ElementMetadata, ElementPropertyMap, ElementReference, QueryJoin, QueryJoinKey,
        SourceChange,
    },
    query::{ContinuousQuery, QueryBuilder},
};
use drasi_query_cypher::CypherParser;
use rill_subscriptions::*;
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
pub struct Native {
    state: State,
    rt: tokio::runtime::Runtime,
    query: ContinuousQuery,
    time: u64,
}
fn key(k: Option<Cell>) -> String {
    k.map(|p| format!("{},{},{}", p[0], p[1], p[2]))
        .unwrap_or_else(|| "*".into())
}
fn keys(q: Query) -> Vec<String> {
    cells(q, 64, 512)
        .map(|v| v.into_iter().map(|k| key(Some(k))).collect())
        .unwrap_or_else(|| vec![key(None)])
}
impl Native {
    pub fn new() -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let query=rt.block_on(async{let registry=Arc::new(FunctionRegistry::new());let parser=Arc::new(CypherParser::new(registry.clone()));QueryBuilder::new("MATCH (q:Subscription)-[:CELL]-(r:Row) WHERE r.x >= q.x0 AND r.x <= q.x1 AND r.y >= q.y0 AND r.y <= q.y1 AND r.z >= q.z0 AND r.z <= q.z1 AND r.value >= q.v0 AND r.value <= q.v1 RETURN q.id AS subscription, r.id AS record, q.tags AS required, r.tags AS tags, r.rev AS revision",parser).with_function_registry(registry).with_joins(vec![QueryJoin{id:"CELL".into(),keys:vec![QueryJoinKey{label:"Subscription".into(),property:"cell".into()},QueryJoinKey{label:"Row".into(),property:"cell".into()}]}]).build().await});
        Self {
            state: State::default(),
            rt,
            query,
            time: 0,
        }
    }
    fn metadata(&self, label: &str, id: u64, k: &str) -> ElementMetadata {
        ElementMetadata {
            reference: ElementReference::new("rill", &format!("{label}:{id}:{k}")),
            labels: Arc::new([Arc::from(label)]),
            effective_from: self.time,
        }
    }
    fn row(&self, r: Record, k: &str) -> Element {
        Element::Node {
            metadata: self.metadata("Row", r.id, k),
            properties: ElementPropertyMap::from(
                json!({"cell":k,"id":r.id.to_string(),"x":r.point[0],"y":r.point[1],"z":r.point[2],"value":r.value,"tags":r.tags.to_string(),"rev":r.revision.to_string()}),
            ),
        }
    }
    fn subscription(&self, id: u64, q: Query, k: &str) -> Element {
        Element::Node {
            metadata: self.metadata("Subscription", id, k),
            properties: ElementPropertyMap::from(
                json!({"cell":k,"id":id.to_string(),"x0":q.lower[0],"x1":q.upper[0],"y0":q.lower[1],"y1":q.upper[1],"z0":q.lower[2],"z1":q.upper[2],"v0":q.min_value,"v1":q.max_value,"tags":q.required_tags.to_string()}),
            ),
        }
    }
    fn process(&self, change: SourceChange, pairs: &mut HashSet<(u64, u64)>) {
        fn pair(v: &QueryVariables) -> (u64, u64) {
            let read = |k: &str| match &v[k] {
                VariableValue::String(s) => s.parse().unwrap(),
                _ => panic!("Drasi u64 string projection"),
            };
            (read("subscription"), read("record"))
        }
        for diff in self
            .rt
            .block_on(self.query.process_source_change(change))
            .unwrap()
        {
            match diff {
                QueryPartEvaluationContext::Adding { after, .. } => {
                    pairs.insert(pair(&after));
                }
                QueryPartEvaluationContext::Removing { before, .. } => {
                    pairs.insert(pair(&before));
                }
                QueryPartEvaluationContext::Updating { before, after, .. } => {
                    pairs.insert(pair(&before));
                    pairs.insert(pair(&after));
                }
                QueryPartEvaluationContext::Noop => {}
                _ => panic!("unexpected Drasi aggregation"),
            }
        }
    }
}
impl Backend for Native {
    fn apply(&mut self, b: &Batch) -> Result<Vec<Event>, Error> {
        let p = self.state.prepare(b)?;
        if p.records.is_empty() && p.queries.is_empty() {
            return Ok(Vec::new());
        }
        self.time += 1;
        let mut pairs = HashSet::new();
        for &(_, old, new) in &p.records {
            let oldkeys: HashSet<_> = old
                .map(|r| vec![key(Some(cell(r.point, 64))), key(None)])
                .unwrap_or_default()
                .into_iter()
                .collect();
            let newkeys: HashSet<_> = new
                .map(|r| vec![key(Some(cell(r.point, 64))), key(None)])
                .unwrap_or_default()
                .into_iter()
                .collect();
            for k in oldkeys.difference(&newkeys) {
                self.process(
                    SourceChange::Delete {
                        metadata: self.metadata("Row", old.unwrap().id, k),
                    },
                    &mut pairs,
                );
            }
            if let Some(r) = new {
                for k in &newkeys {
                    let element = self.row(r, k);
                    self.process(
                        if oldkeys.contains(k) {
                            SourceChange::Update { element }
                        } else {
                            SourceChange::Insert { element }
                        },
                        &mut pairs,
                    );
                }
            }
        }
        for &(id, old, new) in &p.queries {
            let oldkeys: HashSet<_> = old.map(keys).unwrap_or_default().into_iter().collect();
            let newkeys: HashSet<_> = new.map(keys).unwrap_or_default().into_iter().collect();
            for k in oldkeys.difference(&newkeys) {
                self.process(
                    SourceChange::Delete {
                        metadata: self.metadata("Subscription", id, k),
                    },
                    &mut pairs,
                );
            }
            if let Some(q) = new {
                for k in &newkeys {
                    let element = self.subscription(id, q, k);
                    self.process(
                        if oldkeys.contains(k) {
                            SourceChange::Update { element }
                        } else {
                            SourceChange::Insert { element }
                        },
                        &mut pairs,
                    );
                }
            }
        }
        let mut old = HashMap::with_capacity(pairs.len());
        for &(s, id) in &pairs {
            old.insert(
                (s, id),
                self.state.queries.get(&s).and_then(|q| {
                    self.state
                        .records
                        .get(&id)
                        .copied()
                        .filter(|r| q.matches(r))
                }),
            );
        }
        self.state.commit(&p);
        let mut out = Vec::new();
        for (s, id) in pairs {
            let new = self.state.queries.get(&s).and_then(|q| {
                self.state
                    .records
                    .get(&id)
                    .copied()
                    .filter(|r| q.matches(r))
            });
            emit(s, old[&(s, id)], new, &mut out);
        }
        Ok(out)
    }
}
