use crate::model::*;
use rill_subscriptions::*;
use std::collections::{BTreeSet, HashMap};
const GLOBAL: Cell = [i32::MIN; 3];
pub fn input(state: &State) -> (Vec<(Cell, Record)>, Vec<(Cell, Query)>) {
    let records = state
        .records
        .values()
        .flat_map(|r| [(cell(r.point, 64), *r), (GLOBAL, *r)])
        .collect();
    let unique: BTreeSet<_> = state.queries.values().copied().collect();
    let queries = unique
        .into_iter()
        .flat_map(|q| {
            cells(q, 64, 512)
                .unwrap_or_else(|| vec![GLOBAL])
                .into_iter()
                .map(move |c| (c, q))
        })
        .collect();
    (records, queries)
}
#[derive(Default)]
pub struct Relations {
    pub kind: u8,
    results: HashMap<Query, Vec<Record>>,
}
impl Relations {
    pub fn new(kind: u8) -> Self {
        Self {
            kind,
            results: HashMap::new(),
        }
    }
}
impl Evaluator for Relations {
    fn update(&mut self, s: &State, _: &Prepared) {
        let (records, queries) = input(s);
        self.results.clear();
        let output: Vec<(Query, Record)> = match self.kind {
            #[cfg(feature = "datafrog")]
            0 => {
                let records = datafrog::Relation::from_vec(records);
                let queries = datafrog::Relation::from_vec(queries);
                let out = datafrog::Relation::from_join(&queries, &records, |_, q, r| {
                    q.matches(r).then_some((*q, *r))
                });
                out.elements.into_iter().flatten().collect()
            }
            #[cfg(feature = "ascent")]
            1 => {
                let mut program = ascent_program::AscentProgram::default();
                program.rows = records;
                program.queries = queries;
                program.run();
                program.output
            }
            #[cfg(feature = "crepe")]
            2 => crepe_program::evaluate(records, queries),
            _ => panic!("feature-disabled native relation adapter"),
        };
        for (q, r) in output {
            self.results.entry(q).or_default().push(r);
        }
    }
    fn query(&mut self, _: &State, q: Query, out: &mut Vec<Record>) {
        if let Some(rows) = self.results.get(&q) {
            out.extend_from_slice(rows);
        }
    }
}
#[cfg(feature = "ascent")]
mod ascent_program {
    use super::*;
    ascent::ascent! {
     pub struct AscentProgram;
     relation rows(Cell,Record);
     relation queries(Cell,Query);
     relation output(Query,Record);
     output(*q,*r) <-- queries(c,q),rows(c,r),if q.matches(r);
    }
}
#[cfg(feature = "crepe")]
mod crepe_program {
    use super::*;
    pub fn evaluate(
        records: Vec<(Cell, Record)>,
        queries: Vec<(Cell, Query)>,
    ) -> Vec<(Query, Record)> {
        let mut runtime = Crepe::new();
        runtime.extend(records.into_iter().map(|(c, r)| Rows(c, r)));
        runtime.extend(queries.into_iter().map(|(c, q)| Queries(c, q)));
        let (rows,) = runtime.run();
        rows.into_iter().map(|r| (r.0, r.1)).collect()
    }

    crepe::crepe! {
     @input
     pub struct Rows(Cell,Record);
     @input
     pub struct Queries(Cell,Query);
     @output
     pub struct Output(Query,Record);
     Output(q,r) <- Queries(c,q), Rows(c,r), (q.matches(&r));
    }
}
