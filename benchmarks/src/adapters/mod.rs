use crate::model::*;
#[cfg(feature="rstar")] mod reverse_rstar;
#[cfg(any(feature="datafrog",feature="ascent",feature="crepe"))] mod relations;
#[cfg(feature="differential_dataflow")] mod differential;
#[cfg(any(feature="incremental",feature="reactive_graph",feature="floem_reactive",feature="sycamore_reactive",feature="salsa",feature="futures_signals"))] mod reactive;
pub fn names()->Vec<String>{let mut out=Vec::new();
 #[cfg(feature="rstar")] {out.extend(["native-rstar","native-rstar-shared","native-rstar-delta","native-rstar-delta-shared"].map(String::from));}
 #[cfg(feature="datafrog")] out.push("native-datafrog".into());
 #[cfg(feature="ascent")] out.push("native-ascent".into());
 #[cfg(feature="crepe")] out.push("native-crepe".into());
 #[cfg(feature="differential_dataflow")] out.push("native-differential".into());
 #[cfg(feature="incremental")] out.push("native-incremental-cells".into());
 #[cfg(feature="reactive_graph")] out.push("native-reactive-graph-cells".into());
 #[cfg(feature="floem_reactive")] out.push("native-floem-cells".into());
 #[cfg(feature="sycamore_reactive")] out.push("native-sycamore-cells".into());
 #[cfg(feature="salsa")] out.push("native-salsa-cells".into());
 #[cfg(feature="futures_signals")] out.push("native-futures-signals-cells".into());
 out
}
pub fn make(name:&str)->Box<dyn Backend>{match name {
 #[cfg(feature="rstar")] "native-rstar-delta"=>Box::new(reverse_rstar::Native::default()),
 #[cfg(feature="rstar")] "native-rstar-delta-shared"=>Box::new(crate::shared::Shared::new(Box::new(reverse_rstar::Native::default()))),
 #[cfg(feature="rstar")] "native-rstar"=>Box::new(Recompute::new(rstar_adapter::Index::default(),false)),
 #[cfg(feature="rstar")] "native-rstar-shared"=>Box::new(Recompute::new(rstar_adapter::Index::default(),true)),
 #[cfg(feature="datafrog")] "native-datafrog"=>Box::new(Recompute::new(relations::Relations::new(0),true)),
 #[cfg(feature="ascent")] "native-ascent"=>Box::new(Recompute::new(relations::Relations::new(1),true)),
 #[cfg(feature="crepe")] "native-crepe"=>Box::new(Recompute::new(relations::Relations::new(2),true)),
 #[cfg(feature="differential_dataflow")] "native-differential"=>Box::new(differential::Differential::new()),
 #[cfg(feature="incremental")] "native-incremental-cells"=>Box::new(Recompute::new(reactive::Reactive::new(reactive::incr::Native::new()),true)),
 #[cfg(feature="reactive_graph")] "native-reactive-graph-cells"=>Box::new(Recompute::new(reactive::Reactive::new(reactive::graph::Native),true)),
 #[cfg(feature="floem_reactive")] "native-floem-cells"=>Box::new(Recompute::new(reactive::Reactive::new(reactive::floem::Native),true)),
 #[cfg(feature="sycamore_reactive")] "native-sycamore-cells"=>Box::new(Recompute::new(reactive::Reactive::new(reactive::sycamore::Native::new()),true)),
 #[cfg(feature="salsa")] "native-salsa-cells"=>Box::new(Recompute::new(reactive::Reactive::new(reactive::salsa_native::Native::new()),true)),
 #[cfg(feature="futures_signals")] "native-futures-signals-cells"=>Box::new(Recompute::new(reactive::Reactive::new(reactive::futures::Native),true)),
 _=>panic!("Unknown or feature-disabled backend {name}")}}
#[cfg(feature="rstar")]
mod rstar_adapter {
 use crate::model::*;use rill_subscriptions::*;use rstar::{RTree,RTreeObject,AABB};
 #[derive(Clone,Copy,PartialEq)]struct Row(Record);
 impl RTreeObject for Row {type Envelope=AABB<[i32;3]>;fn envelope(&self)->Self::Envelope{AABB::from_point(self.0.point)}}
 #[derive(Default)]pub struct Index(RTree<Row>);
 impl Evaluator for Index {
  fn update(&mut self,s:&State,p:&Prepared){
   if p.records.len()>s.records.len()/2 {self.0=RTree::bulk_load(s.records.values().map(|r|Row(*r)).collect());}
   else{for (_,old,new)in &p.records{if let Some(r)=old{assert!(self.0.remove(&Row(*r)).is_some());}if let Some(r)=new{self.0.insert(Row(*r));}}}
  }
  fn query(&mut self,_:&State,q:Query,out:&mut Vec<Record>){out.extend(self.0.locate_in_envelope_intersecting(AABB::from_corners(q.lower,q.upper)).map(|r|r.0).filter(|r|q.matches(r)));}
 }
}
