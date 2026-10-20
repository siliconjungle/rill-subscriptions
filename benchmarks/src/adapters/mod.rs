use crate::model::*;
#[cfg(any(feature="datafrog",feature="ascent",feature="crepe"))] mod relations;
#[cfg(feature="differential_dataflow")] mod differential;
pub fn names()->Vec<String>{let mut out=Vec::new();
 #[cfg(feature="rstar")] {out.extend(["native-rstar","native-rstar-shared"].map(String::from));}
 #[cfg(feature="datafrog")] out.push("native-datafrog".into());
 #[cfg(feature="ascent")] out.push("native-ascent".into());
 #[cfg(feature="crepe")] out.push("native-crepe".into());
 #[cfg(feature="differential_dataflow")] out.push("native-differential".into());
 out
}
pub fn make(name:&str)->Box<dyn Backend>{match name {
 #[cfg(feature="rstar")] "native-rstar"=>Box::new(Recompute::new(rstar_adapter::Index::default(),false)),
 #[cfg(feature="rstar")] "native-rstar-shared"=>Box::new(Recompute::new(rstar_adapter::Index::default(),true)),
 #[cfg(feature="datafrog")] "native-datafrog"=>Box::new(Recompute::new(relations::Relations::new(0),true)),
 #[cfg(feature="ascent")] "native-ascent"=>Box::new(Recompute::new(relations::Relations::new(1),true)),
 #[cfg(feature="crepe")] "native-crepe"=>Box::new(Recompute::new(relations::Relations::new(2),true)),
 #[cfg(feature="differential_dataflow")] "native-differential"=>Box::new(differential::Differential::new()),
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
