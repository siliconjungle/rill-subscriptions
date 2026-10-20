//! Change-driven upstream R-trees in both directions. No full subscriber polling:
//! record deltas query the subscription-box tree; query changes query the point
//! tree. Both sides support incremental remove/insert and bulk initial loading.
use crate::model::*;use rill_subscriptions::*;use rstar::{RTree,RTreeObject,AABB};use std::collections::{HashMap,HashSet};
#[derive(Clone,Copy,PartialEq)]struct Row(Record);
impl RTreeObject for Row{type Envelope=AABB<[i32;3]>;fn envelope(&self)->Self::Envelope{AABB::from_point(self.0.point)}}
#[derive(Clone,Copy,PartialEq)]struct Subscription(u64,Query);
impl RTreeObject for Subscription{type Envelope=AABB<[i32;3]>;fn envelope(&self)->Self::Envelope{AABB::from_corners(self.1.lower,self.1.upper)}}
#[derive(Default)]pub struct Native{state:State,records:RTree<Row>,queries:RTree<Subscription>}
impl Native{
 fn members(&self,q:Query)->HashMap<u64,Record>{self.records.locate_in_envelope_intersecting(AABB::from_corners(q.lower,q.upper)).map(|r|r.0).filter(|r|q.matches(r)).map(|r|(r.id,r)).collect()}
}
impl Backend for Native{fn apply(&mut self,b:&Batch)->Result<Vec<Event>,Error>{
 let p=self.state.prepare(b)?;if p.records.is_empty()&&p.queries.is_empty(){return Ok(Vec::new());}let mut out=Vec::new();let changing:HashSet<_>=p.queries.iter().map(|x|x.0).collect();
 let previous:Vec<_>=p.queries.iter().map(|&(id,old,_)|(id,old.map(|q|self.members(q)).unwrap_or_default())).collect();let mut affected=HashSet::new();
 for &(_,old,new) in &p.records {affected.clear();for row in [old,new].into_iter().flatten(){affected.extend(self.queries.locate_in_envelope_intersecting(AABB::from_point(row.point)).map(|q|q.0));}
  for &id in &affected{if !changing.contains(&id){let q=self.state.queries[&id];emit(id,old.filter(|r|q.matches(r)),new.filter(|r|q.matches(r)),&mut out);}}
 }
 self.state.commit(&p);
 if p.records.len()>self.state.records.len()/2{self.records=RTree::bulk_load(self.state.records.values().map(|r|Row(*r)).collect());}
 else{for &(_,old,new) in &p.records{if let Some(r)=old{assert!(self.records.remove(&Row(r)).is_some());}if let Some(r)=new{self.records.insert(Row(r));}}}
 if p.queries.len()>self.state.queries.len()/2{self.queries=RTree::bulk_load(self.state.queries.iter().map(|(&id,&q)|Subscription(id,q)).collect());}
 else{for &(id,old,new) in &p.queries{if let Some(q)=old{assert!(self.queries.remove(&Subscription(id,q)).is_some());}if let Some(q)=new{self.queries.insert(Subscription(id,q));}}}
 for (id,mut old) in previous{if let Some(q)=self.state.queries.get(&id){for (_,r) in self.members(*q){emit(id,old.remove(&r.id),Some(r),&mut out);}}for r in old.into_values(){emit(id,Some(r),None,&mut out);}}
 Ok(out)
}}
