// Actual differential-dataflow0.25 and timely0.31, with maintained spatial
// equijoins and signed input/output changes. No per-batch relation reconstruction.
use crate::model::*;
use rill_subscriptions::*;
use differential_dataflow::input::InputSession;
use std::{cell::RefCell,rc::Rc,collections::HashMap};
type R=(u64,[i32;3],u64,i64,u64);
type Q=([i32;3],[i32;3],u64,i64,i64);
fn pack(r:Record)->R{(r.id,r.point,r.tags,r.value,r.revision)}
fn unpack(r:R)->Record{Record{id:r.0,point:r.1,tags:r.2,value:r.3,revision:r.4}}
fn query(q:Query)->Q{(q.lower,q.upper,q.required_tags,q.min_value,q.max_value)}
fn matches(q:Q,r:R)->bool{Query{lower:q.0,upper:q.1,required_tags:q.2,min_value:q.3,max_value:q.4}.matches(&unpack(r))}
const GLOBAL:Cell=[i32::MIN;3];
pub struct Differential {
 worker:timely::worker::Worker,
 records:InputSession<u64,(Cell,R),isize>,
 queries:InputSession<u64,(Cell,(u64,Q)),isize>,
 probe:timely::dataflow::operators::probe::Handle<u64>,
 output:Rc<RefCell<Vec<((u64,R),u64,isize)>>>,
 state:State,time:u64,
}
impl Differential {
 pub fn new()->Self{
  let allocator=timely::communication::Allocator::Thread(Default::default());
  let mut worker=timely::worker::Worker::new(timely::worker::Config::default(),allocator,None);
  let mut records=InputSession::new();let mut queries=InputSession::new();let output=Rc::new(RefCell::new(Vec::new()));let capture=output.clone();
  let probe=worker.dataflow::<u64,_,_>(|scope|{
   let rs=records.to_collection(scope);let qs=queries.to_collection(scope);
   let results=qs.join_map(rs,|_,&(id,q),&r|(id,q,r)).filter(|&(_,q,r)|matches(q,r)).map(|(id,_,r)|(id,r));
   let (probe,_)=results.inspect(move |x|capture.borrow_mut().push(x.clone())).probe();probe
  });
  Self{worker,records,queries,probe,output,state:State::default(),time:0}
 }
}
impl Backend for Differential {
 fn apply(&mut self,b:&Batch)->Result<Vec<Event>,Error>{
  let p=self.state.prepare(b)?;if p.records.is_empty()&&p.queries.is_empty(){return Ok(Vec::new());}
  for &(_,old,new)in &p.records{for (r,diff)in [(old,-1),(new,1)]{if let Some(r)=r{self.records.update((cell(r.point,64),pack(r)),diff);self.records.update((GLOBAL,pack(r)),diff);}}}
  for &(id,old,new)in &p.queries{for(q,diff)in [(old,-1),(new,1)]{if let Some(q)=q{for c in cells(q,64,512).unwrap_or_else(||vec![GLOBAL]){self.queries.update((c,(id,query(q))),diff);}}}}
  self.time+=1;self.records.advance_to(self.time);self.queries.advance_to(self.time);self.records.flush();self.queries.flush();
  while self.probe.less_than(&self.time){self.worker.step();}
  let mut changes:HashMap<(u64,u64),HashMap<R,isize>>=HashMap::new();
  for ((id,r),_,weight)in self.output.borrow_mut().drain(..){*changes.entry((id,r.0)).or_default().entry(r).or_default()+=weight;}
  let mut events=Vec::new();
  for ((subscription,record),rows)in changes{let mut old=None;let mut new=None;
   for(r,w)in rows{match w{0=>{},-1=>{assert!(old.replace(r).is_none());},1=>{assert!(new.replace(r).is_none());},_=>panic!("non-set native multiplicity {w}")}}
   match(old,new){(None,None)=>{},(Some(r),None)=>events.push(Event{subscription,record,kind:EventKind::Removed,revision:r.4}), (None,Some(r))=>events.push(Event{subscription,record,kind:EventKind::Added,revision:r.4}), (Some(a),Some(r))=>{if a!=r{events.push(Event{subscription,record,kind:EventKind::Updated,revision:r.4});}}}
  }
  self.state.commit(&p);Ok(events)
 }
}
