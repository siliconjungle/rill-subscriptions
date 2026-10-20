use rill_subscriptions::*;
#[derive(Clone)]
pub struct Scenario {pub name:String,pub initial:Batch,pub batches:Vec<Batch>}
struct Rng(u64);
impl Rng {fn next(&mut self)->u64{self.0=self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);self.0>>32}fn coord(&mut self,span:i32)->i32{(self.next()%span as u64)as i32-span/2}}
fn rid(i:usize)->u64{(i as u64).wrapping_mul(0x9e3779b97f4a7c15).wrapping_add(1<<63)}
fn sid(i:usize)->u64{(i as u64).wrapping_mul(0xd1342543de82ef95).wrapping_add(1<<62)}
pub fn names()->&'static[&'static str]{&["sparse-0.1pct","sparse-1pct","sparse-10pct","sparse-100pct","shared-identical","clustered-overlap","moving-queries","mixed-record-query","subscription-lifecycle","insert-delete-reuse","payload-only","noop","large-box-fallback","tag-value-selective","dense-output"]}
pub fn generate(name:&str,records:usize,subscriptions:usize,steps:usize,seed:u64)->Scenario{
 assert!(names().contains(&name));let mut rng=Rng(seed);let mut rs=Vec::new();let mut qs=Vec::new();let span=if name=="clustered-overlap"{1024}else{16384};
 for i in 0..records {rs.push(Record{id:rid(i),point:[rng.coord(span),rng.coord(128),rng.coord(span)],tags:rng.next()%16,value:rng.coord(100)as i64,revision:0});}
 // Broad spatial predicates remain selective by value; output is complete, never capped.
 if name=="large-box-fallback" {for (i,r) in rs.iter_mut().enumerate(){r.value=i as i64-records as i64/2;}}
 if name=="dense-output" {for (i,r) in rs.iter_mut().enumerate(){r.point=if i<16{[0;3]}else{[10000+(i%100)as i32,0,10000]};}}
 for i in 0..subscriptions {let p=rs[i%rs.len()].point;let radius=if name=="clustered-overlap"{32}else{16};
  let mut q=Query::bounds([p[0]-radius,-128,p[2]-radius],[p[0]+radius,128,p[2]+radius]);
  if name=="shared-identical" {let p=rs[i%16.min(rs.len())].point;q=Query::bounds([p[0]-32,-128,p[2]-32],[p[0]+32,128,p[2]+32]);}
  if name=="dense-output"{q=Query::bounds([-1;3],[1;3]);}
  if name=="large-box-fallback" {q=Query{required_tags:1<<(i%4),min_value:(i%8)as i64-4,max_value:(i%8)as i64+4,..Query::all()};}
  if name=="tag-value-selective"{q.required_tags=1<<(i%4);q.min_value=-10;q.max_value=10;}
  qs.push(q);
 }
 let initial=Batch{records:rs.iter().copied().map(RecordChange::Put).collect(),subscriptions:qs.iter().enumerate().map(|(i,q)|SubscriptionChange::Put(sid(i),*q)).collect()};let mut batches=Vec::new();let mut active=vec![true;subscriptions];
 for step in 0..steps {let mut b=Batch::default();let divisor=match name{"sparse-0.1pct"=>1000,"sparse-10pct"=>10,"sparse-100pct"=>1,_=>100};let mut changes=(records/divisor).max(1);
  if name=="moving-queries"||name=="noop"{changes=0;}if name=="dense-output"{changes=1;}
  for j in 0..changes {let i=if name=="dense-output"{step%16.min(records)}else if name=="insert-delete-reuse"{(j+(step/2)*changes)%records}else{(j+step*changes)%records};let old=rs[i];
   if name=="insert-delete-reuse" && step%2==0 {b.records.push(RecordChange::Remove(old.id));}
   else {let mut r=old;if name!="payload-only"&&name!="dense-output"{r.point[0]=r.point[0].saturating_add(if step%2==0{23}else{-19});r.point[2]=r.point[2].saturating_add(7);}r.revision+=1;
    if name=="tag-value-selective"{r.tags^=1<<(j%4);r.value+=1;}
    rs[i]=r;b.records.push(RecordChange::Put(r));}
  }
  if name=="moving-queries"||name=="mixed-record-query" {for j in 0..(subscriptions/100).max(1){let i=(j+step*17)%subscriptions;qs[i].lower[0]+=21;qs[i].upper[0]+=21;b.subscriptions.push(SubscriptionChange::Put(sid(i),qs[i]));}}
  if name=="subscription-lifecycle" {for j in 0..(subscriptions/100).max(1){let i=j%subscriptions;if active[i]{b.subscriptions.push(SubscriptionChange::Remove(sid(i)));}else{qs[i].lower[0]+=7;qs[i].upper[0]+=7;qs[i].required_tags=if step%4==1{1}else{0};b.subscriptions.push(SubscriptionChange::Put(sid(i),qs[i]));}active[i]=!active[i];}}
  if name=="noop"{b.records.extend(rs.iter().take(20).copied().map(RecordChange::Put));b.subscriptions.extend(qs.iter().take(20).enumerate().map(|(i,q)|SubscriptionChange::Put(sid(i),*q)));}
  if name=="mixed-record-query"&&step%4==3{let i=step%records;b.records.push(RecordChange::Remove(rs[i].id));b.records.push(RecordChange::Put(rs[i]));let q=step%subscriptions;b.subscriptions.push(SubscriptionChange::Remove(sid(q)));b.subscriptions.push(SubscriptionChange::Put(sid(q),qs[q]));}
  batches.push(b);
 }
 Scenario{name:name.into(),initial,batches}
}
