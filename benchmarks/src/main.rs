mod model;
mod shared;
mod scenarios;
mod adapters;
use model::*;
use rill_subscriptions::*;
use serde::{Serialize,Deserialize};
use std::time::Instant;
use std::hint::black_box;
fn names()->Vec<String>{
 let mut n=vec!["rill-shared-grid-64".into(),"delta-scan".into(),"delta-scan-shared".into(),"full-scan".into(),"full-scan-shared".into()];
 for size in [16,32,64,128,256,512] {if size!=64{n.push(format!("rill-shared-grid-{size}"));}n.push(format!("grid-requery-{size}"));n.push(format!("grid-requery-shared-{size}"));}
 n.extend(adapters::names());
 #[cfg(feature="differential_dataflow")] n.push("native-differential-shared".into());
 n
}
fn make(name:&str)->Box<dyn Backend>{
 #[cfg(feature="differential_dataflow")] if name=="native-differential-shared"{return Box::new(shared::Shared::new(adapters::make("native-differential")));}
 if let Some(size)=name.strip_prefix("rill-shared-grid-"){return Box::new(Library(Engine::new(Options{cell_size:size.parse().unwrap(),max_query_cells:512}).unwrap()));}
 if let Some(size)=name.strip_prefix("grid-requery-shared-"){return Box::new(Recompute::new(Grid::new(size.parse().unwrap()),true));}
 if let Some(size)=name.strip_prefix("grid-requery-"){return Box::new(Recompute::new(Grid::new(size.parse().unwrap()),false));}
 match name {"delta-scan"=>Box::new(DeltaScan::new(false)),"delta-scan-shared"=>Box::new(DeltaScan::new(true)),"full-scan"=>Box::new(Recompute::new(Scan,false)),"full-scan-shared"=>Box::new(Recompute::new(Scan,true)),_=>adapters::make(name)}
}
fn mix(mut x:u64)->u64{x^=x>>30;x=x.wrapping_mul(0xbf58476d1ce4e5b9);x^=x>>27;x=x.wrapping_mul(0x94d049bb133111eb);x^(x>>31)}
#[derive(Clone,Copy,Debug,Eq,PartialEq,Serialize,Deserialize)]
struct Digest{rows:usize,xor:u64,sum:u64}
fn digest(events:&[Event])->Digest{
 let mut d=Digest{rows:events.len(),xor:0,sum:0};
 for e in events {let k=match e.kind{EventKind::Added=>1,EventKind::Removed=>2,EventKind::Updated=>3};let x=mix(e.subscription)^mix(e.record.wrapping_add(17)).rotate_left(19)^mix(e.revision).rotate_left(33)^mix(k);d.xor^=x;d.sum=d.sum.wrapping_add(x);}
 black_box(d)
}
fn exact(mut a:Vec<Event>,mut b:Vec<Event>,context:&str){a.sort_unstable();b.sort_unstable();assert_eq!(a,b,"{context}");}
fn verify(name:&str,scene:&scenarios::Scenario)->Vec<Digest>{
 let mut oracle=DeltaScan::new(false);let mut engine=make(name);let expected=oracle.apply(&scene.initial).unwrap();let got=engine.apply(&scene.initial).unwrap();let mut ds=vec![digest(&expected)];exact(got,expected,"initial complete relation");
 for (i,b) in scene.batches.iter().enumerate(){let expected=oracle.apply(b).unwrap();let got=engine.apply(b).unwrap();ds.push(digest(&expected));exact(got,expected,&format!("{} frame{i}",scene.name));}ds
}
fn quantile(v:&[f64],q:f64)->f64{let mut v=v.to_vec();v.sort_by(f64::total_cmp);v[((v.len()as f64*q).ceil()as usize).saturating_sub(1).min(v.len()-1)]}
#[derive(Serialize,Deserialize)]
struct Proof{schema:u32,binary_fingerprint:u64,backend:String,scenario:String,seed:u64,records:usize,subscriptions:usize,steps:usize,expected:Vec<Digest>}
fn binary_fingerprint()->u64{std::fs::read(std::env::current_exe().unwrap()).unwrap().into_iter().fold(0xcbf29ce484222325,|h,b|(h^u64::from(b)).wrapping_mul(0x100000001b3))}
#[derive(Serialize)]
struct Round{setup_ms:f64,setup_rows:usize,batch_ms:Vec<f64>,rows:Vec<usize>,fingerprint_xor:u64}
#[derive(Serialize)]
struct Report{schema:u32,backend:String,scenario:String,seed:u64,records:usize,subscriptions:usize,steps:usize,rounds:Vec<Round>,setup_median_ms:f64,batch_p50_ms:f64,batch_p95_ms:f64,batch_p99_ms:f64,batch_samples:usize,exact_full_vectors_verified:bool,verification: &'static str,scope:&'static str}
fn main(){
 let args:Vec<String>=std::env::args().collect();let opt=|key:&str,default:&str|args.iter().position(|x|x==key).map(|i|args[i+1].clone()).unwrap_or_else(||default.into());
 if args.iter().any(|x|x=="--list"){println!("{}",serde_json::to_string(&names()).unwrap());return;}
 if args.iter().any(|x|x=="--cases"){println!("{}",serde_json::to_string(scenarios::names()).unwrap());return;}
 let name=opt("--backend","rill-shared-grid-64");let case=opt("--case","sparse-1pct");let records=opt("--records","20000").parse().unwrap();let subscriptions=opt("--subscriptions","20000").parse().unwrap();let steps:usize=opt("--steps","8").parse().unwrap();let count:usize=opt("--rounds","3").parse().unwrap();let seed=opt("--seed","71923").parse().unwrap();assert!(records>0&&subscriptions>0&&steps>0&&count>0);
 // Small traces also cross-check the change-driven oracle against a separate
 // full relation reconstruction before checking actual target-size full vectors.
 let small=scenarios::generate(&case,128,129,8,seed);let small_expected=verify("full-scan",&small);let small_got=verify(&name,&small);assert_eq!(small_expected,small_got);
 let scene=scenarios::generate(&case,records,subscriptions,steps,seed);let proof_in=opt("--proof-in","");let proof_out=opt("--proof-out","");
 let verification=if proof_in.is_empty(){"Complete target-size event vectors verified in this process"}else{"Previously verified complete target-size vectors; proof bound to exact executable, backend, dimensions, seed and trace length. Timed batches also consume/check all output digests."};
 let expected=if proof_in.is_empty(){verify(&name,&scene)}else{
  let proof:Proof=serde_json::from_slice(&std::fs::read(&proof_in).unwrap()).unwrap();
  assert_eq!((proof.schema,proof.binary_fingerprint,proof.backend.as_str(),proof.scenario.as_str(),proof.seed,proof.records,proof.subscriptions,proof.steps),(1,binary_fingerprint(),name.as_str(),case.as_str(),seed,records,subscriptions,steps),"proof must match exact executable and full scenario");assert_eq!(proof.expected.len(),steps+1);proof.expected
 };
 if !proof_out.is_empty(){let proof=Proof{schema:1,binary_fingerprint:binary_fingerprint(),backend:name.clone(),scenario:case.clone(),seed,records,subscriptions,steps,expected:expected.clone()};std::fs::write(proof_out,serde_json::to_vec_pretty(&proof).unwrap()).unwrap();}
 if args.iter().any(|x|x=="--verify-only"){println!("{}",serde_json::json!({"backend":name,"case":case,"records":records,"subscriptions":subscriptions,"exact":true,"frames":steps}));return;}
 let mut rounds=Vec::new();
 for _ in 0..count {
  let start=Instant::now();let mut engine=make(&name);let init=engine.apply(&scene.initial).unwrap();let first=digest(&init);drop(init);let setup_ms=start.elapsed().as_secs_f64()*1000.;assert_eq!(first,expected[0]);let mut round=Round{setup_ms,setup_rows:first.rows,batch_ms:Vec::new(),rows:Vec::new(),fingerprint_xor:0};
  for (i,b) in scene.batches.iter().enumerate(){let start=Instant::now();let events=engine.apply(black_box(b)).unwrap();let d=digest(&events);drop(events);let elapsed=start.elapsed().as_secs_f64()*1000.;assert_eq!(d,expected[i+1],"timed output mismatch");round.batch_ms.push(elapsed);round.rows.push(d.rows);round.fingerprint_xor^=d.xor;}
  rounds.push(round);
 }
 let batches:Vec<_>=rounds.iter().flat_map(|r|r.batch_ms.iter().copied()).collect();let setups:Vec<_>=rounds.iter().map(|r|r.setup_ms).collect();
 let report=Report{schema:1,backend:name,scenario:case,seed,records,subscriptions,steps,setup_median_ms:quantile(&setups,0.5),batch_p50_ms:quantile(&batches,0.5),batch_p95_ms:quantile(&batches,0.95),batch_p99_ms:quantile(&batches,0.99),batch_samples:batches.len(),rounds,exact_full_vectors_verified:true,verification,scope:"Exact in-process subscriptions, no network clients, renderer or whole-server capacity. Full input normalization, matching, complete event materialization/consumption/drop included. Setup separate. Small sample screen percentiles are descriptive, not sustained tail-latency qualification."};println!("{}",serde_json::to_string(&report).unwrap());
}
