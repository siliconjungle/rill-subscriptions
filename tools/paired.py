#!/usr/bin/env python3
"""Reproducible, resumable serial controls. No process is timed during a build.
Proofs bind full-vector comparisons to the exact executable and complete trace.
Allocation audits use a separately compiled executable, never these timings.
"""
import argparse,datetime,hashlib,json,math,platform,random,statistics,subprocess
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--binary',required=True);p.add_argument('--candidate',default='rill-shared-grid-128');p.add_argument('--competitors',required=True);p.add_argument('--cases',required=True);p.add_argument('--records',default='20000,100000');p.add_argument('--seeds',default='71923,61937');p.add_argument('--steps',type=int,default=4);p.add_argument('--pairs',type=int,default=9);p.add_argument('--output',default='results/paired');a=p.parse_args();binary=Path(a.binary).resolve();out=Path(a.output);out.mkdir(parents=True,exist_ok=True);proofs=out/'proofs';proofs.mkdir(exist_ok=True)
sha=hashlib.sha256(binary.read_bytes()).hexdigest();names=json.loads(subprocess.check_output([str(binary),'--list']));competitors=a.competitors.split(',') if a.competitors!='all-native' else [n for n in names if n.startswith('native-')];assert a.candidate in names;assert all(n in names and n!=a.candidate for n in competitors)
settings={'schema':1,'binarySha256':sha,'sourceCommit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'host':platform.platform(),'candidate':a.candidate,'competitors':competitors,'cases':a.cases.split(','),'records':[int(n) for n in a.records.split(',')],'seeds':[int(s) for s in a.seeds.split(',')],'steps':a.steps,'pairs':a.pairs,'scope':'Complete output, serial alternating fresh processes; setup and update reported separately. 4-step pairs do not qualify sustained p99. macOS RSS excludes the separate oracle proof process, includes scenario and verification harness.'}
config=out/'configuration.json'
if config.exists():assert json.loads(config.read_text())==settings,'Resume only exact binary/configuration'
else:config.write_text(json.dumps(settings,indent=2)+'\n')
journal=out/'runs.ndjson';runs=[json.loads(l) for l in journal.read_text().splitlines()] if journal.exists() else [];done={r['key'] for r in runs}
def invocation(name,case,n,seed,extra):return [str(binary),'--backend',name,'--case',case,'--records',str(n),'--subscriptions','20000','--steps',str(a.steps),'--rounds','1','--seed',str(seed),*extra]
def proof(name,case,n,seed):
 path=proofs/f'{name}--{case}--{n}--{seed}.json'
 if not path.exists():
  result=subprocess.run(invocation(name,case,n,seed,['--verify-only','--proof-out',str(path)]),capture_output=True,text=True)
  if result.returncode:raise RuntimeError(f'Exact proof failed: {name} {case} {n} {seed}: {result.stderr}')
 return path
jobs=[(c,n,s,other) for c in settings['cases'] for n in settings['records'] for s in settings['seeds'] for other in competitors];random.Random(61937).shuffle(jobs)
for case,n,seed,other in jobs:
 for name in [a.candidate,other]:proof(name,case,n,seed)
 for pair in range(a.pairs):
  order=[a.candidate,other] if pair%2==0 else [other,a.candidate]
  for name in order:
   key=f'{case}/{n}/{seed}/{other}/{pair}/{name}'
   if key in done:continue
   path=proof(name,case,n,seed);result=subprocess.run(['/usr/bin/time','-l',*invocation(name,case,n,seed,['--proof-in',str(path)])],capture_output=True,text=True);rss=next((int(l.split()[0]) for l in result.stderr.splitlines() if 'maximum resident set size' in l),None)
   row={'key':key,'comparator':other,'pair':pair,'backend':name,'case':case,'records':n,'seed':seed,'exit':result.returncode,'maximumResidentBytes':rss,'result':json.loads(result.stdout) if result.returncode==0 else None,'diagnostics':result.stderr,'completedUTC':datetime.datetime.now(datetime.timezone.utc).isoformat()}
   with journal.open('a') as f:f.write(json.dumps(row)+'\n')
   done.add(key);runs.append(row)
   if result.returncode:raise RuntimeError(f'Timed run failed: {key}: {result.stderr}')
 print(case,n,seed,other,'pairs complete',flush=True)
summary=[]
for case,n,seed,other in jobs:
 data=[r for r in runs if (r['case'],r['records'],r['seed'],r['comparator'])==(case,n,seed,other)];by={}
 for r in data:by.setdefault(r['pair'],{})[r['backend']]=r
 comparisons={}
 for metric in ['setup_median_ms','batch_p50_ms','batch_p95_ms','batch_p99_ms']:
  ratios=[p[other]['result'][metric]/p[a.candidate]['result'][metric] for p in by.values()];logs=[math.log(r) for r in ratios];rng=random.Random(71923);means=sorted(statistics.mean(rng.choices(logs,k=len(logs))) for _ in range(5000));comparisons[metric]={'competitor_over_candidate_geomean':math.exp(statistics.mean(logs)),'paired_bootstrap_95pct':[math.exp(means[124]),math.exp(means[4874])],'candidate_strictly_faster_lower_bound':math.exp(means[124])>1}
 summary.append({'case':case,'records':n,'seed':seed,'competitor':other,'pairs':len(by),'comparison':comparisons})
(out/'summary.json').write_text(json.dumps({'status':'MEASURED; inventory and tail/memory acceptance gates separate','settings':settings,'comparisons':summary},indent=2)+'\n')
