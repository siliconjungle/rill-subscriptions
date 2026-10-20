#!/usr/bin/env python3
"""Serial fresh-process screening; every timing also verifies exact target-size output."""
import argparse,datetime,hashlib,json,platform,random,subprocess
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--binary',default='build/target/release/rill-subscriptions-benchmarks');p.add_argument('--output',default='results/screen-20k.json');p.add_argument('--backends');p.add_argument('--cases');p.add_argument('--steps',type=int,default=4);p.add_argument('--records',type=int,default=20000);a=p.parse_args()
binary=Path(a.binary).resolve();names=a.backends.split(',') if a.backends else json.loads(subprocess.check_output([str(binary),'--list']));cases=a.cases.split(',') if a.cases else json.loads(subprocess.check_output([str(binary),'--cases']));jobs=[(n,c) for n in names for c in cases];random.Random(71923).shuffle(jobs)
report={'schema':1,'stage':'screen, not acceptance','status':'RUNNING','startedUTC':datetime.datetime.now(datetime.timezone.utc).isoformat(),'sourceCommit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'binarySha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'lockSha256':hashlib.sha256(Path('Cargo.lock').read_bytes()).hexdigest(),'compiler':subprocess.check_output(['rustup','run','stable','rustc','-Vv'],text=True),'host':platform.platform(),'rssScope':'macOS time maximum resident set size includes full-vector oracle verification, scenario and backend, not just kernel live state.','runs':[]}
out=Path(a.output);out.parent.mkdir(parents=True,exist_ok=True)
proof_dir=out.parent/(out.stem+'-proofs');proof_dir.mkdir(exist_ok=True)
for n,c in jobs:
 cmd=[str(binary),'--backend',n,'--case',c,'--records',str(a.records),'--subscriptions','20000','--steps',str(a.steps),'--rounds','1','--seed','71923']
 proof=proof_dir/(n+'--'+c+'.json');cmd.extend(['--proof-out',str(proof)])
 run=subprocess.run(['/usr/bin/time','-l',*cmd],capture_output=True,text=True)
 rss=next((int(l.split()[0]) for l in run.stderr.splitlines() if 'maximum resident set size' in l),None)
 item={'backend':n,'case':c,'proof':str(proof),'exit':run.returncode,'maximumResidentBytes':rss,'result':json.loads(run.stdout) if run.returncode==0 else None,'diagnostics':run.stderr};report['runs'].append(item);out.write_text(json.dumps(report,indent=2)+'\n')
 print(n,c,('%.4f ms'%item['result']['batch_p50_ms']) if run.returncode==0 else 'FAIL',flush=True)
report['status']='PASS' if all(r['exit']==0 for r in report['runs']) else 'FAIL';report['completedUTC']=datetime.datetime.now(datetime.timezone.utc).isoformat();out.write_text(json.dumps(report,indent=2)+'\n')
