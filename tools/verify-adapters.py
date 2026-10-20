#!/usr/bin/env python3
import json,subprocess,hashlib,datetime,sys
from pathlib import Path
binary=Path(sys.argv[1] if len(sys.argv)>1 else 'build/target/release/rill-subscriptions-benchmarks').resolve()
names=json.loads(subprocess.check_output([str(binary),'--list']));cases=json.loads(subprocess.check_output([str(binary),'--cases']))
report={'binarySha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'startedUTC':datetime.datetime.now(datetime.timezone.utc).isoformat(),'scope':'All compiled adapters,128 records129 subscriptions,8 atomic batches each; complete vectors against full reconstruction and change-driven oracle. Separate target-size proofs are part of performance runs.','status':'RUNNING','runs':[]}
for name in names:
 for case in cases:
  p=subprocess.run([str(binary),'--backend',name,'--case',case,'--records','128','--subscriptions','129','--steps','8','--verify-only'],capture_output=True,text=True)
  result=None; logs=[]
  for line in p.stdout.splitlines():
   try:
    value=json.loads(line)
   except json.JSONDecodeError:
    logs.append(line); continue
   if isinstance(value,dict) and value.get('backend')==name and value.get('case')==case: result=value
   else: logs.append(line)
  code=p.returncode or (0 if result is not None else 1)
  report['runs'].append({'backend':name,'case':case,'exit':code,'result':result,'upstream_stdout':logs,'error':p.stderr if code else None})
  Path('results/adapter-correctness.json').write_text(json.dumps(report,indent=2)+'\n')
 print(name,'PASS' if all(r['exit']==0 for r in report['runs'] if r['backend']==name) else 'FAIL',flush=True)
report['status']='PASS' if all(r['exit']==0 for r in report['runs']) else 'FAIL';Path('results/adapter-correctness.json').write_text(json.dumps(report,indent=2)+'\n');sys.exit(report['status']!='PASS')
