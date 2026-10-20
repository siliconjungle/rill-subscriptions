#!/usr/bin/env python3
"""Cache only primary crates.io metadata, not whole external repositories."""
import json,urllib.request,urllib.error,time,datetime,concurrent.futures
from pathlib import Path
names='differential-dataflow timely dbsp datafrog ascent crepe salsa adapton incremental incremental-map depends incr-compute incr-concurrent salvia recompute anchors reflex reactive_graph reactive_stores leptos_reactive sycamore-reactive floem_reactive reactive-signals futures-signals rxrust carboxyl sodium-rust frp frappe reactives signals concoct lazily dioxus-signals xilem_core drasi-core hydroflow dfir_rs hydro_lang datafusion polonius-engine rstar kiddo kdtree kd-tree roaring croaring bitvec fixedbitset bit-set hibitset rangemap intervaltree segment-tree sqlparser zenoh dust_dds'.split()
def fetch(name):
 import subprocess,re
 n=name.lower();key=('1/'+n if len(n)==1 else '2/'+n if len(n)==2 else '3/'+n[0]+'/'+n if len(n)==3 else n[:2]+'/'+n[2:4]+'/'+n)
 url='https://index.crates.io/'+key
 run=subprocess.run(['curl','-fsS','--retry','2','--max-time','25',url],capture_output=True,text=True)
 if run.returncode:
  return {'name':name,'source':url,'status':'NOT_FOUND_ON_REGISTRY' if '404' in run.stderr else 'DISCOVERY_ERROR','error':run.stderr.strip()}
 rows=[json.loads(line) for line in run.stdout.splitlines() if line.strip()];stable=[r for r in rows if not r.get('yanked') and re.fullmatch(r'\d+\.\d+\.\d+',r['vers'])]
 if not stable:return {'name':name,'source':url,'status':'NO_STABLE_NONYANKED_RELEASE'}
 c=max(stable,key=lambda r:tuple(map(int,r['vers'].split('.'))));version=c['vers']
 return {'name':name,'version':version,'checksum':c['cksum'],'rustVersion':c.get('rust_version'),'publishedUTC':c.get('pubtime'),'source':url,'docs':'https://docs.rs/'+name+'/'+version,'status':'DISCOVERED_NOT_ADAPTED','comparisonClass':'UNCLASSIFIED','normalDependencies':[r['name'] for r in c['deps'] if r.get('kind')=='normal']}
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool: rows=list(pool.map(fetch,names))
Path('research/inventory.json').write_text(json.dumps({'retrievedUTC':datetime.datetime.now(datetime.timezone.utc).isoformat(),'scope':'Broad Rust incremental/reactive/query ecosystem, index and set primitives; discovery is not a benchmark or a claim of matching capabilities. Actual native adapters, algorithm ports and incompatible systems will remain separately labelled.','entries':rows},indent=2)+'\n')
for r in rows:print(r['name'],r.get('version',r['status']),r.get('repository',''))
