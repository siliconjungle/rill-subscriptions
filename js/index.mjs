/** Exact atomic result deltas. Custom adapters must be pure and nonthrowing after validation. */
export class SubscriptionEngine {
  constructor(adapter) {
    for (const name of ['id','revision','equalRecord','equalQuery','queryKey','matches','recordKey','queryKeys','captureRecord','captureQuery','validateQuery']) {
      if (typeof adapter?.[name] !== 'function') throw new TypeError(`Missing adapter.${name}`);
    }
    this.adapter = adapter;
    this.records = new Map(); this.partitions = new Map();
    this.subscriptions = new Map(); this.groups = new Map();
    this.reverse = new Map(); this.broad = new Set(); this.epoch = 0; this.groupCount = 0;
  }
  get recordCount() { return this.records.size; }
  get subscriptionCount() { return this.subscriptions.size; }
  get uniqueQueryCount() { return this.groupCount; }
  record(id) { const e=this.records.get(id); return e ? this.adapter.captureRecord(e.value) : undefined; }
  query(id) { const g=this.subscriptions.get(id); return g ? this.adapter.captureQuery(g.query) : undefined; }
  matchingRecords(id) { const g=this.subscriptions.get(id); return g ? [...g.members].map(id=>this.adapter.captureRecord(this.records.get(id).value)) : []; }
  /** Visit committed projections selected by a transient query. Do not mutate borrowed rows. */
  forEachMatch(query,visit) {
    const a=this.adapter,q=a.captureQuery(query);a.validateQuery(q);
    const keys=a.queryKeys(q);
    if(keys===null){for(const e of this.records.values())if(a.matches(q,e.value))visit(e.value);}
    else for(const key of new Set(keys))for(const row of this.partitions.get(key)??[])if(a.matches(q,row))visit(row);
  }
  select(query) {const rows=[];this.forEachMatch(query,r=>rows.push(this.adapter.captureRecord(r)));return rows;}
  snapshot() { const out=[]; for (const [id,g] of this.subscriptions) for (const r of g.members) out.push([id,this.adapter.captureRecord(this.records.get(r).value)]); return out; }
  apply(batch) { const out=[]; this.applyInto(batch,out); return out; }
  applyInto({records=[],subscriptions=[]},out) {
    const a=this.adapter, rops=new Map(), qops=new Map();
    // Capture and validate every supplied value before mutation, including overwritten queries.
    for (const op of records) {
      if (op.type==='put') {const r=a.captureRecord(op.record); rops.set(a.id(r),r);}
      else if (op.type==='remove') {a.validateId?.(op.id);rops.set(op.id,undefined);}
      else throw new TypeError('Unknown record operation');
    }
    for (const op of subscriptions) {
      a.validateId?.(op.id);
      if (op.type==='put') {const q=a.captureQuery(op.query); a.validateQuery(q); qops.set(op.id,q);}
      else if (op.type==='remove') qops.set(op.id,undefined);
      else throw new TypeError('Unknown subscription operation');
    }
    for (const [id,r] of rops) {const old=this.records.get(id)?.value; if (r===undefined ? old===undefined : old!==undefined&&a.equalRecord(old,r)) rops.delete(id);}
    for (const [id,q] of qops) {const old=this.subscriptions.get(id)?.query; if (q===undefined ? old===undefined : old!==undefined&&a.equalQuery(old,q)) qops.delete(id);}
    const before=[];
    for (const [id] of qops) {const g=this.subscriptions.get(id); before.push([id,g ? new Map([...g.members].map(r=>[r,this.records.get(r).value])) : new Map()]);}
    out.length=0;
    let tests=0;
    for (const [id,r] of rops) {
      const old=this.records.get(id)?.value;
      const oldKey=old===undefined ? undefined : a.recordKey(old), newKey=r===undefined ? undefined : a.recordKey(r);
      const candidates=[];
      if (++this.epoch===Number.MAX_SAFE_INTEGER) {this.epoch=1; for(const bucket of this.groups.values()) for(const g of bucket) g.mark=0;}
      const collect=g=>{if(g.mark!==this.epoch){g.mark=this.epoch;candidates.push(g);}};
      if(old!==undefined) for(const g of this.reverse.get(oldKey)??[]) collect(g);
      if(r!==undefined) for(const g of this.reverse.get(newKey)??[]) collect(g);
      for(const g of this.broad) collect(g);
      for(const g of candidates) {
        const was=old!==undefined&&a.matches(g.query,old), now=r!==undefined&&a.matches(g.query,r);
        tests+=(old!==undefined)+(r!==undefined);
        if(was!==now) {if(now) g.members.add(id); else g.members.delete(id);}
        if(was||now) {const kind=!was?'Added':!now?'Removed':'Updated', revision=a.revision(now?r:old);
          for(const sub of g.subscribers) if(!qops.has(sub)) out.push({subscription:sub,record:id,kind,revision});}
      }
      if(old!==undefined&&r!==undefined&&oldKey===newKey) {
        const e=this.records.get(id);e.value=r;this.partitions.get(newKey)[e.offset]=r;
      } else {
        if(old!==undefined) {const e=this.records.get(id), bucket=this.partitions.get(oldKey), last=bucket.pop();
          if(e.offset<bucket.length){bucket[e.offset]=last;this.records.get(a.id(last)).offset=e.offset;}
          if(!bucket.length)this.partitions.delete(oldKey);this.records.delete(id);}
        if(r!==undefined) {let bucket=this.partitions.get(newKey);if(!bucket)this.partitions.set(newKey,bucket=[]);
          this.records.set(id,{value:r,offset:bucket.length});bucket.push(r);}
      }
    }
    for(const [id,q] of qops) {this.#detach(id);if(q!==undefined)this.#attach(id,q);}
    for(const [sub,old] of before) {
      const g=this.subscriptions.get(sub);
      for(const [id,r] of old) {const now=this.records.get(id)?.value;
        if(now!==undefined&&g&&a.matches(g.query,now)) {if(!a.equalRecord(r,now))out.push({subscription:sub,record:id,kind:'Updated',revision:a.revision(now)});}
        else out.push({subscription:sub,record:id,kind:'Removed',revision:a.revision(r)});
      }
      if(g)for(const id of g.members)if(!old.has(id))out.push({subscription:sub,record:id,kind:'Added',revision:a.revision(this.records.get(id).value)});
    }
    return {changedRecords:rops.size,changedSubscriptions:qops.size,uniqueQueries:this.uniqueQueryCount,predicateTests:tests,emitted:out.length};
  }
  #attach(id,q) {
    const a=this.adapter,key=a.queryKey(q);let bucket=this.groups.get(key);
    if(!bucket)this.groups.set(key,bucket=[]);
    let g=bucket.find(g=>a.equalQuery(g.query,q));
    if(!g) {
      const routed=a.queryKeys(q),keys=routed===null?null:[...new Set(routed)];
      g={query:q,key,keys,subscribers:new Set(),members:new Set(),mark:0};bucket.push(g);this.groupCount++;
      if(keys===null) {this.broad.add(g);for(const [id,e] of this.records)if(a.matches(q,e.value))g.members.add(id);}
      else for(const key of keys) {for(const r of this.partitions.get(key)??[])if(a.matches(q,r))g.members.add(a.id(r));
        let refs=this.reverse.get(key);if(!refs)this.reverse.set(key,refs=new Set());refs.add(g);}
    }
    g.subscribers.add(id);this.subscriptions.set(id,g);
  }
  #detach(id) {
    const g=this.subscriptions.get(id);if(!g)return;
    this.subscriptions.delete(id);g.subscribers.delete(id);if(g.subscribers.size)return;
    const bucket=this.groups.get(g.key);bucket.splice(bucket.indexOf(g),1);if(!bucket.length)this.groups.delete(g.key);this.groupCount--;
    if(g.keys===null)this.broad.delete(g);
    else for(const key of g.keys){const refs=this.reverse.get(key);refs.delete(g);if(!refs.size)this.reverse.delete(key);}
  }
}
const integer=(x,name,lo,hi)=>{if(!Number.isInteger(x)||x<lo||x>hi)throw new RangeError(name);return x;};
const word=(x,name,signed=false)=>{if(typeof x!=='bigint'||x<(signed?-(1n<<63n):0n)||x>(signed?(1n<<63n)-1n:(1n<<64n)-1n))throw new RangeError(name);return x;};
const point=p=>{if(!Array.isArray(p)||p.length!==3)throw new TypeError('Expected three coordinates');return Object.freeze(p.map(v=>integer(v,'coordinate',-2147483648,2147483647)));};
const samePoint=(a,b)=>a.every((x,i)=>x===b[i]);
const sameQuery=(a,b)=>samePoint(a.lower,b.lower)&&samePoint(a.upper,b.upper)&&a.requiredTags===b.requiredTags&&a.minValue===b.minValue&&a.maxValue===b.maxValue;
export function createSpatialAdapter({cellSize=64,maxQueryCells=512}={}) {
  integer(cellSize,'cellSize',1,2147483647);integer(maxQueryCells,'maxQueryCells',1,Number.MAX_SAFE_INTEGER);
  const cell=p=>p.map(v=>Math.floor(v/cellSize));
  return {
    validateId:id=>word(id,'id'),id:r=>r.id,revision:r=>r.revision,
    captureRecord:r=>Object.freeze({id:word(r.id,'id'),point:point(r.point),tags:word(r.tags??0n,'tags'),value:word(r.value??0n,'value',true),revision:word(r.revision??0n,'revision')}),
    captureQuery:q=>Object.freeze({lower:point(q.lower??[-2147483648,-2147483648,-2147483648]),upper:point(q.upper??[2147483647,2147483647,2147483647]),requiredTags:word(q.requiredTags??0n,'requiredTags'),minValue:word(q.minValue??-(1n<<63n),'minValue',true),maxValue:word(q.maxValue??(1n<<63n)-1n,'maxValue',true)}),
    equalRecord:(a,b)=>a.id===b.id&&samePoint(a.point,b.point)&&a.tags===b.tags&&a.value===b.value&&a.revision===b.revision,
    equalQuery:sameQuery,queryKey:q=>`${q.lower}|${q.upper}|${q.requiredTags}|${q.minValue}|${q.maxValue}`,
    validateQuery:q=>{if(q.minValue>q.maxValue||q.lower.some((v,i)=>v>q.upper[i]))throw new RangeError('Inverted query bounds');},
    matches:(q,r)=>r.point.every((v,i)=>v>=q.lower[i]&&v<=q.upper[i])&&(r.tags&q.requiredTags)===q.requiredTags&&r.value>=q.minValue&&r.value<=q.maxValue,
    recordKey:r=>cell(r.point).join(','),
    queryKeys:q=>{const lo=cell(q.lower),hi=cell(q.upper);let n=1;for(let d=0;d<3;d++){n*=hi[d]-lo[d]+1;if(n>maxQueryCells)return null;}
      const keys=[];for(let x=lo[0];x<=hi[0];x++)for(let y=lo[1];y<=hi[1];y++)for(let z=lo[2];z<=hi[2];z++)keys.push(`${x},${y},${z}`);return keys;}
  };
}
export class Engine extends SubscriptionEngine {constructor(options){super(createSpatialAdapter(options));}}
