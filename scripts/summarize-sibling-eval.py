#!/usr/bin/env python3
"""Summarize the paired sibling quality evaluation without exposing queries."""
import hashlib, json, statistics
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]

def load(label):
 return [json.loads(line) for line in (ROOT/'validation'/f'siblings-{label}.jsonl').read_text().splitlines()]

def metrics(rows,kind=None):
 q=[r for r in rows if r.get('type')=='query' and r['category']!='negative' and (kind is None or r['kind']==kind)]
 hits=sum(r['returned'] for r in q)
 return {'queries':len(q),'target_top1':sum(r['rank']==1 for r in q),'target_top3':sum(r['rank'] is not None and r['rank']<=3 for r in q),'target_top10':sum(r['rank'] is not None for r in q),'visible_target_top10':sum(r['visible_target_rank'] is not None for r in q),'mean_returned':statistics.mean(r['returned'] for r in q) if q else 0,'mean_distinct_excerpts':statistics.mean(r['returned']-r['duplicate_excerpts'] for r in q) if q else 0,'same_source_duplicate_hits':sum(r['duplicate_same_source_excerpts'] for r in q),'returned_hits':hits,'hits_with_query_term':sum(r['query_visible_count'] for r in q),'max_response_bytes':max((r['response_bytes'] for r in q),default=0),'over_budget_queries':sum(r['response_bytes']>16384 for r in q),'source_matches':sum(r['excerpt_source_matches'] for r in q),'source_checked':sum(r['excerpts_checked'] for r in q),'mean_excerpt_bytes':sum(r['excerpt_bytes'] for r in q)/hits if hits else 0,'budget_truncated_queries':sum(bool(r['truncated']) for r in q),'unsettled_queries':sum(r.get('pending_changes')!=0 for r in q)}

def main():
 before,after=load('before'),load('after'); manifest=json.loads((ROOT/'validation/private-cache/sibling-quality/manifest.json').read_text())
 paired={}; regressions=[]; rank_changes=[]
 bq={(r['project'],r['id']):r for r in before if r.get('type')=='query'}; aq={(r['project'],r['id']):r for r in after if r.get('type')=='query'}
 for key,b in bq.items():
  a=aq.get(key)
  if not a:continue
  if b['rank']!=a['rank']:rank_changes.append({'project':key[0],'id':key[1],'kind':b['kind'],'before_rank':b['rank'],'after_rank':a['rank']})
  if b['visible_target_rank'] is not None and a['visible_target_rank'] is None:regressions.append({'project':key[0],'id':key[1],'kind':b['kind'],'before_rank':b['rank'],'after_rank':a['rank']})
 for regression in regressions:
  project=next(p for p in manifest['projects'] if p['name']==regression['project'])
  query=next(q for q in project['queries'] if q['id']==regression['id'])
  key=hashlib.sha256((project['name']+query['id']).encode()).hexdigest()+'.json'
  response=json.loads((ROOT/'validation/private-cache/sibling-quality/responses-after'/key).read_text())
  if query['kind']=='project':
   regression['all_query_terms_visible_in_target']=any(h.get('relative_path')==query['path'] and all(term.casefold() in h['excerpt'].casefold() for term in query['query'].split()) for h in response['results'])
 for name in [p['name'] for p in manifest['projects']]:
  paired[name]={label:metrics([r for r in rows if r.get('project')==name]) for label,rows in [('before',before),('after',after)]}
 report={'builds':{'before':before[0]['binary_sha256'],'after':after[0]['binary_sha256']},'expected_roots':len(manifest['projects']),'expected_queries':sum(len(p['queries']) for p in manifest['projects']),'completed':{label:sum(r.get('type')=='complete' for r in rows) for label,rows in [('before',before),('after',after)]},'failures':{label:[r for r in rows if r.get('type')=='failure'] for label,rows in [('before',before),('after',after)]},'metrics':{kind:{label:metrics(rows,None if kind=='all' else kind) for label,rows in [('before',before),('after',after)]} for kind in ['all','project','sessions']},'negative_queries':{label:{'count':sum(r.get('category')=='negative' for r in rows),'unexpected_hits':sum(r['returned'] for r in rows if r.get('category')=='negative')} for label,rows in [('before',before),('after',after)]},'visibility_regressions':regressions,'rank_changes':rank_changes,'by_category':{category:{label:metrics([r for r in rows if r.get('category')==category]) for label,rows in [('before',before),('after',after)]} for category in ['identifier','components','heading','known_event']},'coverage':{label:[r for r in rows if r.get('type')=='coverage'] for label,rows in [('before',before),('after',after)]},'by_project':paired}
 representatives={}
 for p in manifest['projects']:
  old=representatives.get(p['family'])
  if old is None or (p['name'].count('/'),p['name'])<(old['name'].count('/'),old['name']):representatives[p['family']]=p
 names={p['name'] for p in representatives.values()}
 report['family_representatives']={'projects':sorted(names),'metrics':{kind:{label:metrics([r for r in rows if r.get('project') in names],kind) for label,rows in [('before',before),('after',after)]} for kind in ['project','sessions']}}
 first=ROOT/'validation/siblings-after-first-attempt.jsonl'
 report['first_attempt_failures']=[r for r in map(json.loads,first.read_text().splitlines()) if r.get('type')=='failure'] if first.exists() else []
 report['retry_evidence']='siblings-after-retry.jsonl' if first.exists() else None
 (ROOT/'validation/sibling-quality-summary.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:v for k,v in report.items() if k not in ['by_project','visibility_regressions','coverage','by_category','rank_changes','family_representatives']},indent=2));print('visibility_regressions',len(regressions))
if __name__=='__main__':main()
