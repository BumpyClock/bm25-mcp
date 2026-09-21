"""Source-grounded relevance evaluation; private raw responses stay in ignored cache."""
from pathlib import Path
import sys, os, json, tempfile, shutil, hashlib, time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from acceptance import Client,settle
ROOT=Path(__file__).resolve().parents[1]
PROJECT=ROOT.parent/'neutron'
OUT=ROOT/'validation/private-cache/quality';OUT.mkdir(parents=True,exist_ok=True)
code=[
 ('exact_media','identifier','h264_parameter_set_count',['engine/crates/media/src/media.rs'],{}),
 ('split_media','identifier_components','h264 parameter set count',['engine/crates/media/src/media.rs'],{}),
 ('exact_storage','identifier','save_envelope_durable',['framework/crates/app-storage/src/envelope.rs'],{}),
 ('camel_storage','identifier','LoadOutcome',['framework/crates/app-storage/src/envelope.rs'],{}),
 ('exact_error','error_message','expected an integer from 0 through 65535',['framework/crates/app-manifest/src/error.rs'],{}),
 ('clipboard','plain_language','read data from platform clipboard',['engine/crates/gpui/src/app.rs','engine/crates/gpui/src/platform.rs'],{}),
 ('corrupt_storage','plain_language','malformed TOML archived backup recovery',['framework/crates/app-storage/src/envelope.rs'],{}),
 ('keyboard_focus','plain_language','move keyboard focus next tab stop',['engine/crates/gpui/src/tab_stop.rs'],{}),
 ('manifest_identity','plain_language','validate app identifier macOS bundle underscores',['framework/crates/app-manifest/src/error.rs'],{}),
 ('filtered_clipboard','path_filter','read_from_clipboard',['engine/crates/gpui/src/app.rs'],{'path_glob':'engine/crates/gpui/src/app.rs'}),
 ('synonym_storage','semantic_gap','protect newer saved settings from being overwritten',['framework/crates/app-storage/src/envelope.rs'],{}),
 ('negative','negative','qzxnonexistentquality742619',[],{}),
]
sessions=[
 ('button_error','E0599 aria_label Button',['0035800f-5cfb-4867-a853-ff5d6ae86e0d']),
 ('segmented_examples','segmented Params Headers Body Settings',['f6337f95-4ee5-439a-9d98-35010b429dea']),
 ('request_tabs','TabBar requests segmented List users Create user',['e76644b0-a3d7-472b-8989-4af2fbe7699f']),
 ('tab_test','suffix stop_propagation Tab tests',['a9cbbddf-a967-4e3e-8111-ac61252f8e89','41c52581-7f91-42e5-83be-87aeccec8890']),
 ('generic_error','error',[]),
]
def strings(v):
 if isinstance(v,str):yield v
 elif isinstance(v,dict):
  for x in v.values():yield from strings(x)
 elif isinstance(v,list):
  for x in v:yield from strings(x)
def extract_source(hit):
 ref=hit['source_reference']; provider,relative=ref['path'].split(':',1)
 home={'copilot':Path.home()/'.copilot','codex':Path.home()/'.codex','claude':Path.home()/'.claude'}[provider]
 with (home/relative).open('rb') as f:
  f.seek(ref['start_byte']);raw=f.read(ref['end_byte']-ref['start_byte'])
 try:event=json.loads(raw)
 except ValueError:
  # Source bounds can include JSONL newline or use inclusive event offsets.
  with (home/relative).open() as f:
   for n,line in enumerate(f,1):
    if n==ref['start_line']:event=json.loads(line);break
 return event
rows=[]
def emit(row):
 rows.append(row)
 with (ROOT/'validation/retrieval-quality.jsonl').open('a') as f:f.write(json.dumps(row)+'\n')
 print(row['id'],flush=True)
(ROOT/'validation/retrieval-quality.jsonl').write_text('')
with tempfile.TemporaryDirectory(prefix='bm25-quality-') as scratch:
 env=os.environ.copy(); total=0
 for variable,default,folders in [('CODEX_HOME','.codex',['sessions','archived_sessions']),('CLAUDE_CONFIG_DIR','.claude',['projects']),('COPILOT_HOME','.copilot',['session-state'])]:
  original=Path(env.get(variable,str(Path.home()/default)));frozen=Path(scratch)/variable;frozen.mkdir(mode=0o700);env[variable]=str(frozen)
  for folder in folders:
   for entry in (original/folder).rglob('*.jsonl'):
    if not entry.is_file() or entry.is_symlink():continue
    target=frozen/entry.relative_to(original);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(entry,target);target.chmod(0o600);total+=1
 client=Client(ROOT/'target/release/bm25-mcp',PROJECT,ROOT/'validation/private-cache/neutron',env)
 try:
  emit({'id':'environment','binary_sha256':hashlib.sha256((ROOT/'target/release/bm25-mcp').read_bytes()).hexdigest(),'snapshot_files':total,'method':'Targets selected from source before querying; top 10 chunks, 65536-byte budget; one real repository, Copilot histories.'})
  ready,seconds=settle(client,'search_project',query='h264_parameter_set_count');emit({'id':'code_ready','seconds':seconds,'status':ready['status'],'diagnostics':ready['coverage']['diagnostics']})
  for name,category,query,expected,extra in code:
   r=client.tool('search_project',{'query':query,'limit':10,'max_response_bytes':65536,**extra});(OUT/(name+'.json')).write_text(json.dumps(r))
   hits=r['results'];paths=[h['relative_path'] for h in hits];rank=next((n for n,p in enumerate(paths,1) if p in expected),None)
   fidelity=[]
   for h in hits:
    text=(PROJECT/h['relative_path']).read_text(errors='replace');fidelity.append(h['excerpt'] in text)
   emit({'id':name,'kind':'project','category':category,'query':query,'expected_paths':expected,'rank':rank,'returned':len(hits),'unique_paths':len(set(paths)),'paths':paths,'excerpt_fidelity':fidelity,'truncated':r['truncated'],'status':r['status']})
  ready,seconds=settle(client,'search_sessions',query='error',arguments={'agent':'copilot'});emit({'id':'sessions_ready','seconds':seconds,'status':ready['status'],'diagnostics':ready['coverage']['diagnostics']})
  for name,query,events in sessions:
   r=client.tool('search_sessions',{'query':query,'agent':'copilot','limit':10,'max_response_bytes':65536});(OUT/(name+'.json')).write_text(json.dumps(r));hits=r['results']
   ids=[h['event_id'] for h in hits];fidelity=[]
   for h in hits:
    event=extract_source(h);fidelity.append(any(h['excerpt'] in s for s in strings(event)))
   emit({'id':name,'kind':'sessions','query':query,'target_event_rank':next((n for n,e in enumerate(ids,1) if e in events),None),'returned':len(hits),'unique_events':len(set(zip([h['session_id'] for h in hits],ids))),'unique_excerpts':len(set(h['excerpt'] for h in hits)),'roles':[h['role'] for h in hits],'excerpt_fidelity':fidelity,'truncated':r['truncated'],'status':r['status']})
   if name=='button_error' and hits:
    c=client.tool('search_sessions',{'mode':'context','match_id':hits[0]['match_id'],'before_events':2,'after_events':2,'max_response_bytes':65536});(OUT/'context.json').write_text(json.dumps(c));ev=c.get('context',[])
    emit({'id':'context','kind':'context','events':len(ev),'keys':list(ev[0]) if ev else [],'same_session':all(e.get('session_id')==hits[0]['session_id'] for e in ev),'contains_match_event':any(e.get('event_id')==hits[0]['event_id'] for e in ev),'truncated':c.get('truncated'),'has_cursor':bool(c.get('cursor'))})
  for tool,args,name in [('search_project',{'query':'h264_parameter_set_count'},'default_project'),('search_sessions',{'query':'error','agent':'copilot'},'default_sessions')]:
   r=client.tool(tool,args);(OUT/(name+'.json')).write_text(json.dumps(r));emit({'id':name,'returned':len(r['results']),'truncated':r['truncated']})
 finally:client.close();time.sleep(4)
