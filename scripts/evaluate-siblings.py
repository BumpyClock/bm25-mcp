#!/usr/bin/env python3
"""Compare source-grounded lexical retrieval across sibling repositories and worktrees.

Raw queries, responses, paths and original project names stay in private storage.
Schema 2 JSONL is fsynced per record, with opaque project labels and safe metrics.
The private manifest keeps original names as private_name and canonical report
names in name so summarize-sibling-eval.py can still join paired runs. --only
accepts either name. Labels are stable for this manifest, not across preparation.
Do not pair schema 2 output with legacy reports; rerun both builds on the same
manifest. Progress exports allowlisted phases and numeric fields only.
"""
import argparse, concurrent.futures, hashlib, json, os, re, shutil, subprocess, time, uuid
from pathlib import Path
from acceptance import (
    Client,
    JsonlWriter,
    is_settled,
    public_coverage,
    public_number,
    public_progress,
    public_status,
    settle,
)

ROOT=Path(__file__).resolve().parents[1]
BASE=ROOT.parent
PRIVATE=ROOT/'validation/private-cache/sibling-quality'
TOKEN=re.compile(r'[A-Za-z][A-Za-z0-9_]{5,}')
EXT={'.rs','.go','.py','.ts','.tsx','.js','.jsx','.swift','.sh','.ps1','.cs','.c','.cpp','.h'}

def git(root,*args):
 return subprocess.check_output(['git','-C',str(root),*args],stderr=subprocess.DEVNULL)

def family(root):
 try:return str(Path(git(root,'rev-parse','--path-format=absolute','--git-common-dir').decode().strip()).resolve())
 except (subprocess.CalledProcessError,FileNotFoundError):return None

def project_roots():
 roots=[p for p in sorted(BASE.iterdir()) if p.is_dir() and (p/'.git').exists()]
 for container in ['oss','copilot-worktrees']:
  for parent,dirs,files in os.walk(BASE/container):
   p=Path(parent)
   if '.git' in dirs or '.git' in files:
    roots.append(p);dirs[:]=[]
   elif len(p.relative_to(BASE/container).parts)>3:dirs[:]=[]
 return sorted(roots,key=lambda p:str(p.relative_to(BASE)))

def candidate_files(project):
 raw=git(project,'ls-files','-z','--cached','--others','--exclude-standard')
 files=list(dict.fromkeys(x.decode('utf8','surrogateescape') for x in raw.split(b'\0') if x))
 ignored=subprocess.run(['git','-C',str(project),'check-ignore','--no-index','-z','--stdin'],input=b'\0'.join(os.fsencode(x) for x in files)+b'\0',stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
 excludes=set(os.fsdecode(x) for x in ignored.stdout.split(b'\0') if x)
 return [x for x in files if x not in excludes and not (project/x).is_symlink()]

def chunks_words(text):
 return list(dict.fromkeys(TOKEN.findall(text)))

def code_queries(project):
 files=candidate_files(project)
 candidates=[]
 for rel in files:
  p=project/rel
  if p.suffix not in EXT or any(x in {'vendor','vendors','node_modules','dist','target','.git'} for x in p.relative_to(project).parts):continue
  try:
   if p.stat().st_size>300000:continue
   text=p.read_text(encoding='utf8')
  except (OSError,UnicodeError):continue
  words=[w for w in chunks_words(text) if len(w)>=12 and ('_' in w or re.search('[a-z][A-Z]',w)) and not w.isupper()]
  if not words:continue
  words.sort(key=lambda w:hashlib.sha256((rel+'\0'+w).encode()).digest())
  candidates.append((rel,words[0],text.find(words[0])))
 candidates.sort(key=lambda c:hashlib.sha256(c[0].encode()).digest())
 picked=[];parents=set()
 for c in candidates:
  parent=str(Path(c[0]).parent)
  if parent not in parents:picked.append(c);parents.add(parent)
  if len(picked)==5:break
 for c in candidates:
  if len(picked)>=5:break
  if c not in picked:picked.append(c)
 queries=[]
 for n,(rel,word,offset) in enumerate(picked):
  text=(project/rel).read_text();line=text[:offset].count('\n')+1
  queries.append({'id':f'identifier_{n+1}','kind':'project','category':'identifier','query':word,'path':rel,'line':line,'needle':word})
  split=re.sub(r'([a-z0-9])([A-Z])',r'\1 \2',word).replace('_',' ')
  if split!=word:queries.append({'id':f'components_{n+1}','kind':'project','category':'components','query':split,'path':rel,'line':line,'needle':word})
 docs=sorted([x for x in files if Path(x).suffix=='.md' and not any(y in x.split('/') for y in ['vendor','skills_archive','node_modules'])],key=lambda x:(0 if Path(x).name.lower()=='readme.md' else 1,len(x)))
 for rel in docs:
  if sum(q['category']=='heading' for q in queries)>=2:break
  try:text=(project/rel).read_text(encoding='utf8')
  except (OSError,UnicodeError):continue
  if len(text)>100000:continue
  for line,body in enumerate(text.splitlines(),1):
   if not re.match(r'^#{1,4}\s',body):continue
   terms=chunks_words(body)
   if len(terms)<3:continue
   queries.append({'id':f"heading_{1+sum(q['category']=='heading' for q in queries)}",'kind':'project','category':'heading','query':' '.join(terms[:6]),'path':rel,'line':line,'needle':body.lstrip('# ').strip()});break
 queries.append({'id':'negative','kind':'project','category':'negative','query':'qzxnonexistentsiblingquality742619'})
 return queries

def records(path):
 with path.open('rb') as f:
  line=0
  while True:
   raw=f.readline(2*1024*1024+1)
   if not raw:break
   line+=1
   if not raw.endswith(b'\n') and len(raw)>2*1024*1024:
    while raw and not raw.endswith(b'\n'):raw=f.readline(2*1024*1024)
    continue
   try:yield line,json.loads(raw)
   except (ValueError,UnicodeError):continue

def session_candidates(snapshot,roots):
 groups={family(p):[] for p in roots};cwd_cache={}
 for provider,home in [('codex',snapshot/'CODEX_HOME'),('claude',snapshot/'CLAUDE_CONFIG_DIR'),('copilot',snapshot/'COPILOT_HOME')]:
  for path in sorted(home.rglob('*.jsonl')):
   owner=None; local=[]
   for line,r in records(path):
    t=r.get('type');payload=r.get('payload') or {};data=r.get('data') or {}
    cwd=(payload.get('cwd') if provider=='codex' and t in ['session_meta','turn_context'] else r.get('cwd') if provider=='claude' else (data.get('context') or {}).get('cwd') if provider=='copilot' and t in ['session.start','session.resume'] else None)
    if isinstance(cwd,str) and cwd:
     if cwd not in cwd_cache:cwd_cache[cwd]=family(Path(cwd)) if Path(cwd).is_dir() else None
     if owner is None:owner=cwd_cache[cwd]
     elif cwd_cache[cwd] and owner!=cwd_cache[cwd]:owner=None;break
    text=None
    if provider=='copilot' and t=='user.message':text=data.get('content')
    if provider=='claude' and t=='user':
     content=(r.get('message') or {}).get('content');text=content if isinstance(content,str) else None
    if provider=='codex' and t=='response_item' and payload.get('type')=='message' and payload.get('role')=='user':
     content=payload.get('content',[])
     if isinstance(content,list):text=' '.join(c.get('text','') for c in content if isinstance(c,dict))
    if isinstance(text,str) and 60<=len(text)<=1200 and '<environment_context>' not in text and '<INSTRUCTIONS>' not in text:
     terms=[w for w in chunks_words(text) if len(w)>=7 and w.lower() not in {'implement','requested','project','changes','existing','instructions','current','should','please','continue','files','without','before','after','working'}]
     if len(terms)>=3:
      local.append({'kind':'sessions','category':'known_event','query':' '.join(terms[:5]),'agent':provider,'path':provider+':'+str(path.relative_to(home)).replace('\\','/'),'line':line})
    if line>300 and len(local)>=5:break
   if owner in groups:groups[owner].extend(local[:5])
 for owner,items in groups.items():
  items.sort(key=lambda q:hashlib.sha256((q['path']+str(q['line'])).encode()).digest());groups[owner]=items[:3]
 return groups

def prepare():
 PRIVATE.mkdir(parents=True,exist_ok=True,mode=0o700);PRIVATE.chmod(0o700)
 snapshot=PRIVATE/'snapshot';snapshot.mkdir(exist_ok=True,mode=0o700);snapshot.chmod(0o700)
 roots=project_roots();count=0
 for variable,default,folders in [('CODEX_HOME','.codex',['sessions','archived_sessions']),('CLAUDE_CONFIG_DIR','.claude',['projects']),('COPILOT_HOME','.copilot',['session-state'])]:
  original=Path(os.environ.get(variable,str(Path.home()/default)));frozen=snapshot/variable;frozen.mkdir(exist_ok=True,mode=0o700);frozen.chmod(0o700)
  for folder in folders:
   for entry in (original/folder).rglob('*.jsonl'):
    if not entry.is_file() or entry.is_symlink():continue
    target=frozen/entry.relative_to(original);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(entry,target);target.chmod(0o600);count+=1
 sessions=session_candidates(snapshot,roots); projects=[]
 for p in roots:
  queries=code_queries(p)
  for i,q in enumerate(sessions.get(family(p),[]),1):queries.append({**q,'id':f'session_{i}'})
  projects.append({'name':str(p.relative_to(BASE)),'root':str(p),'family':family(p),'queries':queries})
 manifest={'snapshot_files':count,'projects':projects,'excluded':[{'name':p.name,'reason':'setup/config backup, not a project'} for p in BASE.iterdir() if p.is_dir() and 'backup-' in p.name]}
 private_json(PRIVATE/'manifest.json', manifest)
 print(json.dumps({'projects':len(projects),'queries':sum(len(p['queries']) for p in projects),'snapshot_files':count}),flush=True)

def strings(x):
 if isinstance(x,str):yield x
 elif isinstance(x,dict):
  for v in x.values():yield from strings(v)
 elif isinstance(x,list):
  for v in x:yield from strings(v)

def private_json(path, value):
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), 'w', encoding='utf8') as stream:
        path.chmod(0o600)
        json.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())


def report_manifest():
    path = PRIVATE / 'manifest.json'
    manifest = json.loads(path.read_text())
    if manifest.get('report_schema_version') != 2:
        # Preserve the primary-checkout ordering used by the summary consumer,
        # without using path hashes or exposing directory/worktree names.
        ordered = sorted(manifest['projects'], key=lambda p: (p['name'].count('/'), p['name']))
        for index, project in enumerate(ordered, 1):
            project['private_name'] = project['name']
            project['name'] = f'project-{index:04d}'
            for number, query in enumerate(project['queries'], 1):
                query['private_id'] = query['id']
                query['id'] = f'query-{number:04d}'
        manifest['report_schema_version'] = 2
        manifest['identity_scope'] = uuid.uuid4().hex
        staged = PRIVATE / ('manifest-' + uuid.uuid4().hex + '.json')
        try:
            private_json(staged, manifest)
            staged.replace(path)
        finally:
            staged.unlink(missing_ok=True)
    if not re.fullmatch(r'[0-9a-f]{32}', manifest.get('identity_scope', '')):
        raise ValueError('Invalid report identity scope')
    for project in manifest['projects']:
        if not re.fullmatch(r'project-[0-9]{4,}', project['name']):
            raise ValueError('Invalid public project label')
        if any(not re.fullmatch(r'query-[0-9]{4,}', query['id']) for query in project['queries']):
            raise ValueError('Invalid public query label')
    return manifest


def failure_code(error):
    if isinstance(error, TimeoutError):
        return 'timeout'
    if isinstance(error, OSError):
        return 'io_error'
    return 'evaluation_error'


def evaluate(binary, label, jobs, only=None, timeout=3600, output_suffix="", observer=None):
    if not re.fullmatch(r'[A-Za-z0-9_-]+', label or '') or not re.fullmatch(r'[A-Za-z0-9_-]*', output_suffix):
        raise ValueError('Invalid report label')
    if jobs < 1 or timeout <= 0:
        raise ValueError('Jobs and timeout must be positive')
    PRIVATE.chmod(0o700)
    manifest = report_manifest()
    projects = [p for p in manifest['projects'] if not only or p['name'] in only or p.get('private_name') in only]
    env = os.environ.copy()
    for key in ['CODEX_HOME', 'CLAUDE_CONFIG_DIR', 'COPILOT_HOME']:
        env[key] = str(PRIVATE / 'snapshot' / key)
    sha = hashlib.sha256(binary.read_bytes()).hexdigest()
    out = ROOT / 'validation' / f'siblings-{label}{output_suffix}.jsonl'
    cache = PRIVATE / ('cache-' + label)
    response_dir = PRIVATE / ('responses-' + label)
    response_dir.mkdir(exist_ok=True, mode=0o700)
    response_dir.chmod(0o700)

    with out.open('w', encoding='utf8') as stream:
        writer = JsonlWriter(stream, durable=True)
        writer.write({'type':'environment', 'schema_version':2, 'identity_scope':manifest['identity_scope'],
                      'run_id':uuid.uuid4().hex, 'binary_sha256':sha, 'projects':len(projects),
                      'snapshot_files':public_number(manifest['snapshot_files'])})

        def run(project):
            name = project['name']
            client = None
            root = Path(project['root'])
            started = time.monotonic()
            deadline = started + timeout

            def remaining():
                budget = deadline - time.monotonic()
                if budget <= 0:
                    raise TimeoutError('evaluation deadline exceeded')
                return budget

            try:
                family_cache = cache / hashlib.sha256(project['family'].encode()).hexdigest()
                client = Client(
                    binary,
                    root,
                    family_cache,
                    env,
                )
                kinds = {q['kind'] for q in project['queries']}
                for kind in ['project', 'sessions']:
                    if kind not in kinds:
                        continue

                    def observe(state):
                        coverage = state.get('coverage') if isinstance(state, dict) else {}
                        row = {'type':'progress', 'project':name, 'kind':kind,
                               'seconds':time.monotonic() - started,
                               'status':public_status(state.get('status')),
                               'pending_changes':public_number(coverage.get('pending_changes')),
                               'progress':public_progress(state)}
                        writer.write(row)
                        if observer is not None:
                            observer(row)

                    ready, seconds = settle(
                        client,
                        'search_' + kind,
                        timeout=remaining(),
                        observer=observe,
                    )
                    writer.write({'type':'coverage', 'project':name, 'kind':kind, 'seconds':seconds,
                                  'status':public_status(ready['status']),
                                  'coverage':public_coverage(ready.get('coverage')),
                                  'progress':public_progress(ready)})
                    for q in [q for q in project['queries'] if q['kind'] == kind]:
                        args = {'query':q['query'], 'limit':10}
                        if kind == 'sessions':
                            args['agent'] = q['agent']
                        begin = time.perf_counter()
                        r = client.tool('search_' + kind, args, timeout=remaining())
                        ms = (time.perf_counter() - begin) * 1000
                        private_json(response_dir / (hashlib.sha256((name + q['id']).encode()).hexdigest() + '.json'), r)
                        if not is_settled(r):
                            writer.write({'type':'failure', 'project':name, 'kind':kind,
                                          'id':q['id'], 'stage':'query', 'error':'not_settled',
                                          'status':public_status(r['status']),
                                          'coverage':public_coverage(r.get('coverage'))})
                            return
                        hits = r.get('results', [])

                        def expected(h):
                            if kind == 'project':
                                return h.get('relative_path') == q.get('path')
                            ref = h.get('source_reference', {})
                            return ref.get('path') == q.get('path') and ref.get('start_line', 0) <= q['line'] <= ref.get('end_line', 0)

                        rank = next((n for n, h in enumerate(hits, 1) if expected(h)), None)
                        visible_rank = next((n for n, h in enumerate(hits, 1) if expected(h) and (kind == 'sessions' or q.get('needle', '') in h['excerpt'])), None)
                        exact = []
                        query_visible = []
                        for h in hits:
                            if kind == 'project':
                                raw = (root / h['relative_path']).read_bytes()
                                text = h['excerpt'].encode()
                                offset = h['start_byte'] + h.get('excerpt_byte_offset', 0)
                                exact.append(raw[offset:offset + len(text)] == text)
                            else:
                                ref = h['source_reference']
                                provider, path = ref['path'].split(':', 1)
                                folder = {'codex':'CODEX_HOME', 'claude':'CLAUDE_CONFIG_DIR', 'copilot':'COPILOT_HOME'}[provider]
                                with (PRIVATE / 'snapshot' / folder / path).open('rb') as f:
                                    f.seek(ref['start_byte'])
                                    event = json.loads(f.read(ref['end_byte'] - ref['start_byte']))
                                exact.append(any(h['excerpt'] in s for s in strings(event)))
                            query_visible.append(any(term.casefold() in h['excerpt'].casefold() for term in q['query'].split()))
                        ids = [(h['source_reference']['path'], h['source_reference']['start_line'], h.get('event_id'), h['excerpt']) for h in hits] if kind == 'sessions' else [(h['relative_path'], h['start_byte'], h['excerpt']) for h in hits]
                        writer.write({'type':'query', 'project':name, 'id':q['id'], 'kind':kind,
                                      'category':q['category'] if q['category'] in ('identifier', 'components', 'heading', 'negative', 'known_event') else 'unknown',
                                      'rank':rank, 'visible_target_rank':visible_rank, 'returned':len(hits),
                                      'duplicate_same_source_excerpts':len(ids)-len(set(ids)),
                                      'duplicate_excerpts':len(hits)-len(set(h['excerpt'] for h in hits)),
                                      'excerpt_source_matches':sum(exact), 'excerpts_checked':len(exact),
                                      'query_visible_count':sum(query_visible),
                                      'response_bytes':len(json.dumps(r, ensure_ascii=False, separators=(',', ':')).encode()),
                                      'excerpt_bytes':sum(len(h['excerpt'].encode()) for h in hits),
                                      'truncated':r.get('truncated') if type(r.get('truncated')) is bool else None,
                                      'latency_ms':ms, 'status':public_status(r['status']),
                                      'pending_changes':public_number(r['coverage'].get('pending_changes')),
                                      'coverage':public_coverage(r.get('coverage')),
                                      'progress':public_progress(r)})
                writer.write({'type':'complete', 'project':name, 'seconds':time.monotonic()-started})
            except Exception as error:
                writer.write({'type':'failure', 'project':name, 'error':failure_code(error)})
            finally:
                if client:
                    client.close()

        # Registration precedes ranking within a family, without holding up
        # independent families. Completed facts are never accumulated in futures.
        groups = {}
        for project in projects:
            groups.setdefault(project['family'], []).append(project)

        def group(members):
            registered = []
            for project in members:
                registration = None
                try:
                    family_cache = cache / hashlib.sha256(project['family'].encode()).hexdigest()
                    registration = Client(
                        binary,
                        Path(project['root']),
                        family_cache,
                        env,
                    )
                    registered.append(project)
                    writer.write({'type':'registered', 'project':project['name']})
                except Exception as error:
                    writer.write({'type':'failure', 'project':project['name'], 'stage':'registration', 'error':failure_code(error)})
                finally:
                    if registration:
                        registration.close()
            time.sleep(4)
            for project in registered:
                run(project)

        with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as pool:
            for future in concurrent.futures.as_completed([pool.submit(group, members) for members in groups.values()]):
                future.result()
    print('complete', flush=True)

if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--prepare',action='store_true');parser.add_argument('--binary',type=Path);parser.add_argument('--label');parser.add_argument('--jobs',type=int,default=3);parser.add_argument('--only',action='append');parser.add_argument('--timeout',type=int,default=3600);parser.add_argument('--output-suffix',default='');args=parser.parse_args()
 try:
  if args.prepare:prepare()
  else:evaluate(args.binary.resolve(),args.label,args.jobs,args.only,args.timeout,args.output_suffix)
 except Exception as error:
  print(json.dumps({'type':'failure','error':failure_code(error)}),flush=True)
  raise SystemExit(1) from None
