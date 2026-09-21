#!/usr/bin/env python3
"""Measure MCP acceptance without exporting indexed content or changing real projects."""
import argparse
import datetime
import hashlib
import platform
import concurrent.futures
import json
import os
from pathlib import Path
import statistics
import shutil
import subprocess
import tempfile
import time

class Client:
    def __init__(self, binary, project, cache, env):
        self.process = subprocess.Popen([str(binary), 'serve', '--project', str(project), '--cache-dir', str(cache)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, env=env)
        self.sequence = 0
        self.rpc('initialize', {'protocolVersion':'2025-11-25','capabilities':{},'clientInfo':{'name':'acceptance','version':'1'}})
        self.process.stdin.write(json.dumps({'jsonrpc':'2.0','method':'notifications/initialized'})+'\n')
        self.process.stdin.flush()
    def rpc(self, method, params):
        self.sequence += 1
        self.process.stdin.write(json.dumps({'jsonrpc':'2.0','id':self.sequence,'method':method,'params':params})+'\n')
        self.process.stdin.flush()
        while True:
            line = self.process.stdout.readline()
            if not line:
                raise RuntimeError('MCP process exited')
            result = json.loads(line)
            if result.get('id') == self.sequence:
                if 'error' in result or result.get('result',{}).get('isError'):
                    raise RuntimeError('MCP request failed')
                return result['result']
    def tool(self, name, arguments):
        return self.rpc('tools/call', {'name':name,'arguments':arguments})['structuredContent']
    def close(self):
        self.process.stdin.close()
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()

def settle(client, tool, query='index', timeout=900, arguments=None):
    start = time.monotonic()
    while True:
        result = client.tool(tool, {'query':query, **(arguments or {})})
        if result['status'] in ('ready','degraded') and result['coverage'].get('pending_changes') == 0:
            return result, time.monotonic()-start
        if time.monotonic()-start > timeout:
            raise RuntimeError('Reconciliation timeout: '+tool)
        time.sleep(.1)

def measure(client, count, worker):
    samples = []
    partial = 0
    peak = 0
    queries = ['index', 'tokenizer', 'memory', 'file not found', 'search query', 'nonexistentqzxacceptance']
    deadline = time.monotonic()+900
    contexts = []
    session = client.tool('search_sessions', {'query':'error'})
    if session.get('results'):
        contexts = [session['results'][0]['match_id']]
    for _ in range(10):
        client.tool('search_project', {'query':'index'})
    attempt = 0
    while len(samples) < count:
        n = attempt+worker
        if contexts and n % 10 == 9:
            tool, arguments = 'search_sessions', {'mode':'context','match_id':contexts[0]}
        elif n % 5 == 4:
            tool, arguments = 'search_sessions', {'query':queries[n%len(queries)],'agent':['codex','claude','copilot'][n%3]}
        else:
            tool, arguments = 'search_project', {'query':queries[n%len(queries)]}
        start = time.perf_counter()
        try:
            result = client.tool(tool, arguments)
        except RuntimeError:
            if arguments.get('mode') != 'context':
                raise
            contexts = []
            partial += 1
            attempt += 1
            continue
        elapsed = (time.perf_counter()-start)*1000
        peak = max(peak, (result['coverage'].get('memory') or {}).get('peak_observed_rss_bytes',0))
        if result['status'] in ('ready','degraded') and result['coverage'].get('pending_changes') == 0:
            samples.append(elapsed)
        else:
            partial += 1
            time.sleep(.01)
        attempt += 1
        if time.monotonic() > deadline:
            raise RuntimeError('Query sampling timeout')
    return samples, partial, peak

def emit(value):
    print(json.dumps(value),flush=True)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', type=Path, required=True)
    parser.add_argument('--binary', type=Path, default=Path(__file__).resolve().parents[1]/'target'/'release'/('bm25-mcp.exe' if os.name=='nt' else 'bm25-mcp'))
    parser.add_argument('--requests', type=int, default=1000)
    parser.add_argument('--cache-dir', type=Path)
    parser.add_argument('--snapshot-sessions', action='store_true', help='Freeze actual JSONL histories in a private temporary directory for repeatable warm-query measurements')
    args = parser.parse_args()
    emit({'phase':'environment','project':args.project.name,'platform':platform.platform(),
          'architecture':platform.machine(),'logical_cpus':os.cpu_count(),
          'timestamp_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
          'binary_sha256':hashlib.sha256(args.binary.read_bytes()).hexdigest(),
          'persistent_cache':args.cache_dir is not None})
    with tempfile.TemporaryDirectory(prefix='bm25-acceptance-') as scratch:
        clients = []
        passed = True
        environment = os.environ.copy()
        if args.snapshot_sessions:
            files = 0
            total_bytes = 0
            providers = [('CODEX_HOME', '.codex', ['sessions','archived_sessions']), ('CLAUDE_CONFIG_DIR', '.claude', ['projects']), ('COPILOT_HOME', '.copilot', ['session-state'])]
            for variable, default, folders in providers:
                original = Path(environment.get(variable, str(Path.home()/default)))
                frozen = Path(scratch)/variable
                frozen.mkdir(mode=0o700)
                environment[variable] = str(frozen)
                for folder in folders:
                    source = original/folder
                    if not source.is_dir():
                        continue
                    for entry in source.rglob('*.jsonl'):
                        if not entry.is_file() or entry.is_symlink():
                            continue
                        target = frozen/entry.relative_to(original)
                        target.parent.mkdir(parents=True, exist_ok=True)
                        with entry.open('rb') as reader, target.open('xb') as writer:
                            shutil.copyfileobj(reader, writer, 1024*1024)
                        target.chmod(0o600)
                        files += 1
                        total_bytes += target.stat().st_size
            emit({'phase':'session_snapshot','files':files,'bytes':total_bytes,'contents':'unchanged actual histories; temporary copies removed after run'})
        try:
            for _ in range(4):
                clients.append(Client(args.binary.resolve(), args.project.resolve(), args.cache_dir.resolve() if args.cache_dir else Path(scratch)/'cache', environment))
            for tool in ('search_project','search_sessions'):
                result, elapsed = settle(clients[0],tool)
                emit({'phase':'initial','tool':tool,'seconds':elapsed,'status':result['status'],'error_count':result['coverage']['error_count'],'diagnostics':result['coverage'].get('diagnostics',{})})
            expected_agents = {'BM25-Turbo-Rust-Python-WASM-CLI':['codex'], 't3code':['codex'], 'neutron':['codex','copilot']}
            for agent in expected_agents.get(args.project.name, []):
                result, _ = settle(clients[0], 'search_sessions', 'error', arguments={'agent':agent})
                found = bool(result['results'])
                passed = passed and found
                emit({'phase':'real_history','agent':agent,'query':'error','matching_results':len(result['results']),'pass':found})
            judgments = {
                'BM25-Turbo-Rust-Python-WASM-CLI': [('score_deterministic', 'bm25_turbo/src/scoring.rs')],
                't3code': [('GitHubCliAuthenticationError', 'apps/server/src/sourceControl/GitHubCli.ts'), ('GitHub CLI is not authenticated', 'apps/server/src/sourceControl/GitHubCli.ts')],
                'neutron': [('h264_parameter_set_count', 'engine/crates/media/src/media.rs')],
            }
            for query, expected in judgments.get(args.project.name, []):
                result, _ = settle(clients[0], 'search_project', query)
                paths = [hit['relative_path'] for hit in result['results']]
                found = expected in paths
                passed = passed and found
                emit({'phase':'relevance','query':query,'expected_path':expected,'top_k':10,'rank':paths.index(expected)+1 if found else None,'pass':found,'judgment':'known symbol definition or exact error location'})
            for concurrency in (1,4):
                with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
                    results = list(executor.map(lambda pair: measure(pair[1],args.requests//concurrency,pair[0]),enumerate(clients[:concurrency])))
                samples = sorted(x for r in results for x in r[0])
                p95 = samples[min(len(samples)-1,int(len(samples)*.95))]
                peak = max(r[2] for r in results)
                passed = passed and p95 < 100 and peak <= 512*1024*1024
                emit({'phase':'queries','clients':concurrency,'requests':len(samples),'median_ms':statistics.median(samples),'p95_ms':p95,'latency_target_pass':p95<100,'partial_responses':sum(r[1] for r in results),'peak_owner_rss_bytes':peak,'memory_target_pass':peak<=512*1024*1024,'mix':'project, provider-filtered sessions, context when available'})
        finally:
            for client in clients:
                client.close()
            time.sleep(4)
        if not passed:
            raise SystemExit(1)

if __name__ == '__main__':
    main()
