#!/usr/bin/env python3
"""Measure the real MCP round trip without writing source or session content."""
import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess
import tempfile
import time

parser = argparse.ArgumentParser()
parser.add_argument('--project', type=Path, required=True)
parser.add_argument('--requests', type=int, default=1000)
parser.add_argument('--binary', type=Path, default=Path(__file__).resolve().parents[1] / 'target/release/bm25-mcp')
parser.add_argument('--empty-session-homes', action='store_true')
args = parser.parse_args()
with tempfile.TemporaryDirectory(prefix='bm25-mcp-benchmark-') as scratch:
    env = os.environ.copy()
    if args.empty_session_homes:
        for name in ['CODEX_HOME', 'CLAUDE_CONFIG_DIR', 'COPILOT_HOME']:
            env[name] = str(Path(scratch) / name)
    process = subprocess.Popen([str(args.binary), 'serve', '--project', str(args.project.resolve()), '--cache-dir', str(Path(scratch)/'cache')], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
    request_id = 0
    def rpc(method, params):
        global request_id
        request_id += 1
        process.stdin.write(json.dumps({'jsonrpc':'2.0','id':request_id,'method':method,'params':params})+'\n')
        process.stdin.flush()
        while True:
            line = process.stdout.readline()
            if not line:
                raise RuntimeError('MCP process exited')
            value = json.loads(line)
            if value.get('id') == request_id:
                if 'error' in value or value.get('result',{}).get('isError'):
                    raise RuntimeError('MCP request failed: '+json.dumps(value))
                return value['result']
    try:
        rpc('initialize',{'protocolVersion':'2025-11-25','capabilities':{},'clientInfo':{'name':'benchmark','version':'1'}})
        process.stdin.write(json.dumps({'jsonrpc':'2.0','method':'notifications/initialized'})+'\n'); process.stdin.flush()
        start=time.monotonic()
        for tool in ['search_project','search_sessions']:
            while True:
                result=rpc('tools/call',{'name':tool,'arguments':{'query':'index'}})['structuredContent']
                if result['status'] in ['ready','degraded']:
                    print(json.dumps({'phase':'reconciled','tool':tool,'elapsed_seconds':time.monotonic()-start,'status':result['status'],'error_count':result['coverage']['error_count']}),flush=True)
                    break
                if time.monotonic()-start>300:
                    raise RuntimeError('Reconciliation did not settle within 300 seconds')
                time.sleep(.2)
        samples=[]; partial=0; attempts=0; peak_rss=0
        queries=['BM25','tokenizer','memory','index search','nonexistentbenchmarkqueryqzx']
        deadline=time.monotonic()+300
        while len(samples)<args.requests:
            start=time.perf_counter()
            result=rpc('tools/call',{'name':'search_project','arguments':{'query':queries[attempts%len(queries)]}})['structuredContent']
            elapsed=(time.perf_counter()-start)*1000
            attempts+=1
            peak_rss=max(peak_rss,(result['coverage'].get('memory') or {}).get('peak_observed_rss_bytes',0))
            if result['status']=='ready':
                if attempts>20:
                    samples.append(elapsed)
            else:
                partial+=1
                time.sleep(.02)
            if time.monotonic()>deadline:
                raise RuntimeError('Could not collect enough reconciled queries within 300 seconds')
        ordered=sorted(samples)
        print(json.dumps({'requests':len(samples),'median_ms':statistics.median(samples),'p95_ms':ordered[min(len(ordered)-1,int(len(ordered)*.95))],'partial_responses_excluded':partial,'peak_observed_owner_rss_bytes':peak_rss,'scope':'single-client reconciled MCP round trip; does not establish all acceptance gates'}),flush=True)
    finally:
        process.stdin.close()
        try: process.wait(timeout=5)
        except subprocess.TimeoutExpired: process.kill();process.wait()
        # Owner shutdown detection is bounded independently of frontend EOF.
        time.sleep(4)
