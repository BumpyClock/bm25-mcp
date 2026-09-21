#!/usr/bin/env python3
"""Exercise updates in an isolated Git repository with four active MCP clients."""
import argparse
import hashlib
import platform
import json
import threading
import os
from pathlib import Path
import subprocess
import tempfile
import time
from acceptance import Client, settle, emit

parser = argparse.ArgumentParser()
parser.add_argument('--huge-session-mib', type=int, default=64)
parser.add_argument('--binary', type=Path)
args = parser.parse_args()
binary = args.binary or Path(__file__).resolve().parents[1]/'target'/'release'/('bm25-mcp.exe' if os.name=='nt' else 'bm25-mcp')
emit({'phase':'environment','platform':platform.platform(),'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'huge_session_mib':args.huge_session_mib,'clients':4})
with tempfile.TemporaryDirectory(prefix='bm25-updates-') as scratch:
    base = Path(scratch)
    root = base/'project'
    root.mkdir()
    env = os.environ.copy()
    for name in ['CODEX_HOME','CLAUDE_CONFIG_DIR','COPILOT_HOME']:
        env[name] = str(base/name)
        Path(env[name]).mkdir()
    def git(*args):
        subprocess.run(['git','-C',str(root),*args],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
    git('init','-q')
    git('config','user.name','Acceptance')
    git('config','user.email','acceptance@example.test')
    for i in range(100):
        (root/('source%d.rs'%i)).write_text(('fn parseHTTPResponse() { /* exact error: request timed out */ }\n'*100)+'\nbranchalphamarker file%d\n'%i)
    git('add','.')
    git('commit','-qm','Initial acceptance fixture')
    git('branch','alpha')
    git('checkout','-qb','beta')
    for i in range(10):
        (root/('source%d.rs'%i)).write_text('branchbetamarker\n'*100+'file%d\n'%i)
    git('commit','-qam','Second acceptance fixture')
    git('checkout','-q','alpha')
    clients = []
    readers = []
    reader_errors = []
    stop = threading.Event()
    passed = True
    def read_continuously(client):
        try:
            while not stop.is_set():
                client.tool('search_project', {'query':'parseHTTPResponse'})
                stop.wait(.01)
        except Exception as error:
            reader_errors.append(type(error).__name__)
    def wait_marker(marker, count=1, timeout=90, path_glob=None):
        start=time.monotonic()
        while True:
            arguments={'query':marker,'limit':50,'max_response_bytes':65536}
            if path_glob:
                arguments['path_glob']=path_glob
            result=clients[0].tool('search_project',arguments)
            if len({hit['relative_path'] for hit in result['results']})>=count and result['status']=='ready':
                return time.monotonic()-start,result
            if time.monotonic()-start>timeout:
                raise RuntimeError('update did not converge')
            time.sleep(.01)
    try:
        clients=[Client(binary,root,base/'cache',env) for _ in range(4)]
        settle(clients[0],'search_project')
        for client in clients[1:]:
            reader = threading.Thread(target=read_continuously,args=(client,))
            reader.start()
            readers.append(reader)
        for count in [1,10]:
            marker='editmarker'+('single' if count==1 else 'batch')
            for i in range(count):
                (root/('source%d.rs'%i)).write_text((marker+' fn updatedFunction() {}\n')*100+'file%d\n'%i)
            start=time.monotonic()
            elapsed,result=wait_marker(marker,count)
            passed = passed and elapsed < 2
            emit({'phase':'ordinary_edit','files':count,'seconds':time.monotonic()-start,'target_pass':elapsed<2,'peak_owner_rss_bytes':result['coverage']['memory']['peak_observed_rss_bytes']})
        git('reset','--hard','-q')
        for branch,marker in [('beta','branchbetamarker'),('alpha','branchalphamarker'),('beta','branchbetamarker')]:
            start=time.monotonic()
            git('checkout','-q',branch)
            elapsed,result=wait_marker(marker,10,path_glob="source?.rs")
            emit({'phase':'branch','branch':branch,'seconds':time.monotonic()-start,'peak_owner_rss_bytes':result['coverage']['memory']['peak_observed_rss_bytes'],'diagnostics':result['coverage'].get('diagnostics',{})})
        # Valid multi-megabyte giant line exercises bounded chunking, not a size cutoff.
        with (root/'huge.txt').open('w') as f:
            for _ in range(4096):
                f.write('ordinarytoken '*512)
            f.write(' giantfiletailmarker')
        elapsed,result=wait_marker('giantfiletailmarker',timeout=180)
        emit({'phase':'huge_text','bytes':(root/'huge.txt').stat().st_size,'seconds':elapsed,'peak_owner_rss_bytes':result['coverage']['memory']['peak_observed_rss_bytes']})
        session_dir = Path(env['CODEX_HOME'])/'sessions'
        session_dir.mkdir()
        session = session_dir/'huge.jsonl'
        with session.open('w') as f:
            f.write(json.dumps({'type':'session_meta','payload':{'id':'huge-session','cwd':str(root)}})+'\n')
            f.write('{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"')
            block = 'sessionword ' * 8192
            for _ in range((args.huge_session_mib*1024*1024)//len(block)):
                f.write(block)
            f.write('hugesessiontailmarker"}]}}\n')
        start = time.monotonic()
        while True:
            result = clients[0].tool('search_sessions',{'query':'hugesessiontailmarker'})
            if result.get('results') and result['coverage'].get('pending_changes') == 0:
                break
            if time.monotonic()-start > 600:
                raise RuntimeError('Huge session did not converge')
            time.sleep(.1)
        peak = result['coverage']['memory']['peak_observed_rss_bytes']
        passed = passed and peak <= 512*1024*1024
        emit({'phase':'huge_session_record','bytes':session.stat().st_size,'seconds':time.monotonic()-start,'peak_owner_rss_bytes':peak,'memory_target_pass':peak<=512*1024*1024,'concurrent_readers':4})
    finally:
        stop.set()
        for reader in readers:
            reader.join(timeout=35)
        for client in clients:
            client.close()
        time.sleep(4)

    if reader_errors or not passed:
        emit({'phase':'acceptance_failure','reader_errors':reader_errors,'targets_pass':passed})
        raise SystemExit(1)
