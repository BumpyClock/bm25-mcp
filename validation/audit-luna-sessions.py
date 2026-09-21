#!/usr/bin/env python3
"""Validate logical target identity and copy context through the MCP API."""
import hashlib
import json
import os
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from acceptance import Client, settle

class AuditClient(Client):
    def rpc(self, method, params):
        self.sequence += 1
        self.process.stdin.write(json.dumps({"jsonrpc":"2.0","id":self.sequence,"method":method,"params":params}) + "\n")
        self.process.stdin.flush()
        while True:
            line = self.process.stdout.readline()
            if not line:
                raise RuntimeError("MCP process exited")
            result = json.loads(line)
            if result.get("id") == self.sequence:
                if "error" in result or result.get("result", {}).get("isError"):
                    raise RuntimeError("MCP request failed: " + json.dumps(result))
                return result["result"]

PRIVATE = ROOT / 'validation/private-cache/luna-eval'
manifest = json.loads((PRIVATE / 'manifest.json').read_text())
partitions = json.loads((PRIVATE / 'partitions.json').read_text())
env = os.environ.copy()
for key in ['CODEX_HOME', 'CLAUDE_CONFIG_DIR', 'COPILOT_HOME']:
    env[key] = str(PRIVATE / 'snapshot' / key)
selected = sys.argv[1] if len(sys.argv) > 1 else None
output_name = 'luna-session-audit' + ('-' + selected if selected else '') + '.jsonl'
clients = {}
output_path = ROOT / 'validation' / output_name
completed = {(r['project'], r['id']) for r in map(json.loads, output_path.read_text().splitlines())} if output_path.exists() else set()
with output_path.open('a') as output:
    for partition, names in partitions.items():
        if selected and partition != selected:
            continue
        for project in manifest['projects']:
            queries = [q for q in project['queries'] if q['kind'] == 'sessions' and (project['name'], q['id']) not in completed]
            if project['name'] not in names or not queries:
                continue
            response_dir = PRIVATE / ('responses-luna-' + partition)
            available = [q for q in queries if (response_dir / (hashlib.sha256((project['name'] + q['id']).encode()).hexdigest() + '.json')).exists()]
            if not available:
                print(project['name'], 'no completed session queries; skipped', flush=True)
                continue
            queries = available
            cache = PRIVATE / ('cache-luna-' + partition) / hashlib.sha256(project['family'].encode()).hexdigest()
            client_key = (partition, project['family'])
            if client_key not in clients:
                clients[client_key] = AuditClient(ROOT / 'validation/bin/bm25-mcp-luna-eval', Path(project['root']), cache, env)
                settle(clients[client_key], 'search_sessions', timeout=900)
            client = clients[client_key]
            for query in queries:
                key = hashlib.sha256((project['name'] + query['id']).encode()).hexdigest() + '.json'
                original = json.loads((PRIVATE / ('responses-luna-' + partition) / key).read_text())
                logical_rank = None
                groups = set()
                copy_checks = 0
                context_checks = 0
                for rank, hit in enumerate(original['results'], 1):
                    copies = []
                    cursor = None
                    while True:
                        args = {'mode': 'copies', 'match_id': hit['match_id'], 'limit': 50}
                        if cursor:
                            args['cursor'] = cursor
                        response = client.tool('search_sessions', args)
                        copies.extend(response['copies'])
                        cursor = response.get('cursor')
                        if not cursor:
                            break
                    copy_checks += len(copies)
                    groups.add(tuple(sorted((c['source_reference']['path'], c['source_reference']['start_line']) for c in copies)))
                    for copy in copies:
                        ref = copy['source_reference']
                        if ref['path'] == query['path'] and ref['start_line'] <= query['line'] <= ref['end_line']:
                            logical_rank = logical_rank or rank
                            context = client.tool('search_sessions', {'mode': 'context', 'match_id': copy['match_id'], 'before_events': 0, 'after_events': 0})
                            assert any(c['match_id'] == copy['match_id'] for c in context['context'])
                            assert all(c['source_reference']['path'] == ref['path'] for c in context['context'])
                            context_checks += 1
                row = {'project': project['name'], 'id': query['id'], 'agent': query['agent'], 'logical_target_rank': logical_rank, 'distinct_occurrence_groups': len(groups), 'returned': len(original['results']), 'copies_checked': copy_checks, 'target_context_checks': context_checks}
                output.write(json.dumps(row) + '\n')
                output.flush()
            print(project['name'], 'audited', flush=True)
for client in clients.values():
    client.close()
