#!/usr/bin/env python3
"""Aggregate partition metrics without exporting query text or session content."""
import importlib.util
import json
import math
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('summary', ROOT / 'scripts/summarize-sibling-eval.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
rows = []
for partition in 'abc':
    rows.extend(map(json.loads, (ROOT / f'validation/siblings-luna-{partition}.jsonl').read_text().splitlines()))
initial_rows = list(rows)
retry = ROOT / 'validation/siblings-luna-c-self-isolated.jsonl'
if retry.exists():
    rows = [r for r in rows if r.get('project') != 'bm25-mcp']
    rows.extend(json.loads(line) for line in retry.read_text().splitlines() if json.loads(line).get('type') != 'environment')
manifest = json.loads((ROOT / 'validation/private-cache/luna-eval/manifest.json').read_text())
queries = [r for r in rows if r['type'] == 'query']
latencies = sorted(r['latency_ms'] for r in queries)
report = {
    'binary_sha256': next(r['binary_sha256'] for r in rows if r['type'] == 'environment'),
    'expected_roots': len(manifest['projects']),
    'repository_families': len({p['family'] for p in manifest['projects']}),
    'expected_queries': sum(len(p['queries']) for p in manifest['projects']),
    'completed_roots': sum(r['type'] == 'complete' for r in rows),
    'executed_queries': len(queries),
    'failures': [r for r in rows if r['type'] == 'failure'],
    'initial_unsettled_queries': [{k:r.get(k) for k in ['project','id','status','pending_changes']} for r in initial_rows if r['type']=='query' and r.get('pending_changes')!=0],
    'apayee_retry_failures': [json.loads(line) for line in (ROOT/'validation/siblings-luna-a-retry.jsonl').read_text().splitlines() if json.loads(line)['type']=='failure'] if (ROOT/'validation/siblings-luna-a-retry.jsonl').exists() else [],
    'metrics': {kind: m.metrics(rows, kind) for kind in ['project', 'sessions']},
    'categories': {kind: m.metrics([r for r in rows if r.get('category') == kind]) for kind in ['identifier', 'components', 'heading']},
    'negative_queries': {'count': sum(r['category'] == 'negative' for r in queries), 'unexpected_hits': sum(r['returned'] for r in queries if r['category'] == 'negative')},
    'latency_by_kind_ms': {kind: {f'p{n}': values[math.ceil(len(values)*n/100)-1] for n in [50,95,99]} for kind in ['project','sessions'] for values in [sorted(r['latency_ms'] for r in queries if r['kind']==kind)]},
    'latency_ms_concurrent': {f'p{n}': latencies[math.ceil(len(latencies)*n/100)-1] for n in [50,95,99]},
    'coverage': [r for r in rows if r['type'] == 'coverage'],
    'by_project': {p['name']: {kind: m.metrics([r for r in rows if r.get('project') == p['name']],kind) for kind in ['project','sessions']} for p in manifest['projects']},
    'exclusions': manifest['excluded'],
}
audit = ROOT / 'validation/luna-session-audit.jsonl'
if audit.exists():
    records = list(map(json.loads, audit.read_text().splitlines()))
    report['logical_sessions'] = {'queries': len(records), 'top1': sum(r['logical_target_rank'] == 1 for r in records), 'top3': sum(r['logical_target_rank'] is not None and r['logical_target_rank'] <= 3 for r in records), 'top10': sum(r['logical_target_rank'] is not None for r in records), 'copies_checked': sum(r['copies_checked'] for r in records), 'target_context_checks': sum(r['target_context_checks'] for r in records), 'providers': {k:sum(r['agent']==k for r in records) for k in ['codex','claude','copilot']}}
(ROOT / 'validation/luna-eval-summary.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({k:v for k,v in report.items() if k not in ['coverage','by_project']},indent=2))
