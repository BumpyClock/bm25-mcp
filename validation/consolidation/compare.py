"""Check semantic equivalence and summarize paired local measurements."""
import gzip
import hashlib
import json
from pathlib import Path
import statistics

root = Path(__file__).resolve().parent

def read(name):
    path = root / (name + '.json')
    if path.exists():
        return json.loads(path.read_text())
    return json.loads(gzip.decompress(path.with_suffix('.json.gz').read_bytes()))

def semantic_case(case):
    return {key: value for key, value in case.items() if not key.startswith('latency_')}

before, after = read('stages-before'), read('stages-after')
for index, (a, b) in enumerate(zip(before, after)):
    assert a == b, f'Exact differential failed at record {index}: {a["query"]}'
assert len(before) == len(after)
summary = {'exact_differential': {'records': len(before), 'equal': True, 'sha256': hashlib.sha256(json.dumps(before, sort_keys=True).encode()).hexdigest()}}

before, after = read('ranking-before'), read('ranking-after')
assert [semantic_case(case) for case in before['cases']] == [semantic_case(case) for case in after['cases']]
assert before['raw_vs_enhanced'] == after['raw_vs_enhanced']
assert [semantic_case(case) for case in before['high_fanout']['cases']] == [semantic_case(case) for case in after['high_fanout']['cases']]
summary['ranking_cases_equal'] = len(before['cases'])
summary['latency_ms'] = {}
for label, a, b in [('enhanced_fixture_mean', [case for case in before['cases'] if case['mode']=='enhanced'], [case for case in after['cases'] if case['mode']=='enhanced']), ('high_fanout', [before['high_fanout']['cases'][1]], [after['high_fanout']['cases'][1]])]:
    summary['latency_ms'][label] = {'before': {key: statistics.mean(case['latency_ms_'+key] for case in a) for key in ['p50','p95']}, 'after': {key: statistics.mean(case['latency_ms_'+key] for case in b) for key in ['p50','p95']}}
summary['individual_query_latency_ms'] = [{'query': a['query'], 'kind': a['kind'], 'before_p95': a['latency_ms_p95'], 'after_p95': b['latency_ms_p95']} for a,b in zip(before['cases'],after['cases']) if a['mode']=='enhanced']
summary['fixture_direct_store_indexing'] = {'fixture_indexing_before_ms': before['indexing_ms'], 'fixture_indexing_after_ms': after['indexing_ms'], 'note': 'This fixture uses Store directly; it does not measure scanner publication confirmation.'}

before, after = read('stress-before'), read('stress-after')
for name in before['cases']:
    a, b = before['cases'][name], after['cases'][name]
    for key in ['candidate_count', 'admission_counts', 'additional_retrievals', 'final_pool_size', 'expansion_probes', 'deterministic']:
        assert a[key] == b[key]
    summary['latency_ms']['stress_'+name] = {'before': a['latency_ms'], 'after': b['latency_ms']}

before, after = read('real-before'), read('real-after')
assert before['regressions'] == after['regressions']
for a,b in zip(before['queries'], after['queries']):
    assert {key: value for key,value in a.items() if key not in ['baseline_ms','latency_ms']} == {key: value for key,value in b.items() if key not in ['baseline_ms','latency_ms']}
summary['real_ingest_regressions_equal'] = True

summary['status_latency'] = {side: read('status-'+side) for side in ['before','after']}
summary['precise_updates'] = {}
for side in ['before','after']:
    data = [json.loads(line) for line in (root/('updates-'+side+'.json')).read_text().splitlines()]
    assert all(record.get('pass', True) for record in data)
    for record in data:
        if record['phase'] in ['session_append_suffix', 'session_changed_one_of_eight']:
            summary['precise_updates'].setdefault(record['phase'], {})[side] = {key: record.get(key) for key in ['pass','server_counters','source_read_ratio','total_reconciliation_elapsed_ms','p50_search_during_ingestion_ms','p95_search_during_ingestion_ms','search_probe_samples']}
for phase, values in summary['precise_updates'].items():
    assert values['before']['server_counters'] == values['after']['server_counters'], phase
(root/'comparison.json').write_text(json.dumps(summary, indent=2)+'\n')
print(json.dumps(summary, indent=2))
