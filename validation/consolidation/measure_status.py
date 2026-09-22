"""Disposable, phase-observed MCP status latency during session publication."""
import json
import os
from pathlib import Path
import statistics
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
from acceptance import Client, STATUS_URI, is_settled, settle

binary = Path(sys.argv[1]).resolve()
with tempfile.TemporaryDirectory() as directory:
    base = Path(directory)
    root = base / 'project'
    root.mkdir()
    (root / 'anchor.txt').write_text('anchor')
    codex = base / 'codex'
    (codex / 'sessions').mkdir(parents=True)
    env = dict(os.environ, CODEX_HOME=str(codex), CLAUDE_CONFIG_DIR=str(base / 'claude'), COPILOT_HOME=str(base / 'copilot'))
    client = Client(binary, root, base / 'cache', env)
    try:
        settle(client, 'search_project', 'anchor', poll_interval=.02)
        settle(client, 'search_sessions', 'statusmarker', poll_interval=.02)
        records = [json.dumps({'type': 'session_meta', 'payload': {'cwd': str(root), 'id': 'status-fixture'}})]
        for index in range(4000):
            records.append(json.dumps({'type': 'response_item', 'payload': {'type': 'message', 'id': str(index), 'role': 'user', 'content': [{'type': 'text', 'text': f'statusmarker{index} ' + 'payload ' * 64}]}}))
        (codex / 'sessions' / 'status.jsonl').write_text('\n'.join(records) + '\n')
        samples = []
        deadline = time.monotonic() + 90
        active = False
        while time.monotonic() < deadline:
            start = time.perf_counter()
            response = client.rpc('resources/read', {'uri': STATUS_URI}, timeout=10)
            elapsed = (time.perf_counter() - start) * 1000
            state = json.loads(response['contents'][0]['text'])['sessions']
            phase = state['progress']['phase']
            samples.append({'phase': phase, 'ms': elapsed})
            active |= not is_settled(state)
            if active and is_settled(state):
                break
            time.sleep(.005)
        else:
            raise TimeoutError('status sampling did not settle')
        settle(client, 'search_sessions', 'statusmarker3999', poll_interval=.02)
        def summary(values):
            values = sorted(values)
            return {'samples': len(values), 'p50_ms': statistics.median(values) if values else None, 'p95_ms': values[round((len(values)-1)*.95)] if values else None, 'max_ms': max(values) if values else None}
        durable = [sample['ms'] for sample in samples if sample['phase'] == 'durable_transaction']
        assert durable, 'No durable-transaction phase was observed'
        print(json.dumps({'records': 4000, 'poll_interval_seconds': .005, 'all': summary([sample['ms'] for sample in samples]), 'during_durable_transaction': summary(durable), 'phase_samples': {phase: sum(sample['phase'] == phase for sample in samples) for phase in sorted(set(sample['phase'] for sample in samples))}, 'limits': 'Phase is sampled at response time; this is a shared developer machine, not a blocked-writer latency guarantee.'}))
    finally:
        client.close()
