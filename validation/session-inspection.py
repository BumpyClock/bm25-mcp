#!/usr/bin/env python3
"""Read session logs and report aggregate schemas without retaining content."""
import collections
import datetime
import json
import os
import pathlib
import re

DEST = pathlib.Path(__file__).with_name('session-findings.json')
SAFE = re.compile(r'^[A-Za-z_][A-Za-z0-9_.-]{0,79}$')
CONTAINERS = {'payload', 'message', 'data', 'context', 'content', 'metadata', 'session', 'git', 'toolRequests', 'tool_calls', 'function', 'item', 'result', 'output'}
ASSOCIATION = {'cwd', 'workingDirectory', 'working_directory', 'projectPath', 'project_path', 'repository', 'repositoryPath', 'repository_path', 'gitRoot', 'git_root'}
PAIRING = {'call_id', 'tool_call_id', 'toolCallId', 'toolUseId', 'tool_use_id', 'parentUuid', 'parentId', 'id', 'uuid'}

def kind(value):
    if value is None: return 'null'
    if isinstance(value, bool): return 'boolean'
    if isinstance(value, dict): return 'object'
    if isinstance(value, list): return 'array'
    if isinstance(value, str): return 'string'
    return 'number'

def scan(name, root, pattern):
    paths = sorted(root.glob(pattern)) if root.exists() else []
    counters = {key: collections.Counter() for key in ['event_types', 'nested_types', 'roles', 'schema_fields', 'association_fields', 'pairing_fields']}
    stats = dict(files_discovered=len(paths), files_read=0, files_changed_during_read=0, bytes_read=0, valid_records=0, malformed_lines=0, malformed_final_lines=0, complete_unterminated_final_lines=0, blank_lines=0, nonobject_records=0, files_with_association=0, records_with_association=0, read_errors=0)
    files_by_event = collections.Counter()
    associations_by_event = collections.Counter()
    pairing = collections.Counter()
    association_variants = collections.Counter()
    def enum(value):
        return value if isinstance(value, str) and SAFE.fullmatch(value) else '<other>'
    def walk(obj, prefix='', depth=0):
        found = False
        if not isinstance(obj, dict): return found
        for key, value in obj.items():
            field = key if isinstance(key, str) and SAFE.fullmatch(key) else '<other-key>'
            field = f'{prefix}.{field}' if prefix else field
            counters['schema_fields'][f'{field}:{kind(value)}'] += 1
            if key in ASSOCIATION and isinstance(value, str) and value:
                counters['association_fields'][field] += 1
                found = True
            if key in PAIRING:
                counters['pairing_fields'][field] += 1
            if key == 'role': counters['roles'][f'{field}={enum(value)}'] += 1
            if key == 'type' and prefix: counters['nested_types'][f'{field}={enum(value)}'] += 1
            if depth < 4 and key in CONTAINERS:
                if isinstance(value, dict): found = walk(value, field, depth+1) or found
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict): found = walk(item, field+'[]', depth+1) or found
        return found
    for path in paths:
        associated = False
        events = set()
        call_ids, result_ids, association_values = set(), set(), set()
        item_ids = collections.defaultdict(set)
        try:
            before = path.stat()
            with path.open('rb') as stream:
                for raw in stream:
                    stats['bytes_read'] += len(raw)
                    if not raw.strip():
                        stats['blank_lines'] += 1
                        continue
                    try: record = json.loads(raw)
                    except (ValueError, UnicodeDecodeError):
                        stats['malformed_lines'] += 1
                        if not raw.endswith(b'\n'): stats['malformed_final_lines'] += 1
                        continue
                    stats['valid_records'] += 1
                    if not raw.endswith(b'\n'): stats['complete_unterminated_final_lines'] += 1
                    if not isinstance(record, dict):
                        stats['nonobject_records'] += 1
                        continue
                    event = enum(record.get('type', '<missing>'))
                    counters['event_types'][event] += 1
                    events.add(event)
                    if name == 'codex':
                        payload = record.get('payload', {})
                        if isinstance(payload, dict):
                            if event in ('session_meta', 'turn_context') and isinstance(payload.get('cwd'), str): association_values.add(payload['cwd'])
                            if event == 'response_item':
                                subtype = payload.get('type')
                                if subtype in ('function_call', 'custom_tool_call') and isinstance(payload.get('call_id'), str): call_ids.add(payload['call_id'])
                                if subtype in ('function_call_output', 'custom_tool_call_output') and isinstance(payload.get('call_id'), str): result_ids.add(payload['call_id'])
                                if isinstance(payload.get('id'), str): item_ids['response_item'].add(payload['id'])
                            if event == 'event_msg' and payload.get('type') == 'item_completed' and isinstance(payload.get('item'), dict) and isinstance(payload['item'].get('id'), str): item_ids['item_completed'].add(payload['item']['id'])
                    elif name == 'claude':
                        if isinstance(record.get('cwd'), str): association_values.add(record['cwd'])
                        message = record.get('message', {})
                        if isinstance(message, dict) and isinstance(message.get('content'), list):
                            for block in message['content']:
                                if not isinstance(block, dict): continue
                                if block.get('type') == 'tool_use' and isinstance(block.get('id'), str): call_ids.add(block['id'])
                                if block.get('type') == 'tool_result' and isinstance(block.get('tool_use_id'), str): result_ids.add(block['tool_use_id'])
                    elif name == 'copilot':
                        data = record.get('data', {})
                        if isinstance(data, dict):
                            context = data.get('context', {})
                            if event in ('session.start', 'session.resume') and isinstance(context, dict) and isinstance(context.get('cwd'), str): association_values.add(context['cwd'])
                            if event == 'tool.execution_start' and isinstance(data.get('toolCallId'), str): call_ids.add(data['toolCallId'])
                            if event == 'tool.execution_complete' and isinstance(data.get('toolCallId'), str): result_ids.add(data['toolCallId'])
                    has_association = walk(record)
                    if has_association:
                        stats['records_with_association'] += 1
                        associations_by_event[event] += 1
                        associated = True
            after = path.stat()
            stats['files_changed_during_read'] += (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns)
            stats['files_read'] += 1
            stats['files_with_association'] += associated
            files_by_event.update(events)
            pairing.update({'unique_call_ids_per_file_sum': len(call_ids), 'unique_result_ids_per_file_sum': len(result_ids), 'paired_ids_per_file_sum': len(call_ids & result_ids), 'calls_without_result': len(call_ids-result_ids), 'results_without_call': len(result_ids-call_ids), 'codex_item_ids_in_both_event_streams': len(item_ids['response_item'] & item_ids['item_completed'])})
            association_variants[str(len(association_values))] += 1
        except OSError:
            stats['read_errors'] += 1
    return {'discovery_root_exists': root.exists(), 'coverage': 'All discovered matching files streamed in full; live files are not atomic snapshots.', **stats, **{key: dict(sorted(value.items())) for key, value in counters.items()}, 'files_by_event': dict(sorted(files_by_event.items())), 'associations_by_event': dict(sorted(associations_by_event.items())), 'pairing_validation': dict(pairing), 'files_by_distinct_authoritative_cwd_count': dict(association_variants)}

sources = {
    'codex': (pathlib.Path(os.environ.get('CODEX_HOME', str(pathlib.Path.home()/'.codex')))/'sessions', '**/*.jsonl'),
    'claude': (pathlib.Path(os.environ.get('CLAUDE_CONFIG_DIR', str(pathlib.Path.home()/'.claude')))/'projects', '**/*.jsonl'),
    'copilot': (pathlib.Path(os.environ.get('COPILOT_HOME', str(pathlib.Path.home()/'.copilot')))/'session-state', '**/events.jsonl'),
}
report = {'generated_at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'privacy': 'No message content, tool argument/output values, source paths, filenames, or identifier values retained.', 'platform': 'macOS only; Windows discovery, path canonicalization, file watching, and live-write behavior untested.', 'sources': {name: scan(name, *config) for name, config in sources.items()}}
report['importer_contract_findings'] = {
    'verdict': 'All three observed formats are parseable and contain project-attribution and tool-pairing evidence; production importer correctness remains unproven.',
    'codex': [
        'Use session_meta.payload.cwd and turn_context.payload.cwd as attribution evidence; tool-specific cwd is not authoritative session ownership.',
        'response_item.payload has role/content messages and function/custom call/output variants, joined by call_id.',
        'event_msg item_completed and response_item share item IDs: blindly indexing both duplicates content.',
        'Handle compaction, forks, subagents, encrypted content, array/string outputs and unknown event variants explicitly. Do not claim encrypted content is searchable.'
    ],
    'claude': [
        'User/assistant records have cwd, sessionId, uuid, parentUuid; message.content is either string or typed block array.',
        'tool_use.id links to tool_result.tool_use_id; preserve tool results as tool output even when wrapped in a user role.',
        'Only two local files were available: sidechains, subagents, restored branches and older versions remain insufficiently validated.'
    ],
    'copilot': [
        'session.start/resume data.context.cwd provides attribution evidence.',
        'user.message and assistant.message carry data.content; tool.execution_start/complete join by data.toolCallId.',
        'Assistant toolRequests overlaps execution-start data: avoid duplicated call text; completed results contain content and detailedContent.',
        'One observed call has no completion: unfinished or interrupted tools must remain representable.'
    ],
    'incremental_reader': [
        'Use per-file byte offsets and file identity; defer unterminated tails, detect truncation/replacement, and persist checkpoints with index updates.',
        'No malformed or partial lines were observed; this is not validation of crash, rotation, truncation, or concurrent-write recovery.',
        'Logs are read sequentially, not an atomic global snapshot; current sessions can grow between runs.'
    ],
    'project_scope_limitations': [
        'Every inspected file had exactly one distinct authoritative cwd. Files spanning projects were not observed.',
        'Cwd presence alone does not prove current filesystem existence or Git ownership; resolve Git common-dir for shared worktree history and define moved/deleted repo behavior.',
        'No repository path values, actual common-dir grouping, or Windows filesystem semantics were tested.'
    ],
    'discovery_limitations': [
        'Inspector honors CODEX_HOME, CLAUDE_CONFIG_DIR and COPILOT_HOME when set; official support and semantics of each environment variable were not established here.',
        'Coverage is only matching active-session JSONL files in selected roots; archive roots, alternate profiles and additional stores were not scanned.'
    ]
}
DEST.write_text(json.dumps(report, indent=2)+'\n')
print(json.dumps({name: {key: value for key, value in source.items() if key in ['files_discovered', 'files_read', 'bytes_read', 'valid_records', 'files_with_association', 'malformed_lines', 'files_changed_during_read']} for name, source in report['sources'].items()}, indent=2))
