#!/usr/bin/env python3
"""Produce a temporary, content-bearing corpus and a content-free manifest."""
import argparse
import collections
import datetime
import functools
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

LIMIT = 16384

@functools.lru_cache(None)
def association(cwd):
    if not Path(cwd).is_dir():
        return ('missing_directory', None)
    p = subprocess.run(['git', '-C', cwd, 'rev-parse', '--path-format=absolute', '--git-common-dir'], capture_output=True, text=True)
    if p.returncode:
        return ('existing_nongit_directory', None)
    return ('resolved_git', str(Path(p.stdout.strip()).resolve()))

def records(path, stats):
    with path.open('rb') as stream:
        for raw in stream:
            stats['bytes_read'] += len(raw)
            if not raw.endswith(b'\n'):
                stats['unterminated_lines_deferred'] += 1
                continue
            try:
                record = json.loads(raw)
            except (ValueError, UnicodeDecodeError):
                stats['malformed_lines'] += 1
                continue
            if isinstance(record, dict):
                stats['records'] += 1
                yield record

def cwd_for(source, r):
    if source == 'codex' and r.get('type') in ('session_meta', 'turn_context'):
        return r.get('payload', {}).get('cwd')
    if source == 'claude':
        return r.get('cwd')
    if source == 'copilot' and r.get('type') in ('session.start', 'session.resume'):
        return r.get('data', {}).get('context', {}).get('cwd')

def text_blocks(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, list):
        for b in value:
            if isinstance(b, dict) and b.get('type') in ('text', 'input_text', 'output_text') and isinstance(b.get('text'), str):
                yield b['text']

def argument(value):
    return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False) if value is not None else ''

def pieces(source, r, own_names, own_calls, stats):
    t = r.get('type')
    if source == 'codex':
        if t != 'response_item':
            return
        p = r.get('payload', {})
        t = p.get('type')
        if t == 'message' and p.get('role') in ('user', 'assistant'):
            if p.get('channel') == 'analysis':
                stats['analysis_messages_excluded'] += 1
                return
            yield from text_blocks(p.get('content'))
        elif t in ('function_call', 'custom_tool_call'):
            name = p.get('name', '')
            if name in own_names:
                own_calls.add(p.get('call_id'))
            elif name.split('__')[-1] in ('search_project', 'search_sessions'):
                stats['unmapped_search_tool_calls'] += 1
            yield name + '\n' + argument(p.get('arguments', p.get('input')))
        elif t in ('function_call_output', 'custom_tool_call_output'):
            if p.get('call_id') in own_calls:
                stats['own_tool_results_excluded'] += 1
            else:
                yield from text_blocks(p.get('output'))
    elif source == 'claude':
        m = r.get('message', {})
        if t not in ('user', 'assistant') or m.get('role') not in ('user', 'assistant'):
            return
        content = m.get('content')
        if isinstance(content, str):
            yield content
        elif isinstance(content, list):
            for b in content:
                if not isinstance(b, dict):
                    continue
                if b.get('type') == 'text':
                    yield b.get('text', '')
                elif b.get('type') == 'tool_use':
                    name = b.get('name', '')
                    if name in own_names:
                        own_calls.add(b.get('id'))
                    elif name.split('__')[-1] in ('search_project', 'search_sessions'):
                        stats['unmapped_search_tool_calls'] += 1
                    yield name + '\n' + argument(b.get('input'))
                elif b.get('type') == 'tool_result':
                    if b.get('tool_use_id') in own_calls:
                        stats['own_tool_results_excluded'] += 1
                    else:
                        yield from text_blocks(b.get('content'))
    else:
        d = r.get('data', {})
        if t in ('user.message', 'assistant.message'):
            yield from text_blocks(d.get('content'))
        elif t == 'tool.execution_start':
            name = d.get('toolName', '')
            identity = d.get('mcpServerName', '') + '::' + d.get('mcpToolName', '')
            if name in own_names or identity in own_names:
                own_calls.add(d.get('toolCallId'))
            elif name.split('__')[-1] in ('search_project', 'search_sessions') or d.get('mcpToolName') in ('search_project', 'search_sessions'):
                stats['unmapped_search_tool_calls'] += 1
            yield name + '\n' + argument(d.get('arguments'))
        elif t == 'tool.execution_complete':
            if d.get('toolCallId') in own_calls:
                stats['own_tool_results_excluded'] += 1
                return
            result = d.get('result', {})
            if isinstance(result, dict):
                # Equal summary and detail represent the same output; preserve distinct fields.
                seen = set()
                for key in ('content', 'detailedContent'):
                    for text in text_blocks(result.get(key)):
                        if text not in seen:
                            seen.add(text)
                            yield text

def dedup_key(source, r):
    if source == 'codex':
        if r.get('type') != 'response_item':
            return None
        p = r.get('payload', {})
        identity = p.get('id') or p.get('call_id')
        if identity:
            return (p.get('type'), identity, hashlib.sha256(json.dumps(p, sort_keys=True).encode()).digest())
    else:
        identity = r.get('uuid') if source == 'claude' else r.get('id')
        if identity:
            return (identity, hashlib.sha256(json.dumps(r, sort_keys=True).encode()).digest())

def chunks(text):
    encoded = text.encode('utf-8')
    start = 0
    while start < len(encoded):
        end = min(start + LIMIT, len(encoded))
        while end < len(encoded) and encoded[end] & 0xc0 == 0x80:
            end -= 1
        yield encoded[start:end].decode('utf-8')
        start = end

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repo', type=Path, default=Path.home()/'Projects/t3code')
    parser.add_argument('--own-tool-name', action='append', default=[], help='Explicit provider-mapped tool identity for this MCP only; Copilot may use server::tool.')
    args = parser.parse_args()
    state, target = association(str(args.repo.resolve()))
    if state != 'resolved_git':
        raise SystemExit('Benchmark repository must exist and resolve to a Git common directory.')
    roots = {
        'codex': (Path(os.environ.get('CODEX_HOME', str(Path.home()/'.codex')))/'sessions', '**/*.jsonl'),
        'claude': (Path(os.environ.get('CLAUDE_CONFIG_DIR', str(Path.home()/'.claude')))/'projects', '**/*.jsonl'),
        'copilot': (Path(os.environ.get('COPILOT_HOME', str(Path.home()/'.copilot')))/'session-state', '**/events.jsonl'),
    }
    fd, temp = tempfile.mkstemp(prefix='bm25-session-corpus-', suffix='.jsonl')
    report = {'generated_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'repo': args.repo.name, 'temporary_corpus': temp, 'max_chunk_bytes': LIMIT, 'sources': {}, 'limits': [
        'Canonical text only; reasoning, system metadata, images, encrypted fields, compaction summaries, external_tool variants, and attachments omitted.',
        'Codex response_item only; repeated identifiable equal payloads deduplicated per file, distinct outputs retained. No cross-file fork deduplication.',
        'Deleted or unresolved directories excluded; ambiguous files spanning multiple repository identities excluded. No inferred ownership.',
        'Complete newline-terminated records only. Sequential two-pass reads are not snapshots; live growth or replacement is counted.',
        'No MCP identity is guessed from a bare search tool name. Own-output exclusion requires explicit --own-tool-name mapping; unmapped candidates are counted.',
        'Source discovery and association counts cover all logs; extraction, duplicate, own-tool and chunk counts cover matched repository logs only.',
        'Input records streamed one line at a time; maximum individual JSON record size is not capped. UTF-8 chunking never truncates long outputs.',
        'macOS observations only. Standard active-session roots only; archive/profile coverage unvalidated.'
    ]}
    file_id = 0
    repository_bytes = collections.Counter()
    with os.fdopen(fd, 'w') as output:
        for source, (root, pattern) in roots.items():
            stats = collections.Counter({key: 0 for key in ('files_without_cwd', 'files_with_missing_directory', 'files_with_existing_nongit_directory', 'files_with_resolved_git', 'matched_files', 'read_errors', 'unmapped_search_tool_calls', 'own_tool_results_excluded', 'duplicate_records_skipped')})
            paths = sorted(root.glob(pattern))
            stats['files_discovered'] = len(paths)
            for path in paths:
                scan = collections.Counter()
                try:
                    before = path.stat()
                    cwds = set()
                    for r in records(path, scan):
                        cwd = cwd_for(source, r)
                        if isinstance(cwd, str) and cwd:
                            cwds.add(cwd)
                    stats['source_bytes_scanned'] += scan['bytes_read']
                    stats['source_records_scanned'] += scan['records']
                    stats['malformed_lines'] += scan['malformed_lines']
                    stats['unterminated_lines_deferred'] += scan['unterminated_lines_deferred']
                    states = [association(cwd) for cwd in cwds]
                    stats['files_without_cwd'] += not states
                    for kind in {s[0] for s in states}:
                        stats['files_with_' + kind] += 1
                    identities = {s[1] for s in states if s[1]}
                    stats['files_multiple_git_identities'] += len(identities) > 1
                    if len(identities) == 1 and all(s[0] == 'resolved_git' for s in states):
                        common = Path(next(iter(identities)))
                        repository_bytes[common.parent.name if common.name == '.git' else common.name] += before.st_size
                    if identities != {target} or any(s[0] != 'resolved_git' for s in states):
                        continue
                    stats['matched_files'] += 1
                    stats['matched_source_bytes'] += before.st_size
                    seen, own_calls = set(), set()
                    for r in records(path, collections.Counter()):
                        key = dedup_key(source, r)
                        if key and key in seen:
                            stats['duplicate_records_skipped'] += 1
                            continue
                        if key:
                            seen.add(key)
                        for text in pieces(source, r, set(args.own_tool_name), own_calls, stats):
                            if not isinstance(text, str) or not text.strip():
                                continue
                            stats['canonical_text_fields'] += 1
                            for piece in chunks(text):
                                output.write(json.dumps({'file_id': file_id, 'text': piece}, ensure_ascii=False) + '\n')
                                stats['chunks'] += 1
                                stats['text_bytes'] += len(piece.encode())
                    after = path.stat()
                    stats['matched_files_changed_during_read'] += (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns)
                    file_id += 1
                except OSError:
                    stats['read_errors'] += 1
            report['sources'][source] = dict(stats)
    report['repository_source_bytes_by_short_name'] = dict(repository_bytes.most_common())
    report['corpus_jsonl_bytes'] = Path(temp).stat().st_size
    report['total_chunks'] = sum(s.get('chunks', 0) for s in report['sources'].values())
    report['total_text_bytes'] = sum(s.get('text_bytes', 0) for s in report['sources'].values())
    Path(__file__).with_name('session-corpus-manifest.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))

if __name__ == '__main__':
    main()
