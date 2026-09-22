#!/usr/bin/env python3
"""Exercise isolated project and session updates with bounded MCP probes."""

import argparse
import hashlib
import json
import os
import platform
from pathlib import Path
import statistics
import threading
import subprocess
import tempfile
import time

from acceptance import (
    Client,
    STATUS_URI,
    debug_trace,
    emit,
    public_coverage,
    public_progress,
    public_status,
    settle,
)

SESSION_SOURCE_COUNT = 8
PROGRESS_COUNTER_FIELDS = (
    'files_discovered',
    'files_completed',
    'records_processed',
    'records_inspected',
    'bytes_read',
    'bytes_hashed_for_verification',
    'chunks_prepared',
    'chunks_committed',
)


def status_document(client, kind, timeout):
    response = client.rpc('resources/read', {'uri': STATUS_URI}, timeout=timeout)
    if not isinstance(response, dict) or not isinstance(response.get('contents'), list):
        raise RuntimeError('invalid status response')
    if not response['contents'] or not isinstance(response['contents'][0], dict):
        raise RuntimeError('invalid status response')
    text = response['contents'][0].get('text')
    if not isinstance(text, str):
        raise RuntimeError('invalid status response')
    document = json.loads(text)
    state = document.get(kind) if isinstance(document, dict) else None
    if not isinstance(state, dict):
        raise RuntimeError('invalid status response')
    return state


def status_epoch(state):
    coverage = state.get('coverage') or {}
    progress = state.get('progress') or {}
    values = []
    for key in ('safe_epoch', 'epoch'):
        if key in coverage:
            values.append((key, coverage[key]))
    if 'reconciled_at' in coverage:
        values.append(('reconciled_at', coverage['reconciled_at']))
    if 'run_id' in progress:
        values.append(('run_id', progress['run_id']))
    return tuple(values)


def reconciliation_marker(state):
    coverage = state.get('coverage') or {}
    return status_epoch(state), coverage.get('reconciled_at')


def scan_change_marker(state):
    return reconciliation_marker(state), (state.get('progress') or {}).get(
        'run_id'
    )


def changed_since(baseline):
    baseline_marker = scan_change_marker(baseline)
    if not any(value for value in baseline_marker):
        return None

    def predicate(state):
        return scan_change_marker(state) != baseline_marker

    return predicate


def settle_after_new_run(client, tool, query, baseline, timeout, arguments=None):
    observations = []
    tracker = {
        'run_changed': False,
        'epoch_changed': False,
        'reconciled_at_changed': False,
        'new_scan_observed': False,
        'active_refresh_observed': False,
    }
    baseline_epoch = status_epoch(baseline)
    baseline_reconciled_at = (baseline.get('coverage') or {}).get(
        'reconciled_at'
    )
    baseline_run_id = safe_run_id(public_progress(baseline))

    def observe(state):
        observations.append(state)
        if baseline_epoch and status_epoch(state) != baseline_epoch:
            tracker['epoch_changed'] = True
        current_reconciled_at = (state.get('coverage') or {}).get(
            'reconciled_at'
        )
        if (
            baseline_reconciled_at is not None
            and current_reconciled_at is not None
            and current_reconciled_at != baseline_reconciled_at
        ):
            tracker['reconciled_at_changed'] = True
        current_run_id = safe_run_id(public_progress(state))
        if (
            baseline_run_id is not None
            and current_run_id is not None
            and current_run_id != baseline_run_id
        ):
            tracker['run_changed'] = True
        tracker['new_scan_observed'] = tracker['epoch_changed']
        progress = public_progress(state)
        if (
            state.get('status') in ('building', 'refreshing')
            or progress.get('phase') not in (
                None,
                'idle',
                'complete',
                'cancelled',
                'failed',
            )
        ):
            tracker['active_refresh_observed'] = True

    def new_run_observed(_state):
        return tracker['epoch_changed']

    result, elapsed = settle(
        client,
        tool,
        query,
        timeout=timeout,
        arguments=arguments,
        observer=observe,
        status_predicate=new_run_observed,
    )
    return (
        result,
        elapsed,
        observations,
        tracker,
        observations[-1] if observations else {},
    )


def counter_snapshot(progress):
    return {
        key: progress[key]
        for key in PROGRESS_COUNTER_FIELDS
        if key in progress
    }


def counter_delta(before, after):
    result = {}
    for key in PROGRESS_COUNTER_FIELDS:
        if key not in before or key not in after or after[key] < before[key]:
            continue
        result[key] = after[key] - before[key]
    return result


def record_run_sample(runs, state):
    progress = public_progress(state)
    run_id = safe_run_id(progress)
    if run_id is None:
        return
    row = runs.setdefault(
        str(run_id),
        {'run_id': run_id, 'samples': 0},
    )
    row['samples'] += 1
    for key in PROGRESS_COUNTER_FIELDS:
        value = progress.get(key)
        if type(value) not in (int, float):
            continue
        row[key] = max(row.get(key, 0), value)


def run_metric_rows(states):
    runs = {}
    for state in states:
        record_run_sample(runs, state)
    return sorted(runs.values(), key=lambda row: row['run_id'])


def merge_run_metric_rows(existing, states):
    runs = {}
    for row in existing:
        run_id = safe_run_id(row)
        if run_id is None:
            continue
        copied = dict(row)
        runs[str(run_id)] = copied
    for state in states:
        record_run_sample(runs, state)
    return sorted(runs.values(), key=lambda row: row['run_id'])


def precise_scope_pass(progress):
    return (
        progress.get('files_discovered') == 1
        and progress.get('files_completed') == 1
        and progress.get('records_inspected') == 2
        and progress.get('records_processed') == 2
        and progress.get('chunks_prepared', 0)
        >= progress.get('chunks_committed', 0)
        > 0
    )


def wait_status_ready(client, kind, timeout=30):
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('status did not settle')
        state = status_document(client, kind, timeout=remaining)
        coverage = state.get('coverage') or {}
        if (
            state.get('status') in ('ready', 'degraded')
            and coverage.get('pending_changes') == 0
        ):
            return state
        time.sleep(min(1, remaining))


def phase_was_observed(states, phase, final_progress=None):
    progress_values = [public_progress(state) for state in states]
    if final_progress is not None:
        progress_values.append(final_progress)
    for progress in progress_values:
        if progress.get('phase') == phase:
            return True
        if phase in (progress.get('phase_timings_ms') or {}):
            return True
        if any(
            entry.get('phase') == phase
            for entry in progress.get('phase_history', [])
            if isinstance(entry, dict)
        ):
            return True
        if any(
            entry.get('phase') == phase
            for entry in (progress.get('phase_transitions') or {}).get(
                'history', []
            )
            if isinstance(entry, dict)
        ):
            return True
    return False


def result_contains_marker(result, marker):
    needle = marker.casefold()
    return any(
        isinstance(hit, dict)
        and needle in str(hit.get('excerpt', '')).casefold()
        for hit in result.get('results', [])
    )


def safe_run_id(progress):
    value = progress.get('run_id')
    return value if type(value) in (int, float) and value >= 0 else None


def write_session_source(path, session_id, cwd, text, event_id=None):
    with path.open('w') as file:
        file.write(json.dumps(session_meta(session_id, cwd)) + '\n')
        file.write(
            json.dumps(session_record(session_id, cwd, text, event_id=event_id))
            + '\n'
        )
        file.flush()
        os.fsync(file.fileno())
    path.chmod(0o600)


def append_session_events(path, session_id, cwd, events):
    bytes_written = 0
    with path.open('a') as file:
        for event_id, text in events:
            line = json.dumps(
                session_record(session_id, cwd, text, event_id=event_id)
            ) + '\n'
            file.write(line)
            bytes_written += len(line.encode())
        file.flush()
        os.fsync(file.fileno())
    path.chmod(0o600)
    return bytes_written


def wait_marker(
    client,
    tool,
    marker,
    count=1,
    timeout=90,
    arguments=None,
    path_glob=None,
    baseline=None,
):
    deadline = time.monotonic() + timeout
    arguments = dict(arguments or {})
    arguments.update(query=marker)
    if path_glob:
        arguments['path_glob'] = path_glob
    predicate = changed_since(baseline) if baseline is not None else None
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('update did not converge')
        result, _ = settle(
            client,
            tool,
            marker,
            timeout=remaining,
            arguments={key: value for key, value in arguments.items() if key != 'query'},
            status_predicate=predicate,
        )
        matching_paths = {
            hit.get('relative_path')
            for hit in result.get('results', [])
            if isinstance(hit, dict)
            and marker.casefold() in str(hit.get('excerpt', '')).casefold()
        }
        if (
            result.get('status') in ('ready', 'degraded')
            and len(matching_paths) >= count
            and result_contains_marker(result, marker)
        ):
            return result
        time.sleep(min(.25, max(0, deadline - time.monotonic())))


def safe_percentile(samples, fraction):
    if not samples:
        return None
    ordered = sorted(samples)
    return ordered[min(len(ordered) - 1, int(len(ordered) * fraction))]


def session_record(session_id, cwd, text, event_id=None):
    return {
        'type': 'response_item',
        'payload': {
            'id': event_id or session_id,
            'type': 'message',
            'role': 'user',
            'content': [{'type': 'input_text', 'text': text}],
            'cwd': cwd,
        },
    }


def session_meta(session_id, cwd):
    return {
        'type': 'session_meta',
        'payload': {'id': session_id, 'cwd': cwd},
    }


def start_session_probes(
    search_client,
    status_client,
    marker,
    stop,
    started,
    baseline=None,
):
    baseline_marker = scan_change_marker(baseline) if baseline is not None else None
    baseline_progress = public_progress(baseline) if baseline is not None else {}
    state = {
        'latencies': [],
        'first_progress_seconds': None,
        'first_progress': None,
        'first_searchable_seconds': None,
        'independently_sampled_chunks_committed': None,
        'latest_progress': {},
        'run_metrics': {},
        'errors': 0,
        'status_probe_failed': False,
        'search_probe_failed': False,
        'probe_failed': False,
    }
    lock = threading.Lock()

    def status_probe():
        while not stop.is_set():
            try:
                snapshot = status_document(status_client, 'sessions', timeout=.75)
                progress = public_progress(snapshot)
                with lock:
                    state['latest_progress'] = progress
                    record_run_sample(state['run_metrics'], snapshot)
                current_run_id = safe_run_id(progress)
                counter_changed = any(
                    type(progress.get(field)) in (int, float)
                    and progress.get(field) > baseline_progress.get(field, -1)
                    for field in PROGRESS_COUNTER_FIELDS
                )
                marker_changed = (
                    baseline_marker is not None
                    and scan_change_marker(snapshot) != baseline_marker
                )
                fresh_owner_progress = (
                    baseline_marker is None
                    and current_run_id is not None
                    and current_run_id > 0
                    and (
                        progress.get('first_progress_ms') is not None
                        or any(
                            progress.get(field, 0) > 0
                            for field in PROGRESS_COUNTER_FIELDS
                        )
                        or progress.get('phase') not in (None, 'idle', 'waiting')
                    )
                )
                new_work_observed = (
                    marker_changed or counter_changed or fresh_owner_progress
                )
                has_work = any(
                    progress.get(field, 0) > 0
                    for field in (
                        'files_discovered',
                        'files_completed',
                        'records_processed',
                        'bytes_read',
                        'chunks_prepared',
                        'chunks_committed',
                    )
                )
                if new_work_observed and (
                    progress.get('phase') not in (None, 'idle', 'waiting')
                    or has_work
                    or fresh_owner_progress
                ):
                    with lock:
                        if state['first_progress_seconds'] is None:
                            state['first_progress_seconds'] = time.monotonic() - started
                            state['first_progress'] = progress
            except (TimeoutError, RuntimeError, OSError, ValueError, KeyError, TypeError):
                with lock:
                    state['errors'] += 1
                    state['status_probe_failed'] = True
                    state['probe_failed'] = True
                stop.set()
                return
            stop.wait(.25)

    def search_probe():
        while not stop.is_set():
            try:
                begin = time.perf_counter()
                result = search_client.tool(
                    'search_sessions',
                    {'query': marker, 'limit': 10},
                    timeout=.75,
                )
                elapsed = (time.perf_counter() - begin) * 1000
                with lock:
                    state['latencies'].append(elapsed)
                    if (
                        state['first_searchable_seconds'] is None
                        and result_contains_marker(result, marker)
                    ):
                        state['first_searchable_seconds'] = time.monotonic() - started
                        state['independently_sampled_chunks_committed'] = state[
                            'latest_progress'
                        ].get(
                            'chunks_committed'
                        )
            except (TimeoutError, RuntimeError, OSError, ValueError, KeyError, TypeError):
                with lock:
                    state['errors'] += 1
                    state['search_probe_failed'] = True
                    state['probe_failed'] = True
                stop.set()
                return
            stop.wait(.25)

    status_thread = threading.Thread(
        target=status_probe,
        name='session-status-probe',
        daemon=True,
    )
    search_thread = threading.Thread(
        target=search_probe,
        name='session-search-probe',
        daemon=True,
    )
    status_thread.start()
    search_thread.start()
    return state, lock, (status_thread, search_thread)


def finish_session_probes(stop, threads, state, lock):
    stop.set()
    for thread in threads:
        thread.join(timeout=2)
    with lock:
        snapshot = dict(state)
        snapshot['latencies'] = list(state['latencies'])
        snapshot['run_metrics'] = sorted(
            state['run_metrics'].values(),
            key=lambda row: row['run_id'],
        )
    return snapshot


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--huge-session-mib', type=int, default=64)
    parser.add_argument('--synthetic-session-records', type=int, default=1000)
    parser.add_argument('--synthetic-session-mib', type=int, default=4)
    parser.add_argument('--binary', type=Path)
    args = parser.parse_args()
    if args.huge_session_mib < 1:
        raise ValueError('huge session size must be positive')
    if not 1 <= args.synthetic_session_records <= 10000:
        raise ValueError('synthetic session records must be between 1 and 10000')
    if not 1 <= args.synthetic_session_mib <= 32:
        raise ValueError('synthetic session size must be between 1 and 32 MiB')

    binary = args.binary or Path(__file__).resolve().parents[1] / 'target' / 'release' / (
        'bm25-mcp.exe' if os.name == 'nt' else 'bm25-mcp'
    )
    emit(
        {
            'phase': 'environment',
            'platform': platform.system(),
            'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
            'huge_session_mib': args.huge_session_mib,
            'synthetic_session_records': args.synthetic_session_records,
            'synthetic_session_mib': args.synthetic_session_mib,
            'clients': 6,
            'search_probe_rate_hz': 4,
        }
    )
    with tempfile.TemporaryDirectory(
        prefix='.bm25-updates-',
        dir=Path.cwd(),
    ) as scratch:
        base = Path(scratch)
        root = base / 'project'
        root.mkdir()
        env = os.environ.copy()
        for name in ['CODEX_HOME', 'CLAUDE_CONFIG_DIR', 'COPILOT_HOME']:
            env[name] = str(base / name)
            (base / name).mkdir(mode=0o700)

        def git(*git_args):
            subprocess.run(
                ['git', '-C', str(root), *git_args],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                env=env,
            )

        git('init', '-q')
        git('config', 'user.name', 'Acceptance')
        git('config', 'user.email', 'acceptance@example.test')
        for i in range(100):
            (root / ('source%d.rs' % i)).write_text(
                (
                    'fn parseHTTPResponse() { /* exact error: request timed out */ }\n'
                    * 100
                )
                + 'branchalphamarker file%d\n' % i
            )
        git('add', '.')
        git('commit', '-qm', 'Initial acceptance fixture')
        git('branch', 'alpha')
        git('checkout', '-qb', 'beta')
        for i in range(10):
            (root / ('source%d.rs' % i)).write_text(
                'branchbetamarker\n' * 100 + 'file%d\n' % i
            )
        git('commit', '-qam', 'Second acceptance fixture')
        git('checkout', '-q', 'alpha')

        # Materialize the complete session fixture before any owner/client
        # starts. This makes the initial session run a genuine cold scan
        # rather than a later watcher-triggered revalidation.
        session_dir = Path(env['CODEX_HOME']) / 'sessions'
        session_dir.mkdir(mode=0o700, exist_ok=True)
        session_dir.chmod(0o700)
        session_cwd = str(root)
        first_marker = 'synthetic_session_first_marker'
        tail_marker = 'synthetic_session_tail_marker'
        early = session_dir / 'synthetic-early.jsonl'
        write_session_source(
            early,
            'synthetic-early',
            session_cwd,
            first_marker + ' synthetic early record',
        )
        source_paths = {}
        source_markers = {}
        for index in range(SESSION_SOURCE_COUNT):
            old_marker = f'synthetic_source_{index:02d}_old_marker'
            source_path = session_dir / f'synthetic-source-{index:02d}.jsonl'
            write_session_source(
                source_path,
                f'synthetic-source-{index:02d}',
                session_cwd,
                old_marker,
                event_id=f'synthetic-source-{index:02d}-old',
            )
            source_paths[index] = source_path
            source_markers[index] = old_marker
        slow = session_dir / 'synthetic-slow.jsonl'
        filler_size = max(
            256,
            (args.synthetic_session_mib * 1024 * 1024)
            // args.synthetic_session_records,
        )
        filler = 'synthetic_session_payload ' * (
            filler_size // len('synthetic_session_payload ') + 1
        )
        record_count = max(1, args.synthetic_session_records - 1)
        with slow.open('w') as file:
            file.write(json.dumps(session_meta('synthetic-slow', session_cwd)) + '\n')
            for index in range(record_count):
                marker = tail_marker if index == record_count - 1 else ''
                record = session_record(
                    'synthetic-slow',
                    session_cwd,
                    f'{marker} synthetic record {index} {filler[:filler_size]}',
                    event_id=f'synthetic-slow-{index}',
                )
                file.write(json.dumps(record) + '\n')
        slow.chmod(0o600)
        session_file_bytes = sum(
            path.stat().st_size for path in [early, slow, *source_paths.values()]
        )

        clients = []
        readers = []
        reader_errors = []
        stop = threading.Event()
        probe_stop = None
        probe_threads = ()
        passed = True

        def read_continuously(client):
            while not stop.is_set():
                try:
                    client.tool(
                        'search_project',
                        {'query': 'parseHTTPResponse'},
                        timeout=.75,
                    )
                except (TimeoutError, RuntimeError, OSError, ValueError, KeyError, TypeError):
                    reader_errors.append('reader_error')
                    return
                stop.wait(.25)

        def initial_status(client):
            return status_document(client, 'project', timeout=5)

        try:
            session_start = time.monotonic()
            clients = []
            for index in range(6):
                debug_trace('stage', stage='initial_client_initialize', client_index=index)
                clients.append(
                    Client(
                        binary,
                        root,
                        base / 'cache',
                        env,
                        label=f'initial-{index}',
                    )
                )
            probe_stop = threading.Event()
            probe_state, probe_lock, probe_threads = start_session_probes(
                clients[4],
                clients[5],
                first_marker,
                probe_stop,
                session_start,
            )
            debug_trace('stage', stage='initial_project_settle')
            initial, _ = settle(clients[0], 'search_project')
            emit(
                {
                    'phase': 'cold',
                    'status': public_status(initial.get('status')),
                    'coverage': public_coverage(initial.get('coverage')),
                    'progress': public_progress(initial),
                }
            )
            for client in clients[1:4]:
                reader = threading.Thread(
                    target=read_continuously,
                    args=(client,),
                    daemon=True,
                )
                reader.start()
                readers.append(reader)

            def wait_project_marker(
                marker,
                count=1,
                timeout=90,
                path_glob=None,
                baseline=None,
            ):
                return wait_marker(
                    clients[0],
                    'search_project',
                    marker,
                    count=count,
                    timeout=timeout,
                    path_glob=path_glob,
                    baseline=baseline,
                )

            append_baseline = initial_status(clients[0])
            append_start = time.monotonic()
            with (root / 'source99.rs').open('a') as file:
                file.write('smallappendmarker appended complete record\n')
            append_result = wait_project_marker(
                'smallappendmarker',
                timeout=90,
                baseline=append_baseline,
            )
            emit(
                {
                    'phase': 'project_small_append',
                    'seconds': time.monotonic() - append_start,
                    'bytes_changed': len(
                        'smallappendmarker appended complete record\n'.encode()
                    ),
                    'coverage': public_coverage(append_result.get('coverage')),
                    'progress': public_progress(append_result),
                }
            )

            for count in [1, 10]:
                marker = 'editmarker' + ('single' if count == 1 else 'batch')
                baseline = initial_status(clients[0])
                for i in range(count):
                    (
                        root / ('source%d.rs' % i)
                    ).write_text(
                        (marker + ' fn updatedFunction() {}\n') * 100
                        + 'file%d\n' % i
                    )
                start = time.monotonic()
                result = wait_project_marker(marker, count, baseline=baseline)
                elapsed = time.monotonic() - start
                target_pass = elapsed < 2
                passed = passed and target_pass
                emit(
                    {
                        'phase': 'ordinary_edit',
                        'files': count,
                        'seconds': elapsed,
                        'target_pass': target_pass,
                        'coverage': public_coverage(result.get('coverage')),
                        'progress': public_progress(result),
                        'peak_owner_rss_bytes': (
                            public_coverage(result.get('coverage'))
                            .get('memory', {})
                            .get('peak_observed_rss_bytes')
                        ),
                    }
                )

            git('reset', '--hard', '-q')
            for branch, marker in [
                ('beta', 'branchbetamarker'),
                ('alpha', 'branchalphamarker'),
                ('beta', 'branchbetamarker'),
            ]:
                baseline = initial_status(clients[0])
                git('checkout', '-q', branch)
                start = time.monotonic()
                result = wait_project_marker(
                    marker,
                    10,
                    path_glob='source?.rs',
                    baseline=baseline,
                )
                elapsed = time.monotonic() - start
                emit(
                    {
                        'phase': 'branch',
                        'branch': branch,
                        'seconds': elapsed,
                        'coverage': public_coverage(result.get('coverage')),
                        'progress': public_progress(result),
                    }
                )

            # Keep this bounded text fixture separate from the session
            # responsiveness probe; it exercises long-line chunking only.
            with (root / 'huge.txt').open('w') as file:
                for _ in range(4096):
                    file.write('ordinarytoken ' * 512)
                file.write(' giantfiletailmarker')
            result = wait_project_marker('giantfiletailmarker', timeout=180)
            emit(
                {
                    'phase': 'huge_text',
                    'bytes': (root / 'huge.txt').stat().st_size,
                    'coverage': public_coverage(result.get('coverage')),
                    'progress': public_progress(result),
                }
            )

            # Stop ordinary project readers before collecting the final
            # session-cold result; the dedicated session probes were connected
            # before the owner started and have remained independently capped.
            stop.set()
            for reader in readers:
                reader.join(timeout=5)
            readers.clear()
            stop.clear()

            cold_statuses = []
            debug_trace('stage', stage='session_cold_settle')
            final_session, cold_settle_elapsed = settle(
                clients[0],
                'search_sessions',
                tail_marker,
                timeout=600,
                observer=cold_statuses.append,
            )
            total_reconciliation_seconds = time.monotonic() - session_start
            probe = finish_session_probes(
                probe_stop,
                probe_threads,
                probe_state,
                probe_lock,
            )
            probe_stop = None
            probe_threads = ()
            final_status = cold_statuses[-1] if cold_statuses else {}
            final_progress = public_progress(final_status)
            final_coverage = public_coverage(final_status.get('coverage'))
            latencies = probe['latencies']
            total_reconciled = (
                final_status.get('status') in ('ready', 'degraded')
                and final_coverage.get('pending_changes') == 0
                and result_contains_marker(final_session, tail_marker)
            )
            cold_probe_pass = (
                not probe['probe_failed']
                and probe['first_progress_seconds'] is not None
                and probe['first_searchable_seconds'] is not None
            )
            passed = passed and total_reconciled and cold_probe_pass
            emit(
                {
                    'phase': 'session_cold',
                    'scratch_batching_measurement': 'unmeasured',
                    'records_expected': 1
                    + record_count
                    + SESSION_SOURCE_COUNT,
                    'session_source_count': SESSION_SOURCE_COUNT + 2,
                    'bytes_changed': session_file_bytes,
                    'bytes_source_read': final_progress.get('bytes_read'),
                    'bytes_source_read_basis': 'run_total',
                    'source_read_ratio': (
                        final_progress.get('bytes_read') / session_file_bytes
                        if session_file_bytes and final_progress.get('bytes_read') is not None
                        else None
                    ),
                    'bytes_hashed_for_verification': final_progress.get(
                        'bytes_hashed_for_verification'
                    ),
                    'server_counters': counter_snapshot(final_progress),
                    'counter_scope': 'final_run_snapshot',
                    'observed_run_metrics': merge_run_metric_rows(
                        probe['run_metrics'],
                        cold_statuses,
                    ),
                    'observed_run_metrics_scope': 'sampled_lower_bound',
                    'first_observable_progress_seconds': probe[
                        'first_progress_seconds'
                    ],
                    'first_observable_progress': probe['first_progress'],
                    'first_committed_searchable_seconds': probe[
                        'first_searchable_seconds'
                    ],
                    'independently_sampled_chunks_committed': probe[
                        'independently_sampled_chunks_committed'
                    ],
                    'total_reconciliation_seconds': total_reconciliation_seconds,
                    'settle_elapsed_seconds': cold_settle_elapsed,
                    'total_reconciliation_elapsed_ms': final_progress.get(
                        'elapsed_ms'
                    ),
                    'first_searchable_independent_of_chunk_counter': True,
                    'p50_search_during_ingestion_ms': statistics.median(latencies)
                    if latencies
                    else None,
                    'p95_search_during_ingestion_ms': safe_percentile(latencies, .95),
                    'search_probe_samples': len(latencies),
                    'probe_rate_hz': 4,
                    'probe_errors': probe['errors'],
                    'probe_failed': probe['probe_failed'],
                    'probe_observation_pass': cold_probe_pass,
                    'total_reconciliation': total_reconciled,
                    'status': public_status(final_status.get('status')),
                    'coverage': final_coverage,
                    'progress': final_progress,
                    'pass': total_reconciled and cold_probe_pass,
                }
            )

            reconnect_baseline = status_document(
                clients[0],
                'sessions',
                timeout=5,
            )
            reconnect_baseline_progress = public_progress(reconnect_baseline)
            for client in clients:
                client.close()
            clients = []
            reconnect_start = time.monotonic()
            time.sleep(4)
            debug_trace('stage', stage='reconnect_client_initialize')
            clients.append(
                Client(
                    binary,
                    root,
                    base / 'cache',
                    env,
                    label='reconnect',
                )
            )
            reconnect_client = clients[0]

            (
                unchanged_result,
                unchanged_elapsed,
                unchanged_states,
                unchanged_tracker,
                unchanged_status,
            ) = (
                settle_after_new_run(
                    reconnect_client,
                    'search_sessions',
                    first_marker,
                    reconnect_baseline,
                    timeout=600,
                )
            )
            unchanged_progress = public_progress(unchanged_status)
            unchanged_coverage = public_coverage(unchanged_status.get('coverage'))
            unchanged_ready = (
                unchanged_status.get('status') in ('ready', 'degraded')
                and unchanged_coverage.get('pending_changes') == 0
            )
            unchanged_prefix_visible = result_contains_marker(
                unchanged_result,
                first_marker,
            )
            unchanged_zero_work = all(
                unchanged_progress.get(key) == 0
                for key in ('records_processed', 'chunks_prepared', 'chunks_committed')
            )
            unchanged_prefix_verification = phase_was_observed(
                unchanged_states,
                'prefix_verification',
                unchanged_progress,
            )
            unchanged_pass = (
                unchanged_tracker['epoch_changed']
                and unchanged_prefix_verification
                and unchanged_ready
                and unchanged_prefix_visible
                and unchanged_zero_work
            )
            passed = passed and unchanged_pass
            emit(
                {
                    'phase': 'session_unchanged_reconnect',
                    'scratch_batching_measurement': 'unmeasured',
                    'reconnect_after_all_leases_closed': True,
                    'owner_stop_grace_seconds': 4,
                    'total_reconciliation_seconds': (
                        time.monotonic() - reconnect_start
                    ),
                    'settle_elapsed_seconds': unchanged_elapsed,
                    'total_reconciliation_elapsed_ms': unchanged_progress.get(
                        'elapsed_ms'
                    ),
                    'baseline_run_id': safe_run_id(reconnect_baseline_progress),
                    'final_run_id': safe_run_id(unchanged_progress),
                    'run_changed': unchanged_tracker['run_changed'],
                    'epoch_changed': unchanged_tracker['epoch_changed'],
                    'reconciled_at_changed': unchanged_tracker[
                        'reconciled_at_changed'
                    ],
                    'new_scan_observed': unchanged_tracker['new_scan_observed'],
                    'active_refresh_observed': unchanged_tracker[
                        'active_refresh_observed'
                    ],
                    'prefix_verification_observed': unchanged_prefix_verification,
                    'prefix_visible': unchanged_prefix_visible,
                    'full_ready': unchanged_ready,
                    'unchanged_counters': {
                        key: unchanged_progress.get(key)
                        for key in (
                            'records_processed',
                            'chunks_prepared',
                            'chunks_committed',
                        )
                    },
                    'unchanged_zero_work_pass': unchanged_zero_work,
                    'bytes_changed': 0,
                    'bytes_source_read': unchanged_progress.get('bytes_read'),
                    'bytes_source_read_basis': 'run_total',
                    'source_read_ratio': None,
                    'server_counters': counter_snapshot(unchanged_progress),
                    'counter_scope': 'final_run_snapshot',
                    'observed_run_metrics': run_metric_rows(unchanged_states),
                    'observed_run_metrics_scope': 'sampled_lower_bound',
                    'progress': unchanged_progress,
                    'coverage': unchanged_coverage,
                    'pass': unchanged_pass,
                }
            )

            debug_trace('stage', stage='append_probe_initialize')
            append_search_client = Client(
                binary,
                root,
                base / 'cache',
                env,
                label='append-search-probe',
            )
            append_status_client = Client(
                binary,
                root,
                base / 'cache',
                env,
                label='append-status-probe',
            )
            clients.extend([append_search_client, append_status_client])
            wait_status_ready(append_search_client, 'sessions')
            wait_status_ready(append_status_client, 'sessions')
            wait_status_ready(reconnect_client, 'sessions')
            append_marker = 'synthetic_session_append_marker'
            append_baseline = status_document(
                reconnect_client,
                'sessions',
                timeout=5,
            )
            append_before = public_progress(append_baseline)
            append_start = time.monotonic()
            append_probe_stop = threading.Event()
            append_probe_state, append_probe_lock, append_probe_threads = (
                start_session_probes(
                    append_search_client,
                    append_status_client,
                    append_marker,
                    append_probe_stop,
                    append_start,
                    baseline=append_baseline,
                )
            )
            probe_stop = append_probe_stop
            probe_threads = append_probe_threads
            append_bytes = append_session_events(
                slow,
                'synthetic-slow',
                session_cwd,
                [
                    (
                        'synthetic-slow-append-1000',
                        append_marker + ' complete appended event 1000',
                    ),
                    (
                        'synthetic-slow-append-1001',
                        append_marker + ' complete appended event 1001',
                    ),
                ],
            )
            debug_trace('stage', stage='append_settle')
            (
                append_result,
                append_elapsed,
                append_states,
                append_tracker,
                append_status,
            ) = settle_after_new_run(
                reconnect_client,
                'search_sessions',
                append_marker,
                append_baseline,
                timeout=600,
            )
            append_probe = finish_session_probes(
                append_probe_stop,
                append_probe_threads,
                append_probe_state,
                append_probe_lock,
            )
            probe_stop = None
            probe_threads = ()
            append_after = public_progress(append_status)
            append_coverage = public_coverage(append_status.get('coverage'))
            append_old_result = reconnect_client.tool(
                'search_sessions',
                {'query': first_marker, 'limit': 10},
                timeout=30,
            )
            append_ready = (
                append_status.get('status') in ('ready', 'degraded')
                and append_coverage.get('pending_changes') == 0
            )
            append_old_visible = result_contains_marker(
                append_old_result,
                first_marker,
            )
            append_new_visible = result_contains_marker(
                append_result,
                append_marker,
            )
            append_prefix_verification = phase_was_observed(
                append_states,
                'prefix_verification',
                append_after,
            )
            append_precise_scope = precise_scope_pass(append_after)
            append_probe_pass = (
                not append_probe['probe_failed']
                and append_probe['first_progress_seconds'] is not None
                and append_probe['first_searchable_seconds'] is not None
            )
            append_pass = (
                append_tracker['epoch_changed']
                and append_prefix_verification
                and append_ready
                and append_old_visible
                and append_new_visible
                and append_precise_scope
                and append_probe_pass
            )
            passed = passed and append_pass
            append_read = append_after.get('bytes_read')
            append_read_basis = 'final_run_total'
            emit(
                {
                    'phase': 'session_append_suffix',
                    'scratch_batching_measurement': 'unmeasured',
                    'events_appended': 2,
                    'baseline_run_id': safe_run_id(append_before),
                    'final_run_id': safe_run_id(append_after),
                    'run_changed': append_tracker['run_changed'],
                    'epoch_changed': append_tracker['epoch_changed'],
                    'reconciled_at_changed': append_tracker[
                        'reconciled_at_changed'
                    ],
                    'new_scan_observed': append_tracker['new_scan_observed'],
                    'active_refresh_observed': append_tracker[
                        'active_refresh_observed'
                    ],
                    'prefix_verification_observed': append_prefix_verification,
                    'old_prefix_visible': append_old_visible,
                    'append_visible': append_new_visible,
                    'precise_scope_pass': append_precise_scope,
                    'full_ready': append_ready,
                    'total_reconciliation_seconds': (
                        time.monotonic() - append_start
                    ),
                    'settle_elapsed_seconds': append_elapsed,
                    'total_reconciliation_elapsed_ms': append_after.get(
                        'elapsed_ms'
                    ),
                    'bytes_changed': append_bytes,
                    'bytes_source_read': append_read,
                    'bytes_source_read_basis': append_read_basis,
                    'source_read_ratio': (
                        append_read / append_bytes
                        if append_read is not None and append_bytes
                        else None
                    ),
                    'server_counters': counter_snapshot(append_after),
                    'server_counter_deltas': None,
                    'counter_scope': 'final_run_snapshot',
                    'observed_run_metrics': merge_run_metric_rows(
                        append_probe['run_metrics'],
                        append_states,
                    ),
                    'observed_run_metrics_scope': 'sampled_lower_bound',
                    'first_observable_progress_seconds': append_probe[
                        'first_progress_seconds'
                    ],
                    'first_committed_searchable_seconds': append_probe[
                        'first_searchable_seconds'
                    ],
                    'first_searchable_independent_of_chunk_counter': True,
                    'p50_search_during_ingestion_ms': (
                        statistics.median(append_probe['latencies'])
                        if append_probe['latencies']
                        else None
                    ),
                    'p95_search_during_ingestion_ms': safe_percentile(
                        append_probe['latencies'],
                        .95,
                    ),
                    'search_probe_samples': len(append_probe['latencies']),
                    'probe_rate_hz': 4,
                    'probe_errors': append_probe['errors'],
                    'probe_failed': append_probe['probe_failed'],
                    'probe_observation_pass': append_probe_pass,
                    'progress': append_after,
                    'coverage': append_coverage,
                    'pass': append_pass,
                }
            )

            changed_index = 3
            debug_trace('stage', stage='changed_probe_initialize')
            changed_search_client = Client(
                binary,
                root,
                base / 'cache',
                env,
                label='changed-search-probe',
            )
            changed_status_client = Client(
                binary,
                root,
                base / 'cache',
                env,
                label='changed-status-probe',
            )
            clients.extend([changed_search_client, changed_status_client])
            wait_status_ready(changed_search_client, 'sessions')
            wait_status_ready(changed_status_client, 'sessions')
            wait_status_ready(reconnect_client, 'sessions')
            changed_baseline = status_document(
                reconnect_client,
                'sessions',
                timeout=5,
            )
            changed_before = public_progress(changed_baseline)
            changed_old_marker = source_markers[changed_index]
            changed_new_marker = (
                f'synthetic_source_{changed_index:02d}_new_marker'
            )
            changed_start = time.monotonic()
            changed_probe_stop = threading.Event()
            changed_probe_state, changed_probe_lock, changed_probe_threads = (
                start_session_probes(
                    changed_search_client,
                    changed_status_client,
                    changed_new_marker,
                    changed_probe_stop,
                    changed_start,
                    baseline=changed_baseline,
                )
            )
            probe_stop = changed_probe_stop
            probe_threads = changed_probe_threads
            changed_path = source_paths[changed_index]
            write_session_source(
                changed_path,
                f'synthetic-source-{changed_index:02d}',
                session_cwd,
                changed_new_marker,
                event_id=f'synthetic-source-{changed_index:02d}-new',
            )
            changed_bytes = changed_path.stat().st_size
            debug_trace('stage', stage='changed_settle')
            (
                changed_result,
                changed_elapsed,
                changed_states,
                changed_tracker,
                changed_status,
            ) = (
                settle_after_new_run(
                    reconnect_client,
                    'search_sessions',
                    changed_new_marker,
                    changed_baseline,
                    timeout=600,
                )
            )
            changed_probe = finish_session_probes(
                changed_probe_stop,
                changed_probe_threads,
                changed_probe_state,
                changed_probe_lock,
            )
            probe_stop = None
            probe_threads = ()
            changed_after = public_progress(changed_status)
            changed_coverage = public_coverage(changed_status.get('coverage'))
            changed_old_result = reconnect_client.tool(
                'search_sessions',
                {'query': changed_old_marker, 'limit': 10},
                timeout=30,
            )
            unaffected_old_result = reconnect_client.tool(
                'search_sessions',
                {'query': source_markers[4], 'limit': 10},
                timeout=30,
            )
            changed_ready = (
                changed_status.get('status') in ('ready', 'degraded')
                and changed_coverage.get('pending_changes') == 0
            )
            changed_new_visible = result_contains_marker(
                changed_result,
                changed_new_marker,
            )
            changed_old_absent = not result_contains_marker(
                changed_old_result,
                changed_old_marker,
            )
            unaffected_old_visible = result_contains_marker(
                unaffected_old_result,
                source_markers[4],
            )
            changed_prefix_verification = phase_was_observed(
                changed_states,
                'prefix_verification',
                changed_after,
            )
            changed_precise_scope = precise_scope_pass(changed_after)
            changed_probe_pass = (
                not changed_probe['probe_failed']
                and changed_probe['first_progress_seconds'] is not None
                and changed_probe['first_searchable_seconds'] is not None
            )
            changed_pass = (
                SESSION_SOURCE_COUNT >= 8
                and changed_tracker['epoch_changed']
                and changed_ready
                and changed_prefix_verification
                and changed_new_visible
                and changed_old_absent
                and unaffected_old_visible
                and changed_precise_scope
                and changed_probe_pass
            )
            passed = passed and changed_pass
            changed_read = changed_after.get('bytes_read')
            changed_read_basis = 'final_run_total'
            emit(
                {
                    'phase': 'session_changed_one_of_eight',
                    'scratch_batching_measurement': 'unmeasured',
                    'session_source_count': SESSION_SOURCE_COUNT,
                    'changed_source_index': changed_index,
                    'unchanged_source_count': SESSION_SOURCE_COUNT - 1,
                    'baseline_run_id': safe_run_id(changed_before),
                    'final_run_id': safe_run_id(changed_after),
                    'run_changed': changed_tracker['run_changed'],
                    'epoch_changed': changed_tracker['epoch_changed'],
                    'reconciled_at_changed': changed_tracker[
                        'reconciled_at_changed'
                    ],
                    'new_scan_observed': changed_tracker['new_scan_observed'],
                    'active_refresh_observed': changed_tracker[
                        'active_refresh_observed'
                    ],
                    'prefix_verification_observed': changed_prefix_verification,
                    'new_target_visible': changed_new_visible,
                    'old_target_absent': changed_old_absent,
                    'unaffected_old_visible': unaffected_old_visible,
                    'precise_scope_pass': changed_precise_scope,
                    'full_ready': changed_ready,
                    'total_reconciliation_seconds': (
                        time.monotonic() - changed_start
                    ),
                    'settle_elapsed_seconds': changed_elapsed,
                    'total_reconciliation_elapsed_ms': changed_after.get(
                        'elapsed_ms'
                    ),
                    'bytes_changed': changed_bytes,
                    'bytes_source_read': changed_read,
                    'bytes_source_read_basis': changed_read_basis,
                    'source_read_ratio': (
                        changed_read / changed_bytes
                        if changed_read is not None and changed_bytes
                        else None
                    ),
                    'server_counters': counter_snapshot(changed_after),
                    'server_counter_deltas': None,
                    'counter_scope': 'final_run_snapshot',
                    'observed_run_metrics': merge_run_metric_rows(
                        changed_probe['run_metrics'],
                        changed_states,
                    ),
                    'observed_run_metrics_scope': 'sampled_lower_bound',
                    'first_observable_progress_seconds': changed_probe[
                        'first_progress_seconds'
                    ],
                    'first_committed_searchable_seconds': changed_probe[
                        'first_searchable_seconds'
                    ],
                    'first_searchable_independent_of_chunk_counter': True,
                    'p50_search_during_ingestion_ms': (
                        statistics.median(changed_probe['latencies'])
                        if changed_probe['latencies']
                        else None
                    ),
                    'p95_search_during_ingestion_ms': safe_percentile(
                        changed_probe['latencies'],
                        .95,
                    ),
                    'search_probe_samples': len(changed_probe['latencies']),
                    'probe_rate_hz': 4,
                    'probe_errors': changed_probe['errors'],
                    'probe_failed': changed_probe['probe_failed'],
                    'probe_observation_pass': changed_probe_pass,
                    'progress': changed_after,
                    'coverage': changed_coverage,
                    'pass': changed_pass,
                }
            )

            huge_session_dir = Path(env['CODEX_HOME']) / 'sessions'
            huge_session = huge_session_dir / 'huge.jsonl'
            huge_baseline = status_document(clients[0], 'sessions', timeout=5)
            with huge_session.open('w') as file:
                file.write(
                    json.dumps(
                        session_meta('huge-session', session_cwd)
                    )
                    + '\n'
                )
                file.write(
                    '{"type":"response_item","payload":{"type":"message",'
                    '"role":"user","content":[{"type":"input_text","text":"'
                )
                block = 'sessionword ' * 8192
                for _ in range(
                    (args.huge_session_mib * 1024 * 1024) // len(block)
                ):
                    file.write(block)
                file.write('hugesessiontailmarker"}]}}\n')
            huge_session.chmod(0o600)
            huge_result = wait_marker(
                clients[0],
                'search_sessions',
                'hugesessiontailmarker',
                timeout=600,
                baseline=huge_baseline,
            )
            memory = public_coverage(huge_result.get('coverage')).get('memory', {})
            peak = memory.get('peak_observed_rss_bytes')
            huge_pass = peak is None or peak <= 512 * 1024 * 1024
            passed = passed and huge_pass
            emit(
                {
                    'phase': 'huge_session_record',
                    'bytes': huge_session.stat().st_size,
                    'coverage': public_coverage(huge_result.get('coverage')),
                    'progress': public_progress(huge_result),
                    'memory_target_pass': huge_pass,
                    'concurrent_readers': 0,
                }
            )
        finally:
            stop.set()
            if probe_stop is not None:
                probe_stop.set()
            for reader in readers:
                reader.join(timeout=5)
            for thread in probe_threads:
                thread.join(timeout=2)
            for client in clients:
                client.close()
            time.sleep(4)

        if reader_errors or not passed:
            emit(
                {
                    'phase': 'acceptance_failure',
                    'reader_errors': len(reader_errors),
                    'targets_pass': passed,
                }
            )
            raise SystemExit(1)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        debug_trace(
            'failure',
            error='timeout' if isinstance(error, TimeoutError) else 'acceptance_error',
        )
        emit(
            {
                'phase': 'failure',
                'error': 'timeout' if isinstance(error, TimeoutError) else 'acceptance_error',
            }
        )
        raise SystemExit(1) from None
