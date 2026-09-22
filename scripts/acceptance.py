#!/usr/bin/env python3
"""Measure MCP acceptance without exporting indexed content or changing real projects."""
import argparse
import datetime
import hashlib
import platform
import concurrent.futures
import json
import math
import os
from pathlib import Path
import queue
import re
import statistics
import shutil
import subprocess
import sys
import tempfile
import threading
import time

REQUEST_TIMEOUT = 30
STATUS_URI = 'bm25://indexing/status'
MAX_PUBLIC_NUMBER = 2**63 - 1
MAX_PUBLIC_STRING = 128
MAX_PUBLIC_TRANSITIONS = 64
GRACEFUL_CLOSE_TIMEOUT = .5
FORCED_CLEANUP_TIMEOUT = .05
_DEBUG_TRACE_FILE = os.environ.get('BM25_DEBUG_TRACE_FILE')
_DEBUG_TRACE_LOCK = threading.Lock()
_DEBUG_TRACE_STARTED = time.monotonic()


def debug_trace(event, **fields):
    """Optionally record bounded protocol-stage diagnostics outside public JSONL."""
    if not _DEBUG_TRACE_FILE:
        return
    safe = {'event': event, 'elapsed_ms': (time.monotonic() - _DEBUG_TRACE_STARTED) * 1000}
    for key, value in fields.items():
        if type(value) in (str, int, float, bool) and (
            not isinstance(value, str) or len(value) <= 80
        ):
            safe[key] = value
    try:
        with _DEBUG_TRACE_LOCK:
            path = Path(_DEBUG_TRACE_FILE)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open('a', encoding='utf8') as stream:
                stream.write(json.dumps(safe, allow_nan=False) + '\n')
                stream.flush()
                os.fsync(stream.fileno())
            path.chmod(0o600)
    except (OSError, ValueError):
        pass

PUBLIC_PHASES = (
    'idle', 'starting', 'discovery', 'discovering', 'project_discovery',
    'session_discovery', 'ownership', 'scanning', 'reading', 'source_read',
    'parsing', 'json_inspection', 'normalization', 'hashing', 'verifying',
    'prefix_verification', 'preparing', 'temp_file_ops', 'tokenization',
    'temp_dedup_writes', 'scratch_writes', 'indexing', 'committing',
    'durable_txn', 'durable_transaction', 'reconciling', 'refreshing',
    'finalizing', 'complete', 'cancelled', 'failed', 'waiting',
)
PUBLIC_PROGRESS_COUNTERS = (
    'files_discovered', 'files_completed', 'records_processed', 'bytes_read',
    'bytes_hashed_for_verification', 'chunks_prepared', 'chunks_committed',
    'current_source_processed_bytes', 'records_inspected', 'phase_elapsed_ms',
    'elapsed_ms', 'first_progress_ms', 'first_commit_ms',
    'phase_transitions_dropped',
)
PUBLIC_WORK_DURATIONS = (
    'discovery_ms', 'ownership_ms', 'prefix_verification_ms',
    'json_inspection_ms', 'normalization_ms', 'temp_file_ops_ms',
    'tokenization_ms', 'temp_dedup_writes_ms', 'durable_txn_ms',
    'scratch_lookup_ms', 'scratch_begin_ms', 'scratch_insert_ms',
    'scratch_commit_ms', 'scratch_transaction_lifetime_ms',
)
PUBLIC_WORK_COUNTS = (
    'retries', 'cancellations', 'sources_started', 'sources_completed',
    'records_skipped', 'chunks_prepared', 'chunks_committed',
    'discovery_count', 'ownership_count', 'prefix_verification_count',
    'json_inspection_count', 'normalization_count', 'temp_file_ops_count',
    'tokenization_count', 'temp_dedup_writes_count', 'durable_txn_count',
    'scratch_lookup_count', 'scratch_begin_count', 'scratch_insert_count',
    'scratch_state_writes', 'scratch_state_transactions',
)
PUBLIC_WORK_BYTES = (
    'json_inspection_bytes', 'tokenization_bytes', 'temp_file_ops_bytes',
    'scratch_state_write_bytes',
)
PUBLIC_WORK_PHASES = (
    'discovery', 'ownership', 'prefix_verification', 'json_inspection',
    'normalization', 'temp_file_ops', 'tokenization', 'temp_dedup_writes',
    'durable_txn',
)
PUBLIC_MEMORY_NUMBERS = (
    'peak_observed_rss_bytes', 'observed_rss_bytes', 'target_bytes', 'current_bytes',
    'high_water_bytes',
)
PUBLIC_COVERAGE_NUMBERS = (
    'pending_changes', 'excluded_count', 'error_count', 'safe_epoch',
    'sources_total', 'sources_reconciled', 'sources_changed',
)


class McpRequestError(RuntimeError):
    """A matched error response leaves the protocol stream synchronized."""


class JsonlWriter:
    """Serialize complete records; flush makes each record visible to live readers."""
    def __init__(self, stream, durable=False):
        self.stream = stream
        self.durable = durable
        self.lock = threading.Lock()

    def write(self, value):
        line = json.dumps(value, allow_nan=False) + '\n'
        with self.lock:
            self.stream.write(line)
            self.stream.flush()
            if self.durable:
                os.fsync(self.stream.fileno())


def public_number(value):
    if type(value) not in (int, float) or not math.isfinite(value):
        return None
    if abs(value) > MAX_PUBLIC_NUMBER:
        return None
    return value


def public_counter(value):
    value = public_number(value)
    return value if value is not None and value >= 0 else None


def public_run_id(value):
    if type(value) in (int, float):
        return public_counter(value)
    if not isinstance(value, str) or len(value) > MAX_PUBLIC_STRING:
        return None
    if re.fullmatch(
        r'(?:[0-9]{1,20}|run[-_][0-9]{1,20}|[0-9a-fA-F]{32}|'
        r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-'
        r'[0-9a-fA-F]{4}-[0-9a-fA-F]{12})',
        value,
    ) is None:
        return None
    return value


def public_timestamp(value):
    if not isinstance(value, str) or len(value) > 64 or '\n' in value or '\r' in value:
        return None
    if re.fullmatch(
        r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'
        r'(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})',
        value,
    ) is None:
        return None
    try:
        parsed = datetime.datetime.fromisoformat(
            value[:-1] + '+00:00' if value.endswith('Z') else value
        )
    except ValueError:
        return None
    return value if parsed.tzinfo is not None else None


def public_status(value):
    return value if value in ('building', 'refreshing', 'ready', 'degraded') else 'unknown'


def public_diagnostics(value):
    allowed = {
        'git_metadata_excluded', 'symlink_excluded', 'unchanged_index_reuse',
        'content_cache_hit', 'content_cache_miss', 'content_cache_write_error',
        'binary_excluded', 'unsupported_encoding', 'ownership_excluded',
        'unsupported_record', 'malformed_record', 'source_read', 'ownership',
        'metadata', 'timestamp', 'non_file_change', 'outside_root',
        'watcher_unavailable', 'other',
    }
    if not isinstance(value, dict):
        return {}
    result = {}
    for key in sorted(value, key=str):
        raw = value[key]
        number = public_number(raw)
        if number is None:
            continue
        category = key if isinstance(key, str) and key in allowed else 'other'
        result[category] = result.get(category, 0) + number
    return result


def public_memory(value):
    if not isinstance(value, dict):
        return {}
    result = {}
    for key in PUBLIC_MEMORY_NUMBERS:
        number = public_counter(value.get(key))
        if number is not None:
            result[key] = number
    if type(value.get('pressure')) is bool:
        result['pressure'] = value['pressure']
    return result


def public_coverage(value):
    if not isinstance(value, dict):
        return {}
    result = {}
    for key in PUBLIC_COVERAGE_NUMBERS:
        number = public_counter(value.get(key))
        if number is not None:
            result[key] = number
    timestamp = public_timestamp(value.get('reconciled_at'))
    if timestamp is not None:
        result['reconciled_at'] = timestamp
    diagnostics = public_diagnostics(value.get('diagnostics'))
    if diagnostics:
        result['diagnostics'] = diagnostics
    memory = public_memory(value.get('memory'))
    if memory:
        result['memory'] = memory
    return result


def public_work(value):
    if not isinstance(value, dict):
        return {}
    result = {}
    for key in PUBLIC_WORK_DURATIONS + PUBLIC_WORK_COUNTS + PUBLIC_WORK_BYTES:
        number = public_counter(value.get(key))
        if number is not None:
            result[key] = number
    for section, fields in (
        ('durations', PUBLIC_WORK_DURATIONS),
        ('counts', PUBLIC_WORK_COUNTS),
        ('bytes', PUBLIC_WORK_BYTES),
    ):
        nested = value.get(section)
        if not isinstance(nested, dict):
            continue
        safe = {}
        for key in fields:
            number = public_counter(nested.get(key))
            if number is not None:
                safe[key] = number
        if safe:
            result[section] = safe
    for phase in PUBLIC_WORK_PHASES:
        nested = value.get(phase)
        if not isinstance(nested, dict):
            continue
        safe = {}
        for key in ('elapsed_ms', 'duration_ms', 'count', 'retries', 'cancellations'):
            number = public_counter(nested.get(key))
            if number is not None:
                safe[key] = number
        if safe:
            result[phase] = safe
    return result


def public_phase_transitions(progress):
    source = progress.get('phase_transitions')
    if source is None:
        source = progress.get('transitions')
    if source is None and (
        'phase_history' in progress or 'phase_transitions_dropped' in progress
    ):
        source = {
            'history': progress.get('phase_history'),
            'drop_count': progress.get('phase_transitions_dropped'),
        }
    if not isinstance(source, dict):
        return {}
    history = source.get('history')
    safe_history = []
    if isinstance(history, list):
        for entry in history[-MAX_PUBLIC_TRANSITIONS:]:
            if not isinstance(entry, dict):
                continue
            phase = entry.get('phase')
            sequence = public_counter(entry.get('sequence'))
            if phase not in PUBLIC_PHASES or sequence is None:
                continue
            safe_entry = {'phase': phase, 'sequence': sequence}
            elapsed = public_counter(entry.get('elapsed_ms'))
            if elapsed is not None:
                safe_entry['elapsed_ms'] = elapsed
            timestamp = public_timestamp(entry.get('at'))
            if timestamp is not None:
                safe_entry['at'] = timestamp
            safe_history.append(safe_entry)
    result = {'history': safe_history}
    sequence = public_counter(source.get('sequence'))
    if sequence is not None:
        result['sequence'] = sequence
    drop_count = public_counter(source.get('drop_count'))
    if drop_count is None:
        drop_count = public_counter(source.get('dropped'))
    if drop_count is not None:
        result['drop_count'] = drop_count
    return result


def public_progress(state):
    progress = state.get('progress') if isinstance(state, dict) else None
    if not isinstance(progress, dict):
        return {}
    phase = progress.get('phase')
    result = {'phase': phase if phase in PUBLIC_PHASES else 'unknown'}
    run_id = public_run_id(progress.get('run_id'))
    if run_id is not None:
        result['run_id'] = run_id
    for field in PUBLIC_PROGRESS_COUNTERS:
        number = public_counter(progress.get(field))
        if number is not None:
            result[field] = number
    for field in ('last_progress_at', 'first_progress_at', 'first_commit_at'):
        timestamp = public_timestamp(progress.get(field))
        if timestamp is not None:
            result[field] = timestamp
    phase_timings = progress.get('phase_timings_ms')
    if isinstance(phase_timings, dict):
        safe_timings = {}
        for phase in PUBLIC_PHASES:
            number = public_counter(phase_timings.get(phase))
            if number is not None:
                safe_timings[phase] = number
        if safe_timings:
            result['phase_timings_ms'] = safe_timings
    work = public_work(progress.get('work'))
    if work:
        result['work'] = work
    transitions = public_phase_transitions(progress)
    if transitions:
        result['phase_transitions'] = transitions
        if 'phase_history' in progress:
            result['phase_history'] = transitions['history']
    return result


class Client:
    def __init__(
        self,
        binary,
        project,
        cache,
        env,
        request_timeout=REQUEST_TIMEOUT,
        label=None,
    ):
        self.request_timeout = request_timeout
        self.label = label
        self.lock = threading.Lock()
        self.closed = threading.Event()
        self.worker = None
        self.buffer = bytearray()
        # Unbuffered pipes have no Python buffered-I/O lock that could strand
        # timeout cleanup behind a blocked read or write (including on Windows).
        self.process = subprocess.Popen([str(binary), 'serve', '--project', str(project), '--cache-dir', str(cache)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0, env=env)
        self.sequence = 0
        try:
            self.rpc('initialize', {'protocolVersion':'2025-11-25','capabilities':{},'clientInfo':{'name':'acceptance','version':'1'}})
            self._request('notifications/initialized', None, request_timeout, notification=True)
        except BaseException:
            self.close(force=True)
            raise

    def _exchange(self, payload, request_id, deadline):
        remaining = memoryview(payload)
        while remaining:
            if self.closed.is_set() or time.monotonic() >= deadline:
                raise TimeoutError('MCP request deadline exceeded')
            written = self.process.stdin.write(remaining)
            if not written:
                raise RuntimeError('MCP process exited')
            remaining = remaining[written:]
        self.process.stdin.flush()
        if request_id is None:
            return None
        while True:
            if self.closed.is_set() or time.monotonic() >= deadline:
                raise TimeoutError('MCP request deadline exceeded')
            newline = self.buffer.find(b'\n')
            if newline < 0:
                if len(self.buffer) > 16 * 1024 * 1024:
                    raise RuntimeError('MCP response exceeds limit')
                data = self.process.stdout.read(65536)
                if not data:
                    raise RuntimeError('MCP process exited')
                self.buffer.extend(data)
                continue
            line = self.buffer[:newline]
            del self.buffer[:newline + 1]
            result = json.loads(line)
            if not isinstance(result, dict):
                raise RuntimeError('Invalid MCP response')
            if result.get('id') == request_id and 'method' not in result:
                if 'error' in result or result.get('result',{}).get('isError'):
                    raise McpRequestError('MCP request failed')
                return result['result']

    def _request(self, method, params, timeout, notification=False):
        budget = min(self.request_timeout, timeout) if timeout is not None else self.request_timeout
        if not math.isfinite(budget) or budget <= 0:
            raise TimeoutError('MCP request deadline exceeded')
        deadline = time.monotonic() + budget
        debug_trace(
            'request_start',
            method=method,
            label=self.label or 'unlabeled',
            budget_ms=budget * 1000,
            notification=notification,
        )
        if not self.lock.acquire(
            timeout=max(0, budget - FORCED_CLEANUP_TIMEOUT)
        ):
            debug_trace(
                'request_timeout',
                method=method,
                label=self.label or 'unlabeled',
                reason='lock',
            )
            self.close(force=True, deadline=deadline)
            raise TimeoutError('MCP request deadline exceeded')
        try:
            if self.closed.is_set():
                raise RuntimeError('MCP connection is closed')
            self.sequence += 1
            message = {'jsonrpc':'2.0', 'method':method}
            if not notification:
                message.update(id=self.sequence, params=params)
            payload = (json.dumps(message) + '\n').encode('utf8')
            outcome = queue.Queue(maxsize=1)

            def exchange():
                try:
                    outcome.put((True, self._exchange(payload, None if notification else self.sequence, deadline)))
                except McpRequestError:
                    outcome.put((False, 'request_error'))
                except Exception:
                    # Protocol responses and OS errors can contain private paths.
                    outcome.put((False, None))

            self.worker = threading.Thread(target=exchange, name='mcp-rpc', daemon=True)
            self.worker.start()
            try:
                ok, result = outcome.get(
                    timeout=max(
                        0,
                        deadline - time.monotonic() - FORCED_CLEANUP_TIMEOUT,
                    )
                )
            except queue.Empty:
                debug_trace(
                    'request_timeout',
                    method=method,
                    label=self.label or 'unlabeled',
                    reason='deadline',
                )
                self.close(force=True, deadline=deadline)
                raise TimeoutError('MCP request deadline exceeded') from None
            if time.monotonic() >= deadline:
                debug_trace(
                    'request_timeout',
                    method=method,
                    label=self.label or 'unlabeled',
                    reason='deadline_after_response',
                )
                self.close(force=True, deadline=deadline)
                raise TimeoutError('MCP request deadline exceeded')
            if not ok:
                if result == 'request_error':
                    debug_trace(
                        'request_failed',
                        method=method,
                        label=self.label or 'unlabeled',
                        reason='protocol',
                    )
                    raise McpRequestError('MCP request failed')
                debug_trace(
                    'request_failed',
                    method=method,
                    label=self.label or 'unlabeled',
                    reason='transport',
                )
                self.close(force=True, deadline=deadline)
                raise RuntimeError('MCP request failed') from None
            self.worker.join(
                timeout=max(
                    0,
                    deadline - time.monotonic() - FORCED_CLEANUP_TIMEOUT,
                )
            )
            if self.worker.is_alive() or time.monotonic() >= deadline:
                debug_trace(
                    'request_timeout',
                    method=method,
                    label=self.label or 'unlabeled',
                    reason='worker_cleanup',
                )
                self.close(force=True, deadline=deadline)
                raise TimeoutError('MCP request deadline exceeded')
            debug_trace(
                'request_complete',
                method=method,
                label=self.label or 'unlabeled',
            )
            return result
        finally:
            self.lock.release()

    def rpc(self, method, params, timeout=None):
        return self._request(method, params, timeout)

    def tool(self, name, arguments, timeout=None):
        return self.rpc('tools/call', {'name':name,'arguments':arguments}, timeout=timeout)['structuredContent']

    def close(self, force=False, deadline=None):
        self.closed.set()
        active = self.worker is not None and self.worker.is_alive()
        if active:
            force = True
        if not force and not active:
            try:
                self.process.stdin.close()
            except (OSError, ValueError):
                pass
            try:
                self.process.wait(timeout=GRACEFUL_CLOSE_TIMEOUT)
            except subprocess.TimeoutExpired:
                force = True
        if force and self.process.poll() is None:
            self.process.kill()
        # Close both descriptors before joining the worker. This is what
        # unblocks a worker stuck in a pipe read or write after a deadline.
        for stream in (self.process.stdin, self.process.stdout):
            try:
                stream.close()
            except (OSError, ValueError):
                pass
        if force:
            wait_budget = FORCED_CLEANUP_TIMEOUT
            if deadline is not None:
                wait_budget = max(0, min(wait_budget, deadline - time.monotonic()))
            try:
                self.process.wait(timeout=wait_budget)
            except subprocess.TimeoutExpired:
                pass
        if self.worker is not None and self.worker is not threading.current_thread():
            join_budget = FORCED_CLEANUP_TIMEOUT
            if deadline is not None:
                join_budget = max(0, min(join_budget, deadline - time.monotonic()))
            self.worker.join(timeout=join_budget)


def is_settled(response):
    """Validate a status/search readiness envelope; null pending work is unfinished."""
    if not isinstance(response, dict) or response.get('status') not in (
        'building', 'refreshing', 'ready', 'degraded',
    ):
        raise RuntimeError('Invalid readiness response')
    coverage = response.get('coverage')
    if not isinstance(coverage, dict) or 'pending_changes' not in coverage:
        raise RuntimeError('Invalid readiness response')
    pending = coverage['pending_changes']
    if pending is not None and (type(pending) is not int or pending < 0):
        raise RuntimeError('Invalid readiness response')
    return response['status'] in ('ready', 'degraded') and pending == 0


def settle(
    client,
    tool,
    query='index',
    timeout=900,
    arguments=None,
    observer=None,
    poll_interval=1,
    status_predicate=None,
):
    """Return a settled search, re-observing status after races under one deadline.

    status_predicate applies only to status snapshots: ordinary search responses
    do not carry the progress fields that a caller's predicate may require.
    """
    start = time.monotonic()
    if not math.isfinite(timeout) or timeout <= 0:
        raise TimeoutError('Reconciliation deadline exceeded')
    deadline = start + timeout
    kind = {'search_project':'project', 'search_sessions':'sessions'}[tool]
    if not math.isfinite(poll_interval) or poll_interval < 0:
        raise ValueError('poll interval must be finite and non-negative')

    def remaining():
        budget = deadline - time.monotonic()
        if budget <= 0:
            raise TimeoutError('Reconciliation deadline exceeded')
        return budget

    while True:
        response = client.rpc('resources/read', {'uri':STATUS_URI}, timeout=remaining())
        if not isinstance(response, dict) or not isinstance(response.get('contents'), list):
            raise RuntimeError('Invalid status response')
        if not response['contents'] or not isinstance(response['contents'][0], dict):
            raise RuntimeError('Invalid status response')
        text = response['contents'][0].get('text')
        if not isinstance(text, str):
            raise RuntimeError('Invalid status response')
        try:
            document = json.loads(text)
        except ValueError:
            raise RuntimeError('Invalid status response') from None
        state = document.get(kind) if isinstance(document, dict) else None
        ready = is_settled(state)
        if observer is not None:
            observer(state)
        if ready and (status_predicate is None or status_predicate(state)):
            result = client.tool(tool, {'query':query, **(arguments or {})}, timeout=remaining())
            remaining()
            if is_settled(result):
                return result, time.monotonic()-start
        time.sleep(min(poll_interval, remaining()))

def measure(client, count, worker, timeout=900):
    samples = []
    partial = 0
    peak = 0
    queries = ['index', 'tokenizer', 'memory', 'file not found', 'search query', 'nonexistentqzxacceptance']
    deadline = time.monotonic() + timeout

    def remaining():
        budget = deadline - time.monotonic()
        if budget <= 0:
            raise TimeoutError('Query sampling timeout')
        return budget

    contexts = []
    session = client.tool('search_sessions', {'query':'error'}, timeout=remaining())
    if session.get('results'):
        contexts = [session['results'][0]['match_id']]
    for _ in range(10):
        client.tool('search_project', {'query':'index'}, timeout=remaining())
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
            result = client.tool(tool, arguments, timeout=remaining())
        except McpRequestError:
            if arguments.get('mode') != 'context':
                raise
            contexts = []
            partial += 1
            attempt += 1
            continue
        elapsed = (time.perf_counter()-start)*1000
        peak = max(peak, (result['coverage'].get('memory') or {}).get('peak_observed_rss_bytes',0))
        if is_settled(result):
            samples.append(elapsed)
        else:
            partial += 1
            time.sleep(.01)
        attempt += 1
        remaining()
    return samples, partial, peak

_stdout_writer = JsonlWriter(sys.stdout)


def emit(value):
    _stdout_writer.write(value)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', type=Path, required=True)
    parser.add_argument('--binary', type=Path, default=Path(__file__).resolve().parents[1]/'target'/'release'/('bm25-mcp.exe' if os.name=='nt' else 'bm25-mcp'))
    parser.add_argument('--requests', type=int, default=1000)
    parser.add_argument('--timeout', type=float, default=900,
                        help='overall seconds allowed for each readiness/measurement phase')
    parser.add_argument('--cache-dir', type=Path)
    parser.add_argument('--snapshot-sessions', action='store_true', help='Freeze actual JSONL histories in a private temporary directory for repeatable warm-query measurements')
    args = parser.parse_args()
    if args.requests < 1 or not math.isfinite(args.timeout) or args.timeout <= 0:
        raise ValueError('requests and timeout must be positive')
    emit({'phase':'environment','schema_version':2,'project':'project-0001','platform':platform.system(),
          'architecture':platform.machine(),'logical_cpus':os.cpu_count(),
          'timestamp_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
          'binary_sha256':hashlib.sha256(args.binary.read_bytes()).hexdigest(),
          'persistent_cache':args.cache_dir is not None})
    with tempfile.TemporaryDirectory(prefix='.bm25-acceptance-', dir=Path.cwd()) as scratch:
        clients = []
        passed = True
        environment = os.environ.copy()
        providers = [
            ('CODEX_HOME', '.codex', ['sessions', 'archived_sessions']),
            ('CLAUDE_CONFIG_DIR', '.claude', ['projects']),
            ('COPILOT_HOME', '.copilot', ['session-state']),
        ]
        original_homes = {
            variable: Path(environment.get(variable, str(Path.home() / default)))
            for variable, default, _ in providers
        }
        for variable, _, _ in providers:
            frozen = Path(scratch) / variable
            frozen.mkdir(mode=0o700)
            environment[variable] = str(frozen)
        if args.snapshot_sessions:
            files = 0
            total_bytes = 0
            for variable, _, folders in providers:
                original = original_homes[variable]
                frozen = Path(environment[variable])
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
                clients.append(Client(
                    args.binary.resolve(),
                    args.project.resolve(),
                    args.cache_dir.resolve() if args.cache_dir else Path(scratch)/'cache',
                    environment,
                    request_timeout=min(REQUEST_TIMEOUT, args.timeout),
                ))
            for tool in ('search_project','search_sessions'):
                def observe(state, tool=tool):
                    emit({
                        'phase': 'progress',
                        'tool': tool,
                        'status': public_status(state.get('status')),
                        'coverage': public_coverage(state.get('coverage')),
                        'progress': public_progress(state),
                    })

                result, elapsed = settle(
                    clients[0],
                    tool,
                    timeout=args.timeout,
                    observer=observe,
                )
                emit({'phase':'initial','tool':tool,'seconds':elapsed,'status':public_status(result['status']),
                      'coverage':public_coverage(result.get('coverage')),
                      'progress':public_progress(result)})
            expected_agents = {'BM25-Turbo-Rust-Python-WASM-CLI':['codex'], 't3code':['codex'], 'neutron':['codex','copilot']}
            for agent in expected_agents.get(args.project.name, []):
                result, _ = settle(
                    clients[0],
                    'search_sessions',
                    'error',
                    timeout=args.timeout,
                    arguments={'agent':agent},
                )
                found = bool(result['results'])
                passed = passed and found
                emit({'phase':'real_history','agent':agent,'matching_results':len(result['results']),'pass':found})
            judgments = {
                'BM25-Turbo-Rust-Python-WASM-CLI': [('score_deterministic', 'bm25_turbo/src/scoring.rs')],
                't3code': [('GitHubCliAuthenticationError', 'apps/server/src/sourceControl/GitHubCli.ts'), ('GitHub CLI is not authenticated', 'apps/server/src/sourceControl/GitHubCli.ts')],
                'neutron': [('h264_parameter_set_count', 'engine/crates/media/src/media.rs')],
            }
            for number, (query, expected) in enumerate(judgments.get(args.project.name, []), 1):
                result, _ = settle(
                    clients[0],
                    'search_project',
                    query,
                    timeout=args.timeout,
                )
                paths = [hit['relative_path'] for hit in result['results']]
                found = expected in paths
                passed = passed and found
                emit({'phase':'relevance','id':f'relevance_{number}','top_k':10,'rank':paths.index(expected)+1 if found else None,'pass':found,'judgment':'known symbol definition or exact error location'})
            for concurrency in (1,4):
                with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
                    results = list(executor.map(
                        lambda pair: measure(
                            pair[1],
                            max(1, args.requests // concurrency),
                            pair[0],
                            args.timeout,
                        ),
                        enumerate(clients[:concurrency]),
                    ))
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
    try:
        main()
    except Exception as error:
        emit({'phase':'failure', 'error':'timeout' if isinstance(error, TimeoutError) else 'acceptance_error'})
        raise SystemExit(1) from None
