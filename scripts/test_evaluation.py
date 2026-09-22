"""Synthetic protocol and public-report contracts; no provider/project discovery."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import time
import unittest
from unittest import mock
import uuid

import acceptance
from acceptance import (
    Client,
    JsonlWriter,
    public_coverage,
    public_progress,
    settle,
)


def load_script(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


evaluation = load_script('evaluate_siblings', 'evaluate-siblings.py')
summary = load_script('summarize_siblings', 'summarize-sibling-eval.py')
updates = load_script('exercise_updates', 'exercise-updates.py')
benchmark = load_script('benchmark_mcp', 'benchmark-mcp.py')
pressure = load_script('check_pressure', 'check-pressure.py')
SECRET = 'sensitive_customer_query_and_transcript'


class FixtureTest(unittest.TestCase):
    def setUp(self):
        self.root = Path.cwd() / ('.evaluation-tests-' + uuid.uuid4().hex)
        self.root.mkdir(mode=0o700)
        self.addCleanup(shutil.rmtree, self.root)


class PublicSchemaTests(unittest.TestCase):
    def test_progress_allowlist_preserves_safe_time_work_and_transitions(self):
        state = {
            'progress': {
                'phase': 'normalization',
                'run_id': '01234567-89ab-cdef-0123-456789abcdef',
                'files_discovered': 4,
                'bytes_read': 123,
                'last_progress_at': '2026-09-21T19:43:41.909Z',
                'work': {
                    'normalization_ms': 12,
                    'durable_txn_ms': 4,
                    'retries': 1,
                    'normalization_count': 2,
                    'durable_txn_count': 1,
                    'json_inspection_bytes': 512,
                    'tokenization_bytes': 128,
                    'scratch_state_writes': 3,
                    'scratch_state_write_bytes': 256,
                    'scratch_state_transactions': 1,
                    'scratch_lookup_ms': 3,
                    'scratch_lookup_count': 4,
                    'scratch_begin_ms': 2,
                    'scratch_begin_count': 1,
                    'scratch_insert_ms': 5,
                    'scratch_insert_count': 3,
                    'scratch_commit_ms': 1,
                    'scratch_transaction_lifetime_ms': 12,
                    'private_metric': SECRET,
                    'durations': {'tokenization_ms': 9},
                    'counts': {'chunks_committed': 2},
                },
                'phase_transitions': {
                    'sequence': 4,
                    'drop_count': 1,
                    'history': [
                        {'phase': 'discovery', 'sequence': 1, 'at': 'not-a-time'},
                        {'phase': 'normalization', 'sequence': 4, 'elapsed_ms': 12},
                        {'phase': SECRET, 'sequence': 5},
                    ],
                },
                'phase_history': [
                    {'phase': 'scratch_writes', 'sequence': 5, 'elapsed_ms': 18},
                ],
                'phase_transitions_dropped': 2,
                'phase_timings_ms': {
                    'normalization': 12,
                    SECRET: 99,
                },
                'current_path': SECRET,
                'exception': SECRET,
            },
            'coverage': {
                'pending_changes': 0,
                'error_count': 1,
                'reconciled_at': '2026-09-21T19:43:41+00:00',
                'errors': [SECRET],
                'diagnostics': {'unsupported_record': 2, SECRET: 3},
                'memory': {'peak_observed_rss_bytes': 100, SECRET: 9},
            },
        }
        progress = public_progress(state)
        coverage = public_coverage(state['coverage'])
        self.assertEqual(progress['phase'], 'normalization')
        self.assertEqual(progress['run_id'], '01234567-89ab-cdef-0123-456789abcdef')
        self.assertEqual(progress['last_progress_at'], '2026-09-21T19:43:41.909Z')
        self.assertEqual(progress['work']['normalization_ms'], 12)
        self.assertEqual(progress['work']['normalization_count'], 2)
        self.assertEqual(progress['work']['json_inspection_bytes'], 512)
        self.assertEqual(progress['work']['tokenization_bytes'], 128)
        self.assertEqual(progress['work']['scratch_state_writes'], 3)
        self.assertEqual(progress['work']['scratch_lookup_count'], 4)
        self.assertEqual(progress['work']['scratch_insert_ms'], 5)
        self.assertEqual(
            progress['work']['scratch_transaction_lifetime_ms'],
            12,
        )
        self.assertEqual(progress['work']['durations']['tokenization_ms'], 9)
        self.assertEqual(progress['phase_transitions']['drop_count'], 1)
        self.assertEqual(len(progress['phase_transitions']['history']), 2)
        self.assertEqual(progress['phase_timings_ms']['normalization'], 12)
        actual_shape = public_progress(
            {
                'progress': {
                    'phase': 'durable_transaction',
                    'phase_history': [
                        {
                            'phase': 'scratch_writes',
                            'sequence': 5,
                            'elapsed_ms': 18,
                        }
                    ],
                    'phase_transitions_dropped': 2,
                }
            }
        )
        self.assertEqual(actual_shape['phase_transitions']['drop_count'], 2)
        self.assertEqual(
            actual_shape['phase_transitions']['history'][0]['phase'],
            'scratch_writes',
        )
        self.assertEqual(actual_shape['phase_history'][0]['sequence'], 5)
        self.assertEqual(coverage['reconciled_at'], '2026-09-21T19:43:41+00:00')
        self.assertNotIn(SECRET, json.dumps(progress))
        self.assertNotIn(SECRET, json.dumps(coverage))

    def test_progress_does_not_export_arbitrary_run_ids_or_timestamps(self):
        state = {
            'progress': {
                'phase': 'reading',
                'run_id': SECRET,
                'last_progress_at': SECRET,
            },
        }
        self.assertNotIn(SECRET, json.dumps(public_progress(state)))


class SessionScenarioTests(unittest.TestCase):
    def test_marker_visibility_requires_exact_excerpt_match(self):
        result = {
            'results': [
                {'excerpt': 'synthetic_source_03_new_marker'},
                {'excerpt': 'synthetic_source_04_old_marker'},
            ]
        }
        self.assertTrue(
            updates.result_contains_marker(
                result,
                'synthetic_source_03_new_marker',
            )
        )
        self.assertFalse(
            updates.result_contains_marker(
                result,
                'synthetic_source_03_old_marker',
            )
        )

    def test_probe_timeout_stops_the_unusable_client_without_retrying(self):
        class TimeoutClient:
            def __init__(self):
                self.calls = 0

            def tool(self, *args, **kwargs):
                self.calls += 1
                raise TimeoutError('probe deadline')

        class StatusClient:
            def rpc(self, method, params, timeout):
                return {
                    'contents': [
                        {
                            'text': json.dumps(
                                {
                                    'project': {},
                                    'sessions': {
                                        'status': 'ready',
                                        'coverage': {
                                            'pending_changes': 0,
                                            'reconciled_at': 'safe',
                                        },
                                        'progress': {
                                            'phase': 'complete',
                                            'run_id': 1,
                                        },
                                    },
                                }
                            )
                        }
                    ]
                }

        search_client = TimeoutClient()
        stop = threading.Event()
        state, lock, threads = updates.start_session_probes(
            search_client,
            StatusClient(),
            'marker',
            stop,
            time.monotonic(),
        )
        deadline = time.monotonic() + 1
        while not state['probe_failed'] and time.monotonic() < deadline:
            time.sleep(.01)
        snapshot = updates.finish_session_probes(stop, threads, state, lock)
        self.assertEqual(search_client.calls, 1)
        self.assertTrue(snapshot['search_probe_failed'])
        self.assertTrue(snapshot['probe_failed'])
        self.assertIsNone(snapshot['first_searchable_seconds'])

    def test_progress_probe_ignores_pre_mutation_terminal_snapshot(self):
        old = {
            'status': 'ready',
            'coverage': {
                'pending_changes': 0,
                'reconciled_at': 'old',
            },
            'progress': {
                'run_id': 4,
                'phase': 'complete',
                'records_processed': 8,
                'chunks_committed': 8,
            },
        }
        new = {
            'status': 'refreshing',
            'coverage': {
                'pending_changes': 1,
                'reconciled_at': 'new',
            },
            'progress': {
                'run_id': 5,
                'phase': 'normalization',
                'records_processed': 1,
                'first_progress_ms': 2,
            },
        }

        class SequenceStatus:
            def __init__(self):
                self.states = [old, new]
                self.calls = 0

            def rpc(self, method, params, timeout):
                state = self.states[min(self.calls, len(self.states) - 1)]
                self.calls += 1
                return {
                    'contents': [
                        {'text': json.dumps({'project': {}, 'sessions': state})}
                    ]
                }

        class EmptySearch:
            def tool(self, *args, **kwargs):
                return {'results': []}

        status_client = SequenceStatus()
        stop = threading.Event()
        state, lock, threads = updates.start_session_probes(
            EmptySearch(),
            status_client,
            'new-marker',
            stop,
            time.monotonic(),
            baseline=old,
        )
        deadline = time.monotonic() + 1
        while state['first_progress'] is None and time.monotonic() < deadline:
            time.sleep(.01)
        snapshot = updates.finish_session_probes(stop, threads, state, lock)
        self.assertIsNotNone(snapshot['first_progress'])
        self.assertEqual(snapshot['first_progress']['run_id'], 5)
        self.assertNotEqual(snapshot['first_progress']['run_id'], 4)

    def test_counter_delta_and_prefix_phase_are_observation_only(self):
        before = {
            'bytes_read': 10,
            'chunks_prepared': 2,
            'chunks_committed': 2,
        }
        after = {
            'bytes_read': 30,
            'chunks_prepared': 3,
            'chunks_committed': 3,
        }
        self.assertEqual(
            updates.counter_delta(before, after),
            {'bytes_read': 20, 'chunks_prepared': 1, 'chunks_committed': 1},
        )
        state = {
            'progress': {
                'phase': 'complete',
                'phase_history': [
                    {
                        'phase': 'prefix_verification',
                        'sequence': 2,
                        'elapsed_ms': 4,
                    }
                ],
            }
        }
        self.assertTrue(
            updates.phase_was_observed([state], 'prefix_verification')
        )

    def test_precise_scope_gate_rejects_full_scan_counters(self):
        precise = {
            'files_discovered': 1,
            'files_completed': 1,
            'records_inspected': 2,
            'records_processed': 2,
            'chunks_prepared': 2,
            'chunks_committed': 2,
        }
        full_scan = dict(precise, files_discovered=8, files_completed=8)
        self.assertTrue(updates.precise_scope_pass(precise))
        self.assertFalse(updates.precise_scope_pass(full_scan))

    def test_settle_after_new_run_requires_new_epoch_or_refresh(self):
        states = [
            {
                'status': 'refreshing',
                'coverage': {'pending_changes': None, 'reconciled_at': 'a'},
                'progress': {'run_id': 2, 'phase': 'prefix_verification'},
            },
            {
                'status': 'ready',
                'coverage': {'pending_changes': 0, 'reconciled_at': 'b'},
                'progress': {'run_id': 2, 'phase': 'complete'},
            },
        ]
        calls = []

        class FakeClient:
            def rpc(self, method, params, timeout):
                calls.append(method)
                state = states.pop(0)
                return {
                    'contents': [
                        {
                            'text': json.dumps(
                                {'project': {}, 'sessions': state}
                            )
                        }
                    ]
                }

            def tool(self, tool, args, timeout):
                calls.append(tool)
                return {
                    'status': 'ready',
                    'coverage': {'pending_changes': 0},
                    'results': [{'match_id': 'prefix'}],
                }

        baseline = {
            'status': 'ready',
            'coverage': {'pending_changes': 0, 'reconciled_at': 'a'},
            'progress': {'run_id': 1, 'phase': 'complete'},
        }
        with mock.patch.object(updates.time, 'sleep'):
            result, _, observations, tracker, final_status = (
                updates.settle_after_new_run(
                    FakeClient(),
                    'search_sessions',
                    'prefix',
                    baseline,
                    timeout=5,
                )
            )
        self.assertTrue(tracker['run_changed'])
        self.assertTrue(tracker['epoch_changed'])
        self.assertTrue(tracker['reconciled_at_changed'])
        self.assertTrue(tracker['new_scan_observed'])
        self.assertEqual(final_status['progress']['phase'], 'complete')
        self.assertEqual(len(observations), 2)
        self.assertEqual(calls, ['resources/read', 'resources/read', 'search_sessions'])
        self.assertEqual(result['results'][0]['match_id'], 'prefix')


SERVER = r'''
import json, sys, time
mode = sys.argv[1]
def send(message):
    print(json.dumps(message), flush=True)
for line in sys.stdin:
    request = json.loads(line)
    method = request['method']
    if mode == 'blocked_initialize' and method == 'initialize':
        time.sleep(30)
    if method == 'notifications/initialized':
        if mode == 'blocked_write':
            time.sleep(30)
        continue
    if method != 'initialize' and mode == 'blocked_read':
        sys.stdout.write('{"jsonrpc":"2.0","id":')
        sys.stdout.flush()
        time.sleep(30)
    if mode == 'search_deadline':
        if method == 'resources/read':
            send({'jsonrpc':'2.0', 'id':request['id'], 'result':{'contents':[{'text':'{"project":{"status":"ready","coverage":{"pending_changes":0}}}'}]}})
            continue
        if method == 'tools/call':
            time.sleep(30)
    if method != 'initialize' and mode == 'exit':
        sys.exit(0)
    if method != 'initialize' and mode == 'notifications_forever':
        while True:
            send({'jsonrpc':'2.0', 'method':'notifications/progress'})
    if mode == 'request_error' and method == 'first':
        send({'jsonrpc':'2.0', 'id':request['id'], 'error':{'message':'private_path_and_content'}})
        continue
    if mode == 'malformed' and method != 'initialize':
        print('{malformed response', flush=True)
        continue
    if mode == 'notifications':
        send({'jsonrpc':'2.0', 'method':'notifications/progress', 'params':{'private':'secret'}})
        send({'jsonrpc':'2.0', 'id':-1, 'result':{'wrong':True}})
        send({'jsonrpc':'2.0', 'id':request['id'], 'method':'notifications/unrelated'})
    send({'jsonrpc':'2.0', 'id':request['id'], 'result':{'method':method}})
'''


class ClientTests(FixtureTest):
    def client(self, mode, request_timeout=2):
        popen = subprocess.Popen
        def fake_server(command, **kwargs):
            return popen([sys.executable, '-u', '-c', SERVER, mode], **kwargs)
        with mock.patch('acceptance.subprocess.Popen', side_effect=fake_server):
            client = Client(
                'synthetic-server',
                self.root,
                self.root / 'cache',
                os.environ.copy(),
                request_timeout=request_timeout,
            )
        self.addCleanup(client.close)
        return client

    def assert_deadline(self, mode, params):
        client = self.client(mode)
        started = time.monotonic()
        with self.assertRaises(TimeoutError):
            client.rpc('blocked', params, timeout=.15)
        self.assertLess(time.monotonic() - started, 1)
        self.assertIsNotNone(client.process.poll())
        self.assertTrue(client.process.stdin.closed)
        self.assertTrue(client.process.stdout.closed)
        with self.assertRaisesRegex(RuntimeError, 'connection is closed'):
            client.rpc('must_not_reuse', {})
        self.assertFalse(client.worker.is_alive())
        self.assertTrue(client.worker.daemon)

    def test_deadline_includes_partial_line_blocked_read_and_closes_connection(self):
        self.assert_deadline('blocked_read', {})

    def test_initialize_uses_the_same_request_deadline(self):
        started = time.monotonic()
        with self.assertRaises(TimeoutError):
            self.client('blocked_initialize', request_timeout=.15)
        self.assertLess(time.monotonic() - started, 1)

    def test_deadline_includes_blocked_write_and_closes_connection(self):
        self.assert_deadline('blocked_write', {'payload':'x' * (4 * 1024 * 1024)})

    def test_notifications_do_not_extend_deadline(self):
        self.assert_deadline('notifications_forever', {})

    def test_lock_wait_timeout_closes_the_frontend(self):
        client = self.client('notifications')
        self.assertTrue(client.lock.acquire())
        started = time.monotonic()
        try:
            with self.assertRaises(TimeoutError):
                client.rpc('blocked_by_other_request', {}, timeout=.15)
        finally:
            client.lock.release()
        self.assertLess(time.monotonic() - started, 1)
        self.assertIsNotNone(client.process.poll())
        self.assertTrue(client.process.stdin.closed)
        self.assertTrue(client.process.stdout.closed)

    def test_notifications_and_unrelated_ids_are_filtered_without_losing_next_response(self):
        client = self.client('notifications')
        self.assertEqual(client.rpc('first', {}), {'method':'first'})
        self.assertEqual(client.rpc('second', {}), {'method':'second'})

    def test_eof_invalidates_connection(self):
        client = self.client('exit')
        with self.assertRaisesRegex(RuntimeError, 'MCP request failed'):
            client.rpc('exit', {})
        self.assertTrue(client.process.stdout.closed)
        with self.assertRaisesRegex(RuntimeError, 'connection is closed'):
            client.rpc('second', {})

    def test_malformed_response_invalidates_connection_without_leaking_content(self):
        client = self.client('malformed')
        with self.assertRaisesRegex(RuntimeError, 'MCP request failed'):
            client.rpc('malformed', {})
        self.assertTrue(client.process.stdin.closed)
        self.assertTrue(client.process.stdout.closed)
        with self.assertRaisesRegex(RuntimeError, 'connection is closed'):
            client.rpc('second', {})

    def test_matching_error_response_does_not_desynchronize_connection(self):
        client = self.client('request_error')
        with self.assertRaisesRegex(RuntimeError, '^MCP request failed$'):
            client.rpc('first', {})
        self.assertEqual(client.rpc('second', {}), {'method':'second'})

    def test_settle_bounds_blocked_status_and_final_search_by_total_budget(self):
        for mode in ('blocked_read', 'search_deadline'):
            with self.subTest(mode=mode):
                client = self.client(mode)
                start = time.monotonic()
                with self.assertRaises(TimeoutError):
                    settle(client, 'search_project', timeout=.15)
                self.assertLess(time.monotonic() - start, 1)
                self.assertIsNotNone(client.process.poll())
                self.assertTrue(client.process.stdout.closed)
                with self.assertRaisesRegex(RuntimeError, 'connection is closed'):
                    settle(client, 'search_project', timeout=.15)

    def test_settle_propagates_transport_failure_without_reconnecting(self):
        for mode in ('exit', 'malformed'):
            with self.subTest(mode=mode):
                client = self.client(mode)
                with mock.patch('acceptance.subprocess.Popen') as reconnect:
                    with self.assertRaisesRegex(RuntimeError, '^MCP request failed$'):
                        settle(client, 'search_project', timeout=.5)
                    with self.assertRaisesRegex(RuntimeError, 'connection is closed'):
                        settle(client, 'search_project', timeout=.5)
                reconnect.assert_not_called()
                self.assertTrue(client.closed.is_set())


class SettleTests(unittest.TestCase):
    def setUp(self):
        self.now = 0
        self.clock = mock.patch('acceptance.time.monotonic', side_effect=lambda: self.now)
        self.clock.start()
        self.addCleanup(self.clock.stop)
        self.sleeps = mock.patch('acceptance.time.sleep', side_effect=self.advance)
        self.sleep = self.sleeps.start()
        self.addCleanup(self.sleeps.stop)

    def advance(self, seconds):
        self.now += seconds

    def status_response(self, state):
        return {'contents': [{'text': json.dumps({'project': state})}]}

    def ready_client(self):
        client = mock.Mock()
        client.rpc.return_value = self.status_response({
            'status': 'ready', 'coverage': {'pending_changes': 0},
        })
        client.tool.return_value = {
            'status': 'ready', 'coverage': {'pending_changes': 0}, 'results': [],
        }
        return client

    def test_status_search_race_resumes_observation_until_recovery(self):
        ready = {'status': 'ready', 'coverage': {'pending_changes': 0}}
        refreshing = {'status': 'refreshing', 'coverage': {'pending_changes': None}}
        final = {**ready, 'results': []}
        states = [ready, refreshing, ready]
        searches = [{**refreshing, 'results': []}, final]
        calls = []
        observed = []
        clock = [0]

        class FakeClient:
            def rpc(self, method, params, timeout):
                calls.append(method)
                return {'contents': [{'text': json.dumps({'project': states.pop(0)})}]}

            def tool(self, tool, args, timeout):
                calls.append(tool)
                return searches.pop(0)

        def sleep(seconds):
            clock[0] += seconds

        with mock.patch('acceptance.time.monotonic', side_effect=lambda: clock[0]), \
             mock.patch('acceptance.time.sleep', side_effect=sleep) as sleeps:
            actual, elapsed = settle(
                FakeClient(), 'search_project', timeout=5, observer=observed.append,
            )
        self.assertIs(actual, final)
        self.assertEqual(calls, [
            'resources/read', 'search_project', 'resources/read',
            'resources/read', 'search_project',
        ])
        self.assertEqual(observed, [ready, refreshing, ready])
        self.assertEqual(sleeps.call_args_list, [mock.call(1), mock.call(1)])
        self.assertEqual(elapsed, 2)

    def test_resources_only_until_settled_then_exactly_one_requested_search(self):
        states = [
            {'status':'building', 'coverage':{'pending_changes':0}},
            {'status':'ready', 'coverage':{'pending_changes':2}},
            {'status':'degraded', 'coverage':{'pending_changes':0}},
        ]
        calls = []
        observed = []
        result = {
            'status': 'degraded', 'coverage': {'pending_changes': 0},
            'results': ['requested-result'],
        }

        class FakeClient:
            def rpc(self, method, params, timeout):
                calls.append((method, params, timeout))
                state = states.pop(0)
                return {'contents':[{'text':json.dumps({'sessions':state, 'project':{}})}]}

            def tool(self, tool, args, timeout):
                if states:
                    raise AssertionError('ranked search before settled')
                calls.append((tool, args, timeout))
                return result

        with mock.patch('acceptance.time.sleep') as sleep:
            actual, elapsed = settle(FakeClient(), 'search_sessions', 'requested query',
                                     timeout=5, arguments={'agent':'codex'}, observer=observed.append)
        self.assertIs(actual, result)
        self.assertGreaterEqual(elapsed, 0)
        self.assertEqual([call[0] for call in calls], ['resources/read'] * 3 + ['search_sessions'])
        self.assertTrue(all(call[1] == {'uri':'bm25://indexing/status'} for call in calls[:3]))
        self.assertEqual(calls[-1][1], {'query':'requested query', 'agent':'codex'})
        self.assertEqual(sleep.call_args_list, [mock.call(1), mock.call(1)])
        self.assertEqual(len(observed), 3)
        self.assertTrue(all(0 < call[2] <= 5 for call in calls))
        self.assertLessEqual(calls[-1][2], calls[0][2])

    def test_timeout_before_ready_never_searches(self):
        client = mock.Mock()
        client.rpc.return_value = {'contents':[{'text':json.dumps({
            'project':{'status':'building', 'coverage':{'pending_changes':1}},
        })}]}
        started = time.monotonic()
        with self.assertRaises(TimeoutError):
            settle(client, 'search_project', timeout=.04)
        self.assertLess(time.monotonic() - started, .5)
        client.tool.assert_not_called()

    def test_final_search_uses_remaining_total_deadline(self):
        clock = [0]
        budgets = []
        class FakeClient:
            def rpc(self, method, params, timeout):
                budgets.append(timeout)
                clock[0] += 4
                return {'contents':[{'text':'{"project":{"status":"ready","coverage":{"pending_changes":0}}}'}]}
            def tool(self, tool, args, timeout):
                budgets.append(timeout)
                clock[0] += 2
                return {}
        with mock.patch('acceptance.time.monotonic', side_effect=lambda: clock[0]):
            with self.assertRaises(TimeoutError):
                settle(FakeClient(), 'search_project', timeout=5)
        self.assertEqual(budgets, [5, 1])

    def test_content_predicate_waits_for_a_new_status_epoch(self):
        states = [
            {
                'status': 'ready',
                'coverage': {'pending_changes': 0, 'safe_epoch': 7},
            },
            {
                'status': 'ready',
                'coverage': {'pending_changes': 0, 'safe_epoch': 8},
            },
        ]
        calls = []

        class FakeClient:
            def rpc(self, method, params, timeout):
                calls.append(method)
                state = states.pop(0)
                return {
                    'contents': [
                        {'text': json.dumps({'project': state, 'sessions': {}})}
                    ]
                }

            def tool(self, tool, args, timeout):
                calls.append(tool)
                return {'status': 'ready', 'coverage': {'pending_changes': 0}}

        actual, _ = settle(
            FakeClient(),
            'search_project',
            timeout=5,
            status_predicate=lambda state: state['coverage']['safe_epoch'] == 8,
        )
        self.assertEqual(actual['status'], 'ready')
        self.assertEqual(calls, ['resources/read', 'resources/read', 'search_project'])

    def test_repeated_races_expire_the_original_deadline(self):
        client = self.ready_client()
        client.tool.return_value = {
            'status': 'refreshing', 'coverage': {'pending_changes': None}, 'results': [],
        }
        observed = []
        with self.assertRaisesRegex(TimeoutError, 'Reconciliation deadline exceeded'):
            settle(client, 'search_project', timeout=2.5, observer=observed.append)
        self.assertEqual(self.now, 2.5)
        self.assertEqual(self.sleep.call_args_list, [mock.call(1), mock.call(1), mock.call(.5)])
        self.assertEqual([call.kwargs['timeout'] for call in client.rpc.call_args_list], [2.5, 1.5, .5])
        self.assertEqual([call.kwargs['timeout'] for call in client.tool.call_args_list], [2.5, 1.5, .5])
        self.assertEqual(len(observed), 3)
        self.assertEqual(
            [call[0] for call in client.method_calls],
            ['rpc', 'tool', 'rpc', 'tool', 'rpc', 'tool'],
        )

    def test_empty_ready_and_completed_degraded_searches_are_valid(self):
        for status in ('ready', 'degraded'):
            with self.subTest(status=status):
                client = self.ready_client()
                state = {
                    'status': status,
                    'coverage': {'pending_changes': 0, 'error_count': 2 if status == 'degraded' else 0},
                }
                client.rpc.return_value = self.status_response(state)
                client.tool.return_value = {**state, 'results': []}
                actual, elapsed = settle(client, 'search_project', timeout=5)
                self.assertIs(actual, client.tool.return_value)
                self.assertEqual(actual['results'], [])
                self.assertEqual(elapsed, 0)
                client.rpc.assert_called_once()
                client.tool.assert_called_once()
        self.sleep.assert_not_called()

    def test_non_ready_searches_return_to_bounded_status_observation(self):
        for status, pending in (
            ('building', 0), ('refreshing', 0), ('ready', 1), ('ready', None),
            ('degraded', 1), ('degraded', None),
        ):
            with self.subTest(status=status, pending=pending):
                self.now = 0
                self.sleep.reset_mock()
                client = self.ready_client()
                final = client.tool.return_value
                client.tool.side_effect = [
                    {'status': status, 'coverage': {'pending_changes': pending}, 'results': []},
                    final,
                ]
                ready = {'status': 'ready', 'coverage': {'pending_changes': 0}}
                waiting = [
                    {'status': 'building', 'coverage': {'pending_changes': None},
                     'progress': {'records_processed': count}}
                    for count in (1, 2, 3)
                ]
                client.rpc.side_effect = [
                    self.status_response(state) for state in [ready, *waiting, ready]
                ]
                observations = []
                actual, elapsed = settle(
                    client, 'search_project', timeout=5, poll_interval=.25,
                    observer=observations.append,
                )
                self.assertIs(actual, final)
                self.assertEqual(elapsed, 1)
                self.assertEqual(observations, [ready, *waiting, ready])
                self.assertEqual(
                    [call[0] for call in client.method_calls],
                    ['rpc', 'tool', 'rpc', 'rpc', 'rpc', 'rpc', 'tool'],
                )
                self.assertEqual(self.sleep.call_args_list, [mock.call(.25)] * 4)

    def test_status_predicate_is_reapplied_after_race_but_not_to_search_progress(self):
        client = self.ready_client()
        ready = {'status': 'ready', 'coverage': {'pending_changes': 0}}
        states = [
            {**ready, 'progress': {'run_id': run_id}}
            for run_id in (2, 1, 2)
        ]
        client.rpc.side_effect = [self.status_response(state) for state in states]
        final = client.tool.return_value
        client.tool.side_effect = [
            {'status': 'refreshing', 'coverage': {'pending_changes': None}, 'results': []},
            final,
        ]
        predicate = mock.Mock(side_effect=lambda state: state['progress']['run_id'] == 2)
        actual, _ = settle(
            client, 'search_project', timeout=5, status_predicate=predicate,
        )
        self.assertIs(actual, final)
        self.assertEqual(predicate.call_args_list, [mock.call(state) for state in states])
        self.assertEqual(client.tool.call_count, 2)
        self.assertNotIn('progress', actual)

    def test_malformed_status_protocol_fails_instead_of_polling(self):
        for response in (
            None, [], {}, {'contents': []}, {'contents': [None]},
            {'contents': [{'text': None}]}, {'contents': [{'text': SECRET}]},
            {'contents': [{'text': '[]'}]}, {'contents': [{'text': '{"sessions":{}}'}]},
        ):
            with self.subTest(response=response):
                client = self.ready_client()
                client.rpc.return_value = response
                with self.assertRaisesRegex(RuntimeError, '^Invalid (status|readiness) response$'):
                    settle(client, 'search_project', timeout=5)
                client.rpc.assert_called_once()
                client.tool.assert_not_called()
        self.sleep.assert_not_called()

    def test_malformed_status_and_search_readiness_are_not_retried(self):
        malformed = [
            None, [], {}, {'status': SECRET, 'coverage': {'pending_changes': 0}},
            {'status': 'ready'}, {'status': 'ready', 'coverage': []},
            {'status': 'ready', 'coverage': {}},
        ] + [
            {'status': 'ready', 'coverage': {'pending_changes': pending}}
            for pending in (False, True, -1, 0.0, '0', [], {})
        ]
        for stage in ('status', 'search'):
            for response in malformed:
                with self.subTest(stage=stage, response=response):
                    client = self.ready_client()
                    if stage == 'status':
                        client.rpc.return_value = self.status_response(response)
                    else:
                        client.tool.return_value = response
                    with self.assertRaisesRegex(RuntimeError, '^Invalid readiness response$'):
                        settle(client, 'search_project', timeout=5)
                    client.rpc.assert_called_once()
                    self.assertEqual(client.tool.call_count, int(stage == 'search'))
        self.sleep.assert_not_called()

    def test_transport_and_timeout_failures_propagate_without_retry(self):
        for stage in ('rpc', 'tool'):
            for error in (OSError(SECRET), TimeoutError(SECRET), RuntimeError(SECRET)):
                with self.subTest(stage=stage, error=type(error).__name__):
                    client = self.ready_client()
                    getattr(client, stage).side_effect = error
                    with self.assertRaises(type(error)) as caught:
                        settle(client, 'search_project', timeout=5)
                    self.assertIs(caught.exception, error)
                    client.rpc.assert_called_once()
                    self.assertEqual(client.tool.call_count, int(stage == 'tool'))
        self.sleep.assert_not_called()

    def test_status_and_observer_time_count_against_the_same_deadline(self):
        client = self.ready_client()

        def slow_status(*args, **kwargs):
            self.advance(2)
            return client.rpc.return_value

        client.rpc.side_effect = slow_status
        with self.assertRaises(TimeoutError):
            settle(
                client, 'search_project', timeout=3,
                observer=lambda state: self.advance(2),
            )
        client.tool.assert_not_called()
        self.sleep.assert_not_called()

    def test_invalid_poll_interval_is_rejected_without_protocol_io(self):
        for interval in (-1, float('inf'), float('nan')):
            with self.subTest(interval=interval):
                client = self.ready_client()
                with self.assertRaises(ValueError):
                    settle(client, 'search_project', poll_interval=interval)
                client.rpc.assert_not_called()


class WriterTests(FixtureTest):
    def test_concurrent_records_are_whole_visible_and_synced(self):
        path = self.root / 'results.jsonl'
        with path.open('w') as stream, mock.patch('acceptance.os.fsync', wraps=os.fsync) as sync:
            writer = JsonlWriter(stream, durable=True)
            threads = [threading.Thread(target=lambda n=n: [writer.write({'worker':n, 'row':i}) for i in range(20)])
                       for n in range(4)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=3)
                self.assertFalse(thread.is_alive())
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual(len(rows), 80)
            self.assertEqual(len({(r['worker'], r['row']) for r in rows}), 80)
            self.assertEqual(sync.call_count, 80)

    def test_acceptance_report_omits_project_paths_queries_and_raw_diagnostics(self):
        binary = self.root / 'binary'
        binary.write_bytes(b'synthetic')
        project = self.root / 't3code'
        project.mkdir()
        output = io.StringIO()
        response = {'status':'ready', 'coverage':{'error_count':0, 'diagnostics':{SECRET:10}},
                    'results':[{'relative_path':'apps/server/src/sourceControl/GitHubCli.ts'}]}
        with mock.patch.object(acceptance, '_stdout_writer', JsonlWriter(output)), \
             mock.patch.object(acceptance, 'Client'), \
             mock.patch.object(acceptance, 'settle', return_value=(response, .1)), \
             mock.patch.object(acceptance, 'measure', return_value=([1], 0, 100)), \
             mock.patch.object(acceptance.time, 'sleep'), \
             mock.patch.object(sys, 'argv', ['acceptance.py', '--project', str(project),
                                             '--binary', str(binary), '--requests', '4']):
            acceptance.main()
        public = output.getvalue()
        for sensitive in (str(project), 't3code', 'GitHubCliAuthenticationError',
                          'GitHub CLI is not authenticated', 'apps/server', SECRET):
            self.assertNotIn(sensitive, public)
        rows = [json.loads(line) for line in public.splitlines()]
        self.assertEqual(rows[0]['project'], 'project-0001')
        self.assertEqual(sum(r['phase'] == 'relevance' for r in rows), 2)


class MeasurementTests(FixtureTest):
    def response(self, status='ready', pending=0):
        return {
            'status': status, 'coverage': {'pending_changes': pending},
            'results': [],
        }

    def test_acceptance_excludes_later_non_ready_latency_samples(self):
        client = mock.Mock()
        ready = self.response()
        client.tool.side_effect = (
            [ready] * 11
            + [self.response('refreshing', None), self.response('ready', 2),
               self.response('ready', None), ready, self.response('degraded')]
        )
        with mock.patch.object(acceptance.time, 'sleep'):
            samples, partial, _ = acceptance.measure(client, 2, 0, timeout=5)
        self.assertEqual(len(samples), 2)
        self.assertEqual(partial, 3)
        self.assertEqual(client.tool.call_count, 16)

    def test_context_transport_failure_is_not_mistaken_for_a_stale_match(self):
        for error in (
            RuntimeError('MCP connection is closed'),
            acceptance.McpRequestError('MCP request failed'),
        ):
            with self.subTest(error=type(error).__name__):
                client = mock.Mock()
                ready = self.response()
                session = {**ready, 'results': [{'match_id': 'match'}]}
                client.tool.side_effect = [session] + [ready] * 10 + [error, ready]
                if isinstance(error, acceptance.McpRequestError):
                    samples, partial, _ = acceptance.measure(client, 1, 9, timeout=5)
                    self.assertEqual(len(samples), 1)
                    self.assertEqual(partial, 1)
                    self.assertEqual(client.tool.call_count, 13)
                else:
                    with self.assertRaises(RuntimeError) as caught:
                        acceptance.measure(client, 1, 9, timeout=5)
                    self.assertIs(caught.exception, error)
                    self.assertEqual(client.tool.call_count, 12)

    def test_benchmark_excludes_later_non_ready_latency_samples(self):
        client = mock.Mock()
        ready = self.response()
        client.rpc.return_value = {
            'contents': [{'text': json.dumps({'project': ready, 'sessions': ready})}],
        }
        client.tool.side_effect = [ready, ready, self.response('building', None), ready]
        rows = []
        with mock.patch.object(benchmark, 'Client', return_value=client), \
             mock.patch.object(benchmark, 'emit', side_effect=rows.append), \
             mock.patch.object(benchmark.time, 'sleep'), \
             mock.patch.object(benchmark.tempfile, 'TemporaryDirectory',
                               return_value=contextlib.nullcontext(str(self.root))), \
             mock.patch.object(sys, 'argv', [
                 'benchmark-mcp.py', '--project', str(self.root), '--requests', '1',
             ]):
            benchmark.main()
        measurement = next(row for row in rows if row['phase'] == 'queries')
        self.assertEqual(measurement['requests'], 1)
        self.assertEqual(measurement['partial_responses_excluded'], 1)
        self.assertEqual(client.tool.call_count, 4)
        client.close.assert_called_once()

    def test_pressure_baseline_compares_the_settled_responses_without_an_unchecked_reread(self):
        binary = self.root / 'binary'
        binary.write_bytes(b'synthetic')
        clients = []
        rows = []
        ready = {**self.response(), 'results': [{'relative_path': 'fixture.rs', 'score': 1}]}

        def make_client(*args, **kwargs):
            client = mock.Mock()
            client.rpc.return_value = {
                'contents': [{'text': json.dumps({'project': ready})}],
            }
            client.tool.side_effect = [ready, self.response('refreshing', None)]
            clients.append(client)
            return client

        with mock.patch.object(pressure, 'Client', side_effect=make_client), \
             mock.patch.object(pressure, 'make_fixture'), \
             mock.patch.object(pressure, 'emit', side_effect=rows.append), \
             mock.patch.object(pressure, 'wait_pressure', side_effect=TimeoutError('stop after baseline')), \
             mock.patch.object(pressure.tempfile, 'TemporaryDirectory',
                               return_value=contextlib.nullcontext(str(self.root))), \
             mock.patch.object(sys, 'argv', ['check-pressure.py', '--binary', str(binary)]):
            with self.assertRaisesRegex(TimeoutError, 'stop after baseline'):
                pressure.main()
        baseline = next(row for row in rows if row['phase'] == 'baseline_compare')
        expected = [{'path': 'fixture.rs', 'score': 1.0}]
        self.assertEqual(baseline['normal'], expected)
        self.assertEqual(baseline['pressure_owner'], expected)
        for client in clients[:2]:
            client.tool.assert_called_once_with(
                'search_project', pressure.project_arguments(pressure.READ_QUERY),
                timeout=mock.ANY,
            )


class EvaluationTests(FixtureTest):
    def setUp(self):
        super().setUp()
        self.private = self.root / 'validation' / 'private-cache' / 'sibling-quality'
        self.private.mkdir(parents=True)
        self.binary = self.root / 'fake-binary'
        self.binary.write_bytes(b'synthetic binary')
        self.projects = []
        for number in range(3):
            project_root = self.root / f'{SECRET}-{number}'
            project_root.mkdir()
            (project_root / (SECRET + '.rs')).write_text(SECRET)
            self.projects.append({
                'name':f'{SECRET}-{number}', 'root':str(project_root), 'family':'synthetic-family',
                'queries':[{'id':f'{SECRET}-{i}', 'kind':'project', 'category':'identifier',
                            'query':SECRET, 'path':SECRET + '.rs', 'needle':SECRET} for i in range(2)],
            })
        (self.private / 'manifest.json').write_text(json.dumps({'projects':self.projects, 'snapshot_files':0}))
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(mock.patch.object(evaluation, 'ROOT', self.root))
        self.stack.enter_context(mock.patch.object(evaluation, 'PRIVATE', self.private))
        self.stack.enter_context(mock.patch.object(evaluation.time, 'sleep'))
        self.stack.enter_context(mock.patch.object(summary, 'ROOT', self.root))
        self.output = self.stack.enter_context(contextlib.redirect_stdout(io.StringIO()))

    def response(self):
        return {
            'status':'ready', 'coverage':{
                'pending_changes':0,
                'diagnostics':{'unsupported_record':3, SECRET:4, 'source_read':SECRET},
                'memory':{'peak_observed_rss_bytes':1024},
                'errors':[SECRET],
            },
            'progress':{'phase':'reading', 'records_processed':2, 'bytes_read':100,
                        'last_progress_at':SECRET, 'run_id':SECRET, 'current_path':SECRET},
            'results':[{'relative_path':SECRET + '.rs', 'excerpt':SECRET, 'start_byte':0}],
            'truncated':False,
        }

    def fake_client(self, hook=None):
        test = self
        class FakeClient:
            def __init__(self, binary, root, cache, env):
                self.root = root
                self.queries = 0
            def rpc(self, method, params, timeout=None):
                return {'contents':[{'text':json.dumps({'project':test.response()})}]}
            def tool(self, tool, args, timeout=None):
                if args['query'] != 'index':
                    self.queries += 1
                    if hook:
                        hook(self)
                return test.response()
            def close(self):
                pass
        return FakeClient

    def rows(self, label='after'):
        return [json.loads(line) for line in (self.root / 'validation' / f'siblings-{label}.jsonl').read_text().splitlines()]

    def test_later_non_ready_query_is_not_published_as_a_successful_measurement(self):
        client_type = self.fake_client()

        class RacedClient(client_type):
            def tool(self, tool, args, timeout=None):
                result = super().tool(tool, args, timeout)
                if args['query'] != 'index' and self.queries == 2:
                    result.update(status='refreshing', results=[])
                    result['coverage']['pending_changes'] = None
                return result

        with mock.patch.object(evaluation, 'Client', RacedClient):
            evaluation.evaluate(self.binary, 'after', 1)
        rows = self.rows()
        self.assertEqual(sum(row['type'] == 'query' for row in rows), 3)
        self.assertFalse(any(row['type'] == 'complete' for row in rows))
        failures = [row for row in rows if row['type'] == 'failure']
        self.assertEqual(len(failures), 3)
        self.assertEqual({row['error'] for row in failures}, {'not_settled'})
        self.assertNotIn(SECRET, json.dumps(rows))

    def test_completed_rows_visible_during_later_query_and_failing_worktree(self):
        query_blocked, query_release = threading.Event(), threading.Event()
        project_blocked, project_release = threading.Event(), threading.Event()
        errors = []
        observed = []
        def hook(client):
            if client.root == Path(self.projects[0]['root']) and client.queries == 2:
                query_blocked.set()
                if not query_release.wait(5):
                    raise TimeoutError(SECRET)
            if client.root == Path(self.projects[1]['root']):
                project_blocked.set()
                if not project_release.wait(5):
                    raise TimeoutError(SECRET)
                raise TimeoutError(SECRET)
        def evaluate():
            try:
                evaluation.evaluate(self.binary, 'after', 2, observer=observed.append)
            except Exception as error:
                errors.append(error)
        with mock.patch.object(evaluation, 'Client', self.fake_client(hook)):
            thread = threading.Thread(target=evaluate, daemon=True)
            thread.start()
            try:
                self.assertTrue(query_blocked.wait(3))
                rows = self.rows()
                self.assertEqual(sum(r['type'] == 'query' for r in rows), 1)
                self.assertEqual(sum(r['type'] == 'coverage' for r in rows), 1)
                self.assertFalse(any(r['type'] == 'complete' for r in rows))
                query_release.set()
                self.assertTrue(project_blocked.wait(3))
                rows = self.rows()
                self.assertEqual(sum(r['type'] == 'complete' for r in rows), 1)
                prefix = (self.root / 'validation' / 'siblings-after.jsonl').read_bytes()
            finally:
                query_release.set()
                project_release.set()
                thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        rows = self.rows()
        self.assertEqual(sum(r['type'] == 'complete' for r in rows), 2)
        self.assertEqual([r['error'] for r in rows if r['type'] == 'failure'], ['timeout'])
        public = (self.root / 'validation' / 'siblings-after.jsonl').read_text()
        self.assertTrue(public.encode().startswith(prefix))
        self.assertNotIn(SECRET, public + self.output.getvalue() + json.dumps(observed))
        self.assertNotIn('expected_path', public)
        self.assertEqual(rows[0]['schema_version'], 2)
        self.assertTrue(all(r['project'].startswith('project-') for r in rows[1:]))
        self.assertTrue(all(r['progress']['phase'] == 'reading' for r in observed))
        self.assertTrue(all(r['progress']['bytes_read'] == 100 for r in observed))
        responses = list((self.private / 'responses-after').glob('*.json'))
        self.assertEqual(len(responses), 4)
        self.assertIn(SECRET, responses[0].read_text())
        if os.name != 'nt':
            self.assertEqual(responses[0].stat().st_mode & 0o777, 0o600)

    def test_manifest_labels_and_existing_summary_consumer_remain_usable(self):
        with mock.patch.object(evaluation, 'Client', self.fake_client()):
            evaluation.evaluate(self.binary, 'before', 1)
            identity_scope = json.loads(
                (self.private / 'manifest.json').read_text()
            )['identity_scope']
            evaluation.evaluate(self.binary, 'after', 1)
            evaluation.evaluate(self.binary, 'filtered', 1, only=[self.projects[1]['name']])
        self.assertEqual(
            json.loads((self.private / 'manifest.json').read_text())['identity_scope'],
            identity_scope,
        )
        before = [r for r in self.rows('before') if r['type'] == 'query']
        after = [r for r in self.rows() if r['type'] == 'query']
        self.assertEqual([(r['project'], r['id']) for r in before], [(r['project'], r['id']) for r in after])
        filtered = [r for r in self.rows('filtered') if r['type'] == 'query']
        self.assertEqual({r['project'] for r in filtered}, {before[2]['project']})
        # Exercise the consumer's private-response lookup on a visibility regression.
        path = self.root / 'validation' / 'siblings-after.jsonl'
        rows = self.rows()
        query = next(r for r in rows if r['type'] == 'query')
        query['visible_target_rank'] = None
        path.write_text(''.join(json.dumps(r) + '\n' for r in rows))
        summary.main()
        report = json.loads((self.root / 'validation' / 'sibling-quality-summary.json').read_text())
        self.assertEqual(report['completed'], {'before':3, 'after':3})
        self.assertEqual(len(report['visibility_regressions']), 1)
        self.assertTrue(report['visibility_regressions'][0]['all_query_terms_visible_in_target'])
        self.assertEqual(report['metrics']['project']['after']['queries'], 6)
        self.assertTrue(all(value['after']['queries'] == 2 for value in report['by_project'].values()))
        self.assertNotIn(SECRET, json.dumps(report))

    def test_registration_failure_is_redacted_and_other_projects_continue(self):
        base_client = self.fake_client()
        test = self
        class FailingClient(base_client):
            def __init__(self, binary, root, cache, env):
                if root == Path(test.projects[1]['root']):
                    raise OSError(SECRET)
                super().__init__(binary, root, cache, env)
        with mock.patch.object(evaluation, 'Client', FailingClient):
            evaluation.evaluate(self.binary, 'after', 1)
        rows = self.rows()
        failures = [r for r in rows if r['type'] == 'failure']
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]['stage'], 'registration')
        self.assertEqual(failures[0]['error'], 'io_error')
        self.assertEqual(sum(r['type'] == 'complete' for r in rows), 2)
        self.assertNotIn(SECRET, json.dumps(rows))


if __name__ == '__main__':
    unittest.main()
