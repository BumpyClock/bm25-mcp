#!/usr/bin/env python3
"""Measure steady-state MCP round trips after status-based reconciliation."""

import argparse
import os
from pathlib import Path
import statistics
import tempfile
import time

from acceptance import (
    Client,
    emit,
    is_settled,
    public_coverage,
    public_memory,
    public_progress,
    public_status,
    settle,
)


def percentile(samples, fraction):
    ordered = sorted(samples)
    return ordered[min(len(ordered) - 1, int(len(ordered) * fraction))]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', type=Path, required=True)
    parser.add_argument('--requests', type=int, default=1000)
    parser.add_argument(
        '--binary',
        type=Path,
        default=Path(__file__).resolve().parents[1] / 'target/release/bm25-mcp',
    )
    parser.add_argument('--empty-session-homes', action='store_true')
    parser.add_argument('--timeout', type=float, default=300)
    args = parser.parse_args()
    if args.requests < 1 or args.timeout <= 0:
        raise ValueError('requests and timeout must be positive')

    binary = args.binary.resolve()
    with tempfile.TemporaryDirectory(
        prefix='.bm25-mcp-benchmark-',
        dir=Path.cwd(),
    ) as scratch:
        scratch = Path(scratch)
        env = os.environ.copy()
        # Always isolate provider homes. The flag remains accepted for
        # compatibility with older invocations; benchmark fixtures are empty
        # regardless of its value.
        for name in ['CODEX_HOME', 'CLAUDE_CONFIG_DIR', 'COPILOT_HOME']:
            env[name] = str(scratch / name)
            (scratch / name).mkdir(mode=0o700)

        client = None
        deadline = time.monotonic() + args.timeout

        def remaining():
            budget = deadline - time.monotonic()
            if budget <= 0:
                raise TimeoutError('benchmark deadline exceeded')
            return budget

        try:
            client = Client(
                binary,
                args.project.resolve(),
                scratch / 'cache',
                env,
                request_timeout=min(30, args.timeout),
            )
            for tool in ['search_project', 'search_sessions']:
                result, elapsed = settle(
                    client,
                    tool,
                    timeout=remaining(),
                )
                emit(
                    {
                        'phase': 'reconciled',
                        'tool': tool,
                        'elapsed_seconds': elapsed,
                        'status': public_status(result.get('status')),
                        'coverage': public_coverage(result.get('coverage')),
                        'progress': public_progress(result),
                        'search_used_only_after_status': True,
                    }
                )

            samples = []
            partial = 0
            attempts = 0
            peak_rss = 0
            queries = [
                'BM25',
                'tokenizer',
                'memory',
                'index search',
                'nonexistentbenchmarkqueryqzx',
            ]
            while len(samples) < args.requests:
                begin = time.perf_counter()
                result = client.tool(
                    'search_project',
                    {'query': queries[attempts % len(queries)]},
                    timeout=remaining(),
                )
                elapsed = (time.perf_counter() - begin) * 1000
                attempts += 1
                coverage = result.get('coverage') or {}
                memory = public_memory(coverage.get('memory'))
                peak_rss = max(
                    peak_rss,
                    memory.get('peak_observed_rss_bytes', 0)
                )
                if is_settled(result):
                    samples.append(elapsed)
                else:
                    partial += 1
                    time.sleep(min(.02, remaining()))

            emit(
                {
                    'phase': 'queries',
                    'requests': len(samples),
                    'median_ms': statistics.median(samples),
                    'p95_ms': percentile(samples, .95),
                    'partial_responses_excluded': partial,
                    'peak_observed_owner_rss_bytes': peak_rss,
                    'scope': (
                        'single-client reconciled MCP round trip; '
                        'does not establish ingestion acceptance gates'
                    ),
                }
            )
        finally:
            if client is not None:
                client.close()
            time.sleep(4)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        emit(
            {
                'phase': 'failure',
                'error': 'timeout' if isinstance(error, TimeoutError) else 'benchmark_error',
            }
        )
        raise SystemExit(1) from None
