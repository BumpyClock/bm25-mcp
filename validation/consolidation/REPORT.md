# Architectural consolidation verification

Implemented in two separately gated waves. No dependency change, database
migration, or checkpoint-version change was made. This report records validation
before the subsequently authorized commit and push.

## Reviewed state

- HEAD and architectural baseline: `dc0f3415bd65a2605239961a12009b4bdbeded85`.
- Initial working tree: clean. Validation ran against the uncommitted implementation.
- Read the supplied instructions, machine-local instructions, README, ranking
  contracts, coverage/progress changes, scanners, store, runtime and tests.
  No repository or ancestor AGENTS.md was present. No concurrent source edits
  were observed or overwritten.
- Baseline: 169 Rust tests, 43 Python tests, formatting, Clippy, diff check and
  release builds passed. Baseline release exercises and evaluations also passed.
- The recent coverage ledger, Python `settle()` response validation and deadline,
  source-specific session reconciliation, private progress, immediate evaluation
  output, scratch batching and prior ranking fixes were retained.

## Ownership changes

| Previous owners | New owner / contract |
| --- | --- |
| Root/session epoch, scanned and safe-epoch atomics, pending path mutexes, full/in-flight flags | One `reconciliation::Controller` per collection; named scope and run tickets |
| Worker-local coverage ledgers and separate coverage mutexes | The same `CollectionCoverage` implementation, owned by the controller |
| Watcher callbacks mutating lifecycle fields | Callbacks report scope and watcher diagnostics through controller methods |
| Project/session loops calculating completion and merging retries | Concrete `Run::project` / `Run::sessions` effect adapters; controller returns completion |
| Unqualified successful source reports | Store-issued source/version/eligibility or absence verification; run-bound publisher |
| Response code reconstructing readiness from atomics | `Controller::snapshot` and `validates`; tools only convert coverage and adapt result shapes |
| Candidate ID sets and separate retrieval score maps | One `Admission` record per ID, with all lane evidence and optional raw-query BM25 |
| Caller-assembled ranking inputs | `AdmittedPool::hydrate` produces an opaque `ScorablePool` with persisted evidence, plan and same-snapshot statistics |

Removed runtime mutation paths include `epoch`, `scanned`, `safe_epoch`,
`changed_paths`, `force_full`, `in_flight`, worker-local ledger ownership,
`take_pending_scope`, `merge_pending_scope`, `coverage_snapshot`, and
`search_status`. Git discovery and lease/watcher resource management remain
provider/runtime responsibilities; they no longer mutate lifecycle facts.

Removed retrieval synchronization obligations include `original_ids`,
`lexical_ids`, `definition_ids`, `path_ids`, `expansion_ids`, `expanded_scores`,
and `meaningful_scores`. The old free-standing final-pool constructor is gone.
The production ranker cannot accept policy-test candidates or a tuple of display
fragments and caller-supplied statistics. Policy-test helpers now use the explicit
`ranking::testing` namespace and a separate opaque input type.

See [the invariant document](../../docs/reconciliation-ownership.md) for fact
ownership, caller assumptions, observation boundaries and the original tests.

## Publication and read consistency

1. An observed scoped/full change advances the collection's observed revision
   and queues work. Taking a ticket consumes only that scope; subsequent changes
   accumulate separately. Tickets include collection, incarnation, run sequence,
   observed revision and scope. Stale or mis-scoped completion is rejected.
2. Beginning a run advances the publication revision and makes coverage
   uncertain. Existing database invalidation runs without the controller lock.
   Only after it succeeds can unaffected verified rows be read.
3. Existing source transactions and verification remain authoritative. A short
   indexed store lookup confirms the published version and eligibility, quarantine,
   or absence before source metadata can enter the ledger. A direct, run-bound
   publisher replaces the source outcome. No event history is accumulated.
4. Completion updates the ledger and publication revision under the controller
   lock. Failure/cancellation retains unfinished scope and uncertainty; committed
   outcomes survive. Restart begins unknown and reconstructs coverage.
5. A response captures a collection token before database work and validates it
   afterward. An observed change or incompatible publication suppresses results,
   context/copies and cursors conservatively. Reads during publication retain
   uncertain coverage. Unrelated collections' global database generation changes
   do not invalidate this token; copies cursors keep their existing generation rule.

No controller lock spans file reads, hashing, parsing, tokenization, database
work, ranking or protocol output. Status snapshots copy aggregate in-memory facts,
not source rows, and do not acquire a query permit. Progress counters are never
publication evidence. A source may retain a verified searchable prefix with a
pending tail; settled degraded coverage remains valid.

## Intentional publication changes

These are distinct from mechanical ownership changes; ranking policy did not change.

- **Committed repair followed by cancellation.** The frozen baseline reproduction
  in `cancelled-repair-baseline.txt` committed one repaired source but retained both
  old source errors, plus the run failure (`3`). The new controller regression
  retains the untouched source error plus run failure (`2`), while pending coverage
  stays unknown. `reproduce_cancelled_repair.rs` records the old worker/ledger path.
- **Publication during a read.** The former response check compared observed-change
  epochs only. The new token additionally detects relevant publication boundaries.
  The barrier test pauses after a real SQLite commit and before metadata publication;
  status remains uncertain and an incompatible token is rejected.
- Review caught an oversized first precise scope introduced during this refactor.
  The 1,024-path full-reconciliation fallback was restored and tested. This is
  isolated in `wave1-followup.patch`; it is not a policy change.

## Verification

Wave 1 passed 174 Rust tests and 43 Python tests, Clippy, formatting and diff checks
before retrieval edits. The transition model enumerates all `12^4 = 20,736`
length-four event sequences, including guarded no-ops, and asserts after every
step. Its reference state uses outstanding event identities and source facts,
not production epochs or ledger delta arithmetic. Longer targeted tests cover
full/incremental convergence, scope overflow, stale/mis-scoped completion,
watcher recovery, preparation-time changes, failed commit, interrupted publication,
cancellation after repair and unrelated-collection writes. Existing coverage tests
retain rejected sources without rows, failed-discovery diagnostics, untouched
pending tails, deletions, checkpoint recovery and restart reconstruction.

Final result: **178 Rust tests and 43 Python tests passed**. Admission properties
cover uniqueness, protected reservations under 160 saturation/order scenarios,
idempotent overlap, explicit zero raw scores, all-lane scope checks and persisted
body/statistics hydration after a concurrent database replacement. Existing oracle,
meaningful-query, boundary/long-token, session-copy, MCP and recovery tests pass.
The existing blocked-writer status test remains in the full suite.

Exact differential capture (`evaluate_stage_contracts`) compared 147 fixed-clock
cases on identical corpora and source transitions. Raw and ranked identities,
source/chunk metadata, text digests, score bits, full traces, overlap diagnostics,
probe counts and pool sizes are **identical**. This includes ten ranking variants,
project/session saturation, replacement, invalidation, verification and deletion.
The 143 existing ranking evaluation cases, high-fanout admission, stress budgets
and real-ingest regression outputs also match. Both real-source smoke runs indexed
the same frozen baseline source tree to avoid comparing different corpora.

All semantic comparisons are executable with `python3 validation/consolidation/compare.py`.
The exact captures are compressed losslessly as `stages-before.json.gz` and
`stages-after.json.gz`. `comparison.json` contains the detailed checks and numbers.
No flaky test or evaluation failure was observed. Intermediate compiler errors
and an unused fixture-helper Clippy diagnostic were corrected before the final
gates; development logs are retained.

## Measurements

Same macOS arm64 machine, fixed evaluation clock `2026-09-21T00:00:00Z`.
The final baseline and changed measurement suites ran serially. Initial captures
made alongside the disposable exercise are retained in `initial/` and are not
used in the comparisons below. This is a shared developer machine, not an
isolated benchmark host. These are observations, not a general speedup claim.

| Workload | Before p50 / p95 ms | After p50 / p95 ms |
| --- | ---: | ---: |
| Enhanced fixture, mean of 13 query percentiles | 3.085 / 3.300 | 2.825 / 3.096 |
| 240 mentions plus protected declaration | 38.388 / 39.296 | 38.310 / 39.590 |
| 220 identical large chunks | 112.838 / 113.877 | 101.384 / 102.654 |
| 220 distinct large chunks | 457.813 / 460.050 | 446.594 / 450.535 |
| Status sampled during durable session transactions | 1.351 / 1.756 | 1.290 / 1.512 |

Query/high-fanout samples use five warmups and 50 observations; stress uses
five warmups and 30 observations. Status uses a disposable 4,000-record session,
5 ms polling, 320 total samples per run and 19 samples per run observed in the
durable-transaction phase. Phase is observed at response time; the deterministic
blocked-writer test establishes independence, while this sampling measures local
RPC latency. Fixture mean percentiles are not pooled service percentiles.

Individual query p95 regressions remain visible:

| Query / scope | Before ms | After ms |
| --- | ---: | ---: |
| `Foo::Bar::bazValue` / project | 0.459 | 0.548 |
| `"Zone Not Found"` / session | 1.626 | 1.701 |
| Cloud-sync natural query / project | 0.544 | 0.730 |
| `HybridPersistenceCoordinator` / session | 1.544 | 1.645 |
| `subscription active state` / project | 0.450 | 0.513 |
| `panic` / project | 0.351 | 0.452 |

All precise-update work counters match before/after:

| Update | Files / records inspected | Read and hashed bytes | Prepared / committed chunks | Run elapsed before → after |
| --- | ---: | ---: | ---: | ---: |
| Large session suffix | 1 / 2 | 8,884,796 | 2 / 2 | 92 → 102 ms |
| One changed source among eight | 1 / 2 | 1,644 | 1 / 1 | 33 → 34 ms |

Those elapsed values are single observations. Concurrent search probes collected
only three samples per update; their local p95s were 174.373 → 163.393 ms for
the suffix and 163.340 → 160.003 ms for the one-of-eight update. They do not
establish a latency distribution. The new publication confirmation performs an
indexed lookup per outcome; the work counters do not measure that lookup separately.
Full-prefix verification cost remains: the suffix fixture reads about 14,907 times
the changed bytes. Optimizing that cost remains a separate task.

## Commands and artifacts

Executed successfully on the final implementation:

```text
cargo fmt --package bm25-mcp -- --check
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings -D unsafe-code
python3 -m unittest discover -s scripts -p 'test_*.py'
git diff --check
cargo build --locked --release
cargo build --locked --release --examples
target/release/examples/evaluate_stage_contracts
target/release/examples/evaluate_ranking
target/release/examples/evaluate_ranking_stress
target/release/examples/evaluate_real_ranking /Users/adityasharma/Projects/bm25-consolidation-baseline/src
python3 scripts/exercise-updates.py --binary target/release/bm25-mcp --huge-session-mib 8 --synthetic-session-records 1000 --synthetic-session-mib 4
python3 validation/consolidation/measure_status.py target/release/bm25-mcp
python3 validation/consolidation/compare.py
```

Baseline sources were exported from the reviewed revision into a separate local
scratch directory, without restoring or modifying this checkout. Equivalent
baseline release commands and the cancellation reproduction were run there.
`wave1.patch`, `wave1-followup.patch` and `wave2.patch` reconstruct all implementation,
test and documentation changes in order; reconstruction was checked in a disposable
directory. `wave1-gate.txt` records the first gate's source hashes. Final gate logs,
before/after JSON and JSONL exercise captures, and comparison scripts are retained.

## Compatibility and limits

MCP names, arguments, response schemas, progress privacy, raw Store search APIs,
SQLite schema, tokenizer, checkpoints, parser policy, source ownership and session
copies/context remain unchanged. No dependency was added. Rust policy-test imports
move to `ranking::testing`, and ledger snapshots now use the internal coverage
snapshot type rather than `tools::Coverage`; these are library helper type changes,
not wire changes.

The model is bounded, not a proof for every filesystem or scheduler interleaving.
Watchers cannot promise that files remain unchanged after response validation.
Coverage is rebuilt after restart; there is no atomic SQLite/memory commit or
durable coverage journal. Prefix verification and heuristic declaration recognition
remain unchanged. Windows, production-corpus distributions, peak lock duration
at very large source counts and the isolated cost of publication-confirmation
lookups were not measured here.
