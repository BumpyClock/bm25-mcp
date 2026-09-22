# Reconciliation and retrieval ownership

Reviewed HEAD: `dc0f3415bd65a2605239961a12009b4bdbeded85`. The initial working
tree was clean. No repository or ancestor AGENTS.md was present; the supplied
instructions and machine-local preferences apply. This is a consolidation of
the current implementation, including the coverage/progress work in this HEAD.

## Contracts established before editing

| Fact | Current authority and writers | Observation and caller assumption | Evidence |
| --- | --- | --- | --- |
| Pending scope and observed changes | Runtime root/session fields; watchers, reconnect and periodic workers | An observed change suppresses reads until scope invalidation succeeds; precise invalidation preserves other sources | Runtime routing tests; `indexing_progress`, `recovery` |
| Completion and safe reads | Worker-local epochs plus published coverage mutex; response code compares epochs | Older work must never settle a newer change; active work takes precedence over diagnostics | Runtime status tests; `collection_coverage`; Python settled-response tests |
| Source outcomes | `CollectionCoverage` keyed ledger, updated from scanner reports | Replace rather than accumulate; retain untouched errors, pending tails and rejected sources without rows | `collection_coverage` repair, deletion, discovery, cancellation and restart tests |
| Searchable source data | Store transactions and provider/project verification | Chunks, postings, metadata and session checkpoints publish atomically after verification | `checkpoints`, `oracle`, `recovery`, session-stream tests |
| Work counters | `ProgressReporter`, scanner effects | Prepared work is observational; committed counters advance after successful transactions; polling is passive and path-free | `indexing_progress`; runtime blocked-writer status test |
| Candidate admission | Store retrieval and reservation function, with parallel ID/score maps | Unique pool of at most 200; protected definition/meaningful/expansion budgets and filtered lanes | `meaningful_admission`, `ranking_store` |
| Body evidence and raw baseline | Persisted postings/lengths and raw scoring kernel in the retrieval transaction | Display fragments cannot substitute for indexed evidence or raw-query multiplicity | `oracle`, boundary-evidence and multiplicity tests |
| Relevance and selection | Ranker using hydrated evidence and corpus statistics | Relevance and MMR scores have distinct meanings; fixed-clock output is deterministic | `ranking_policy`, evaluation harnesses |

No defect is inferred from file size, atomics, or module count. The structural
risks are split lifecycle ownership, unqualified completion reports, and parallel
candidate evidence containers. Any demonstrated behavior correction will be
recorded separately with its reproduction.

## Target ownership and publication protocol

One reconciliation controller per collection owns pending scope, active run
identity, observed revision, safe-read boundary, the existing coverage ledger,
watcher diagnostics and publication revision. Watchers submit scope; workers
perform provider-specific effects; transports consume snapshots and validation
decisions. Project discovery and provider parsing remain separate.

A run ticket binds collection, controller incarnation, run sequence, observed
revision and consumed scope. Scope may be widened before invalidation (for Git
membership discovery). New observations accumulate separately from consumed
work; finishing an older run cannot consume them. An empty pending queue differs
from a full/unknown scope and from a precise empty scope.

The controller marks a run active before database invalidation. Once invalidation
commits, verified unaffected rows may be read, with unsettled coverage. Source
publication uses existing transactions and verification; coverage publication
follows successful effects. Finishing publishes ledger and revision together
under the controller lock. Reads capture a collection token before database
work and validate after it. A change or incompatible publication suppresses
results and cursors conservatively. Reads spanning active work cannot become
settled retroactively. The lock never spans file, parser, database, ranking or
protocol work. Status reads use only compact memory snapshots.

SQLite commit and memory publication are deliberately not atomic. The active
run keeps coverage uncertain throughout that gap. Failure leaves unfinished
scope for retry; process restart begins unknown and reconstructs coverage.
Unrelated collections' global database generation changes do not invalidate
collection read tokens. Copies cursors retain their existing database-generation
contract.

Progress run IDs measure work. Controller run IDs reject stale completions.
Observed revisions identify changes; publication revisions identify memory/index
boundaries. Database generation identifies SQLite views and existing cursor
validity. Source versions bind indexed content/checkpoints. These counters are
not interchangeable.

## Wave gates

Wave 1: controller transition model, deterministic publication/failure tests,
coverage/checkpoint/status/runtime regressions, formatting, complete Rust tests,
Clippy, Python tests and diff check. Only then change retrieval ownership.

Wave 2: single admission owner and snapshot-bound scoring input; unchanged
policy and budgets; property and differential tests plus the same complete gates.
Release evaluations and disposable indexing capture before/after behavior and
work counts. Measurement artifacts and final limitations are recorded under
`validation/consolidation/`.

## Implemented boundaries

`reconciliation::Controller` replaces root/session epochs, scanned/safe epochs,
pending path mutexes, force-full/in-flight flags, watcher-error mutexes, the
worker-local ledgers and independently published coverage. Watcher routing and
Git discovery submit `Scope`; `Run::project` and `Run::sessions` perform the
existing provider-specific effects and return the controller's completion
decision. The transport uses `snapshot`/`validates`; it only adapts wire shapes.

`ScanReport::record_source` requires a store-issued `SourcePublication` check of
the committed source version and eligibility (or quarantine). Confirmed removal
requires absence. These short indexed lookups use the existing database lock;
they do not hold the controller lock. A run-bound publisher updates the existing
ledger directly, with no event queue or retained history. Final scan publication
is idempotent. Source versions are retained in the internal outcomes. Standalone
scanner APIs still return reports for existing ingestion/evaluation callers.
Only the controller's concrete project/session adapters can complete runtime
runs; an arbitrary successful report cannot establish runtime readiness.

The publication revision changes on begin, successful invalidation, source
publication, completion and watcher-diagnostic changes. Observed changes have a
separate revision. Reads begun during a run retain uncertain coverage even if a
transaction has committed. A revision change at response validation suppresses
results/context/copies and cursors. A new controller incarnation rejects old read
and run tokens. Memory and progress are wire observations, never readiness proof.

`store::admission::Admission` owns each retrieved ID and all its lane evidence,
including optional raw-query BM25. It alone constructs `AdmittedPool`. The pool
retains query scope, raw terms and query plan; its hydration is bounded and uses
the retrieval transaction. `ScorablePool` contains prepared persisted evidence,
statistics and the same plan, with a lifetime tied to the transaction. The
production ranker accepts only that type. `ranking::testing::Candidates` is a
separate opaque policy-test input; display-tokenization helpers cannot satisfy
the production entry point. Retrieval weights, classification, probes, budgets,
raw scorer, final relevance and MMR calculations remain unchanged.

## Observable hardening and reproduction

Two publication changes are intentional and separate from ranking policy:

- `cancellation_after_repair_retains_committed_outcome_and_unvisited_errors`
  repairs one of two rejected sources, then cancels immediately after the source
  transaction. Previously the project scanner returned an error before recording
  the committed repair; the worker applied only the failed run and retained both
  old source errors. The run-bound publication now replaces the repaired source's
  outcome while retaining the untouched error and unfinished-run uncertainty.
- `durable_failure_and_commit_to_metadata_gap_with_restart` pauses after a real
  SQLite commit, before source metadata publication. Coverage stays uncertain;
  publication invalidates an earlier read token. The old response check compared
  observed-change epochs only. The new collection token also detects relevant
  publication changes; it does not compare the unrelated global generation.

These changes do not publish unverified content, alter ranking, change the MCP
schema, or require a database/checkpoint migration.

## Temporary record ownership

The private `record_spool` module owns temporary JSONL records for project
chunks, session chunks, and session parser-state updates. `RecordSpool` accepts
borrowed serializable records; consuming `finish` flushes and closes the writer
and opens a typed `Records` reader. Readers retain one record buffer and can
reopen an independent cursor for project content-cache replay. A shared path
owner attempts removal after the final reader closes, including early exits;
it also cleans up abandoned writers and failed finalization. Files are created
exclusively with private Unix permissions. Cleanup is best-effort on normal
drop, not a process-crash recovery mechanism.

Finalization returns a distinct `FinalizeError`. Both scanners propagate this
error as a failed run before entering the source transaction, rather than
recording it as one source's rejection and continuing. The controller retains
unfinished work for retry, and progress reports `Failed` without advancing
committed chunk counts. Previously committed source outcomes remain published.
Read/decode errors encountered after finalization still flow through the store's
iterator contract and roll back chunks, parser-state updates, and checkpoint
changes in the existing transaction.

This intentionally standardizes failure timing and classification. Project
finalization and session state-update flush failures previously used source
error handling; session reader-open and chunk flush failures were deferred into
transaction iteration. Raw-byte spools, SQLite scratch state, content-cache
storage, and cancellation classification retain their existing ownership.

Tests exercise the spool interface with real temporary files, independent
replay, partial consumption, simultaneous reader drops, malformed records, and
flush/open faults. Scanner/controller regressions verify failure before commit,
preserved source versions and checkpoints, pending coverage, and successful
retry for all three consumers. Store integration verifies atomic rollback when
a spooled parser-state record cannot be decoded.
