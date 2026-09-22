# Session indexing work reduction

Implemented against `c56852d02a483d72d381270e61bef61830648d17` on
2026-09-21 (local time). Validation was performed before committing the series.

## Ordered changes

The following patches apply in order to the reviewed HEAD and reconstruct the
changed source, tests, and documentation. Their application was checked against
the working files. The first patch also adds the opt-in measurement harness.

1. [Share checkpoint hash state](patches/01-share-prefix-hash-state.patch).
2. [Validate tentative appends before publication](patches/02-validate-tentative-appends.patch).
3. [Prepare records once with bounded spill](patches/03-prepare-records-once.patch).
   This includes scratch statement caching, indexed source lookup, and work counters.
4. [Maintain source statistics](patches/04-maintain-source-statistics.patch).

SHA-256, checkpoint schema 7, the streaming JSON walker, scoring, indexing
concurrency, watcher scheduling, and audit frequency are retained. Store schema
changes separately to v3 for maintained statistical contributions.

## Acceptance protocol

A compatible checkpoint selects a tentative suffix offset. Raw records, parsed
captures, normalized chunks, state updates, and diagnostics remain private.
Before accepted data reaches the source transaction, one whole-file pass checks
the prepared suffix and committed prefix, with source identity, length, and
modification-time checks surrounding preparation and validation. Prefix digests
are finalized from clones of the running SHA-256 state at exact boundaries.

A rejected or failed attempt with a checkpoint discards its tentative output and
retries from zero without that checkpoint. It cannot repeatedly reuse an invalid
append checkpoint. Cancellation and spool-finalization failure abort immediately.
There are at most three full-source attempts after the tentative attempt; ordinary
cold reads retain their three-attempt stability limit. Ownership exclusion on a
checkpoint path is resolved by rebuilding before publishing an exclusion.

The source remains quarantined until successful verification and transactional
publication. No changes were made to the controller's read-validation or
publication-lock boundaries. This remains optimistic verification, not an atomic
snapshot against arbitrary concurrent writers.

Complete records are parsed once. A bounded binary capture archive plus typed
record descriptors retain order, raw digests, byte/line bounds, and late session
metadata. Replay preserves first-value duplicate-field handling, tool correlation,
normalization, and the existing diagnostic counts. Once a cwd proves the source
cannot be accepted, later records retain metadata only and skip raw-record hashing.
Ownership discovered late can still require capturing earlier visible content.

Raw-byte and capture storage each allow a 64 KiB inline buffer. Together this adds
at most 128 KiB of retained inline capacity per active source, plus at most 128 KiB
of transient range copies and bounded reader/fragment buffers. Existing metadata,
chunk, tokenizer, and scratch-state budgets still apply. Large records spill using
the same private file owner as typed record spools; no transcript-sized heap value
or per-record collection of open capture files is retained.

Store v3 keeps source chunk counts, token totals, and per-source/term chunk
frequencies, including for quarantined sources. Only newly inserted ordinals are
aggregated on append. Quarantine/restoration uses stored contributions, and source
deletion cascades them. Replacement, parser-state updates, checkpoints, summaries,
and global statistics share the source transaction. Chunk counts also supply the
next append ordinal and verified-source count.

Opening a v2 cache backfills all sources, including empty and quarantined ones,
and advances the schema marker in the same transaction. A failed backfill leaves
v2 retryable. Tokenizer invalidation deletes summaries with the other derived data.
Older executables reject v3 rather than silently leaving summaries stale.

## Measurement method

Apple M1 Max, macOS, Rust 1.98.1 (`aarch64-apple-darwin`), pinned dependencies,
Cargo release profile, unchanged hash implementation. Synthetic files and fresh
SQLite databases isolate each import. Source files are freshly written and then
read through a warm OS cache: “cold import” means a new index, not cold physical
storage. No production cache or installed executable was changed.

The verifier uses one warmup and five measured trials. Preparation uses one
warmup and three measured trials per record shape and ownership outcome. Statistics
use one warmup and five quarantine/append/restore cycles. Tests and benchmark runs
were separated for the paired comparison, but this is a shared workstation with
background load; timings are workload evidence, not service-level guarantees.

Primary logs are `paired-head.log`, `paired-64k.log`, `paired-16k.log`, and
`statistics-final.log`. Earlier stage logs record cloning, parsing, and summary
measurements independently. The final statistics log includes the term-ID index
needed for efficient foreign-key checks during compaction. The paired preparation
runs precede that index and the cached verified-count lookup; neither path is used
by those preparation workloads. Timing binaries also precede the final missing-spool
error-classification check; that failure path is tested separately and is not
exercised by these workloads.

`summarize.py` produces `summary.json` with every sample count, median, minimum,
and maximum. It includes the first measured row even when the Rust test runner
prefixes that row with the test name.

## Results

Times below are milliseconds, shown as median [minimum–maximum].

| Workload | Original HEAD | Changed |
| --- | ---: | ---: |
| Import 1,024 × 4 KiB messages | 1251.30 [1248.49–1261.20] | 785.65 [780.70–787.41] |
| Import 128 × 32 KiB messages | 805.00 [788.07–814.12] | 687.22 [677.71–717.58] |
| Import one 4 MiB message | 715.47 [708.39–717.80] | 669.24 [663.86–672.00] |
| Tiny append to short-record history | 71.21 [70.34–71.85] | 28.64 [28.47–30.30] |
| Tiny append to medium-record history | 69.13 [68.88–69.77] | 28.48 [27.82–29.99] |
| Tiny append to large-record history | 68.47 [67.41–69.28] | 28.86 [28.34–28.95] |
| Reject unrelated short-record history | 255.90 [253.37–304.04] | 80.74 [80.29–81.03] |
| Reject unrelated medium-record history | 92.77 [90.56–93.60] | 56.77 [56.45–57.71] |
| Reject unrelated large-record history | 61.41 [58.80–92.84] | 59.69 [59.40–59.84] |

The small-record import uses about 37% less wall time and tiny appends about 60%
less on these fixtures. The rejected single-large-record difference is too small
relative to the baseline spread to claim a speedup.

For the short-record import, median first progress was 11 ms before and 10 ms
after; first committed/searchable source data was 1251 ms before and 785 ms after.
One source still publishes atomically at the end of its preparation.

The isolated verifier's 64 MiB prefix plus 1 KiB suffix went from approximately
595 ms to 204 ms after hash cloning alone. This measures the verifier, not total
indexing speed; source-read volume is unchanged by cloning alone.

### Work counts and storage

Tests establish exact successful-append counts: source reads `P + 2S`, session
source/record hash input `P + 4S`, and parser input `S`. Cold accepted imports read
the source twice and parse each complete record once. The old path read `2P + 3S`.
Incomplete tails remain included in source validation and are not published.

The changed short-record cold fixture read 8,628,302 source bytes, fed 17,256,604
bytes into source/record hashers, and parsed 4,314,151 JSON bytes. Observed spool
I/O was 23,468,842 bytes read and 14,480,744 written. These include capture replay,
serialized chunks and parser-state updates, so they are larger than source I/O.
There is no claim that fewer source passes alone establishes less total temporary
I/O; equivalent full temporary-I/O counters were unavailable on the original HEAD.

| Statistics workload: 10,000 chunks, 320,000 postings | Original HEAD | Final |
| --- | ---: | ---: |
| Quarantine, 32 repeating terms | 69.588 ms | 0.224 ms |
| Append/restore, 32 repeating terms | 70.240 ms | 0.388 ms |
| Quarantine, 320,000 distinct terms | 2209.816 ms | 2131.368 ms |
| Append/restore, 320,000 distinct terms | 1122.178 ms | 1090.382 ms |
| Database bytes, repeating vocabulary | 16,011,264 | 16,273,408 |
| Database bytes, distinct vocabulary | 48,513,024 | 66,002,944 |

The repeated-vocabulary benefit justifies maintained summaries for this case.
The distinct-vocabulary case has little timing benefit and about 36% more database
storage. Its measured import rose from 1.51 s to 1.81 s (one import per fixture;
not enough samples for a general cold-import claim). Repeating-vocabulary imports
were both about 1.00 s.

A direct SQL probe counts 4,200,436 VM steps to group the repeated historical
postings versus 199 to read the 32 summary rows. For distinct terms, the counts are
8,360,020 versus 1,920,007, with 320,000 result rows in either case. These are
statement work counts, not ratios of end-to-end performance. Summary frequencies
sum to exactly 320,000 in both cases and retain one contribution per chunk/term.

The 64 KiB inline threshold was compared with 16 KiB. Medium-record accepted
imports took 687 ms versus 732 ms; rejected imports took 57 ms versus 97 ms.
Tiny-record and single-large-record timings were similar. The 64 KiB threshold
avoids spilling medium records without retaining a large-record-sized buffer.
Maximum process RSS over the paired preparation/statistics suite was about
39.2 MB for HEAD and 40.6 MB for 64 KiB; this includes the SQL query probe's result
vector and is not an isolated production-memory bound.

### Concurrent search

Three imports ran while another thread repeatedly queried an unaffected project
source. Each query asserted that the expected hit remained available. Median
import time was 1300 ms on HEAD and 797 ms after the changes. Median per-run search
P95 was 0.333 ms before and 0.380 ms after; the largest observed query was 4.253 ms
before and 12.169 ms after. Median durable source-transaction time was 89 ms before
and 107 ms after. The summaries add work during cold insertion even though total
preparation time falls. These runs establish responsiveness for this small query
fixture, not a general concurrent-search latency guarantee. Full samples and
first-progress/commit measurements are in `concurrent-head.log` and
`concurrent-final.log`.

## Validation and limits

Focused runs passed 94 library tests, 8 indexing-progress tests,
3 statistics/oracle tests, and 16 tool tests. The broader run also passed checkpoint,
coverage, MCP, admission, ranking-policy, and ranking-store tests. One of six recovery
tests timed out waiting for automatic owner replacement; it passed immediately
when run alone. All six recovery tests also passed in a subsequent serial run
(`recovery-final.log`). No timeout or assertion was weakened, and the original
timeout's cause is not established.

Clippy with all targets/features and warnings denied passed. Formatting and diff
checks passed. All 43 Python evaluation tests passed. Logs retain the failed full
run and the successful isolated recovery retry rather than describing the initial
run as clean.

Coverage includes independent prefix-range digests, zero/equal/read-boundary
checkpoints, invalid offsets, cancellation, source edits during final validation,
prefix rewrites, ownership rejection, late metadata, duplicate IDs, large values,
incomplete tails, spool finalization faults and cleanup, atomic commit rollback,
restart, tokenizer changes, exact integer statistics, and failed-backfill retry.

The new hash counter counts source and raw-record hash updates, not metadata or
visible-field deduplication hashing. Spool counters are logical file reads/writes,
including retries and buffer prefetch; they exclude SQLite, tokenizer spills, and
visible-field deduplication files. Physical disk bytes, total SQL preparations,
controller publication-lock duration, cold-cache operation, and the full proposed
1 GiB/many-source matrix were not measured. Existing timing/transaction counters
remain available. Scheduling and freshness-policy changes are deferred.

## Reproduce

```sh
cargo test --release --lib benchmark_verification -- --ignored --nocapture
cargo test --release --test session_performance -- --ignored --nocapture --test-threads=1
python3 validation/verification/summarize.py
```

Build comparison revisions in separate target directories; shared Cargo target
paths can replace identically named test executables. The concurrent baseline was
rebuilt in its own target directory after detecting that collision. The saved
comparison executables were checked for distinct hashes. Logs include `/usr/bin/time -l` output where available; wall-clock assertions are deliberately absent from tests.
