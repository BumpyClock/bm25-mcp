# V1 acceptance results

Validated on macOS arm64 on 2026-09-20 (2026-09-21 UTC). The implementation is complete; Windows execution remains pending the user-run gate.

Release executable SHA-256: `c6f62c128e6607c0530e92448e4e95a624bd784dd7546dfd4dcb3ad50eaea130`. All five profiles below used this exact build and exited successfully.

## Automated checks

- 64 tests passed: 46 library tests and 18 integration tests covering MCP behavior, all three session adapters, atomic checkpoints, process interruption, ownership, recovery, query budgets, and upstream BM25 oracle equivalence.
- Root-package formatting, locked release build, and Clippy with `-D warnings -D unsafe-code` passed.
- Project targets use `unsafe_code = "forbid"` and crate-level `#![forbid(unsafe_code)]`. This is not a claim that transitive dependencies contain no unsafe code.
- Vendored upstream Rust source remains unchanged. Its manifest localizes workspace metadata; custom code and path dependencies remain inside this project.

## Real repositories and histories

Each repository received 1,000 requests with one client and 1,000 total requests with four clients. The query mix includes project search, provider-filtered session search, and context. Measurements use reconciled persisted indexes, not empty-cache first builds. Profiles ran concurrently on the same machine. Each used a private snapshot of 175 actual session files (997,887,118 bytes); snapshots were removed afterward. Histories were scoped by their recorded project ownership.

| Repository | Single-client p95 | Four-client p95 | Peak owner RSS |
| --- | ---: | ---: | ---: |
| bm25 | 11.95 ms | 38.32 ms | 48.12 MiB |
| t3code | 8.44 ms | 19.07 ms | 95.72 MiB |
| neutron | 2.01 ms | 5.29 ms | 40.41 MiB |

All query profiles passed the 100 ms warm p95 and 512 MiB soft owner-memory targets. No query samples were excluded for partial freshness. Known symbol definitions ranked first in all three repositories; the t3code exact error location ranked second. Actual Codex history queries passed for all repositories, and actual Copilot CLI history queries passed for neutron. Claude Code is covered by sanitized integration fixtures, not a real-history corpus here.

Coverage is intentionally explicit: session scans reported unsupported record variants (BM25: 11,789; t3code: 2,010; neutron: 29,721), so their status was `degraded`. Unsupported project encodings were excluded and reported (t3code: 210; neutron: 4). Passing latency and relevance checks does not mean every provider record or file encoding was supported. Foreign project sessions, binary files, ignored paths, and symlinks were excluded.

Reconciliation of persisted project indexes took 0.12 s for BM25, 64.05 s for t3code, and 3.48 s for neutron. The unchanged-content fast path reused 88, 21,437, and 1,301 indexed sources respectively. Session readiness waits after project readiness were 69.52 s, 87.09 s, and 152.17 s; copying histories changes file identity and causes validation/reimport.

## Changes, large inputs, and pressure

- 1-file edit: 0.207 s; passed the 2 s target.
- 10-file edit: 0.416 s; passed the 2 s target.
- Huge text: 29,360,148 bytes indexed in 2.82 s.
- One huge session record: 268,370,202 bytes indexed in 34.87 s with four clients; peak owner RSS 22.94 MiB.
- Git branch transitions completed in 0.30–0.36 s; returning to each indexed branch reused all ten changed files from the content cache.
- Forced pressure at a 1 MiB soft target activated disk-backed search. Result paths and scores matched the normal owner. Four readers continued during an edit with zero errors; new text appeared and stale text disappeared.

RSS is sampled owner memory, not an enforced process cap or aggregate client memory. These results establish the tested workloads, not a universal latency or memory bound.

## Evidence and remaining platform gate

- [Full tests](full-tests.log)
- [BM25 corpus](bm25-acceptance.jsonl)
- [t3code corpus](t3code-acceptance.jsonl)
- [neutron corpus](neutron-acceptance.jsonl)
- [Updates and huge inputs](update-acceptance.jsonl)
- [Memory pressure](pressure-acceptance.jsonl)

On Windows run `./scripts/validate-windows.ps1 -Project C:\path\to\project` from the project root. Also check owner survival when the first client closes inside your actual MCP client manager. Windows and Linux execution were not performed here.

## Codex CLI integration follow-up

A real `codex exec --sandbox read-only --ephemeral --json` run in neutron loaded its project-local `.codex/config.toml` and passed project search, Copilot history search (five matches), and context retrieval (six events). The initial run exposed status precedence that mislabeled active reconciliation as `degraded`; active indexing now returns a successful `building` or `refreshing` response with separate diagnostics. Strict uncertain-source suppression remains in place. All 66 tests and Clippy with warnings and unsafe code denied passed after the fix.

[Sanitized CLI evidence](codex-cli-smoke.json) records the updated build hash. Session coverage was still refreshing, so the integration check establishes verified partial results rather than complete indexing. The performance table above remains evidence for its explicitly recorded earlier build.

## Indexing observability and incremental reconciliation follow-up

Synthetic macOS arm64 measurements on 2026-09-22 used isolated project and
provider directories, not personal histories. The earlier real-corpus results
above remain specific to their recorded builds.

The matched cold-session comparison used one 3,837,563-byte source containing
1,000 complete events, three alternating trials per release binary, and the
same deadline-bounded sentinel-search observation policy for both binaries.
The new status resource was sampled separately because the baseline did not
support it. Indexed counts and content fingerprints matched.

| Build | Median cold import | Range |
| --- | ---: | ---: |
| Baseline `8b6956e196b83f926a13635b392330e2d0118a986e468bec15e3b13cbe79fc21` | 1.641 s | 1.581-1.672 s |
| Observed `4420957667ed6a76dc33795e4845359d37914faf4e1b1adcb66f55585330d0e6` | 1.273 s | 1.263-1.345 s |

This is approximately 22% lower median time for the combined change on this
fixture, not an isolated scratch-batching speedup or a real-corpus guarantee.
The observed import performed 1,000 scratch-state writes in two committed
scratch transactions and one durable index transaction. A separate exact
counter regression covers the batch boundary: 1,025 writes require three
scratch transactions with the 512-write batch limit.

Subsequent instrumentation corrections freeze elapsed time at terminal scan
states and separate scratch database-call timings from open-transaction
lifetime. The wall-clock comparison above does not depend on those internal
timing fields; it remains evidence for the exact hashes listed, not a rerun of
the later instrumentation revision.

| Scenario | Observed work and correctness |
| --- | --- |
| Unchanged history after reconnect | Prefix verification remained visible; zero records normalized and zero chunks prepared or newly committed. Existing content remained available after verification. |
| Small append | Two complete new records produced two chunks; the old prefix remained searchable. A 792-byte append required 7,677,502 source bytes read, approximately 9,694 times the changed bytes. |
| One changed source among eight | Exactly one source and two physical records were processed. The replacement used 4,276 source bytes read for 1,069 changed bytes; the old target disappeared, the new target appeared, and an unaffected source remained present. |
| Cancellation and recovery | Scan/checkpoint regressions cover discarded preparation, atomic chunk/checkpoint publication, and reopening from the durable checkpoint. Progress alone does not make data searchable. |
| Slow evaluation family | Synthetic harness tests observe already-flushed query/project records while later work is blocked or fails; timeout tests reject reuse of a closed protocol connection. |

The shipped disposable integration fixture also exercises a 4 MiB,
1,000-record session corpus, an 8 MiB physical session record, project edits,
and branch transitions. A run of build
`5b8824f8b819cc8bdc9160d46fb34c9947742ed982c84e0505e928bb64015a2a`
reported 73,662,464 bytes peak sampled owner RSS for the 8,356,112-byte
physical-record case, below the 512 MiB soft target. That sample is not a hard
memory limit or a bound for arbitrary corpora.

```sh
python3 scripts/exercise-updates.py --binary target/release/bm25-mcp \
  --huge-session-mib 8 --synthetic-session-records 1000 --synthetic-session-mib 4
python3 -m unittest discover -s scripts -p 'test_*.py'
```

It records first observed progress, the first exact sentinel returned by an
independent search probe, reconciliation time, ingestion search latency, and
source-read/changed-byte ratios. Probe sampling is capped at four searches per
second. Per-run counters are not subtracted across run boundaries; sampled
multi-run totals are explicitly lower bounds. A sentinel's first search result
is an observation of that sentinel, not the exact instant of the first
arbitrary chunk commit. The core `first_commit_ms` reports the transaction
boundary separately.

The initial live run timed out before its first scenario and was not reproduced
on subsequent runs; it is not treated as a pass or attributed to an established
root cause. A separate later harness error confused related BM25 hits with an
exact sentinel match and was corrected before these assertions passed.

Full prefix verification remains a measured cost. Temporary spooling remains
proportional to inspected content; these checks do not establish a hard
temporary-storage quota. Hidden resumable staging, early unverified
publication, and further speculative optimizations were not introduced.
Windows execution remains unverified.

Final publication gates passed with release
`1d9a74182fefae9a5043a4caf568d8d4170172540f8934ca456c3e4d4792440e`:
154 Rust tests, 27 Python tests, root-package formatting, all-target Clippy with
warnings and unsafe code denied, and the strict disposable integration command
above. Cold, unchanged-reconnect, append, and changed-one session assertions all
passed. Append and replacement each processed exactly one source and two
physical records. Watcher regressions cover one-sided renames, forced full
scans, ignored project files, and deleted directories whose names end in
`.jsonl`.
