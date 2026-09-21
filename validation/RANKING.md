# Ranking correctness and evaluation

The reviewed baseline was `efa7f606f2403d2f99d1683ccc16c6c02c3f04d3`
(`Add bounded lexical ranking and evaluation harness`). The checkout was clean.
This pass preserves the raw BM25 API, tokenizer version, MCP schema, session
copies/context behavior, and the final 200-candidate bound.

## Findings and regression evidence

| Finding | Disposition | Implementation and regression evidence |
| --- | --- | --- |
| Definitions lost before reranking | Reproduced and fixed | `src/store.rs` adds an indexed declaration lane with 40 reserved slots. `exact_definition_is_admitted_beyond_raw_top_two_hundred` asserts that raw top-200 misses the declaration before checking enhanced rank 1. The timed 240-mention fixture demonstrates the same miss. |
| Fragment retokenization loses indexed occurrences | Reproduced and fixed | `hydrate_body_evidence` reads persisted TF and token length in batches of 64 within the retrieval snapshot. `ranked_search_preserves_a_token_completed_after_the_storage_boundary` and `compound_and_unicode_boundary_evidence_survives_store_lifecycle` use real ingestion, including `SearchIndexBoundary` and `StraßeBoundary`. |
| Declaration case folding loses symbol components | Reproduced and fixed | `declaration_details` retains original names for the shared tokenizer and normalized identities for exact comparison. Policy tests inspect symbol-field evidence for `SearchIndex`, `search index`, and `HTTPResponseParser`, and reject partial definitions. |
| Dotted historical identities lack exact exemption | Reproduced and fixed | `tokenize_with_surfaces_checked` exposes complete lexical surfaces without changing raw term output. Policy tests cover old `Foo.bar`, `Foo.barExtra`, qualified names, case, punctuation, and rejection of components or multi-surface queries. Existing project-qualified reference tiering is retained. |
| Unsupported stopword-only results survive selection | Reproduced and fixed | Eligibility is checked before dedupe/MMR. A real stdio MCP regression first failed with `notes.txt` returned at score zero for `why does indexing fail`; it now returns no hit. Tools tests preserve positive raw retrieval, meaningful matches, and the intentional `is` fallback for both endpoints. |
| Metric grain permits inflated nDCG | Reproduced and fixed | Evaluation collapses chunks to source rankings before computing relevance metrics. Four metric tests cover repeated chunks, multiple sources, no relevant result, and empty judgments. nDCG is asserted within numerical tolerance, never clamped. |
| Evaluation depends on wall-clock time | Fixed | `search_ranked_with_at` supplies an explicit clock; existing APIs still use current time. Evaluation checks exact match-ID ordering and relevance-score bits across every repeated query. |
| Performance evidence was undersampled | Replaced with paired measurements | The same machine ran frozen baseline and changed code serially, using equivalent corpora, a fixed clock, five warmups, and 50 samples per query or 30 per stress case. |

The initial ranker regression run failed on symbol components, dotted identity,
unsupported evidence, and fragment body evidence. The full baseline had 106
passing tests; the completed implementation has 131. Regression coverage is
separate from the quality metrics below: passing a boundary or lifecycle test
does not establish a production relevance improvement.

Additional store tests cover declaration population, replacement, deletion,
invalidation/verification, content-cache restoration after reopening the cache,
and automatic backfill of an index without declaration metadata. They compare
raw scores and term/corpus statistics. The filtered-definition fixture has 45
eligible definitions plus exclusions by collection, path, agent, session,
lower/upper time bounds, and source eligibility; filtering precedes the
40-entry lane limit. Existing oracle, isolation, checkpoint, recovery, copies,
and context tests remain in the complete suite.

## Evaluation method

The clock is `2026-09-21T00:00:00Z`. The historical fixture's 13 queries and
source labels are unchanged: `must` has grade 3 and `useful` grade 1.
Recall and MRR measure the `must` targets. DCG and IDCG use both grades.
All relevance metrics operate on first-hit, deduplicated source rankings.
Duplicate rate still measures repeated chunk text in the uncollapsed top 10.
The retriever returns at most 20 chunks to this evaluation, so a collapsed list
may contain fewer than 20 sources; this is not an exhaustive source retriever.

`ranking-correctness-before.json` captures the reviewed revision with the same
source-level metric code and sample counts. Its only production-source
adjustment was a cached fixed-clock seam at the existing `Utc::now()` call.
`ranking-latest.json` is the current capture. Both include per-query results,
raw-versus-enhanced differences, all nine ablations, and repeatability checks.
The older `ranking-baseline.json` is a tokenizer-v3 historical artifact, not the
controlled baseline for this pass.

The separate high-fanout benchmark has 240 mention sources and one declaration.
The real-ingest fixture in `evaluate_real_ranking` has 248 sources and 252 chunks:
it covers the raw top-200 miss, normal/compound/Unicode storage boundaries,
multi-chunk sources, qualified identity, updates/invalidation/deletion, and
zero-evidence suppression. Neither fixture changes the 13 historical labels.
No private session text is included in the captures.

## Quality results

| Variant | Recall@10 | Recall@20 | MRR | nDCG@10 | Duplicate rate@10 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Raw BM25, before and after | 0.6154 | 0.6154 | 0.5769 | 0.5766 | 0.4000 |
| Enhanced, reviewed baseline | 1.0000 | 1.0000 | 0.9103 | 0.9306 | 0.0000 |
| Enhanced, corrected | 1.0000 | 1.0000 | 0.9231 | 0.9380 | 0.0000 |

Only `persistence coordinator` changes its target rank between the two enhanced
captures, from 3 to 2. No historical query has a relevance-metric regression.
The small aggregate increase is synthetic evidence, not a production quality
claim. The saturation fixture separately changes the enhanced declaration
from absent to rank 1 while raw top-200 still misses it.

| Query | Collection kind | Raw target rank | Corrected enhanced target rank |
| --- | --- | ---: | ---: |
| `HybridPersistenceCoordinator` | project | absent | 1 |
| `persistence coordinator` | project | absent | 2 |
| `manager-search.ts` | project | absent | 1 |
| `src/empty-manager.ts` | project | absent | 1 |
| `Foo::Bar::bazValue` | project | 1 | 1 |
| `"Zone Not Found"` | session | 1 | 1 |
| `why did subscriptions disappear after cloud sync?` | session | 1 | 1 |
| `why did subscriptions disappear after cloud sync?` | project | 1 | 1 |
| `architectural decision atomic checkpoint` | session | 1 | 1 |
| `HybridPersistenceCoordinator` | session | 1 | 1 |
| `subscription active state` | project | 2 | 2 |
| `panic` | project | absent | 1 |
| `fixCache` | project | 1 | 1 |

The corrected ablations are:

| Disabled feature | Recall@10 | MRR | nDCG@10 | Duplicate rate@10 |
| --- | ---: | ---: | ---: | ---: |
| Fields | 1.0000 | 0.9038 | 0.9267 | 0.0000 |
| Classification | 0.8462 | 0.7308 | 0.7598 | 0.0000 |
| Exact tiers | 1.0000 | 0.8462 | 0.8852 | 0.0000 |
| Expansion | 0.9231 | 0.8462 | 0.8611 | 0.0000 |
| Dedupe | 1.0000 | 0.9231 | 0.9154 | 0.3538 |
| Proximity, decay, weighted similarity, or MMR individually | 1.0000 | 0.9231 | 0.9380 | 0.0000 |

An unchanged ablation aggregate demonstrates no measured gain on this fixture.
Direct tests cover those features' contracts independently.

## Controlled performance measurements

Captured on the same macOS arm64 machine with
`rustc 1.98.1 (48a229cea 2026-09-01)` and Cargo 1.98.1. Baseline and current
builds and runs were serial. This is a shared developer machine, not an isolated
benchmark host. Clock, platform, sample counts, lane counts, and database stages
are recorded in the JSON.

All times below are milliseconds. The fixture row is the arithmetic mean of
per-query percentiles across 13 queries, not a pooled service percentile.

| Workload | Before p50 / p95 | After p50 / p95 | Warmups / samples |
| --- | ---: | ---: | ---: |
| Enhanced fixture, mean per-query | 3.586 / 3.747 | 2.890 / 3.061 | 5 / 50 |
| `SearchIndex`, 240 mentions plus declaration | 47.915 / 49.445 | 38.789 / 40.000 | 5 / 50 |
| 220 identical 16,031-byte chunks | 174.476 / 176.006 | 114.548 / 115.310 | 5 / 30 |
| 220 distinct 16,050-16,052-byte chunks | 513.423 / 517.343 | 465.951 / 467.985 | 5 / 30 |

The high-fanout query retrieves 200 lexical candidates and one definition, then
reranks exactly 200 candidates with zero expansion probes. Both stress cases
have 200 lexical candidates, no other admission-lane candidates, zero probes,
and a final pool of 200. Historical fixture pools range from 1 to 21.
Its `panic` and project cloud-sync queries each use one expansion probe; the
other enhanced cases use none. Lane counts can overlap and do not necessarily
sum to the final pool.

Some small queries are slower. Project `Foo::Bar::bazValue` p95 changes from
0.234 to 0.374 ms, `fixCache` from 0.276 to 0.413 ms, and `panic` from 0.205 to
0.255 ms. These costs remain visible rather than hidden by the aggregate.
The new declaration lookup and batched authoritative-evidence reads add work,
while avoiding reconstructed body-frequency work benefits larger pools.
No candidate limit was increased to obtain the results.

The distinct stress process's sampled peak RSS changes from 192.0 MiB to
101.3 MiB. RSS is sampled between queries and includes allocator retention
from the preceding identical case; it is neither a per-request allocation
measurement nor a continuously sampled peak.

| Cost | Before | After |
| --- | ---: | ---: |
| 50-source fixture indexing | 40.044 ms | 46.331 ms |
| Fixture replacement | 0.811 ms | 1.070 ms |
| 241-source high-fanout indexing | 94.292 ms | 100.207 ms |
| Distinct stress indexing | 445.975 ms | 472.352 ms |
| Distinct stress replacement | 2.623 ms | 2.487 ms |
| Distinct stress compaction | 1.342 ms | 1.638 ms |
| Fixture open-index bytes, including WAL/SHM | 3,889,096 | 4,012,696 |
| Fixture bytes after closing/checkpointing | 360,448 | 380,928 |
| Distinct stress open-index bytes | 8,175,744 | 8,192,104 |
| Distinct stress bytes after closing/checkpointing | 3,907,584 | 3,928,064 |

Index, update, compaction, and rebuild costs are single observations, not
percentiles. The checkpointed indexes grow by 20,480 bytes in these fixtures.
WAL sizes depend on checkpoint timing, so open-file totals are reported
separately rather than presented as intrinsic index size.

Declaration backfill took 1.097 ms for the 50-source fixture with seven
declarations, and 6.832 ms for the distinct stress index with no declarations.
Backfill still has to inspect the stored project chunks. The probe drops only
the derived declaration table/version, reopens the index, and verifies raw
statistics and source/chunk/tokenizer state. Opening a verification connection
recreates a 32,768-byte SHM file; the reported post-rebuild file growth is not
additional declaration payload.

`ranking-stress-before.json` and `ranking-stress-latest.json` retain both stress
captures. Earlier four-sample measurements and tokenizer-v3 figures are
historical diagnostics, not controlled comparison points. Windows and
production-corpus latency distributions were not measured here.

## Commands and results

These completed successfully on the final implementation:

```text
cargo fmt --package bm25-mcp -- --check
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings -D unsafe-code
git diff --check
cargo build --locked --release --example evaluate_ranking --example evaluate_ranking_stress --example evaluate_real_ranking
```

The complete test command passed 131 tests. The frozen baseline passed the same
prescribed gates before edits with 106 tests. Focused tests exercised each
regression during implementation; the original stdio MCP failure returned
`why does this happen` with a zero score.

The serial comparison scripts built the frozen baseline examples and current
release examples, then ran each pair with the fixed clock. Their comparison
assertions passed. The current captures can be regenerated with:

```text
cargo run --locked --release --quiet --example evaluate_ranking
cargo run --locked --release --quiet --example evaluate_ranking_stress
cargo run --locked --release --quiet --example evaluate_real_ranking -- src
```

The equivalent release executables were run for the recorded captures.
`evaluate_real_ranking src` passed every real-ingest regression and scanned
20 source files into 257 chunks. Target path ranks for its six smoke queries
were raw/enhanced: `search_ranked` 1/1, `RankingOptions` 2/1,
`tokenize_checked` 1/1, `session copies` 5/1,
`BM25 candidate reranking` 1/1, and `store.rs` 2/1.
An initial invocation mistakenly supplied `--regressions` as a project path
and failed to canonicalize it; the corrected command above passed.

## Migration and remaining limits

Opening an older current-tokenizer index transactionally backfills the additive
declaration table from persisted chunks. It does not require source reingestion,
change body statistics, or expire match IDs. The existing tokenizer-version
migration remains separate and still invalidates/rebuilds obsolete postings.
Content-cache restore uses the normal transactional insertion path.

Declaration recognition supports the existing heuristic declaration keywords,
modifiers, and qualified definition pattern. It does not parse ancestry,
multiline syntax, or every language construct. At most 40 exact definitions
are guaranteed admission independently of mentions, selected by match ID after
filters; excess definitions have no unbounded recall guarantee. Source changes
can change those IDs and therefore the overflow selection.

Body TF and length are authoritative, but metadata and proximity remain bounded
fragment heuristics. Path lookup can still scan source paths. The ranker cannot
recover candidates missing from every admission lane. No embeddings, parser
dependency, remote service, or new public endpoint was added.
