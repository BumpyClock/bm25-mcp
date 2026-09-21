# Ranking evaluation

The checked-in `ranking-latest.json` is produced by
`cargo run --release --quiet --example evaluate_ranking`. It uses a disposable SQLite
database populated by `examples/support/ranking_fixture.rs`, so it contains no
private session text. The harness runs raw BM25, the enhanced ranker, and
leave-one-feature-out variants for each fixed query. It records Recall@10 and
Recall@20, MRR, nDCG@10, top-10 duplicate rate, candidate and expansion-probe
counts, p50/p95 query latency, initial indexing time, replacement/update time,
and database bytes including SQLite sidecars.

The fixture intentionally puts repeated project and tool-result content ahead
of useful answers in raw retrieval. It tests exact symbols and paths,
diagnostic phrases, historical identity, expansion, session recency, and
diversity. The expected labels are in the fixture source (`must` is grade 3;
`useful` is grade 1). These labels are a deterministic regression set, not a
claim about production relevance.

`ranking-baseline.json` is a frozen tokenizer-v3 raw-search capture. New
captures use tokenizer-v4 surface forms and must be compared within the same
revision. Database size is summed across the main database and `-wal`/`-shm`
files; a lone 4096-byte SQLite header is not treated as the complete index.

The release-mode capture on macOS arm64 (rustc 1.98.1, 2026-09-21) measured raw
Recall@10 0.6154, MRR 0.5769, nDCG@10 0.5766, and duplicate rate 0.4000.
Enhanced ranking measured Recall@10 1.0000, MRR 0.9103, nDCG@10 0.9306, and
duplicate rate 0.0000. Enhanced p95 latency averaged 3.94 ms per case across
the 13-case fixture (raw averaged 0.60 ms); indexing took 36.2 ms, replacement
took 0.65 ms, and the database occupied 3,889,096 bytes including sidecars.
The harness uses one warmup and three measured samples per case; p95 is
therefore a small-sample diagnostic rather than a stable service percentile.
The `no_classification` ablation fell to Recall@10 0.8462 and MRR 0.7179;
`no_exact` fell to MRR 0.8333; `no_expansion` fell to Recall@10 0.9231; and
`no_dedupe` raised duplicate rate to 0.3538.

The release stress probe indexes 220 project chunks and retrieves the bounded
200-candidate pool. Identical chunks (16,031 bytes) measured p50 189 ms / p95
193 ms; distinct chunks (16,050–16,052 bytes) measured p50 542 ms / p95 598
ms. This is a
worst-case diagnostic for the bounded reranker, not a normal query target; it
identifies candidate text processing as the next optimization seam.

The ranker is bounded to 200 candidates, uses deterministic lexical probes,
fixed field priors, a 45-day session decay half-life with a 0.25 floor,
weighted Jaccard/MMR diversity, and a final requested limit. It does not use
embeddings, vector search, remote services, or model inference.

The significant implementation seams are `src/query.rs` (classification and
bounded probes), `src/ranking.rs` (field extraction, exact tiers, decay,
dedupe, similarity, and MMR), `src/store.rs` (candidate retrieval and ranked
API), and the three evaluation examples under `examples/`. A future semantic
retriever can feed the existing bounded candidate/reranker seam while keeping
MCP contracts and copies/context behavior intact.

`evaluate_real_ranking` performs a read-only scan of the selected source tree
into another disposable index and records representative path/symbol ordering
in `ranking-real-latest.json`. Its timings include local disk and any
concurrent machine workload, so they are smoke measurements rather than a
benchmark gate.

## Final verification

The final repository gates passed: `cargo fmt --package bm25-mcp -- --check`,
`cargo test --locked --all-targets` (106 passed, 0 failed),
`cargo clippy --locked --all-targets -- -D warnings -D unsafe-code`, and
`git diff --check`. The frozen pre-ranking baseline had 81 passing tests.

The release real-source smoke capture used six target paths. Ranked target
paths moved from raw to enhanced ranks as follows: `search_ranked` 2→1,
`RankingOptions` 2→1, `tokenize_checked` 2→1, `session copies` 5→1,
`BM25 candidate reranking` 1→1, and `store.rs` 3→1. Manual inspection found
symbol definitions promoted, session-copy API code ahead of tool mentions, and
multiple chunks for a requested file query. These are path-level labels and do
not prove best individual-chunk ordering.
