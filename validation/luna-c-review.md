# Partition C sibling evaluation

Partition C ran with `python3 validation/run-luna-eval.py c` against the fresh 180-file history snapshot. The run covered six roots and 93 harness query rows (72 positive project queries, six negative controls, and 15 session queries). The evaluator used release binary SHA-256 `c514f0b895dad3b0c4b9408ac690dba43f6a0d20c0908f15850488e39098b365`. The run completed all six roots with no `failure` rows.

The harness output is in [siblings-luna-c.jsonl](siblings-luna-c.jsonl), and the completion log is in [luna-c-run.log](luna-c-run.log). Response bodies remain in the ignored private cache. No session text was exported.

## Retrieval results

Project target ranks use the sampled source file as the target. They are a location check, not a semantic relevance judgment.

| Root | Stable positive project queries | Target top 1 | Target top 3 | Target top 10 | Source-faithful excerpts | Project status | Peak owner RSS |
| --- | ---: | ---: | ---: | ---: | ---: | --- | ---: |
| `BM25-Turbo-Rust-Python-WASM-CLI` | 12 | 5 | 8 | 10 | 120/120 | ready | 18.97 MiB |
| `CLIProxyAPIPlus` | 12 | 4 | 8 | 10 | 120/120 | degraded | 23.70 MiB |
| `copilot-worktrees/CLIProxyAPIPlus/bumpyclock-automatic-memory` | 12 | 4 | 8 | 10 | 120/120 | degraded | 35.80 MiB |
| `copilot-worktrees/CLIProxyAPIPlus/bumpyclock-silver-umbrella` | 12 | 4 | 8 | 10 | 120/120 | degraded | 42.64 MiB |
| `copilot-worktrees/CLIProxyAPIPlus/copilot-prewarm-refactored-winner` | 12 | 4 | 8 | 10 | 120/120 | degraded | 45.52 MiB |
| `bm25-mcp` | 4 | 4 | 4 | 4 | 40/40 | ready before refresh | 21.42 MiB |

Across the 64 stable positive project queries, the sampled target was top 1 for 25, top 3 for 44, and present in the returned top 10 for 54. Every one of the 640 checked project excerpts matched source bytes. All six negative controls returned zero results.

The `bm25-mcp` root changed while its own evaluation was querying it: eight of its 12 positive queries returned `refreshing` with no results and no pending-change value. Those rows are excluded from the stable ranking denominator. This is the expected index freshness transition while the root report and review artifacts were being prepared, not a retrieval failure.

The concrete sampled misses were:

- BM25 query IDs `identifier_5` and `components_5` target `examples/basic_search/main.rs` for `build_from_corpus`. The target example was outside the returned top 10, while the core implementation and tests in `bm25_turbo/src/index.rs` filled the list. This is a sampled-location miss with strong semantic locality.
- CLIProxy query IDs `identifier_4` and `components_4` target `internal/api/handlers/management/logs_test.go` for `RawURLEncoding`. The target is absent in the direct checkout and all three linked worktrees. The top results are the Kiro JWT base64url implementation and related URL-safe decoding code, a lexical collision with adjacent implementation relevance rather than an empty result.

The linked worktrees reproduce the same two CLIProxy target-location misses and the same 10/12 top-10 rate. Their source-faithful checks all pass.

## Session coverage and latency

The five roots with session queries returned 149/149 source-faithful session excerpts. The harness reports the expected physical source event in all three session queries for CLIProxyAPIPlus and its three linked worktrees. BM25’s physical-source target rows were not interpreted as logical misses because equivalent copied rollout messages can be returned under another source file; the parent review is auditing those copies and context results.

Session reconciliation completed as follows:

| Root | Session status | Reconciliation | Diagnostics | Peak owner RSS |
| --- | --- | ---: | --- | ---: |
| `BM25-Turbo-Rust-Python-WASM-CLI` | degraded | 206.92 s | `ownership_excluded=163`, `unsupported_record=16781` | 30.94 MiB |
| `CLIProxyAPIPlus` | degraded | 100.51 s | `ownership_excluded=174`, `unsupported_record=2264` | 35.62 MiB |
| `copilot-worktrees/CLIProxyAPIPlus/bumpyclock-automatic-memory` | ready | 44.84 s | `ownership_excluded=174`, `unsupported_record=2264` | 42.55 MiB |
| `copilot-worktrees/CLIProxyAPIPlus/bumpyclock-silver-umbrella` | ready | 47.40 s | `ownership_excluded=174`, `unsupported_record=2264` | 45.36 MiB |
| `copilot-worktrees/CLIProxyAPIPlus/copilot-prewarm-refactored-winner` | ready | 60.95 s | `ownership_excluded=174`, `unsupported_record=2264` | 45.97 MiB |

For stable project queries, latency was 4.65 ms median, 47.98 ms p95, and 133.31 ms maximum. Session-query latency was 51.70 ms median, 97.79 ms p95, and 99.31 ms maximum. These are single-client query samples after readiness waits. Partitions A, B, and C were running concurrently on the same machine, so the samples include cross-partition owner and CPU contention; they are not a dedicated four-client stress measurement. The readiness waits also include cold reconciliation, while query samples follow the warm-up `settle` call.

## Independent task searches

After the harness completed, three project-only searches were run against the persisted BM25 cache. Each returned `ready` with zero pending changes.

| Task query | Expected source | Rank | Top-result judgment |
| --- | --- | ---: | --- |
| `How do I load a saved BM25 index with memory mapped zero copy I/O?` | `bm25_turbo/src/persistence.rs` | 3 | Strong result. Ranks 1–2 are README copies containing the same `load_mmap` guidance; the implementation appears at ranks 3–4. |
| `Why does building a BM25 index reject an empty corpus, and where is that validation?` | `bm25_turbo/src/index.rs` | 7 | Strong result. The first result is `bm25_turbo/tests/integration.rs`, which directly tests the empty-corpus rejection; the implementation follows in the same index module. |
| `How is the standalone MCP HTTP server and /mcp endpoint started for a BM25 index?` | `bm25-turbo-cli/src/commands/mcp.rs` | 3 | Strong result. `serve.rs` ranks first because it mounts `/mcp`, `main.rs` ranks second for the standalone command, and the dedicated MCP command ranks third. |

These task queries measured 40.41 ms, 19.86 ms, and 41.12 ms respectively from one warmed client. The persisted index reported `unchanged_index_reuse=88` and `git_metadata_excluded=92` during this check.

## APayee session-timeout follow-up

Both the initial partition-A capture and its retry ended with `RuntimeError: Reconciliation timeout: search_sessions` for APayee ([siblings-luna-a.jsonl](siblings-luna-a.jsonl), [siblings-luna-a-retry.jsonl](siblings-luna-a-retry.jsonl)). A read-only check of the saved private-cache index found 44 session sources and checkpoints. All 44 corresponding frozen snapshot files exist, and every checkpoint is exactly at its file size: 16 ineligible sources have 0 remaining bytes and 28 eligible, verified sources also have 0 remaining bytes. The persisted session index contains 24,105 documents and 19,317,010 tokens; its last file update was 07:43:20, before the retry output finalized at 07:44:04. This rules out a remaining input-byte backlog in the saved checkpoint/index state.

The remaining classification is uncertain from saved artifacts. Runtime code publishes session coverage and advances `sessions.scanned` only after the scan completes while `can_continue()` remains true; the harness waits for that epoch to match and for `pending_changes == 0`. A lease closing or epoch changing late in the pass can leave persisted rows complete while readiness is not published, which is a plausible timeout mechanism. A slow final pass or transaction cannot be excluded because evaluator stderr is discarded and no in-memory epoch/coverage state was captured. No product or index changes were made for this investigation.
