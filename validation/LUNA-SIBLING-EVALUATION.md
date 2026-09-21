# Parallel Luna sibling evaluation

Evaluated release `c514f0b895dad3b0c4b9408ac690dba43f6a0d20c0908f15850488e39098b365` using three parallel `gpt-5.6-luna` agents with `xhigh` reasoning. All 22 roots across 11 repository families were attempted, including linked worktrees, the OSS checkout, and bm25-mcp itself. Two setup-backup directories were excluded. A fresh snapshot contained 180 actual session JSONL files.

## Results

| Measure | Result |
| --- | ---: |
| Planned / executed automated queries | 343 / 340 |
| Roots completing all planned queries | 21 / 22 |
| Project target top 1 | 158 / 264 (59.8%) |
| Project target top 3 | 208 / 264 (78.8%) |
| Project target top 10 | 244 / 264 (92.4%) |
| Logical session target top 1 | 51 / 54 (94.4%) |
| Logical session target top 3 | 53 / 54 (98.1%) |
| Logical session target top 10 | 54 / 54 (100%) |
| Negative controls with no results | 22 / 22 |
| Excerpts matching source content | 3,132 / 3,132 |
| Same-source duplicate hits / over-budget responses | 0 / 0 |
| Copy references / target contexts checked | 768 / 54 |

The three unavailable queries are APayee session queries. Session target ranks above use equivalent logical copies; physical-source matching alone would incorrectly report only 41/54 targets found. The same-source duplicate metric does not assert that similar text never appears in distinct events or source files. All scored final queries had zero pending changes.

## Per-root target retrieval

| Root | Project targets in top 10 | Logical session targets in top 10 |
| --- | ---: | ---: |
| `APayee` | 11/12 | Unavailable: timeout |
| `BM25-Turbo-Rust-Python-WASM-CLI` | 10/12 | 3/3 |
| `CLIProxyAPIPlus` | 10/12 | 3/3 |
| `CLIProxyAPIRust` | 10/12 | 3/3 |
| `PersonalAssistant` | 12/12 | 3/3 |
| `agent-templates` | 11/12 | No sampled queries |
| `bm25-mcp` | 12/12 | No sampled queries |
| `copilot-worktrees/CLIProxyAPIPlus/bumpyclock-automatic-memory` | 10/12 | 3/3 |
| `copilot-worktrees/CLIProxyAPIPlus/bumpyclock-silver-umbrella` | 10/12 | 3/3 |
| `copilot-worktrees/CLIProxyAPIPlus/copilot-prewarm-refactored-winner` | 10/12 | 3/3 |
| `copilot-worktrees/PersonalAssistant/bumpyclock-probable-succotash` | 12/12 | 3/3 |
| `copilot-worktrees/PersonalAssistant/bumpyclock-ubiquitous-spoon` | 12/12 | 3/3 |
| `copilot-worktrees/PersonalAssistant/copilot-prewarm-bookish-carnival` | 12/12 | 3/3 |
| `copilot-worktrees/dotfiles/copilot-prewarm-supreme-fiesta` | 11/12 | 3/3 |
| `copilot-worktrees/neutron/bumpyclock-ideal-waddle` | 12/12 | 3/3 |
| `copilot-worktrees/neutron/bumpyclock-potential-eureka` | 12/12 | 3/3 |
| `copilot-worktrees/neutron/bumpyclock-symmetrical-fishstick` | 12/12 | 3/3 |
| `copilot-worktrees/neutron/copilot-prewarm-super-telegram` | 12/12 | 3/3 |
| `dotfiles` | 11/12 | 3/3 |
| `neutron` | 12/12 | 3/3 |
| `oss/CLIProxyAPIPlus` | 10/12 | No sampled queries |
| `t3code` | 10/12 | 3/3 |

## Qualitative assessment

The agents ran nine additional source-grounded task searches. All found useful navigation results; APayee’s execution/snapshot question was partially answered because execution and resolver code appeared but the central snapshot-definition module did not. Other searches found expected implementations at ranks 1–7, often below relevant documentation, tests, or duplicate physical copies. These are small agent-judged samples, not independent human relevance labels.

Common automated misses involve generic identifiers such as `bytes_stream` and `RawURLEncoding`, or implementation/test references outranking the sampled example. These are location misses; related results can still answer the task. Exact sampled-file recall should not be equated with semantic relevance.

Agent reviews and exact task questions: [partition A](luna-a-review.md), [partition B](luna-b-review.md), [partition C](luna-c-review.md).

## Operational findings

**APayee readiness is unresolved.** Project searches succeeded, but session reconciliation timed out after 900 seconds, then after a separate 300-second persisted-index retry. The saved index had 44/44 checkpoints at EOF, including 28 eligible verified sources. This rules out an unread-byte backlog in that saved state, but does not identify the exact in-memory readiness/finalization cause. No further retry or product modification was made.

**Latency remains a concern for session search.** Project calls measured 3.7 ms median and 65.2 ms p95; session calls measured 51.9 ms median and 219.3 ms p95. Maximum session latency was 268.6 ms. These are serialized calls within each partition while other partitions/indexers share the machine, not a controlled warm MCP acceptance test. The run does not establish the 100 ms warm p95 target. Successful readiness waits reached 715.6 seconds for a project and 430.6 seconds for sessions. Peak owner RSS reported in coverage was 77.25 MiB; this observation is not a fresh memory stress test.

**Self-evaluation needed isolation.** The original bm25-mcp run and an initial retry returned freshness transitions after four positive queries. The harness was writing response JSON inside the watched project after each request. Moving those response writes to an external temporary directory yielded 12/12 project targets with every response ready. The final metrics substitute only that isolated self-run; both earlier attempts remain preserved. This warrants further investigation of ignored-file watcher churn, but no product change is claimed here.

**One copy-audit request failed transiently.** The first partition-B copy audit received a generic MCP error after several worktrees. A resumed check with fresh clients completed the remaining targets without a product change. The original client discarded the detailed error, so its cause cannot be established retrospectively. The first-attempt log remains in the private cache.

## Limits and next priorities

The corpus includes 33 evaluated Codex and 21 Copilot CLI session queries, with no sampled Claude history queries. Agent-templates, bm25-mcp, and the OSS checkout had no suitable session query selected; that does not establish absence of history. Worktrees are correlated, and queries are derived from known source locations rather than exhaustive relevance judgments. This fresh run is not a controlled before/after comparison with the earlier 330-query evaluation. Windows and branch-switch stress were not rerun.

Recommended next work: investigate session readiness/finalization using APayee, capture detailed timeout/epoch diagnostics, measure session latency in an isolated warm run, and then test ranking changes against the demonstrated task-navigation misses. Separately verify whether ignored output writes should trigger project-wide refresh.

Evidence: [aggregate metrics](luna-eval-summary.json), [logical session audit](luna-session-audit.jsonl), the three `siblings-luna-*.jsonl` first attempts, `siblings-luna-a-retry.jsonl`, and `siblings-luna-c-self-isolated.jsonl`. Query manifests and raw responses remain in the ignored private cache. The temporary raw-history snapshot was removed after validation. No product code was changed or published.
