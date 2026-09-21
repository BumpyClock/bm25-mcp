# Luna partition A review

Partition A used build `c514f0b895dad3b0c4b9408ac690dba43f6a0d20c0908f15850488e39098b365` and the fresh 180-file frozen history snapshot. The harness covered seven roots: `APayee`, `CLIProxyAPIRust`, `PersonalAssistant`, three `PersonalAssistant` worktrees, and `oss/CLIProxyAPIPlus`.

The generated manifest planned 109 queries: 84 positive project targets, seven nonexistent-token controls, and 18 session targets. The run scored 106 queries: 91 project queries and 15 session queries. APayee's three session queries were unavailable because cold session reconciliation timed out at 900 seconds. The APayee project index reached `degraded` with zero pending changes and was queryable. A later bounded persisted-index retry also timed out at 300 seconds; that retry is preserved separately and is not merged into these counts.

## Automated results

| Query kind | Scored | Target rank 1 | Target top 3 | Target top 10 | Source-faithful excerpts | Same-source duplicate hits |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Project positives | 84 | 59 | 73 | 79 | 840/840 | 0 |
| Session targets | 15 | 12 | 14 | 15 | 148/148 | 0 |
| Negative controls | 7 | — | — | 7/7 returned zero | — | — |

All positive project targets visible in the returned excerpt were source-faithful. The 24 repeated project excerpts and six repeated session excerpts came from distinct source locations or events; the evaluator's same-source duplicate metric stayed at zero. Two session responses were response-budget truncated. No scored query had pending changes when it was evaluated.

Per-root positive project results were:

| Root | Positive queries | Rank 1 | Top 3 | Top 10 | Session result |
| --- | ---: | ---: | ---: | ---: | --- |
| `APayee` | 12 | 9 | 10 | 11 | 3 unavailable after timeout |
| `CLIProxyAPIRust` | 12 | 6 | 7 | 10 | 0/3 rank 1, 2/3 top 3, 3/3 top 10 |
| `PersonalAssistant` | 12 | 10 | 12 | 12 | 3/3 rank 1 |
| `PersonalAssistant` worktrees (three) | 36 | 30 | 36 | 36 | 9/9 rank 1 |
| `oss/CLIProxyAPIPlus` | 12 | 4 | 8 | 10 | no sampled session queries |

Observed search-call latency was 3.54 ms median, 15.51 ms p95, and 42.53 ms maximum for positive project queries. Session search was 19.44 ms median, 127.03 ms p95, and 135.22 ms maximum. These are descriptive measurements from concurrent partition runs and should not be treated as isolated latency results. Reconciliation waits were much larger: CLIProxyAPIRust sessions took 430.6 seconds, PersonalAssistant sessions 228.8 seconds, and the three worktree session imports took 51.9–79.5 seconds.

Coverage remains separate from ranking. APayee project coverage was `degraded` with four unsupported encodings and 109 Git metadata exclusions; its session readiness did not complete. CLIProxyAPIRust project coverage was `ready` with 3,464 Git metadata exclusions, while session coverage was `degraded` with 29,093 unsupported records and 178 ownership exclusions. PersonalAssistant and its worktrees reported two binary exclusions, four symlink exclusions, ten unsupported encodings, and 171 ownership exclusions; the direct checkout was `degraded` for sessions with 2,974 unsupported records, while the three worktrees reported the same diagnostics under `ready`. The `oss/CLIProxyAPIPlus` project was `degraded` with 19 unsupported encodings and 52 Git metadata exclusions. A `ready` label here does not mean complete provider-record coverage.

## Misses and manual assessment

The generated project misses are target-location misses, not automatic relevance failures.

- In APayee, `identifier_4` (`preview_bytes`) ranked 8 and `components_4` (`preview bytes`) did not return the sampled `crates/apayee-execution/src/spool.rs` location. The method also appears in the transport API, HTTP adapter, and tests, so the component query has several legitimate lexical matches. I do not classify this as a logical retrieval failure.
- In CLIProxyAPIRust, both `identifier_4` and `components_4` missed `src/providers/antigravity_transport.rs` for `bytes_stream`. The term is repeated across general streaming implementations and tests, so this is a broad lexical competition case.
- In `oss/CLIProxyAPIPlus`, both `identifier_4` and `components_4` missed `internal/api/handlers/management/logs_test.go` for `RawURLEncoding`. The generic encoding constant occurs throughout authentication, JWT, and other tests, so the sampled test location is not a strong standalone relevance judgment.

I ran three additional source-grounded, project-only searches against the completed partition caches. Their sanitized results are in [luna-a-manual.jsonl](luna-a-manual.jsonl); no session text is included.

| Project | Task-oriented query | Source-backed expected locations | Result and judgment |
| --- | --- | --- | --- |
| `APayee` | `immutable authored effective snapshots execution lifecycle queue resolve variables` | `crates/apayee-core/src/execution.rs`, `crates/apayee-core/src/snapshot.rs`, `crates/apayee-execution/src/resolver.rs` | `execution.rs` rank 2 and `resolver.rs` rank 4; `snapshot.rs` absent from top 10. This is one meaningful qualitative miss because “snapshots” is central to the request and the source module is the definition location. |
| `CLIProxyAPIRust` | `usage queue completion timing retries cancellation OAuth fingerprint` | `docs/verification/usage-queue.md`, `scripts/go_fixtures/usage_queue_oracle_test.go` | The authoritative usage-queue document ranked 1. The fixture was absent, but related usage-stream and documentation locations filled the list; the task answer is still well supported. |
| `PersonalAssistant` | `WorkIQ fetch calendar Teams messages Graph bridge` | `scripts/workiq/README.md`, `scripts/workiq/workiq-call.mjs` | README ranked 1 and the bridge script ranked 4. This is a strong result for a natural task query. |

## Session ranking and copies

The three CLIProxyAPIRust session targets ranked 3, 5, and 3. Each returned target was source-faithful, and returned physical source references were distinct. Repeated excerpt counts were 1, 2, and 3 respectively, but there were no same-source/event duplicate hits. These should be treated as lexical ordering among separate physical events until logical-copy identity is applied.

The direct PersonalAssistant checkout and its three worktrees returned all twelve sampled session targets at rank 1. Some top hits reported multiple equivalent copies, so physical rank 1 does not establish logical-event diversity. The separate partition-wide logical-copy audit should make that distinction before interpreting session rank or diversity.

## Scope limits

This is a small, automatically sampled lexical retrieval check. Worktrees are correlated, project target locations are single sampled locations, and the session target is a physical occurrence. It does not estimate corpus-wide recall or semantic relevance. APayee's cold session reconciliation timeout is a material operational limitation; the available evidence shows its project index and all 44/44 expected source checkpoints, but readiness/finalization remained unresolved. Latency and reconciliation times were collected while sibling partitions were also active and are not controlled performance measurements.

Evidence: [siblings-luna-a.jsonl](siblings-luna-a.jsonl), [luna-a-run.log](luna-a-run.log), and the private response cache under `validation/private-cache/luna-eval/responses-luna-a`. The raw session responses and query manifest remain private.
