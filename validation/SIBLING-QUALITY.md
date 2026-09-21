# Sibling retrieval quality evaluation

Completed: all 21 roots and all 330 queries on each build. Compact excerpts expose more code targets within the default response budget, and the observed duplicate-field hits are eliminated. Session top-10 target coverage is unchanged. The physical-source top-3 metric declines, largely because repeated copies of one logical Codex message change order across rollout files; cross-file logical-event duplication remains unresolved.

## Results

| Metric | Before | After |
|---|---:|---:|
| Code target file at rank 1 | 147/252 | 147/252 |
| Code target file in top 3 | 197/252 | 197/252 |
| Code target file in top 10 | 220/252 | 232/252 |
| Sampled code identifier/heading visible | 214/252 | 227/252 |
| Session target event at rank 1 | 39/57 | 41/57 |
| Session target event in top 3 | 50/57 | 44/57 |
| Session target event in top 10 | 52/57 | 52/57 |
| Mean code results | 5.83 | 9.86 |
| Mean session results | 8.12 | 9.93 |
| Same-source/event duplicate hits | 6 | 0 |
| Responses with additional budget truncation | 285 | 16 |
| Source-faithful excerpts | 1932 | 3051 |

All 21 nonexistent-token controls return zero results on both builds. All 4,983 checked excerpts match their source, all responses fit 16,384 bytes, and no query was scored with pending reconciliation. Code top-1 and top-3 counts are unchanged: the extra top-10 targets come from fitting more of the existing ranked list into the response.

The six duplicate hits are query-weighted occurrences of two unique Copilot tool-result events, not six independently discovered defects. Distinct events with repeated text remain separate, as do replicated copies of a logical event across different rollout files. Session mean distinct excerpt count rises from 5.05 to 6.75; the raw result-count increase is larger because some additional events repeat text.

Five strict heading-visibility regressions are the same example across neutron and four worktrees. The source file remains rank 1, and all query terms remain visible. The 640-byte window selects a later exact `Neutron Components examples` match, omitting the opening `Neutron Components basic examples` heading that was present in the former full chunk. This is a tradeoff in which occurrence is shown, not a loss of the query match.

Session ranking changes deserve follow-up: one neutron physical-source target moves from rank 1 to 6 on all five roots. All ten hits have the same excerpt and logical message identifiers, but different source paths/offsets. This is ordering among replicated copies of one Codex message, not ten distinct relevant events. The same-source duplicate metric does not detect this pattern. BM25 session targets move 3→5 and 2→3, and a CLIProxyAPIRust target moves 2→4. Other targets improve, including APayee and PersonalAssistant. No previously returned target file or session event drops out of the returned result set.

To reduce repeated-worktree weighting, the summary also includes one primary checkout per repository family. Across those ten checkouts, code top-10 target hits improve 96/120→107/120; session top-10 stays 23/24, session top-3 changes 21/24→19/24, and session top-1 improves 13/24→16/24. These are descriptive counts, not independent statistical estimates.

## Per-root results

Positive queries combine sampled files and session events. “Visible” requires the sampled code identifier/heading; session visibility is event-location presence as defined below.

| Root | Positive queries | Target in top 10, before→after | Visible target, before→after | Mean hits, before→after |
|---|---:|---:|---:|---:|
| `APayee` | 15 | 13→14 | 13→14 | 5.87→9.87 |
| `BM25-Turbo-Rust-Python-WASM-CLI` | 15 | 12→13 | 12→13 | 6.67→10.00 |
| `CLIProxyAPIPlus` | 15 | 13→13 | 13→13 | 6.00→10.00 |
| `CLIProxyAPIRust` | 15 | 12→13 | 12→13 | 5.73→9.87 |
| `PersonalAssistant` | 15 | 15→15 | 15→15 | 6.13→10.00 |
| `agent-templates` | 12 | 6→11 | 6→11 | 5.75→10.00 |
| `copilot-worktrees/CLIProxyAPIPlus/bumpyclock-automatic-memory` | 15 | 13→13 | 13→13 | 6.13→10.00 |
| `copilot-worktrees/CLIProxyAPIPlus/bumpyclock-silver-umbrella` | 15 | 13→13 | 13→13 | 6.13→10.00 |
| `copilot-worktrees/CLIProxyAPIPlus/copilot-prewarm-refactored-winner` | 15 | 13→13 | 13→13 | 6.13→10.00 |
| `copilot-worktrees/PersonalAssistant/bumpyclock-probable-succotash` | 15 | 15→15 | 15→15 | 6.47→10.00 |
| `copilot-worktrees/PersonalAssistant/bumpyclock-ubiquitous-spoon` | 15 | 15→15 | 15→15 | 6.47→10.00 |
| `copilot-worktrees/PersonalAssistant/copilot-prewarm-bookish-carnival` | 15 | 15→15 | 15→15 | 6.47→10.00 |
| `copilot-worktrees/dotfiles/copilot-prewarm-supreme-fiesta` | 15 | 13→14 | 13→14 | 7.13→10.00 |
| `copilot-worktrees/neutron/bumpyclock-ideal-waddle` | 15 | 14→14 | 13→13 | 6.67→10.00 |
| `copilot-worktrees/neutron/bumpyclock-potential-eureka` | 15 | 14→14 | 13→13 | 6.67→10.00 |
| `copilot-worktrees/neutron/bumpyclock-symmetrical-fishstick` | 15 | 14→14 | 13→13 | 6.67→10.00 |
| `copilot-worktrees/neutron/copilot-prewarm-super-telegram` | 15 | 14→14 | 13→13 | 6.67→10.00 |
| `dotfiles` | 15 | 13→14 | 13→14 | 7.13→10.00 |
| `neutron` | 15 | 14→14 | 13→13 | 6.33→10.00 |
| `oss/CLIProxyAPIPlus` | 12 | 10→10 | 10→10 | 5.67→10.00 |
| `t3code` | 15 | 11→13 | 10→13 | 4.20→7.67 |

## Validation and operational observations

- Final source: 73 tests pass; formatting, Clippy with warnings and unsafe code denied, and the locked release build pass.
- The final candidate passes the update/branch stress checks: one-file and ten-file edits complete within the two-second target; returning to tested branches records content-cache hits.
- A 256 MiB session-record test passes with four concurrent readers and about 23 MiB observed peak owner RSS. This is a stress case, not a substitute for real-history import performance.
- APayee hits the evaluator’s 30-minute session reconciliation limit on the first candidate attempt. A retry with a longer limit and the persisted index completes; its measured session wait is about 821 seconds. The original failure is retained in `siblings-after-first-attempt.jsonl`, and the retry is in `siblings-after-retry.jsonl`. The consolidated `siblings-after.jsonl` contains one successful result set per root.
- All evaluator clients and their pinned-binary owners have exited. Initial registration/cache adjustments and concurrent workloads prevent interpreting these durations as a controlled performance comparison.
- Both builds report exclusions/unsupported records. Project coverage is ready on four roots and degraded on seventeen; session coverage is ready on thirteen/degraded on six before, and ready on twelve/degraded on seven after. Only nineteen roots have sampled session queries. Status labels change on APayee, PersonalAssistant, and the dotfiles worktree even though each has identical diagnostic counts across builds; inspect diagnostic counts rather than treating `ready` as proof of complete provider coverage. Degraded reconciliation is accepted for ranking, with diagnostics retained; it does not imply complete support for every history record.

## Build identity

- Baseline: `56badbba8c8f3fb12e1f656a2a7af529d2ffaa61b5aee4adcc2e2ccdb8c2ebbd`.
- Candidate: `27c2ceba68b16588c2b561a59f8f9abb2d1a0db70ed4f48451778f99b2bd02fd`.

## Next quality work

Keep the excerpt and exact-field dedup changes. The next experiment should collapse verified copies of the same logical session event across rollout files while retaining source provenance and context access. The neutron query fills all ten slots with replicated copies despite zero same-source duplicate hits. Evaluate logical-event diversity and field/role weighting on a held-out set of distinct events and repositories while preserving lexical BM25 retrieval. Broaden the relevance judgments beyond automatically sampled target locations before tuning weights. Profile history reconciliation separately, including cached ownership checks and the APayee timeout, before claiming an indexing-speed improvement.

## Scope and method

The comparison uses 21 project roots in 10 Git repository families: nine direct sibling repositories, the independent `oss/CLIProxyAPIPlus` clone, and eleven linked worktrees. The `oss` and `copilot-worktrees` container directories are covered through their repository children. The new `bm25-mcp` directory is the implementation under test. Two setup/configuration backups are excluded: `mac-setup-backup-20260913-121249` and `cliproxyapi-backup-20260913-123134`.

Each build receives the same 330 source-grounded queries: 252 positive project queries, 21 nonexistent-token controls, and 57 known-session-event queries. Project queries sample exact identifiers, identifier components, and documentation headings. Session queries sample 36 Codex events and 21 Copilot CLI events. There was no attributable Claude query sample; fixture coverage is separate from this real-history evaluation.

Both builds read the same private snapshot of 178 actual history files (160 Codex, 16 Copilot CLI, two Claude; 1,009,524,373 bytes). Project trees are read-only but live; recorded Git revisions and dirty state are in `sibling-quality-corpus.json`. Each build has independent persistent caches, separated by repository family. Worktrees in each family run sequentially. Ranked queries wait for `ready` or `degraded` with zero pending changes. Queries request ten results under the default 16,384-byte response budget.

A known-target hit means that the sampled file or session event appears in returned results. For project queries, visible-target hits additionally require the sampled identifier or heading in an excerpt. For sessions, visibility currently means the expected source event was returned; it is not a human judgment that the excerpt answers the query. Excerpt fidelity checks compare project bytes at reported offsets and session excerpts against decoded strings in the referenced frozen JSONL event. The session check proves event-level source fidelity, not which structured field produced a hit.

## Changes under test

Search excerpts are contiguous UTF-8 windows capped at 640 bytes, selected around query matches. Hits expose `excerpt_byte_offset` and `excerpt_truncated`; original chunk/event bounds remain available. Full context pagination is unchanged. Routine excerpt compaction is distinct from response-level budget truncation.

Session normalization suppresses byte-identical message or tool-result fields within the same physical event and semantic context. Whole decoded fields are compared, so repeated fragments within one field survive. Tool arguments, separate events, and distinct semantic contexts remain separate. Checkpoint schema 6 reprocesses old normalized rows. Large-field comparisons spill to disk and verify bytes after hashing.

## Interpretation limits

This is a paired lexical retrieval check, not a semantic-search benchmark or an estimate of overall relevance. Worktree queries are correlated, and repeated session targets within one family are not independent judgments. The target is one sampled source location, not a complete set of relevant results. For example, `build_from_corpus` can reasonably rank its implementation above the sampled example file; that is counted as a target-location miss.

More returned hits can expose an existing lower-ranked target without improving BM25 ordering. Duplicate suppression can change session corpus statistics and scores. Identical excerpts from separate events are intentionally retained; this change does not collapse repeated conversation turns or deduplicate across sessions.

History coverage diagnostics must be considered separately from ranking. Unsupported provider records can leave the index degraded after reconciliation. Concurrent before/after imports and mixed cache state make these runs unsuitable for isolated latency or indexing-speed comparisons. Windows execution remains a separate user-run gate.

## Evidence

- `siblings-before.jsonl` and `siblings-after.jsonl`: per-query metrics and coverage records, without query text or transcript excerpts.
- `sibling-quality-summary.json`: aggregate and per-root paired metrics.
- `sibling-quality-corpus.json`: scope, source revisions, and private query-manifest digest.
- `../scripts/evaluate-siblings.py` and `../scripts/summarize-sibling-eval.py`: evaluation and aggregation.
- `sibling-quality-tests.log`, `sibling-quality-clippy.log`, and `sibling-quality-build.log`: final validation.
- `sibling-quality-stress.jsonl`: differential-edit, branch-switch, and 256 MiB session stress checks.

Raw responses and query text stay in ignored local private storage. Temporary copied history files were removed after validation; original histories were untouched. Re-running collection requires preparing a new snapshot, which may differ from this recorded corpus. No sibling repository or original session history is modified by this evaluation.

Follow-up: [logical event deduplication](LOGICAL-EVENT-DEDUP.md) groups verified message replicas across files and replays the saved session queries. The measurements above remain the historical pre-fix results.
