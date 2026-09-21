# Luna partition B review

Run: `python3 validation/run-luna-eval.py b > validation/luna-b-run.log 2>&1`.
The initial pass completed all 9 partition-B roots and produced no `failure`
rows. The run artifacts are `validation/luna-b-run.log` and
`validation/siblings-luna-b.jsonl`; response bodies remain in the private
`validation/private-cache/luna-eval` tree.

## Generated retrieval checks

The run emitted 141 query rows: 108 non-negative project queries, 24
non-negative session queries, and 9 negative project queries.

| Scope | Non-negative queries | Top 1 | Top 3 | Top 10 | Misses |
| --- | ---: | ---: | ---: | ---: | ---: |
| Project | 108 | 67 | 84 | 103 | 5 |
| Sessions | 24 | 14 | 14 | 14 | 10 |
| Combined | 132 | 81 | 98 | 117 | 15 |

All 9 negative probes returned zero hits. Every returned excerpt passed the
source-byte check: 1,275/1,275 checked excerpts matched their indexed source.
There were no same-source duplicate hits in this partition. Twenty-two
non-negative responses were marked truncated; all response sizes remained
within the harness budget.

The five project misses are ranking misses in the generated sample. The
missed query IDs are `agent-templates/components_3`,
`copilot-worktrees/dotfiles/copilot-prewarm-supreme-fiesta/heading_1`,
`dotfiles/heading_1`, and `t3code/identifier_4` plus `t3code/components_4`.
The t3code target is an example file containing `TopicEventSource`; the top
results are more direct package definitions for that identifier. The
agent-templates target is found at rank 7 with the unsplit `collectionId`
diagnostic query, while the generated `collection Id` form falls outside the
top 10. The dotfiles heading queries return related linker and test files
ahead of the README target.

The ten session misses are `session_1` and `session_2` across the five neutron
roots (`neutron` and its four partition-B worktrees). Session rankings are
physical-occurrence rankings; these rows are not treated as logical misses in
this review. The root audit checked equivalent copies separately.

## Coverage and performance caveats

Project coverage was `ready` for dotfiles and its worktree, and `degraded`
for agent-templates, the neutron roots, and t3code. The degraded states came
from reported coverage conditions: agent-templates had 2 unsupported
encodings; neutron roots reported 8 binary exclusions, 4 unsupported
encodings, and 30 symlink exclusions each; the t3code root reported 64 binary
exclusions, 210 unsupported encodings, and 1 symlink exclusion. Session
coverage reported unsupported records in the dotfiles roots (297 each), the
neutron roots (29,721 each), and t3code (2,010). These diagnostics describe
corpus coverage and do not imply that the returned source hits were invalid.

The partition runner uses `jobs=1`, so its query timings are serialized
single-client observations rather than concurrent throughput. Across
non-negative queries, project latency was median 3.92 ms, p95 88.52 ms, and
maximum 147.54 ms. Session latency was median 158.88 ms, p95 219.30 ms, and
maximum 268.65 ms. Combined latency was median 5.67 ms, p95 177.54 ms, and
maximum 268.65 ms.
The largest reported peak owner RSS was 81,002,496 bytes (about 77 MiB),
below the 512 MiB soft target.

## Manual source-grounded sample

These searches were run after the relevant partition-B caches were ready.
Judgments use source paths and result ordering only; no session excerpts are
included.

| Project | Task-oriented query | Expected source | Rank | Judgment |
| --- | --- | --- | ---: | --- |
| agent-templates | “agent-template compilation across Codex and Claude” | `agent-templates/scripts/agent-compiler.ts` | 3 | Relevant. An identical physical copy ranked 2, and the README ranked 1. |
| dotfiles | “How does managed block reconciliation preserve user shell configuration when updating dotfiles?” | `scripts/link-dotfiles/managed-block.ts` | 6 | Relevant neighborhood. The related test files ranked 1/2 and the linker ranked 4; the implementation was present at rank 6. |
| agent-templates | “Where does the agent registry validate provider model profiles before compilation?” | `scripts/agent-registry.ts` | 5 | Relevant. Compiler files ranked above the registry, and the identical nested registry copy ranked 6. |

The manual results support useful source navigation while showing the expected
lexical tradeoff: task wording can favor nearby documentation, tests, or
duplicate physical copies over the exact implementation file.
