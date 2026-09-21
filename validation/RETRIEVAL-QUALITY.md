# Retrieval quality evaluation

The tested build is useful for targeted lexical lookup, but result diversity and precision need improvement. This evaluation changed no production code.

Build SHA-256: `56badbba8c8f3fb12e1f656a2a7af529d2ffaa61b5aee4adcc2e2ccdb8c2ebbd`. Evaluated 2026-09-20 local time against neutron and a private frozen copy of 176 actual session files. All ranking queries ran after reconciliation completed. Temporary history copies were removed; raw responses remain in ignored private cache.

## Method and limits

Selected known targets from code and original Copilot user events before querying. Requested ten ranked chunks with a 65,536-byte response budget, then separately checked default budgets. Measured target rank, duplication, source fidelity, and context order. This is a small diagnostic sample: 11 positive code queries plus one negative, four targeted session queries plus one broad query. Several code queries target the same module; all targeted session requests come from one real Copilot conversation. It is not an independent or representative benchmark of all projects or providers. Known-file hit rate is not result precision or corpus-wide recall.

## Project search

The expected file ranked first in 7/11 queries, in the top three in 9/11, and in the top ten in 11/11. The nonexistent-token query correctly returned zero results.

| Query | Known-file rank |
| --- | ---: |
| `h264_parameter_set_count` | 1 |
| `h264 parameter set count` | 1 |
| `save_envelope_durable` | 1 |
| `LoadOutcome` | 2 |
| `expected an integer from 0 through 65535` | 1 |
| `read data from platform clipboard` | 1 |
| `malformed TOML archived backup recovery` | 1 |
| `move keyboard focus next tab stop` | 2 |
| `validate app identifier macOS bundle underscores` | 4 |
| `read_from_clipboard` | 1 |
| `protect newer saved settings from being overwritten` | 6 |

The file-level result overstates definition quality in one case: `LoadOutcome` reaches its file at rank 2, but the enum definition itself is rank 9. Function definitions for `h264_parameter_set_count`, `save_envelope_durable`, and path-filtered `read_from_clipboard` are rank 1.

Lower-ranked results can be weak. Only one of the ten results for `h264_parameter_set_count` mentions H.264; it is the correct rank-1 definition. Component-token matches such as “set” and “count” bring unrelated files into the tail. Some lower-ranked files in other queries are legitimately related, so a preselected target rank should not be treated as a full relevance judgment.

All 110 returned code excerpts matched source text at the exact reported byte location. The path filter stayed within its requested file. These checks establish faithful excerpts and locations, not that every excerpt answers the query.

## Session search

| Query | Known user-event rank | Returned chunks | Unique events | Unique excerpts |
| --- | ---: | ---: | ---: | ---: |
| `E0599 aria_label Button` | 1 | 10 | 8 | 8 |
| `segmented Params Headers Body Settings` | 2 | 9 | 8 | 7 |
| `TabBar requests segmented List users Create user` | 2 | 10 | 10 | 8 |
| `suffix stop_propagation Tab tests` | 2 | 10 | 10 | 6 |
| `error` | Not judged | 10 | 7 | 7 |

All four targeted historical requests appeared at rank 1 or 2. All 49 returned session excerpts matched strings in the original JSONL events. Context returned eight chunks from five distinct events, including the target and two neighbors on either side, in original source order; all eight excerpts matched the source. The context remained within the requested session.

The broad `error` query returned only tool output, with ten chunks but seven distinct excerpts. That is lexically valid, but weak evidence of useful decision/history retrieval. The preceding Codex smoke test used the default budget and returned five hits with only three distinct excerpts.

One duplication defect is confirmed directly: a Copilot tool-result event stores identical text in both `result.content` and `result.detailedContent`, and both are returned as separate hits. Other repeated excerpts come from different events, such as a tool dispatch and the receiving user message; those are real source repetitions but still reduce diversity in search results.

## Budget and coverage

With the default 16,384-byte budget, the identifier query returned six hits and the generic session query returned five, both marked truncated. At 65,536 bytes, most queries returned all ten; the segmented-examples query still returned nine. This obeys the response budget, but chunk-sized excerpts consume space that could carry more distinct answers.

Completed coverage remained degraded: four project encoding exclusions and 29,721 unsupported session records were reported. This evaluation establishes relevance within the indexed subset; it does not establish complete history coverage. Ongoing indexing was not counted as an error.

## Recommended next changes

1. Suppress identical fields within the same session event, especially Copilot `content`/`detailedContent`, while retaining provenance and context.
2. Diversify search results across logical events and useful file passages; avoid spending the result budget on repeated excerpts.
3. Evaluate whole-identifier and multi-term coverage boosts so incidental component-token matches do not dominate the tail. Reuse this query set to check definition ranks and avoid regressions.
4. Return tighter query-centered excerpts by default, while preserving context expansion and explicit truncation.
5. Expand evaluation to independent projects and Claude/Codex histories, and classify unsupported records before claiming complete session recall.

[Machine-readable results](retrieval-quality.jsonl) and [evaluation script](evaluate-quality.py).
