# Logical session event deduplication

Verified replicas of a session message now share a representative occurrence before the search result limit. `copy_count` reports matching occurrences, and `search_sessions` with `mode: "copies"` pages through source references and match IDs. Each ID expands its own original context. Nothing is removed from session history or the persistent index.

Grouping requires provider, session and event identity plus exact equality of the complete ordered normalized event content and semantic metadata. Different rollout timestamps are allowed; search date filters apply before grouping. Copies mode lists all currently eligible equivalents, including copies outside the original date filter. Missing identity, conflicting content, and tool records remain separate. Older tool records may use call IDs as fallback event IDs, so they cannot safely be merged. Repeated chunks within a representative occurrence remain searchable. The underlying BM25 statistics are unchanged.

## Saved real-history replay

Replayed all 57 saved session queries across 19 roots in eight repository families, using clones of the previous evaluation's persistent indexes. This includes 36 Codex and 21 Copilot queries. No new session discovery or re-ingestion was needed.

- The expected logical target was returned for 57/57 queries; 55/57 were in the first three results, unchanged from the baseline when evaluated by logical identity.
- Neutron's observed duplicate case changed from ten results representing one message to nine distinct events within the response budget. The repeated message remains first and exposes all 16 stored copies.
- All 554 returned excerpts and source references matched the indexed records; responses respected the default byte budget.
- Search-call latency was 54.0 ms median, 151.1 ms p95, and 402.7 ms maximum. These single-pass cloned-index measurements include cold and warm posting-cache states and exclude copies enumeration. They do not establish the separate 100 ms warm MCP p95 acceptance target.

These are correlated worktrees and automatically sampled targets, not independent human relevance judgments. The replay establishes increased result diversity and retained target retrieval, not improved semantic ranking. There is no real-history Claude query sample. Archived coverage metadata was reused for a comparable response budget; source fidelity was checked against the frozen indexed records, not freshly read histories. Windows execution remains the user-run gate.

Sanitized metrics: [logical-dedup-summary.json](logical-dedup-summary.json). Reproducible harness: [evaluate_session_copies.rs](../examples/evaluate_session_copies.rs). Full responses remain in the ignored private validation cache.

## Regression coverage

Public-tool tests cover copy grouping before top-k, complete-content conflicts, missing identity, provider/session/owner/date filters, repeated chunks, conservative tool handling, copy pagination and cursor expiry, invalidated sources, pressure fallback, and per-occurrence context. An actual stdio MCP test discovers replicated Codex JSONL messages and retrieves each copy's context.

Fielded session records use physical line/byte bounds to separate adjacent repeated IDs in context. Legacy fieldless records retain contiguous-ID context grouping for compatibility; they are not eligible for cross-file message deduplication.

Final validation: 81 tests passed across all targets; package formatting, Clippy with `-D warnings -D unsafe-code`, and the locked release build passed. Release binary SHA-256: `c514f0b895dad3b0c4b9408ac690dba43f6a0d20c0908f15850488e39098b365`. See `logical-dedup-tests.log`, `logical-dedup-clippy.log`, and `logical-dedup-release.log`.
