# Changelog

## Unreleased

- Consolidate collection reconciliation, source-outcome publication, and read
  validation under one controller; retain committed source repairs when later
  work is cancelled and qualify reads across publication boundaries.
- Consolidate bounded candidate admission and require persisted evidence and
  snapshot-consistent statistics at the production ranking boundary, preserving
  retrieval policy, scores, and public schemas.
- Preserve collection-wide pending work, errors, and exclusions across precise
  source updates, including rejected sources without indexed rows; keep failed
  discovery, cancellation, and restart coverage incomplete until reconciled.
- Validate the final search returned by evaluation readiness waits and retry
  raced responses under the original deadline; reject non-ready later queries
  as successful measurements.
- Replay version 6 session checkpoints once to restore inspection diagnostics
  omitted by older checkpoints; retain incremental parsing after reconstruction.
- Expose path-free indexing progress through an MCP status resource, with
  separate prepared and committed chunk counts and phase/work measurements.
- Flush completed evaluation records immediately and replace ranked readiness
  polling with lightweight status reads and deadline-bounded requests.
- Batch temporary parser-state writes and reconcile precise session-file
  changes without reparsing unrelated histories, retaining conservative
  ownership verification and atomic chunk/checkpoint publication.

- Protect bounded meaningful-query admission when discarded stopwords saturate
  the raw candidate pool; preserve canonical terms, original evidence, and the
  existing six-additional-retrieval and 200-candidate limits.
- Reserve bounded exact-definition admission independently of BM25 mentions,
  with derived declaration metadata rebuilt automatically for older indexes.
- Score candidate bodies from persisted token evidence across chunk boundaries;
  preserve declaration camel-case components and complete qualified identities,
  and omit enhanced results supported only by discarded stopwords.
- Evaluate relevance at source grain with a fixed clock, repeated ordering/score
  checks, and larger latency samples; retain raw BM25 and MCP contracts.
- Add a reproducible raw-versus-ranked evaluation harness with feature
  ablations, retrieval metrics, latency percentiles, update timing, and
  WAL-aware database sizing.
- Route MCP search through bounded field-aware lexical reranking with exact
  symbol/path/diagnostic tiers, bounded query probes, session-only decay,
  structural deduplication, weighted lexical similarity, and MMR diversity;
  retain raw BM25 for comparison and compatibility.
- Upgrade lexical normalization to tokenizer v4 with compound, path, dotted,
  kebab, and qualified-name surface forms. Older derived indexes are detected
  and refreshed automatically; stale internal match IDs can expire while
  source transcripts remain unchanged.
- Document field weighting, exact-match tiers, lexical expansion, session
  decay, structural deduplication, weighted similarity, MMR, and deferred
  semantic retrieval.

- Keep context windows tied to physical session occurrences when event IDs repeat within a file.
- Group verified logical session-message replicas before search result limits; expose copy counts and paginated source copies while retaining every original context.

- Suppress exact duplicate visible session fields within an event while preserving distinct tool arguments and events; rebuild older session checkpoints.

- Compact search excerpts around query matches while preserving UTF-8 offsets, source references, and context expansion.

- Report ongoing indexing as successful `building` or `refreshing` responses even when coverage diagnostics exist.

- Add a source-built stdio MCP server with `search_project` and `search_sessions`.
- Add strict BM25 retrieval, bounded excerpts, session context pagination, and explicit freshness and coverage.
- Persist project and Codex, Claude Code, and Copilot CLI session indexes locally; share one owner across active clients.
- Reconcile edits, Git changes, ignore rules, and session appends automatically; reuse cached file content across branch changes.
- Keep worktree code separate while sharing verified repository session history.
- Add bounded streaming ingestion, disk-backed retrieval under memory pressure, crash recovery, and exact own-tool result exclusion.
- Forbid unsafe Rust in project targets and add automated correctness, performance, and Windows validation commands.

Actual Windows execution remains pending the user-run validation script.
