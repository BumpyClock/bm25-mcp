# Changelog

## Unreleased

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
