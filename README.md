# bm25-mcp

Local BM25 lexical search for project text and coding-agent session history over MCP.

## Build and launch

Install Rust 1.89 or newer and a C toolchain (SQLite is bundled), then run:

```sh
cargo build --release
./target/release/bm25-mcp serve --project /path/to/project
```

On Windows, use `target\release\bm25-mcp.exe`. Omit `--project` to discover the current Git worktree root, or use the current directory outside Git. Git must be available to associate worktrees with the same repository.

Example MCP client configuration:

```json
{
  "mcpServers": {
    "bm25-mcp": {
      "command": "/absolute/path/to/bm25-mcp/target/release/bm25-mcp",
      "args": ["serve", "--project", "/absolute/path/to/project"]
    }
  }
}
```

The executable uses stdio for MCP. A shared local owner process handles indexing while clients are attached. Its database and private connection metadata live in the operating system cache directory under `bm25-mcp`; `--cache-dir` selects another local location. Keep the cache outside indexed projects. The last client disconnect stops indexing; the owner exits after a short idle grace period. Broken connections are detected by heartbeat/read timeouts, and reconnecting clients trigger reconciliation of offline changes.

After replacing the executable, close all MCP clients attached to the same
repository, allow the owner to exit, and reconnect. Replacing the file does not
upgrade an already-running shared owner.

Project edits are coalesced and reconciled asynchronously. Changed or uncertain sources are suppressed until verified; unaffected verified results can remain available with partial-coverage status. Git and ignore-file changes trigger membership reconciliation. A bounded content cache reuses previously decoded and tokenized content when returning to a branch. Session imports yield when project edits need indexing.

## Tools

- `search_project`: required `query`; optional `path_glob`, `limit` (default 10, maximum 50), and `max_response_bytes` (default 16384, maximum 65536).
- `search_sessions`: search with `query`, optional `agent` (`codex`, `claude`, `copilot`), `session_id`, UTC RFC3339 `after`/`before`, and the same result limits. Use `mode: "context"` with a returned `match_id`, `before_events`/`after_events` (default 2, maximum 10), and an optional returned `cursor` for surrounding context. Search hits include `copy_count`. Use `mode: "copies"` with a `match_id`, optional `limit`, and returned `cursor` to page through equivalent source occurrences; each copy has a match ID usable in context mode. Copies cursors expire when the index generation changes, so restart that listing after an update.

Queries are plain lexical text, at most 8192 UTF-8 bytes. There are no Boolean,
regex or semantic/vector query operators. Identifier normalization retains
complete case-folded identifiers and camel/snake components. Natural-language
ranking suppresses common stopwords when useful and falls back to the literal
terms if reduction would empty the query; there is no stemming. Incidental
stopword matches without remaining query evidence are not returned by enhanced
search. When the query policy removes stopwords, retained meaningful terms get
a bounded admission opportunity even if discarded words saturate the raw BM25
pool. This is not an exhaustive-recall guarantee. Indexed body evidence is
preserved even when a token crosses a storage
chunk boundary.

Responses include generation, coverage, verification timestamps, and `building`, `ready`, `refreshing`, or `degraded` status. Ongoing indexing is a successful tool response with `building` or `refreshing` status, even when separate coverage diagnostics exist; `degraded` describes a completed reconciliation with coverage issues. Reconciliation can return partial or empty results; check coverage before treating absence as definitive. Date filtering uses inclusive `after` and exclusive `before`; undated session events do not satisfy date filters. Excerpt budgets count serialized UTF-8 bytes, not model tokens. Search excerpts are compact contiguous windows of at most 640 UTF-8 bytes, centered on a query match. Each hit reports `excerpt_byte_offset` within its full decoded indexed chunk and `excerpt_truncated`; the original chunk/event source bounds are preserved. Response-level `truncated` reports further trimming to satisfy the response budget. Session context expansion retains its existing pagination and larger text windows.

Project search excludes ignored files (including ignored tracked files), binary content, and symlinks. Text is streamed in bounded chunks, with no default source-size cutoff. UTF-8 and BOM-marked UTF-16 are supported. Unsupported encodings are reported.

## Indexing progress

Read the MCP resource `bm25://indexing/status` with `resources/read` to observe
indexing without running a ranked search. It returns JSON in `contents[0].text`
with separate `project` and `sessions` objects, each containing `status`,
`coverage`, and `progress`. The resource uses a separate owner connection and
does not acquire an index query permit. The two search tools are unchanged;
progress is never printed as ad hoc lines into MCP stdout.

Progress distinguishes discovered and completed files, processed records,
source bytes read, bytes hashed for verification, chunks prepared, and chunks
committed. It also reports the current phase, the current source's processed
bytes, and the last actual progress timestamp. Counters belong to a scan run;
repeated reads and retries count work again. Verification reads are included
in source bytes read, not additional unique bytes. Prepared chunks can be
discarded after cancellation or a failed safety check. Committed chunks advance
only after the index transaction succeeds; revalidating existing chunks is not
a new commit.

Poll roughly once per second. Bounded phase-transition history preserves short
phases between observations; polling does not advance the last actual progress
timestamp. Work metrics separate discovery, ownership, verification, JSON
processing, temporary-file operations, tokenization, scratch-state writes,
and durable transactions. Some timings are nested and must not be summed as
exclusive wall time. Operation counts distinguish scratch writes from committed
scratch transactions; scratch writes are batched in groups of at most 512.
Scratch lookup, begin, insert, and commit call timings are separate from the
time a batch transaction stays open between records.
`work.json_inspection_bytes` tracks actual parser input separately from source
reads, including progress within one large physical JSON record. The resource
omits source paths, transcript text, and raw error messages.

Unambiguous session-file events reconcile only the affected sources. Other
verified sources remain searchable during those updates. Directory, overflow,
identity-registry, topology, and uncertain events fall back to conservative
full reconciliation. Session appends still validate the required prefix and
ownership conditions before publishing complete new records. Chunks and their
matching parser checkpoint commit atomically; interrupted suffix work resumes
from the last durable checkpoint. Progress visibility does not permit
unverified partial data to become searchable.

### Collection coverage

Coverage describes current source outcomes, not the work in the latest progress
run. Each collection retains compact counts and diagnostic categories keyed by
the existing source identity, including rejected sources with no index rows.
Discovery failures and watcher failures are separate from source outcomes.

| Transition | Coverage rule |
| --- | --- |
| Successful full scan | Replace outcomes from the authoritative discovered set; remove absent outcomes only after successful discovery and durable removals. |
| Successful precise scan | Replace only reconciled source outcomes; preserve untouched sources and discovery failures. |
| Incomplete tail | Keep the source pending even when its complete prefix is searchable. |
| Parse/read failure or rejected ownership | Retain the source's errors/exclusions independently of searchable rows. |
| Confirmed deletion | Remove that source's outcome after its index removal succeeds. |
| Source repair | Replace its old outcome, clearing resolved errors and pending work without accumulating repeated observations. |
| Cancelled or failed scan | Preserve unresolved and unvisited outcomes; report unfinished work as unknown, never clean completion. |
| Failed discovery | Preserve unseen sources and discovery diagnostics until an authoritative full scan succeeds. |
| Watcher failure/recovery | Add/remove the watcher diagnostic independently; recovery does not erase source or discovery problems. |
| Owner restart | Require full reconciliation; an empty in-memory ledger is unknown coverage, not a clean collection. |

Outcomes are published no earlier than the corresponding durable source and
checkpoint operation (or verified exclusion/removal). Active or uncertain work
takes precedence over diagnostics. Completed coverage with errors and no pending
work is `degraded`; it remains a valid settled evaluation target.
`reconciled_at` is the time of the latest completed reconciliation operation,
which can be source-specific. It does not mean every source was freshly verified
at that time. Coverage is reconstructed after restart; there is no new ledger
storage or SQL schema migration. The existing session checkpoint format advances
from version 6 to 7 because older checkpoints omitted inspection-only diagnostic
counts. Older prefixes are replayed once, through the existing atomic checkpoint
replacement path; subsequent precise updates retain suffix-only parsing and
normal prefix verification.

## Development

```sh
cargo test
cargo clippy --all-targets -- -D warnings -D unsafe-code
```

The approved specification, contracts, evidence and implementation tickets are preserved in `planning.jsonl`. Historical feasibility probes in `validation/` are not product acceptance results. The root Cargo package is the standalone product build.

This project reuses attributed BM25 Turbo code under AGPL-3.0-only. See `LICENSE` and `THIRD_PARTY_NOTICES.md`.

## Session discovery and local diagnostics

Session discovery uses `CODEX_HOME` (default `~/.codex`), `CLAUDE_CONFIG_DIR` (default `~/.claude`), and `COPILOT_HOME` (default `~/.copilot`). It reads supported histories below those roots and uses recorded project directories to establish ownership. Source histories are never rewritten.

Identical visible message/result fields within one event are indexed once when they have the same semantic context. Distinct fields, tool arguments, and separate events remain searchable. Existing session checkpoints are reprocessed when this normalization changes. At search time, verified replicas of a logical session message share one representative occurrence before the result limit is applied. Grouping requires matching provider, session/event identifiers, and the complete ordered normalized content, role, tool, and field metadata of the physical event. Rollout timestamps can differ between copies: date filters apply before grouping, and the selected hit retains its own timestamp. `copy_count` counts matching occurrences in the search scope; copies mode lists all currently eligible equivalents with their individual timestamps, including occurrences outside the original date filter. Tool records remain separate because older indexes can use a call ID as a fallback event ID; grouping does not guess identity from ID spelling. Missing IDs, conflicting content, and distinct events remain separate; repeated chunks within the representative event are preserved. All original indexed occurrences remain available through copies/context modes. Raw retrieval and copies/context retain physical occurrences; ranked retrieval may additionally collapse structurally duplicate content for diversity while preserving those physical occurrences for copies/context. This grouping works with existing indexes and does not rewrite session files or change the underlying BM25 corpus statistics.

Session offsets, normalized chunks, deduplication state, and tool-call correlation state commit together. Appends validate the existing byte prefix and parse new complete records; incomplete tails remain pending. Replacements and truncations trigger reconciliation. Huge JSON strings and source text are spooled in bounded pieces. Unknown or malformed provider variants appear in categorized coverage diagnostics.

Verified worktree associations are persisted in a shared identity registry. Sessions from known removed worktrees can retain repository ownership; an unknown deleted directory is not guessed. More-specific registered plain-folder projects take precedence over their parents.

The default own-result exclusions recognize provider tool identities using the MCP server name `bm25-mcp` or `bm25_mcp`. If your client uses another server name, set `BM25_MCP_OWN_TOOL_NAMES` to a JSON array of its exact mapped tool identities. Calls remain searchable; these tools' returned result bodies are excluded. Bare tool-name collisions are not automatically excluded.

`coverage.memory` reports sampled owner RSS, observed peak RSS, the soft target, and pressure. Set `BM25_MCP_MEMORY_MIB` before starting the first client to change the default 512 MiB target. Search uses numeric raw postings, SQLite read snapshots, and a bounded shared posting cache. Broad queries fall back to disk-backed accumulation. Under memory pressure the owner evicts the posting cache, reduces query concurrency, and slows background work. This is a soft target, not an enforced process limit.

## Ranking evaluation

Search keeps the raw `Store::search` BM25 API as its oracle and applies the
bounded field-aware ranker through `Store::search_ranked`. Ranking uses
deterministic query classes, exact symbol/path/diagnostic tiers, bounded
lexical expansion, phrase proximity, session-only time decay, structural
deduplication, weighted Jaccard, and MMR. A separately indexed, bounded
declaration lane prevents ordinary mentions from consuming all exact-definition
admission capacity. Declaration recognition remains heuristic, not a language
parser, and the final reranking pool remains capped at 200 chunks.
Field weights and approximation
limits are documented in [docs/ranking-implementation.md](docs/ranking-implementation.md).
Embeddings and model-based rerankers are intentionally deferred.

Run `cargo run --release --quiet --example evaluate_ranking > validation/ranking-latest.json`
to compare raw, enhanced, and leave-one-feature-out variants on the synthetic
fixture. The report includes Recall@10/20, MRR, nDCG@10, duplicate rate, p50/p95
latency, candidate/probe counts, indexing/update latency, and SQLite database
bytes including WAL/SHM sidecars. Relevance metrics use deduplicated source
rankings; duplicate diagnostics retain the uncollapsed chunk results. Evaluation
uses a fixed timestamp through `Store::search_ranked_with_at`, while normal
search continues to use the current time. Fixture numbers are regression
evidence and do not represent production-wide retrieval quality.

For a read-only source-tree smoke check, run
`cargo run --quiet --example evaluate_real_ranking -- /path/to/tree`; it
reports representative ranked paths and snippets from a disposable index.

## Validation

Run `cargo fmt --package bm25-mcp -- --check`, `cargo test --locked --all-targets`, and `cargo clippy --locked --all-targets -- -D warnings -D unsafe-code`. Project targets forbid unsafe Rust. Third-party dependencies and the unchanged vendored oracle retain their own implementations. Format only the root package; the vendored upstream source is preserved unchanged.

The oracle tests compare both cached and disk-backed rankings with a fresh upstream `BM25Builder` index after updates and compaction. Recovery tests exercise interrupted transactions, stale endpoints, concurrent client startup, owner replacement, offline edits, and worktree isolation.

`scripts/acceptance.py --project /path/to/project` measures 1,000 requests with one and four clients, including project queries, provider-filtered session queries, and context when matches exist. Provider homes are isolated by default. Add `--snapshot-sessions` to explicitly copy actual histories into private temporary homes for repeatable warm measurements; the copies are removed afterward. `scripts/exercise-updates.py` creates a disposable Git repository for ordinary edits, branch transitions, and huge-text checks. `scripts/check-pressure.py` compares normal and forced-pressure owners while four clients read during an edit. These scripts do not change the selected real project or export its indexed contents. Optional `--cache-dir` on the acceptance script retains the derived index between runs.

Evaluation readiness waits use the status resource rather than repeated ranked
searches. Each RPC has a wall-time deadline, including blocked protocol I/O;
a timed-out connection is closed rather than reused. A readiness wait performs
the requested search after a qualifying status observation and validates that
search response's own status and coverage. If a change races the observation,
it resumes bounded status polling under the same original deadline. Empty ready
results and completed degraded coverage with zero pending work are valid;
positive or unknown pending work is not settled. Caller predicates that need
progress fields apply to the status observation. Later evaluation queries are
checked independently; an earlier settled response is not permanent readiness
certification. Separate ingestion probes
measure search latency and first searchable results without conflating them
with readiness observation.

Sibling evaluation writes and flushes each completed progress, coverage, query,
and project record independently. A slow or failed later worktree therefore does
not hide earlier results from its repository family. Public JSONL uses opaque
labels and aggregate fields; raw queries, source paths, transcripts, and
responses remain in the private evaluation workspace.

Run bounded synthetic session checks without reading personal histories:

```sh
python3 scripts/exercise-updates.py --binary target/release/bm25-mcp \
  --huge-session-mib 8 --synthetic-session-records 1000 --synthetic-session-mib 4
python3 -m unittest discover -s scripts -p 'test_*.py'
```

The ingestion probe measures time to first observed progress, first marker
returned by a search, total reconciliation, and p50/p95 search latency. It
reports source bytes read and verification bytes alongside bytes changed;
these are distinct from unique input size. Missing observations remain null.
Synthetic results do not establish performance for a real history collection.

On Windows, run `./scripts/validate-windows.ps1`; pass `-Project C:\path\to\project` to include real-corpus and update measurements using Python's `py -3` launcher. Actual Windows execution remains a user-owned acceptance gate; macOS results do not establish Windows behavior.

Measured v1 results and coverage limitations are in [validation/ACCEPTANCE.md](validation/ACCEPTANCE.md). The paired 21-root retrieval evaluation, including ranking tradeoffs, is in [validation/SIBLING-QUALITY.md](validation/SIBLING-QUALITY.md). The cross-file session deduplication follow-up is in [validation/LOGICAL-EVENT-DEDUP.md](validation/LOGICAL-EVENT-DEDUP.md). Implementation and acceptance progress is recorded in `planning.jsonl`. Files in `validation/` distinguish historical probes, intermediate checks, and product acceptance results.
