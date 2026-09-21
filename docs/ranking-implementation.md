# Implementation map (recorded before ranking edits)

## Existing architecture

`main`/rmcp -> local owner (`runtime`) -> `tools::dispatch` -> `Store::search` ->
Unicode/camel/snake tokenizer -> cached numeric postings or SQLite temporary
accumulator -> filtered BM25 top K -> compact excerpts -> unchanged MCP response.
There are separate project and session tools, not a combined public endpoint.
Project collections are worktree-specific; session collections use the verified
repository common-directory identity. Filters and eligibility precede top K.
SQLite owns chunks, postings, corpus statistics, source verification, and session
checkpoints. No FTS, parser, vector index, or grep wrapper exists.

Scores are positive, higher-is-better Lucene BM25: IDF = log(1 +
(N-df+0.5)/(df+0.5)), TF saturation = tf/(tf+1.5*(0.25+0.75*len/avglen)).
Query token multiplicity is retained. Retrieval uses OR across lexical terms;
“strict” means original-query retrieval, not Boolean AND. Scores have no fixed
upper bound. The fresh upstream oracle protects this raw scoring API.

Project scan -> ignore/encoding checks -> streaming 60-line/16 KiB chunks ->
shared streaming tokenizer -> atomic source/posting/stat replacement. Source
hash/tokenizer-version content cache supports branch changes. Invalidations hide
uncertain rows. Deletion cascades remove stale postings and adjust statistics.
Sessions -> provider normalization/ownership -> individual fields/events split at
16 KiB -> the same tokenizer -> atomic chunks/checkpoint/tool-correlation commit.
Event timestamps, roles, field kinds, commands, session/event IDs and source
ranges already exist. Session event replicas are grouped before baseline top K;
context/copies retain physical records. Git history has one implementation commit.
No AGENTS.md or CLAUDE.md was found in this repository or its ancestor chain.

## Contracts preserved

MCP names, arguments, response shapes, limits, coverage and source references;
match IDs and copies/context semantics; SQLite ownership and transactions;
verified workspace identity and filters; raw Store::search BM25/oracle behavior;
streaming ingestion and tokenizer v4 postings; existing source reconciliation.
Opening an older index detects the tokenizer-version mismatch, invalidates
derived rows, and starts automatic refresh; coverage may be building or
partial while refresh runs. Old internal IDs can expire, while source
transcripts remain unchanged.
No grep/ripgrep path is changed, no network/model work is added. Existing dirty
planning.jsonl and untracked prior evaluation files are outside this change.
Tests are authorized by the brief at tokenizer, ranking, store/update and MCP seams.

## Delta plan

All reranking operates on at most 200 candidates within the retrieval snapshot.
The raw API remains available for paired baselines; MCP uses enhanced ranking.

| Enhancement | Existing support / smallest seam | Persistence/backfill | Rerank only / cost |
| --- | --- | --- | --- |
| Tokenization | v4 shared Camel/Pascal/acronym/snake, Unicode, path, dotted, kebab and qualified surface forms | Existing v3 indexes are invalidated and rebuilt; no new table or public schema | Rerank fields; linear in bounded text |
| BM25F | Body BM25 only; extract identity/signature/comment fields from candidates, reuse corpus IDF and body average length, combine normalized field TF before saturation | None | Yes; heuristic fields/length priors, not corpus-normalized textbook BM25F |
| Classifier | None; deterministic query plan before retrieval | None | Cheap query pass |
| Exact classes | Classify symbols/path/quoted diagnostics in candidates; fetch path candidates through existing `sources.path` authority | No new path index or DDL | Path retrieval plus rerank; bounded output |
| Expansion | None; original retrieval first, at most six reduced/decomposed/morphology/alias probes when thin/weak | None | Existing retrieval reused; bounded probes, provenance retained |
| Proximity | Excerpt phrase matching only; shared token windows over candidates | None | Yes; bounded 15% bonus |
| Decay | Event timestamps already stored; session-only floor/half-life/exact exemption | None | Yes; O(N) |
| Dedupe | Logical event replicas already grouped; add content and overlapping-range collapse without altering copies counts | None | Yes; bounded comparisons |
| Weighted Jaccard | Existing term_stats available; cache salient weighted tokens once per candidate | None | Yes; bounded IDF reads and O(K*N) comparisons |
| Structural similarity | Source/ranges and event identity available | None | Yes; O(K*N) |
| MMR | None; relevance-normalized, exact-tier-aware final selection, separate selection score | None | Yes; O(K*N), cached maximum similarity |
| Diagnostics/evaluation | Existing tests, upstream oracle and historical quality scripts | None | Local opt-in score traces, fixed fixtures, ablations and timing |

The implemented revision keeps
the existing schema and public IDs, but tokenizer v4 invalidation triggers a
refresh/rebuild and may regenerate internal match IDs. No new path index or
DDL is added: exact path handling uses the existing `sources.path` authority.
Project and session tools remain separate. Expansion is bounded to six probes
with derived/morphology/alias weights 0.8/0.6/0.4 and contribution caps; field
extraction remains heuristic with fixed metadata priors. Candidate retrieval
is lexical OR retrieval, so reranking cannot recover a document absent from
the bounded pool. SPLADE, dense embeddings, sqlite-vec/vector KNN, neural
rerankers, HyDE, and remote inference remain deferred; a future bounded
reranker can consume the same candidate and diagnostic seam.

## Ranking contract and evaluation

`Store::search` remains the raw BM25 oracle. MCP searches use
`Store::search_ranked`, which retrieves at most 200 lexical candidates and
then applies the bounded ranker. `search_ranked_with` exposes `RankingOptions`
for controlled ablations. Project field weights are symbol 7, signature 4,
basename 5, path 4, documentation 2, comment 1.5, and body 1. Session field
weights are user request 3, assistant prose 1.5, command 4, tool output 0.5,
diagnostic 5, and mentioned identity 5. These are BM25F-style priors with
fixed metadata length normalization, not independently indexed field stats.

Exact symbol, qualified symbol, path, basename, diagnostic, and historical
identity matches receive deterministic tiers. Proximity contributes at most
15% of lexical relevance. Session timestamps use a 45-day half-life and 0.25
floor; code never decays, and classified exact symbol/path/diagnostic/
historical-identity matches are exempt; ordinary full-query literal matches
use square-root decay. Structural duplicate collapse precedes MMR. MMR uses weighted Jaccard
over at most 128 salient terms plus source/range identity, lambda 0.85, and the
bounded candidate pool. Thin pools are below 20 candidates; weak pools use a
0.25 strongest-score threshold; expansion reserves 40 candidates. Expansion
contributions are capped at 35% of original evidence, or an absolute 0.5 score
for expansion-only hits. Final MCP limits remain 10 by default and 50 maximum.

Declaration extraction is heuristic: it does not parse ancestry, infer a
conclusion field, or replace a language parser. Path lookup can scan existing
source paths when a path-class query needs it. Candidate reranking cannot
recover a lexical miss outside the bounded pool. Features whose ablation
aggregate is unchanged on this fixture are implementation coverage evidence,
not measured production gains.

Run the reproducible harness with:

```text
cargo run --release --quiet --example evaluate_ranking > validation/ranking-latest.json
```

It reports raw versus enhanced results and leave-one-feature-out ablations,
Recall@10/20, MRR, nDCG@10, duplicate rate, candidate/probe counts, p50/p95
latency, initial indexing and update latency, and database bytes including
SQLite `-wal`/`-shm` sidecars. The synthetic fixture is regression evidence,
not a production quality claim. The checked-in baseline is tokenizer-v3 raw
retrieval; current output is tokenizer-v4 and should be compared within the
same fixture and revision. The historical baseline omits WAL/SHM sidecars, so
there is no valid old total-size comparison. Exact matching is lexical and heuristic; dense
retrieval, embeddings, remote services, and neural reranking remain deferred.

## Baseline and verification

Run the unchanged full suite before edits (`/tmp/bm25-ranking-baseline-tests.log`).
Add reproducible fixture evaluation before connecting enhanced ranking; preserve
raw results and metrics. Validate token surface forms, exact-class ordering,
expansion provenance/bounds, event-time decay, similarity/dedupe/MMR, isolation,
update/delete/rename and unchanged MCP context/copies with focused tests and the
full prescribed fmt/test/clippy gates. Benchmark repeated raw/enhanced queries on
the same fixture index, plus read-only project queries. Synthetic evidence does
not establish production-wide quality. No combined public tool or parser rewrite
is justified by this ranking task.
