# Ranking implementation

## Existing architecture

`main`/rmcp -> local owner (`runtime`) -> `tools::dispatch` ->
`Store::search_ranked` -> filtered lexical and exact-admission lanes ->
bounded field-aware scoring and selection -> compact excerpts -> MCP response.
`Store::search` retains the raw BM25 scorer over cached numeric postings or a
SQLite temporary accumulator.
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
No grep/ripgrep path is changed, and no network/model work is added.
Tests are authorized by the brief at tokenizer, ranking, store/update and MCP seams.

## Ranking stages

All reranking operates on at most 200 candidates within the retrieval snapshot.
The raw API remains available for paired baselines; MCP uses enhanced ranking.

| Enhancement | Existing support / smallest seam | Persistence/backfill | Rerank only / cost |
| --- | --- | --- | --- |
| Tokenization | v4 shared Camel/Pascal/acronym/snake, Unicode, path, dotted, kebab and qualified surface forms | Existing v3 indexes are invalidated and rebuilt; no new table or public schema | Rerank fields; linear in bounded text |
| BM25F | Hydrate persisted body TF and length; extract identity/signature/comment fields from candidate text; combine normalized field TF before saturation | Existing postings remain authoritative | Yes; heuristic fields/length priors, not corpus-normalized textbook BM25F |
| Classifier | None; deterministic query plan before retrieval | None | Cheap query pass |
| Exact classes | Classify symbols/path/quoted diagnostics; admit exact definitions through derived symbol metadata and paths through `sources.path` | Derived declaration index with transactional lifecycle and backfill | Independent bounded admission plus rerank |
| Expansion | None; original retrieval first, at most six reduced/decomposed/morphology/alias probes when thin/weak | None | Existing retrieval reused; bounded probes, provenance retained |
| Proximity | Excerpt phrase matching only; shared token windows over candidates | None | Yes; bounded 15% bonus |
| Decay | Event timestamps already stored; session-only floor/half-life/exact exemption | None | Yes; O(N) |
| Dedupe | Logical event replicas already grouped; add content and overlapping-range collapse without altering copies counts | None | Yes; bounded comparisons |
| Weighted Jaccard | Existing term_stats available; cache salient weighted tokens once per candidate | None | Yes; bounded IDF reads and O(K*N) comparisons |
| Structural similarity | Source/ranges and event identity available | None | Yes; O(K*N) |
| MMR | None; relevance-normalized, exact-tier-aware final selection, separate selection score | None | Yes; O(K*N), cached maximum similarity |
| Diagnostics/evaluation | Existing tests, upstream oracle and historical quality scripts | None | Local opt-in score traces, fixed fixtures, ablations and timing |

The declaration index is additive derived metadata. It does not add body
postings, change raw BM25 corpus statistics, or regenerate match IDs.
Tokenizer-version invalidation remains a separate operation that triggers
refresh/rebuild and may regenerate internal match IDs. Exact path handling
continues to use the existing `sources.path` authority.
Project and session tools remain separate. Additional lexical retrieval is
bounded to six calls, including at most one meaningful-query retrieval.
Optional expansion retains derived/morphology/alias weights 0.8/0.6/0.4 and
contribution caps; field
extraction remains heuristic with fixed metadata priors. Candidate retrieval
is lexical OR retrieval supplemented by exact-definition and path admission.
Reranking still cannot recover a document absent from all bounded admission
lanes. SPLADE, dense embeddings, sqlite-vec/vector KNN, neural
rerankers, HyDE, and remote inference remain deferred; a future bounded
reranker can consume the same candidate and diagnostic seam.

## Ranking contract and evaluation

`Store::search` remains the raw BM25 oracle. MCP searches use
`Store::search_ranked`, which combines independent admission lanes into a final
pool of at most 200 candidates. `search_ranked_with` exposes `RankingOptions`
for controlled ablations; `search_ranked_with_at` additionally accepts a fixed
clock for tests and evaluation. Existing callers use the current time.
Project field weights are symbol 7, signature 4,
basename 5, path 4, documentation 2, comment 1.5, and body 1. Session field
weights are user request 3, assistant prose 1.5, command 4, tool output 0.5,
diagnostic 5, and mentioned identity 5. These are BM25F-style priors with
fixed metadata length normalization, not independently indexed field stats.

### Admission and persistence

The private admission owner keeps one candidate record per scoped match ID,
including all overlapping lane evidence and an optional original raw-query
score. Only that owner creates the final bounded pool. Hydration turns the pool
into a scoring input containing persisted body evidence, statistics, and its
query plan in the same retrieval transaction. The production ranker accepts
that input; direct policy-test helpers use a separate opaque type under
`ranking::testing`. This consolidates ownership without changing lane policy,
budgets, weights, relevance, or MMR selection.

The lexical lane retrieves at most 200 candidates. For project identifier
queries, the declaration lane selects at most 40 chunks by complete normalized
symbol, ordered by match ID within the filtered snapshot. These chunks reserve
their places before ordinary lexical candidates fill the final 200-slot pool.
When more than 40 actual definitions qualify, this lane provides deterministic
bounded recall, not exhaustive definition recall. Additional definitions may
still enter through lexical retrieval. Match-ID ordering is stable within an
index snapshot, not a promise of the same selection after source replacement.

Path admission uses the existing source-path authority. Weak or thin lexical
pools may additionally reserve up to 40 expansion candidates. Lane diagnostics
count distinct retrieved chunks before pool truncation; a chunk can belong to
more than one lane, so counts need not sum to the final pool size. Collection,
eligibility, path, agent, session, and time filters apply before each lane's
limit.

When the enhanced query policy removes stopwords, retained meaningful terms
receive a bounded candidate-admission opportunity even if raw BM25 is saturated
by candidates supported only by those discarded words. This lane retrieves
at most 40 candidates, ordered by retained-term BM25 and match ID, and protects
their slots before raw-score-first filling. It uses canonical `QueryPlan.original`
terms directly, preserving compound and long-token digest identities. Merely
changing whitespace, order, or multiplicity does not trigger this lane.
All-stopword fallback and non-reduced query classes keep their existing policy.

Admission reserves definitions first, then meaningful-query results, then
optional expansion results; match-ID overlap consumes only one slot. Each
reservation is at most 40 and unused capacity remains available to the other
candidates. Meaningful reduction currently applies only to natural queries,
so it cannot compete with identifier-definition or exact-path queries. The
existing path priority is unchanged. The final pool remains at most 200.

The meaningful search runs independently of the weak-pool heuristic and the
expansion ablation. It replaces the reduced-query probe for these plans and
uses one of the existing six additional-retrieval slots, leaving at most five
optional probes. There is no refill loop. The weak-score threshold is unchanged.
The returned `probes` list records optional probes; `meaningful_retrievals` and
`additional_retrievals` expose executed search counts, including empty searches.
`admission_counts.meaningful` counts retrieved candidates, and trace admission
evidence retains overlapping lane membership and separate retrieval scores.

Retained terms remain original evidence, not capped synonym evidence.
`baseline_bm25` is the original raw-query score, including its token
multiplicity. For supplemental candidates absent from raw top K, the store
hydrates that score in bounded batches using the existing scoring kernel and
persisted postings in the same snapshot. It neither substitutes a reduced-query
score nor infers zero from absence. These score-only reads cannot admit more
candidates. Public relevance and internal MMR selection scores remain distinct.

`declarations(chunk_id, symbol)` and its symbol lookup index contain only
derived project metadata. Chunk insertion and replacement populate this
metadata in the source transaction; deletion cascades with the chunk.
Invalidation hides it through source eligibility, and verification makes the
same current rows searchable again. Restoring tokenized content through the
content cache uses the same insertion path.

Opening an index without the current declaration metadata version performs a
transactional backfill over persisted project chunks in bounded batches.
This one-time startup operation does not scan source files, rewrite body
postings, change corpus statistics, or expire match IDs. Normal query execution
does not scan source contents to find definitions. Body evidence hydration
reads only the final bounded candidate set's persisted frequencies and lengths,
in batches in the same read transaction.

Exact symbol, qualified symbol, path, basename, diagnostic, and historical
identity matches receive deterministic tiers. Proximity contributes at most
15% of lexical relevance. Session timestamps use a 45-day half-life and 0.25
floor; code never decays, and classified exact symbol/path/diagnostic/
historical-identity matches are exempt; ordinary full-query literal matches
use square-root decay. Structural duplicate collapse precedes MMR. MMR uses weighted Jaccard
over at most 128 salient terms plus source/range identity, lambda 0.85, and the
bounded candidate pool. Thin pools are below 20 candidates; weak pools use a
0.25 strongest-score threshold; optional expansion reserves 40 candidates. Expansion
contributions are capped at 35% of original evidence, or an absolute 0.5 score
for expansion-only hits. Final MCP limits remain 10 by default and 50 maximum.

Declaration extraction retains original spelling for shared-tokenizer field
frequencies and separately case-folded complete identities for exact matching.
This preserves camel/acronym components without treating partial names as
exact declarations. Complete dotted and double-colon-qualified historical
identities receive the same exact-match decay exemption; components and
prefixes do not.

Body scoring uses authoritative persisted term frequencies and token lengths
hydrated in the retrieval snapshot, including tokens that complete in the next
storage chunk. Fragment-local tokenization remains useful for heuristic
metadata and proximity, but does not replace body evidence. Before selection,
candidates must have meaningful original-term evidence, bounded expansion
evidence, or a valid exact match. All-stopword queries keep the existing
literal fallback. Raw BM25 retrieval retains its multiplicity and stopword
semantics.

Declaration extraction is heuristic: it does not parse ancestry, infer a
conclusion field, or replace a language parser. Path lookup can scan existing
source paths when a path-class query needs it. Candidate reranking cannot
recover a miss outside the bounded pool. Features whose ablation
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
not a production quality claim. Relevance metrics collapse returned chunks
to distinct sources before applying rank cutoffs, matching the source-level
judgments and ideal ranking. Duplicate diagnostics use the uncollapsed hits.
The evaluation timestamp is fixed and recorded in report metadata.
The checked-in historical baseline is tokenizer-v3 raw
retrieval; current output is tokenizer-v4 and should be compared within the
same fixture and revision. The historical baseline omits WAL/SHM sidecars, so
there is no valid old total-size comparison. Exact matching is lexical and heuristic; dense
retrieval, embeddings, remote services, and neural reranking remain deferred.

## Verification

The correctness review baseline is
`efa7f606f2403d2f99d1683ccc16c6c02c3f04d3`. It had a clean working tree and
106 passing tests before the correctness changes. Regression tests cover
admission saturation, persisted boundary evidence, declaration spelling,
qualified identities, meaningful evidence, metadata lifecycle, evaluation
grain, and the unchanged MCP/raw-search contracts. Commands, measurements,
and limitations are recorded in [../validation/RANKING.md](../validation/RANKING.md).
Synthetic evidence does not establish production-wide quality.
