//! Durable, transactional BM25 storage.
//!
//! SQLite is the authority for source membership, immutable chunk text,
//! numeric terms, raw postings, and corpus statistics.  Search creates a
//! temporary disk-backed accumulator on the same read snapshot; it never
//! materializes a complete in-memory index.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use anyhow::{Context, Result, anyhow, bail, ensure};
use globset::{Glob, GlobSet, GlobSetBuilder};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use sha2::{Digest, Sha256};

use crate::model::{Chunk, Hit, SearchFilter, Source};
use crate::text::{normalization_version, tokenize_checked};
use crate::{query, ranking};

#[path = "hot.rs"]
mod hot;
#[path = "upstream/mod.rs"]
mod upstream;

#[path = "admission.rs"]
mod admission;
use admission::{Admission, Lane};
pub(crate) use admission::{IndexedPool, ScorablePool};

const SCHEMA_VERSION: &str = "bm25-mcp-store-v2";
const MAX_CONTEXT_ROWS: usize = 4096;
const DECLARATION_INDEX_VERSION: &str = "bm25-mcp-declarations-v1";
// Exact declarations are admitted independently, with match_id order making
// the bounded overflow choice reproducible.
const DEFINITION_RESERVE: usize = 40;
const BODY_EVIDENCE_BATCH: usize = 64;

#[derive(Default)]
struct QueryGate {
    state: Mutex<(usize, bool)>,
    changed: Condvar,
}
impl QueryGate {
    fn enter(&self) -> Result<QueryPermit<'_>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("query gate poisoned"))?;
        while state.0 >= if state.1 { 1 } else { 4 } {
            state = self
                .changed
                .wait(state)
                .map_err(|_| anyhow!("query gate poisoned"))?;
        }
        state.0 += 1;
        Ok(QueryPermit(self))
    }
}
struct QueryPermit<'a>(&'a QueryGate);
impl Drop for QueryPermit<'_> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.state.lock() {
            state.0 -= 1;
            self.0.changed.notify_all();
        }
    }
}

/// Parser state committed with the corresponding normalized session chunks.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SessionCheckpoint {
    pub offset: u64,
    pub state: String,
}

pub type SessionStateUpdate = (String, String, Option<String>);

pub struct SessionStateReader {
    connection: Connection,
}
impl SessionStateReader {
    pub fn get(&self, source: &str, kind: &str, key: &str) -> Result<Option<String>> {
        Ok(self
            .connection
            .query_row(
                "SELECT value FROM session_state WHERE source_key=?1 AND kind=?2 AND key=?3",
                params![source, kind, key],
                |r| r.get(0),
            )
            .optional()?)
    }
}

/// Evidence issued only after observing the corresponding committed source state.
#[derive(Debug)]
pub(crate) struct SourcePublication {
    key: String,
    version: Option<String>,
}

impl SourcePublication {
    pub(crate) fn key(&self) -> &str {
        &self.key
    }
    pub(crate) fn outcome(
        &self,
        mut outcome: crate::coverage::SourceOutcome,
    ) -> (String, crate::coverage::SourceOutcome) {
        outcome.version = self.version.clone();
        (self.key.clone(), outcome)
    }
}

/// A cloneable handle to one path-backed SQLite database.
#[derive(Clone)]
pub struct Store {
    db: Arc<Mutex<Connection>>,
    path: Arc<PathBuf>,
    queries: Arc<QueryGate>,
    hot: Arc<Mutex<hot::Cache>>,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store").field("path", &self.path).finish()
    }
}

impl Store {
    /// Open or create a durable store at `path`.
    pub fn open(path: &Path) -> Result<Self> {
        if rusqlite::version_number() < 3_051_003 {
            bail!("SQLite 3.51.3 or newer is required for the WAL-reset fix");
        }

        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create store directory {}", parent.display()))?;
        }

        let mut conn = Connection::open(path)
            .with_context(|| format!("open SQLite store {}", path.display()))?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        // Query accumulators are derived state.  Keeping SQLite's temporary
        // tables on disk bounds process memory when a common term has many
        // postings.
        conn.pragma_update(None, "temp_store", "FILE")?;
        conn.pragma_update(None, "cache_size", -2048)?;
        conn.pragma_update(None, "mmap_size", 0)?;
        initialize(&mut conn)?;

        Ok(Self {
            db: Arc::new(Mutex::new(conn)),
            path: Arc::new(path.to_path_buf()),
            queries: Arc::new(QueryGate::default()),
            hot: Arc::new(Mutex::new(hot::Cache::default())),
        })
    }

    /// Reduce active readers under measured owner memory pressure.
    pub fn set_memory_pressure(&self, pressure: bool) {
        self.hot.lock().unwrap().pressure(pressure);
        if let Ok(mut state) = self.queries.state.lock() {
            state.1 = pressure;
            self.queries.changed.notify_all();
        }
    }

    /// Replace all chunks for one source in one transaction.
    ///
    /// An iterator error is returned to the caller and rolls back the source,
    /// term rows, postings, statistics, and generation together.
    pub fn replace_source<I>(&self, source: &Source, chunks: I) -> Result<()>
    where
        I: IntoIterator<Item = Result<Chunk>>,
    {
        self.write_source(source, chunks, None, None, std::iter::empty())
    }

    pub fn session_checkpoint(&self, key: &str) -> Result<Option<SessionCheckpoint>> {
        let conn = self.read_connection()?;
        Ok(conn
            .query_row(
                "SELECT offset,state FROM session_checkpoints WHERE source_key=?1",
                [key],
                |r| {
                    Ok(SessionCheckpoint {
                        offset: r.get::<_, i64>(0)? as u64,
                        state: r.get(1)?,
                    })
                },
            )
            .optional()?)
    }

    pub fn replace_session<I>(
        &self,
        source: &Source,
        chunks: I,
        checkpoint: &SessionCheckpoint,
    ) -> Result<()>
    where
        I: IntoIterator<Item = Result<Chunk>>,
    {
        self.write_source(source, chunks, None, Some(checkpoint), std::iter::empty())
    }

    pub fn append_session<I>(
        &self,
        source: &Source,
        expected_version: &str,
        chunks: I,
        checkpoint: &SessionCheckpoint,
    ) -> Result<()>
    where
        I: IntoIterator<Item = Result<Chunk>>,
    {
        self.write_source(
            source,
            chunks,
            Some(expected_version),
            Some(checkpoint),
            std::iter::empty(),
        )
    }

    pub fn session_state_reader(&self) -> Result<SessionStateReader> {
        Ok(SessionStateReader {
            connection: self.read_connection()?,
        })
    }

    pub fn replace_session_with_state<I, J>(
        &self,
        source: &Source,
        chunks: I,
        checkpoint: &SessionCheckpoint,
        updates: J,
    ) -> Result<()>
    where
        I: IntoIterator<Item = Result<Chunk>>,
        J: IntoIterator<Item = Result<SessionStateUpdate>>,
    {
        self.write_source(source, chunks, None, Some(checkpoint), updates)
    }

    pub fn append_session_with_state<I, J>(
        &self,
        source: &Source,
        expected_version: &str,
        chunks: I,
        checkpoint: &SessionCheckpoint,
        updates: J,
    ) -> Result<()>
    where
        I: IntoIterator<Item = Result<Chunk>>,
        J: IntoIterator<Item = Result<SessionStateUpdate>>,
    {
        self.write_source(
            source,
            chunks,
            Some(expected_version),
            Some(checkpoint),
            updates,
        )
    }

    fn write_source<I, J>(
        &self,
        source: &Source,
        chunks: I,
        append_version: Option<&str>,
        checkpoint: Option<&SessionCheckpoint>,
        updates: J,
    ) -> Result<()>
    where
        I: IntoIterator<Item = Result<Chunk>>,
        J: IntoIterator<Item = Result<SessionStateUpdate>>,
    {
        validate_source(source)?;
        let mut conn = self.lock()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let next_generation = current_generation(&tx)?
            .checked_add(1)
            .ok_or_else(|| anyhow!("generation exhausted"))?;
        let old = source_info(&tx, &source.key)?;
        if let Some(expected) = append_version {
            let Some(previous) = &old else {
                bail!("session append source missing");
            };
            if previous.version != expected
                || previous.collection != source.collection
                || previous.kind != source.kind
            {
                bail!("session append source changed");
            }
            if let Some(checkpoint) = checkpoint {
                let prior: i64 = tx.query_row(
                    "SELECT offset FROM session_checkpoints WHERE source_key=?1",
                    [&source.key],
                    |r| r.get(0),
                )?;
                if checked_i64(checkpoint.offset)? < prior {
                    bail!("session checkpoint regressed");
                }
            }
        }
        let append_eligible =
            append_version.is_some() && old.as_ref().is_some_and(|old| old.eligible);
        if let Some(old) = &old
            && old.eligible
            && !append_eligible
        {
            adjust_statistics_for_source(&tx, old, -1)?;
        }

        tx.execute(
            "INSERT INTO sources(key, collection, path, version, kind, eligible, verified)
             VALUES (?1, ?2, ?3, ?4, ?5, 1, 1)
             ON CONFLICT(key) DO UPDATE SET
                 collection=excluded.collection,
                 path=excluded.path,
                 version=excluded.version,
                 kind=excluded.kind,
                 eligible=1,
                 verified=1",
            params![
                source.key,
                source.collection,
                source.path,
                source.version,
                source.kind
            ],
        )?;
        tx.execute(
            "UPDATE sources SET verified_at=?1 WHERE key=?2",
            params![chrono::Utc::now().to_rfc3339(), source.key],
        )?;
        let first_ordinal = if append_version.is_some() {
            tx.query_row(
                "SELECT COALESCE(MAX(ordinal)+1,0) FROM chunks WHERE source_key=?1",
                [&source.key],
                |r| r.get::<_, i64>(0),
            )?
        } else {
            tx.execute("DELETE FROM chunks WHERE source_key=?1", [&source.key])?;
            tx.execute(
                "DELETE FROM session_state WHERE source_key=?1",
                [&source.key],
            )?;
            0
        };
        for (ordinal, item) in chunks.into_iter().enumerate() {
            let ordinal = first_ordinal
                .checked_add(i64::try_from(ordinal)?)
                .ok_or_else(|| anyhow!("too many chunks in source"))?;
            insert_chunk(&tx, source, &item?, ordinal, next_generation)?;
        }
        if let Some(checkpoint) = checkpoint {
            tx.execute(
                "INSERT INTO session_checkpoints(source_key,offset,state) VALUES (?1,?2,?3)
                ON CONFLICT(source_key) DO UPDATE SET offset=excluded.offset,state=excluded.state",
                params![
                    source.key,
                    checked_i64(checkpoint.offset)?,
                    checkpoint.state
                ],
            )?;
        } else {
            tx.execute(
                "DELETE FROM session_checkpoints WHERE source_key=?1",
                [&source.key],
            )?;
        }

        for update in updates {
            let (kind, key, value) = update?;
            if let Some(value) = value {
                tx.execute(
                    "INSERT INTO session_state(source_key,kind,key,value) VALUES (?1,?2,?3,?4)
                    ON CONFLICT(source_key,kind,key) DO UPDATE SET value=excluded.value",
                    params![source.key, kind, key, value],
                )?;
            } else {
                tx.execute(
                    "DELETE FROM session_state WHERE source_key=?1 AND kind=?2 AND key=?3",
                    params![source.key, kind, key],
                )?;
            }
        }
        let new_info = SourceInfo {
            key: source.key.clone(),
            collection: source.collection.clone(),
            kind: source.kind.clone(),
            version: source.version.clone(),
            eligible: true,
        };
        adjust_statistics_from_ordinal(
            &tx,
            &new_info,
            1,
            if append_eligible { first_ordinal } else { 0 },
        )?;
        set_generation(&tx, next_generation)?;
        tx.commit()?;
        Ok(())
    }

    /// Confirm the publication boundary, including verified no-change outcomes and
    /// quarantined failures/exclusions with no searchable rows. This does no file I/O.
    pub(crate) fn confirm_source_publication(
        &self,
        key: &str,
        version: Option<&str>,
    ) -> Result<SourcePublication> {
        let conn = self.lock()?;
        let current = source_info(&conn, key)?;
        match version {
            Some(version) => ensure!(
                current
                    .as_ref()
                    .is_some_and(|source| source.eligible && source.version == version),
                "source publication does not match verified version"
            ),
            None => ensure!(
                current.as_ref().is_none_or(|source| !source.eligible),
                "excluded or failed source remains searchable"
            ),
        }
        Ok(SourcePublication {
            key: key.into(),
            version: current.map(|source| source.version),
        })
    }

    pub(crate) fn confirm_source_removal(&self, key: &str) -> Result<SourcePublication> {
        let conn = self.lock()?;
        ensure!(
            source_info(&conn, key)?.is_none(),
            "source removal not committed"
        );
        Ok(SourcePublication {
            key: key.into(),
            version: None,
        })
    }

    /// List currently known sources in a collection and kind.
    pub fn sources(&self, collection: &str, kind: &str) -> Result<Vec<Source>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare(
            "SELECT key, collection, path, version, kind
             FROM sources WHERE collection=?1 AND kind=?2 ORDER BY path, key",
        )?;
        let rows = stmt.query_map(params![collection, kind], source_from_row)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Remove a source and all of its current immutable chunks.
    pub fn remove_source(&self, key: &str) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let affected = source_info(&tx, key)?;
        if let Some(ref old) = affected
            && old.eligible
        {
            adjust_statistics_for_source(&tx, old, -1)?;
        }
        let removed = tx.execute("DELETE FROM sources WHERE key=?1", params![key])?;
        if removed > 0 {
            let next_generation = next_generation(&tx)?;
            set_generation(&tx, next_generation)?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Suppress one source until a fresh source verification succeeds.
    pub fn invalidate_source(&self, key: &str) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let affected = source_info(&tx, key)?;
        if let Some(ref old) = affected
            && old.eligible
        {
            adjust_statistics_for_source(&tx, old, -1)?;
        }
        let changed = tx.execute(
            "UPDATE sources SET eligible=0, verified=0
             WHERE key=?1 AND eligible=1",
            params![key],
        )?;
        if changed > 0 {
            let generation = next_generation(&tx)?;
            set_generation(&tx, generation)?;
        } else if let Some(old) = affected {
            // No state changed, so restore the contribution we tentatively
            // subtracted above.  This path only occurs for an already
            // ineligible source.
            if old.eligible {
                adjust_statistics_for_source(&tx, &old, 1)?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Suppress every source in one collection/kind pair.
    pub fn invalidate_collection(&self, collection: &str, kind: &str) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "DELETE FROM term_stats WHERE collection=?1 AND kind=?2",
            params![collection, kind],
        )?;
        tx.execute(
            "DELETE FROM stats WHERE collection=?1 AND kind=?2",
            params![collection, kind],
        )?;
        let changed = tx.execute(
            "UPDATE sources SET eligible=0, verified=0
             WHERE collection=?1 AND kind=?2 AND eligible=1",
            params![collection, kind],
        )?;
        if changed > 0 {
            let generation = next_generation(&tx)?;
            set_generation(&tx, generation)?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Restore verified membership and return its existing chunk count.
    pub fn mark_source_verified(&self, key: &str) -> Result<u64> {
        let mut conn = self.lock()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let affected = source_info(&tx, key)?;
        let changed = tx.execute(
            "UPDATE sources SET eligible=1, verified=1, verified_at=?2
             WHERE key=?1 AND eligible=0",
            params![key, chrono::Utc::now().to_rfc3339()],
        )?;
        if changed > 0 {
            let generation = next_generation(&tx)?;
            if let Some(old) = affected {
                adjust_statistics_for_source(&tx, &old, 1)?;
            }
            set_generation(&tx, generation)?;
        }
        let chunks: i64 = tx.query_row(
            "SELECT COUNT(*) FROM chunks WHERE source_key=?1",
            [key],
            |row| row.get(0),
        )?;
        tx.commit()?;
        Ok(chunks as u64)
    }

    /// Search eligible chunks with Lucene BM25.  The candidate accumulator is
    /// a SQLite temporary table, so memory use does not grow with corpus size.
    pub fn search(
        &self,
        query: &str,
        filter: &SearchFilter,
        limit: usize,
    ) -> Result<(u64, Vec<Hit>)> {
        let _permit = self.queries.enter()?;
        let path_glob = compile_path_glob(filter.path_glob.as_deref())?;
        let limit = i64::try_from(limit).context("search limit does not fit SQLite")?;
        let mut conn = self.read_connection()?;
        register_search_functions(&conn, path_glob.clone())?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let generation = current_generation(&tx)?;

        if limit <= 0 {
            tx.commit()?;
            return Ok((generation, Vec::new()));
        }

        let terms = tokenize_checked(query)?;
        if terms.is_empty() {
            tx.commit()?;
            return Ok((generation, Vec::new()));
        }

        let hits = retrieve_terms(
            &self.hot,
            &tx,
            generation,
            &terms,
            filter,
            path_glob.as_ref(),
            limit as usize,
        )?;
        tx.commit()?;
        Ok((generation, hits))
    }

    /// Search using the bounded field aware ranker. The raw `search` method is
    /// deliberately retained as the scoring oracle used by callers and tests.
    pub fn search_ranked(
        &self,
        query_text: &str,
        filter: &SearchFilter,
        limit: usize,
    ) -> Result<(u64, Vec<Hit>)> {
        let result = self.search_ranked_with(
            query_text,
            filter,
            limit,
            ranking::RankingOptions::default(),
        )?;
        Ok((result.generation, result.hits))
    }

    pub fn search_ranked_with(
        &self,
        query_text: &str,
        filter: &SearchFilter,
        limit: usize,
        options: ranking::RankingOptions,
    ) -> Result<ranking::RankedSearch> {
        self.search_ranked_with_at(query_text, filter, limit, options, chrono::Utc::now())
    }

    /// Search using an explicit clock for deterministic ranking tests and
    /// evaluation. Production callers should use [`Self::search_ranked_with`].
    pub fn search_ranked_with_at(
        &self,
        query_text: &str,
        filter: &SearchFilter,
        limit: usize,
        options: ranking::RankingOptions,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<ranking::RankedSearch> {
        let _permit = self.queries.enter()?;
        let path_glob = compile_path_glob(filter.path_glob.as_deref())?;
        let mut plan = query::QueryPlan::new(query_text, options.classification)?;
        let mut conn = self.read_connection()?;
        register_search_functions(&conn, path_glob.clone())?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let generation = current_generation(&tx)?;
        if limit == 0 || plan.original.is_empty() {
            tx.commit()?;
            return Ok(ranking::RankedSearch {
                generation,
                hits: Vec::new(),
                traces: Vec::new(),
                probes: Vec::new(),
                candidate_count: 0,
                admission_counts: ranking::CandidateCounts::default(),
                meaningful_retrievals: 0,
                additional_retrievals: 0,
            });
        }
        // Keep the raw tokenizer's multiplicity and stopword behavior for the
        // first retrieval. QueryPlan terms are the reranker surface, not the
        // baseline oracle query.
        let terms = tokenize_checked(query_text)?;
        let mut admission = Admission::new(filter, path_glob.clone(), &terms);
        let initial = retrieve_terms(
            &self.hot,
            &tx,
            generation,
            &terms,
            filter,
            path_glob.as_ref(),
            ranking::CANDIDATE_LIMIT,
        )?;
        for hit in initial {
            admission.record(Lane::Lexical, hit)?;
        }
        let meaningful_retrievals = usize::from(plan.stopwords_removed);
        if plan.stopwords_removed {
            // Retained terms are already canonical, including long-token digests.
            // Their admission must not depend on discarded stopword scores.
            for hit in retrieve_terms(
                &self.hot,
                &tx,
                generation,
                &plan.original,
                filter,
                path_glob.as_ref(),
                ranking::MEANINGFUL_RESERVE,
            )? {
                admission.record(Lane::Meaningful, hit)?;
            }
        }
        if filter.kind == "project" && plan.class == query::QueryClass::Identifier {
            for hit in retrieve_declaration_hits(&tx, &plan.literal, filter, DEFINITION_RESERVE)? {
                admission.record(Lane::Definition, hit)?;
            }
        }
        // Fallback probes are issued only when the literal pool
        // is thin; this preserves literal evidence and bounds query work.
        let mut active_probes = Vec::new();
        let weak_pool = admission.weak(options);
        if weak_pool {
            for probe in plan.probes.iter().cloned() {
                let probe_terms = tokenize_checked(&probe.query)?;
                for hit in retrieve_terms(
                    &self.hot,
                    &tx,
                    generation,
                    &probe_terms,
                    filter,
                    path_glob.as_ref(),
                    ranking::CANDIDATE_LIMIT,
                )? {
                    admission.record(Lane::Expansion, hit)?;
                }
                active_probes.push(probe);
            }
        }
        plan.probes = active_probes;
        if filter.kind == "project" && matches!(plan.class, query::QueryClass::Path) {
            let literal = crate::text::fold(&plan.literal.replace('\\', "/"));
            let path_sql = format!(
                "SELECT c.match_id,0.0,c.source_key,c.source_version,c.ordinal,c.text,c.start_line,c.end_line,c.start_byte,c.end_byte,c.agent,c.session_id,c.event_id,c.timestamp,c.role,c.tool,s.collection,s.path,s.version,s.kind,s.verified_at,c.field_kind FROM chunks c JOIN sources s ON s.key=c.source_key AND s.eligible=1 WHERE s.collection=?1 AND s.kind='project' AND (bm25_path_normalized(s.path)=?2 OR (length(bm25_path_normalized(s.path))>length(?2) AND substr(bm25_path_normalized(s.path),-length(?2))=?2 AND substr(bm25_path_normalized(s.path),-length(?2)-1,1)='/')) AND bm25_path_matches(s.path) AND (?3 IS NULL OR c.agent=?3) AND (?4 IS NULL OR c.session_id=?4) AND (?5 IS NULL OR c.timestamp>=?5) AND (?6 IS NULL OR c.timestamp<?6) ORDER BY CASE WHEN bm25_path_normalized(s.path)=?2 THEN 0 ELSE 1 END,s.path,c.match_id LIMIT {}",
                ranking::CANDIDATE_LIMIT
            );
            let mut stmt = tx.prepare(&path_sql)?;
            let rows = stmt.query_map(
                params![
                    filter.collection,
                    literal,
                    filter.agent,
                    filter.session_id,
                    filter.after,
                    filter.before
                ],
                hit_from_row,
            )?;
            for hit in rows {
                let hit = hit?;
                admission.record(Lane::Path, hit)?;
            }
        }
        let admission_counts = admission.counts();
        let admitted = admission.finish(&plan, weak_pool);
        let candidate_count = admitted.len();
        let scorable = admitted.hydrate(&tx)?;
        let (hits, traces) = ranking::rank_indexed(scorable, options, now, limit)?;
        let additional_retrievals = meaningful_retrievals + plan.probes.len();
        tx.commit()?;
        Ok(ranking::RankedSearch {
            generation,
            hits,
            traces,
            probes: plan.probes,
            candidate_count,
            admission_counts,
            meaningful_retrievals,
            additional_retrievals,
        })
    }

    /// Enumerate physical occurrences equivalent to a session match at the
    /// same chunk position within the event.  The source and generation are
    /// resolved inside one read transaction so every returned match ID keeps
    /// its normal context-expansion and source-version checks.
    pub fn session_copies_snapshot(
        &self,
        match_id: &str,
        collection: &str,
        offset: usize,
        limit: usize,
    ) -> Result<(u64, Vec<Hit>, bool)> {
        let _permit = self.queries.enter()?;
        if limit == 0 || limit > MAX_CONTEXT_ROWS {
            bail!("invalid copies page");
        }
        let mut conn = self.read_connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let generation = current_generation(&tx)?;
        let anchor: Option<SessionAnchor> = tx
            .query_row(
                "SELECT c.id,c.source_key,c.start_line,c.end_line,
                        c.start_byte,c.end_byte,c.agent,c.session_id,c.event_id,c.field_kind
                 FROM chunks c JOIN sources s ON s.key=c.source_key
                 WHERE c.match_id=?1 AND s.collection=?2
                   AND s.kind='session' AND s.eligible=1",
                params![match_id, collection],
                |row| {
                    Ok(SessionAnchor {
                        chunk_id: row.get(0)?,
                        source_key: row.get(1)?,
                        start_line: row.get(2)?,
                        end_line: row.get(3)?,
                        start_byte: row.get(4)?,
                        end_byte: row.get(5)?,
                        agent: row.get(6)?,
                        session_id: row.get(7)?,
                        event_id: row.get(8)?,
                        field_kind: row.get(9)?,
                    })
                },
            )
            .optional()?;
        let Some(anchor) = anchor else {
            bail!("match_expired: source_changed or unavailable match");
        };

        let trusted = anchor
            .field_kind
            .as_deref()
            .is_some_and(|field_kind| field_kind == "message")
            && meaningful_identity(anchor.agent.as_deref())
            && meaningful_identity(anchor.session_id.as_deref())
            && meaningful_identity(anchor.event_id.as_deref());
        if !trusted {
            if offset != 0 {
                tx.commit()?;
                return Ok((generation, Vec::new(), false));
            }
            let hit = load_session_match_hit(&tx, match_id, collection)?;
            tx.commit()?;
            return Ok((generation, vec![hit], false));
        }

        prepare_session_event_tables(&tx)?;
        tx.execute(
            "INSERT OR IGNORE INTO bm25_session_occurrence_keys(
                 source_key,start_line,end_line,start_byte,end_byte,event_id,agent,session_id)
             SELECT DISTINCT c.source_key,c.start_line,c.end_line,c.start_byte,c.end_byte,
                 c.event_id,c.agent,c.session_id
             FROM chunks c JOIN sources s ON s.key=c.source_key AND s.eligible=1
             WHERE s.collection=?1 AND s.kind='session'
               AND c.agent=?2 AND c.session_id=?3 AND c.event_id=?4
               AND c.field_kind='message'",
            params![
                collection,
                anchor.agent.as_deref(),
                anchor.session_id.as_deref(),
                anchor.event_id.as_deref(),
            ],
        )?;
        populate_session_event_chunks(&tx)?;

        let anchor_occurrence: i64 = tx.query_row(
            "SELECT occurrence_id FROM bm25_session_occurrence_keys
             WHERE source_key=?1 AND start_line=?2 AND end_line=?3
               AND start_byte=?4 AND end_byte=?5 AND event_id=?6
               AND agent=?7 AND session_id=?8",
            params![
                anchor.source_key,
                anchor.start_line,
                anchor.end_line,
                anchor.start_byte,
                anchor.end_byte,
                anchor.event_id.as_deref(),
                anchor.agent.as_deref(),
                anchor.session_id.as_deref(),
            ],
            |row| row.get(0),
        )?;
        let anchor_position: i64 = tx.query_row(
            "SELECT position FROM bm25_session_event_chunks
             WHERE occurrence_id=?1 AND chunk_id=?2",
            params![anchor_occurrence, anchor.chunk_id],
            |row| row.get(0),
        )?;
        let same_event = same_session_event_sql("candidate_key.occurrence_id", "?1");
        let fetch_limit = limit.checked_add(1).context("copies page size overflow")?;
        let offset = i64::try_from(offset).context("copies offset does not fit SQLite")?;
        let sql = format!(
            "SELECT c.match_id,0.0,c.source_key,c.source_version,c.ordinal,
                    c.text,c.start_line,c.end_line,c.start_byte,c.end_byte,
                    c.agent,c.session_id,c.event_id,c.timestamp,c.role,c.tool,
                    s.collection,s.path,s.version,s.kind,s.verified_at,c.field_kind
             FROM bm25_session_occurrence_keys candidate_key
             JOIN bm25_session_event_chunks candidate_chunk
                 ON candidate_chunk.occurrence_id=candidate_key.occurrence_id
                AND candidate_chunk.position=?2
             JOIN chunks c ON c.id=candidate_chunk.chunk_id
             JOIN sources s ON s.key=c.source_key AND s.eligible=1
             WHERE {same_event}
             ORDER BY s.path,c.source_key,c.ordinal,c.match_id
             LIMIT ?3 OFFSET ?4",
            same_event = same_event,
        );
        let mut statement = tx.prepare(&sql)?;
        let rows = statement.query_map(
            params![
                anchor_occurrence,
                anchor_position,
                fetch_limit as i64,
                offset
            ],
            hit_from_row,
        )?;
        let mut hits = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);
        let more = hits.len() > limit;
        hits.truncate(limit);
        tx.commit()?;
        Ok((generation, hits, more))
    }

    /// Expand a match to a bounded source-local context window.
    ///
    /// Chunks are the durable event units supplied by ingestion.  The match
    /// ID includes source version and generation, and the query requires the
    /// source to still be current and eligible, so a replacement can never
    /// resolve an old ID to unrelated new text.
    pub fn context(
        &self,
        match_id: &str,
        collection: &str,
        before: usize,
        after: usize,
    ) -> Result<Vec<Hit>> {
        let (hits, more) =
            self.context_page(match_id, collection, before, after, 0, MAX_CONTEXT_ROWS)?;
        if more {
            bail!("context requires pagination");
        }
        Ok(hits)
    }

    /// Read a bounded page of chunks from the selected event window in one snapshot.
    pub fn context_page(
        &self,
        match_id: &str,
        collection: &str,
        before: usize,
        after: usize,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<Hit>, bool)> {
        let (_, hits, more) =
            self.context_page_snapshot(match_id, collection, before, after, offset, limit)?;
        Ok((hits, more))
    }

    pub fn context_page_snapshot(
        &self,
        match_id: &str,
        collection: &str,
        before: usize,
        after: usize,
        offset: usize,
        limit: usize,
    ) -> Result<(u64, Vec<Hit>, bool)> {
        let _permit = self.queries.enter()?;
        if before > 10 || after > 10 || limit == 0 || limit > MAX_CONTEXT_ROWS {
            bail!("invalid context window");
        }
        let mut conn = self.read_connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let generation = current_generation(&tx)?;
        let target: Option<ContextTarget> = tx
            .query_row(
                "SELECT c.id,c.source_key
             FROM chunks c JOIN sources s ON s.key=c.source_key
             WHERE c.match_id=?1 AND s.collection=?2 AND s.kind='session' AND s.eligible=1",
                params![match_id, collection],
                |r| {
                    Ok(ContextTarget {
                        chunk_id: r.get(0)?,
                        source_key: r.get(1)?,
                    })
                },
            )
            .optional()?;
        let Some(target) = target else {
            bail!("match_expired: source_changed or unavailable match");
        };
        let mut hits = {
            let mut stmt=tx.prepare(
                "WITH ordered AS (
                    SELECT c.id,c.ordinal,c.start_line,c.end_line,c.start_byte,c.end_byte,
                        c.event_id,c.agent,c.session_id,c.field_kind,
                        LAG(c.event_id) OVER (ORDER BY c.ordinal) AS previous_event_id,
                        LAG(c.agent) OVER (ORDER BY c.ordinal) AS previous_agent,
                        LAG(c.session_id) OVER (ORDER BY c.ordinal) AS previous_session_id,
                        LAG(c.start_line) OVER (ORDER BY c.ordinal) AS previous_start_line,
                        LAG(c.end_line) OVER (ORDER BY c.ordinal) AS previous_end_line,
                        LAG(c.start_byte) OVER (ORDER BY c.ordinal) AS previous_start_byte,
                        LAG(c.end_byte) OVER (ORDER BY c.ordinal) AS previous_end_byte,
                        LAG(c.field_kind) OVER (ORDER BY c.ordinal) AS previous_field_kind
                    FROM chunks c WHERE c.source_key=?1
                 ), numbered AS (
                    SELECT id,ordinal,
                        SUM(CASE
                            WHEN NULLIF(TRIM(event_id),'') IS NULL THEN 1
                            WHEN previous_event_id IS event_id
                             AND previous_agent IS agent
                             AND previous_session_id IS session_id
                             AND ((field_kind IS NULL AND previous_field_kind IS NULL)
                                  OR (previous_start_line IS start_line
                                      AND previous_end_line IS end_line
                                      AND previous_start_byte IS start_byte
                                      AND previous_end_byte IS end_byte)) THEN 0
                            ELSE 1
                        END) OVER (
                            ORDER BY ordinal ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) AS occurrence_no
                    FROM ordered
                 ), grouped AS (
                    SELECT occurrence_no,MIN(ordinal) AS first_ordinal
                    FROM numbered GROUP BY occurrence_no
                 ), ranked AS (
                    SELECT occurrence_no,
                        ROW_NUMBER() OVER (ORDER BY first_ordinal) AS position
                    FROM grouped
                 ), selected AS (
                    SELECT occurrence_no FROM ranked WHERE position BETWEEN
                      (SELECT position FROM ranked WHERE occurrence_no=(
                          SELECT occurrence_no FROM numbered WHERE id=?2
                      ))-?3 AND
                      (SELECT position FROM ranked WHERE occurrence_no=(
                          SELECT occurrence_no FROM numbered WHERE id=?2
                      ))+?4
                 )
                 SELECT c.match_id,c.source_key,c.source_version,c.ordinal,c.text,
                     c.start_line,c.end_line,c.start_byte,c.end_byte,c.agent,c.session_id,c.event_id,c.timestamp,c.role,c.tool,
                     s.collection,s.path,s.version,s.kind,s.verified_at,c.field_kind
                 FROM chunks c JOIN sources s ON s.key=c.source_key
                 JOIN numbered n ON n.id=c.id
                 JOIN selected e ON e.occurrence_no=n.occurrence_no
                 WHERE c.source_key=?1 AND s.collection=?5 AND s.kind='session' AND s.eligible=1
                 ORDER BY c.ordinal LIMIT ?6 OFFSET ?7")?;
            let rows = stmt.query_map(
                params![
                    target.source_key,
                    target.chunk_id,
                    before as i64,
                    after as i64,
                    collection,
                    (limit + 1) as i64,
                    i64::try_from(offset)?
                ],
                context_hit_from_row,
            )?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        let more = hits.len() > limit;
        hits.truncate(limit);
        tx.commit()?;
        Ok((generation, hits, more))
    }

    /// Reclaim unreachable vocabulary entries without changing search generations.
    pub fn compact(&self) -> Result<()> {
        let conn = self.lock()?;
        loop {
            let removed = conn.execute(
                "DELETE FROM terms WHERE id IN (
                    SELECT t.id FROM terms t WHERE NOT EXISTS
                    (SELECT 1 FROM postings p WHERE p.term_id=t.id) LIMIT 1024)",
                [],
            )?;
            if removed < 1024 {
                break;
            }
        }
        conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);")?;
        Ok(())
    }

    /// Return the last committed generation.
    pub fn generation(&self) -> Result<u64> {
        let conn = self.read_connection()?;
        let value: i64 = conn.query_row(
            "SELECT CAST(value AS INTEGER) FROM meta WHERE key='generation'",
            [],
            |row| row.get(0),
        )?;
        u64::try_from(value).context("negative generation in store")
    }

    fn lock(&self) -> Result<MutexGuard<'_, Connection>> {
        self.db
            .lock()
            .map_err(|_| anyhow!("store connection poisoned"))
    }

    fn read_connection(&self) -> Result<Connection> {
        let conn = Connection::open(self.path.as_path())
            .with_context(|| format!("open SQLite reader {}", self.path.display()))?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "temp_store", "FILE")?;
        conn.pragma_update(None, "cache_size", -2048)?;
        conn.pragma_update(None, "mmap_size", 0)?;
        Ok(conn)
    }
}

#[derive(Debug)]
struct SourceInfo {
    key: String,
    collection: String,
    kind: String,
    version: String,
    eligible: bool,
}

struct SessionAnchor {
    chunk_id: i64,
    source_key: String,
    start_line: i64,
    end_line: i64,
    start_byte: i64,
    end_byte: i64,
    agent: Option<String>,
    session_id: Option<String>,
    event_id: Option<String>,
    field_kind: Option<String>,
}

struct ContextTarget {
    chunk_id: i64,
    source_key: String,
}

fn initialize(conn: &mut Connection) -> Result<()> {
    let has_meta: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta')",
        [],
        |r| r.get(0),
    )?;
    if has_meta {
        let previous: Option<String> = conn
            .query_row(
                "SELECT value FROM meta WHERE key='schema_version'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        if previous
            .as_deref()
            .is_some_and(|version| version != "bm25-mcp-store-v1" && version != SCHEMA_VERSION)
        {
            bail!(
                "unsupported store schema {}; expected {SCHEMA_VERSION}",
                previous.unwrap()
            );
        }
        if previous.as_deref() == Some("bm25-mcp-store-v1") {
            conn.execute_batch("BEGIN IMMEDIATE; DROP TABLE IF EXISTS postings; DROP TABLE IF EXISTS term_stats;
                DROP TABLE IF EXISTS chunks; DROP TABLE IF EXISTS session_state; DROP TABLE IF EXISTS session_checkpoints; DROP TABLE IF EXISTS sources;
                DROP TABLE IF EXISTS stats; DROP TABLE IF EXISTS terms; DROP TABLE IF EXISTS meta; COMMIT;")?;
        }
    }
    conn.execute_batch(
        "BEGIN;
         CREATE TABLE IF NOT EXISTS meta(
             key TEXT PRIMARY KEY,
             value TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS sources(
             key TEXT PRIMARY KEY,
             collection TEXT NOT NULL,
             path TEXT NOT NULL,
             version TEXT NOT NULL,
             kind TEXT NOT NULL,
             eligible INTEGER NOT NULL CHECK(eligible IN (0,1)),
             verified INTEGER NOT NULL CHECK(verified IN (0,1))
         );
         CREATE TABLE IF NOT EXISTS session_state(
             source_key TEXT NOT NULL REFERENCES sources(key) ON DELETE CASCADE,
             kind TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL,
             PRIMARY KEY(source_key,kind,key)
         );
         CREATE TABLE IF NOT EXISTS session_checkpoints(
             source_key TEXT PRIMARY KEY REFERENCES sources(key) ON DELETE CASCADE,
             offset INTEGER NOT NULL CHECK(offset>=0),
             state TEXT NOT NULL
         );
         CREATE INDEX IF NOT EXISTS sources_collection ON sources(collection,kind,eligible,key);
         CREATE TABLE IF NOT EXISTS chunks(
             id INTEGER PRIMARY KEY,
             match_id TEXT NOT NULL UNIQUE,
             source_key TEXT NOT NULL REFERENCES sources(key) ON DELETE CASCADE,
             source_version TEXT NOT NULL,
             ordinal INTEGER NOT NULL,
             text TEXT NOT NULL,
             start_line INTEGER NOT NULL,
             end_line INTEGER NOT NULL,
             start_byte INTEGER NOT NULL,
             end_byte INTEGER NOT NULL,
             token_len INTEGER NOT NULL,
             agent TEXT,
             session_id TEXT,
             event_id TEXT,
             timestamp TEXT,
             role TEXT,
             tool TEXT,
             UNIQUE(source_key, source_version, ordinal)
         );
         CREATE INDEX IF NOT EXISTS chunks_source_ordinal
             ON chunks(source_key, source_version, ordinal);
         CREATE INDEX IF NOT EXISTS chunks_event_id
             ON chunks(event_id);
         CREATE TABLE IF NOT EXISTS terms(
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             term TEXT NOT NULL UNIQUE
         );
         CREATE TABLE IF NOT EXISTS postings(
             chunk_id INTEGER NOT NULL REFERENCES chunks(id) ON DELETE CASCADE,
             term_id INTEGER NOT NULL REFERENCES terms(id),
             tf INTEGER NOT NULL,
             PRIMARY KEY(chunk_id, term_id)
         );
         CREATE INDEX IF NOT EXISTS postings_term ON postings(term_id, chunk_id);
         CREATE TABLE IF NOT EXISTS stats(
             collection TEXT NOT NULL,
             kind TEXT NOT NULL,
             doc_count INTEGER NOT NULL,
             total_tokens INTEGER NOT NULL,
             PRIMARY KEY(collection, kind)
         );
         CREATE TABLE IF NOT EXISTS term_stats(
             collection TEXT NOT NULL,
             kind TEXT NOT NULL,
             term_id INTEGER NOT NULL REFERENCES terms(id),
             doc_freq INTEGER NOT NULL,
             PRIMARY KEY(collection, kind, term_id)
         );
         CREATE TABLE IF NOT EXISTS declarations(
             chunk_id INTEGER NOT NULL REFERENCES chunks(id) ON DELETE CASCADE,
             symbol TEXT NOT NULL,
             PRIMARY KEY(chunk_id, symbol)
         );
         CREATE INDEX IF NOT EXISTS declarations_symbol
             ON declarations(symbol, chunk_id);
         COMMIT;",
    )?;

    let has_verified_at: bool = conn
        .prepare("PRAGMA table_info(sources)")?
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .iter()
        .any(|name| name == "verified_at");
    if !has_verified_at {
        conn.execute_batch("ALTER TABLE sources ADD COLUMN verified_at TEXT;")?;
    }

    let has_field_kind: bool = conn
        .prepare("PRAGMA table_info(chunks)")?
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .iter()
        .any(|name| name == "field_kind");
    if !has_field_kind {
        conn.execute_batch("ALTER TABLE chunks ADD COLUMN field_kind TEXT;")?;
    }

    // Keep metadata updates separate from DDL so an older partially-created
    // store can be repaired on open.
    conn.execute(
        "INSERT OR IGNORE INTO meta(key,value) VALUES ('schema_version',?1)",
        params![SCHEMA_VERSION],
    )?;
    conn.execute(
        "INSERT OR IGNORE INTO meta(key,value) VALUES ('tokenizer_version',?1)",
        params![normalization_version()],
    )?;
    conn.execute(
        "INSERT OR IGNORE INTO meta(key,value) VALUES ('generation','0')",
        [],
    )?;
    let schema: String = conn.query_row(
        "SELECT value FROM meta WHERE key='schema_version'",
        [],
        |row| row.get(0),
    )?;
    if schema != SCHEMA_VERSION {
        bail!("unsupported store schema {schema}; expected {SCHEMA_VERSION}")
    }
    let tokenizer: String = conn.query_row(
        "SELECT value FROM meta WHERE key='tokenizer_version'",
        [],
        |row| row.get(0),
    )?;
    if tokenizer != normalization_version() {
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch(
            "DELETE FROM sources; DELETE FROM term_stats; DELETE FROM stats; DELETE FROM terms;",
        )?;
        tx.execute(
            "UPDATE meta SET value=?1 WHERE key='tokenizer_version'",
            [normalization_version()],
        )?;
        let generation = next_generation(&tx)?;
        set_generation(&tx, generation)?;
        tx.commit()?;
    }
    rebuild_declarations(conn)?;
    Ok(())
}

fn rebuild_declarations(conn: &mut Connection) -> Result<()> {
    let version: Option<String> = conn
        .query_row(
            "SELECT value FROM meta WHERE key='declaration_index_version'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    if version.as_deref() == Some(DECLARATION_INDEX_VERSION) {
        return Ok(());
    }

    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute("DELETE FROM declarations", [])?;
    let mut last_id = 0_i64;
    loop {
        let rows = {
            let mut stmt = tx.prepare(
                "SELECT c.id,c.text FROM chunks c
                 JOIN sources s ON s.key=c.source_key
                 WHERE c.id>?1 AND s.kind='project'
                 ORDER BY c.id LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![last_id, BODY_EVIDENCE_BATCH as i64], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        if rows.is_empty() {
            break;
        }
        let mut insert =
            tx.prepare_cached("INSERT INTO declarations(chunk_id,symbol) VALUES (?1,?2)")?;
        for (chunk_id, text) in rows {
            last_id = chunk_id;
            if text.is_empty() {
                continue;
            }
            for symbol in ranking::declared_symbols(&text) {
                insert.execute(params![chunk_id, symbol])?;
            }
        }
    }
    tx.execute(
        "INSERT INTO meta(key,value) VALUES ('declaration_index_version',?1)
         ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        [DECLARATION_INDEX_VERSION],
    )?;
    tx.commit()?;
    Ok(())
}

fn validate_source(source: &Source) -> Result<()> {
    if source.key.is_empty() {
        bail!("source key must not be empty")
    }
    if source.collection.is_empty() {
        bail!("source collection must not be empty")
    }
    if source.kind.is_empty() {
        bail!("source kind must not be empty")
    }
    Ok(())
}

fn insert_chunk(
    tx: &Transaction<'_>,
    source: &Source,
    chunk: &Chunk,
    ordinal: i64,
    generation: u64,
) -> Result<()> {
    let id = match_id(&source.key, &source.version, ordinal, generation);
    let tokens = match &chunk.tokens {
        Some(tokens) => tokens.clone(),
        None => tokenize_checked(&chunk.text)?,
    };
    let mut frequencies = HashMap::<String, u32>::new();
    for token in &tokens {
        *frequencies.entry(token.clone()).or_default() += 1;
    }

    tx.execute(
        "INSERT INTO chunks(
             match_id, source_key, source_version, ordinal, text, start_line, end_line,
             start_byte, end_byte, token_len, agent, session_id, event_id,
             timestamp, role, tool, field_kind)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
        params![
            id,
            source.key,
            source.version,
            ordinal,
            chunk.text,
            checked_i64(chunk.start_line)?,
            checked_i64(chunk.end_line)?,
            checked_i64(chunk.start_byte)?,
            checked_i64(chunk.end_byte)?,
            checked_i64(tokens.len() as u64)?,
            chunk.agent,
            chunk.session_id,
            chunk.event_id,
            chunk.timestamp,
            chunk.role,
            chunk.tool,
            chunk.field_kind,
        ],
    )?;

    let chunk_id = tx.last_insert_rowid();
    let mut term_stmt = tx.prepare_cached("INSERT OR IGNORE INTO terms(term) VALUES (?1)")?;
    let mut term_id_stmt = tx.prepare_cached("SELECT id FROM terms WHERE term=?1")?;
    let mut posting_stmt =
        tx.prepare_cached("INSERT INTO postings(chunk_id, term_id, tf) VALUES (?1,?2,?3)")?;
    for (term, tf) in frequencies {
        term_stmt.execute(params![term])?;
        let term_id: i64 = term_id_stmt.query_row(params![term], |row| row.get(0))?;
        posting_stmt.execute(params![chunk_id, term_id, i64::from(tf)])?;
    }
    if source.kind == "project" {
        let mut declaration_stmt =
            tx.prepare_cached("INSERT INTO declarations(chunk_id,symbol) VALUES (?1,?2)")?;
        for symbol in ranking::declared_symbols(&chunk.text) {
            declaration_stmt.execute(params![chunk_id, symbol])?;
        }
    }
    Ok(())
}

fn source_info(tx: &Connection, key: &str) -> Result<Option<SourceInfo>> {
    tx.query_row(
        "SELECT key, collection, kind, version, eligible FROM sources WHERE key=?1",
        params![key],
        |row| {
            Ok(SourceInfo {
                key: row.get(0)?,
                collection: row.get(1)?,
                kind: row.get(2)?,
                version: row.get(3)?,
                eligible: row.get::<_, i64>(4)? != 0,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

fn adjust_statistics_for_source(
    tx: &Transaction<'_>,
    source: &SourceInfo,
    sign: i64,
) -> Result<()> {
    adjust_statistics_from_ordinal(tx, source, sign, 0)
}

fn adjust_statistics_from_ordinal(
    tx: &Transaction<'_>,
    source: &SourceInfo,
    sign: i64,
    first_ordinal: i64,
) -> Result<()> {
    debug_assert!(sign == 1 || sign == -1);
    let (doc_count, total_tokens): (i64, i64) = tx.query_row(
        "SELECT COUNT(id), COALESCE(SUM(token_len),0) FROM chunks
         WHERE source_key=?1 AND ordinal>=?2",
        params![source.key, first_ordinal],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    tx.execute(
        "INSERT INTO stats(collection, kind, doc_count, total_tokens)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(collection,kind) DO UPDATE SET
             doc_count=stats.doc_count + excluded.doc_count,
             total_tokens=stats.total_tokens + excluded.total_tokens",
        params![
            source.collection,
            source.kind,
            sign * doc_count,
            sign * total_tokens
        ],
    )?;
    let mut stmt = tx.prepare(
        "SELECT p.term_id, COUNT(*) FROM chunks c
         JOIN postings p ON p.chunk_id=c.id
         WHERE c.source_key=?1 AND c.ordinal>=?2 GROUP BY p.term_id",
    )?;
    let mut rows = stmt.query(params![source.key, first_ordinal])?;
    let mut update_term = tx.prepare_cached(
        "INSERT INTO term_stats(collection, kind, term_id, doc_freq)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(collection,kind,term_id) DO UPDATE SET
             doc_freq=term_stats.doc_freq + excluded.doc_freq",
    )?;
    while let Some(row) = rows.next()? {
        let term_id: i64 = row.get(0)?;
        let doc_freq: i64 = row.get(1)?;
        update_term.execute(params![
            source.collection,
            source.kind,
            term_id,
            sign * doc_freq
        ])?;
    }
    drop(rows);
    drop(stmt);
    if sign < 0 {
        tx.execute(
            "DELETE FROM term_stats WHERE collection=?1 AND kind=?2 AND doc_freq<=0",
            params![source.collection, source.kind],
        )?;
        tx.execute(
            "DELETE FROM stats WHERE collection=?1 AND kind=?2
             AND doc_count<=0",
            params![source.collection, source.kind],
        )?;
    }
    Ok(())
}

fn create_query_tables(tx: &Transaction<'_>) -> Result<()> {
    tx.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS bm25_query_terms(
             term_id INTEGER PRIMARY KEY,
             weight INTEGER NOT NULL
         );
         CREATE TEMP TABLE IF NOT EXISTS bm25_query_accum(
             chunk_id INTEGER PRIMARY KEY,
             score REAL NOT NULL
         );",
    )?;
    Ok(())
}

fn register_search_functions(conn: &Connection, path_glob: Option<GlobSet>) -> Result<()> {
    let flags = rusqlite::functions::FunctionFlags::SQLITE_UTF8
        | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC;
    conn.create_scalar_function("bm25_score", 6, flags, |ctx| {
        let tf = ctx.get::<u32>(0)?;
        let length = ctx.get::<i64>(1)? as u64;
        let documents = ctx.get::<i64>(2)? as u64;
        let frequency = ctx.get::<i64>(3)? as u64;
        let total = ctx.get::<i64>(4)? as u64;
        let weight = ctx.get::<u32>(5)?;
        Ok(if documents == 0 || frequency == 0 {
            0.0
        } else {
            f64::from(
                upstream::scoring::lucene_score(
                    tf,
                    length,
                    total as f64 / documents as f64,
                    documents,
                    frequency,
                ) * weight as f32,
            )
        })
    })?;
    let query_glob = path_glob;
    conn.create_scalar_function("bm25_path_matches", 1, flags, move |ctx| {
        let path = ctx.get::<String>(0)?;
        Ok(query_glob.as_ref().is_none_or(|glob| glob.is_match(path)))
    })?;
    conn.create_scalar_function("bm25_path_normalized", 1, flags, |ctx| {
        let path = ctx.get::<String>(0)?;
        Ok(crate::text::fold(&path.replace('\\', "/")))
    })?;
    Ok(())
}

fn set_query_terms(tx: &Transaction<'_>, terms: &[String]) -> Result<()> {
    create_query_tables(tx)?;
    tx.execute("DELETE FROM bm25_query_terms", [])?;
    let mut stmt = tx.prepare_cached("INSERT INTO bm25_query_terms(term_id,weight) SELECT id,1 FROM terms WHERE term=?1 ON CONFLICT(term_id) DO UPDATE SET weight=weight+1")?;
    for term in terms {
        stmt.execute(params![term])?;
    }
    Ok(())
}

fn hydrate_baseline_scores(
    tx: &Transaction<'_>,
    candidates: &mut [admission::Candidate],
    terms: &[String],
) -> Result<()> {
    let mut supplemental: Vec<_> = candidates
        .iter_mut()
        .filter(|candidate| candidate.raw_bm25.is_none())
        .collect();
    if !supplemental.is_empty() {
        set_query_terms(tx, terms)?;
    }
    for batch in supplemental.chunks_mut(BODY_EVIDENCE_BATCH) {
        let placeholders = std::iter::repeat_n("?", batch.len())
            .collect::<Vec<_>>()
            .join(",");
        // Score only already-admitted chunks, using the raw scorer's kernel and
        // query multiplicity. Not appearing in raw top K does not mean score zero.
        let sql = format!(
            "SELECT c.match_id,(
                 SELECT COALESCE(SUM(bm25_score(p.tf,c.token_len,st.doc_count,
                     ts.doc_freq,st.total_tokens,qt.weight)),0)
                 FROM postings p
                 JOIN bm25_query_terms qt ON qt.term_id=p.term_id
                 JOIN term_stats ts ON ts.term_id=p.term_id
                     AND ts.collection=s.collection AND ts.kind=s.kind
                 WHERE p.chunk_id=c.id)
             FROM chunks c JOIN sources s ON s.key=c.source_key AND s.eligible=1
             JOIN stats st ON st.collection=s.collection AND st.kind=s.kind
             WHERE c.match_id IN ({placeholders})"
        );
        let mut statement = tx.prepare(&sql)?;
        let scores = statement
            .query_map(
                rusqlite::params_from_iter(
                    batch
                        .iter()
                        .map(|candidate| candidate.hit.match_id.as_str()),
                ),
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, f32>(1)?)),
            )?
            .collect::<rusqlite::Result<HashMap<_, _>>>()?;
        for candidate in batch {
            candidate.raw_bm25 =
                Some(*scores.get(&candidate.hit.match_id).ok_or_else(|| {
                    anyhow!("missing baseline score for {}", candidate.hit.match_id)
                })?);
        }
    }
    for candidate in candidates {
        candidate.hit.score = candidate.raw_bm25.expect("admitted baseline hydrated");
    }
    Ok(())
}

fn retrieve_declaration_hits(
    tx: &Transaction<'_>,
    symbol: &str,
    filter: &SearchFilter,
    limit: usize,
) -> Result<Vec<Hit>> {
    if symbol.is_empty() || limit == 0 {
        return Ok(Vec::new());
    }
    let mut stmt = tx.prepare(
        "SELECT c.match_id,0.0,c.source_key,c.source_version,c.ordinal,c.text,
                c.start_line,c.end_line,c.start_byte,c.end_byte,c.agent,c.session_id,
                c.event_id,c.timestamp,c.role,c.tool,s.collection,s.path,s.version,
                s.kind,s.verified_at,c.field_kind
         FROM declarations d
         JOIN chunks c ON c.id=d.chunk_id
         JOIN sources s ON s.key=c.source_key AND s.eligible=1
         WHERE d.symbol=?1 AND s.collection=?2 AND s.kind=?3
           AND (?4 IS NULL OR c.agent=?4)
           AND (?5 IS NULL OR c.session_id=?5)
           AND (?6 IS NULL OR c.timestamp>=?6)
           AND (?7 IS NULL OR c.timestamp<?7)
           AND bm25_path_matches(s.path)
         ORDER BY c.match_id
         LIMIT ?8",
    )?;
    let rows = stmt.query_map(
        params![
            symbol,
            filter.collection,
            filter.kind,
            filter.agent,
            filter.session_id,
            filter.after,
            filter.before,
            i64::try_from(limit)?,
        ],
        hit_from_row,
    )?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn hydrate_body_evidence(
    tx: &Transaction<'_>,
    candidates: &[admission::Candidate],
) -> Result<HashMap<String, ranking::BodyEvidence>> {
    let mut evidence = HashMap::with_capacity(candidates.len());
    for batch in candidates.chunks(BODY_EVIDENCE_BATCH) {
        if batch.is_empty() {
            continue;
        }
        let placeholders = std::iter::repeat_n("?", batch.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT c.match_id,c.token_len,t.term,p.tf
             FROM chunks c
             LEFT JOIN postings p ON p.chunk_id=c.id
             LEFT JOIN terms t ON t.id=p.term_id
             WHERE c.match_id IN ({placeholders})
             ORDER BY c.match_id,t.term"
        );
        let mut stmt = tx.prepare(&sql)?;
        let mut rows = stmt.query(rusqlite::params_from_iter(
            batch
                .iter()
                .map(|candidate| candidate.hit.match_id.as_str()),
        ))?;
        while let Some(row) = rows.next()? {
            let match_id: String = row.get(0)?;
            let length =
                usize::try_from(row.get::<_, i64>(1)?).context("negative indexed token length")?;
            let entry = evidence
                .entry(match_id)
                .or_insert_with(|| ranking::BodyEvidence {
                    terms: std::collections::BTreeMap::new(),
                    length,
                });
            if let Some(term) = row.get::<_, Option<String>>(2)? {
                let tf = usize::try_from(
                    row.get::<_, i64>(3)
                        .context("missing indexed term frequency")?,
                )
                .context("negative indexed term frequency")?;
                entry.terms.insert(term, tf);
            }
        }
    }
    Ok(evidence)
}

fn retrieve_terms(
    cache: &std::sync::Mutex<hot::Cache>,
    tx: &Transaction<'_>,
    generation: u64,
    terms: &[String],
    filter: &SearchFilter,
    path_glob: Option<&GlobSet>,
    limit: usize,
) -> Result<Vec<Hit>> {
    if terms.is_empty() {
        return Ok(Vec::new());
    }
    if let Some(hits) = hot::search(cache, tx, generation, terms, filter, path_glob, limit)? {
        Ok(hits)
    } else {
        retrieve_ranked_terms(tx, terms, filter, path_glob, limit)
    }
}

fn retrieve_ranked_terms(
    tx: &Transaction<'_>,
    terms: &[String],
    filter: &SearchFilter,
    _path_glob: Option<&GlobSet>,
    limit: usize,
) -> Result<Vec<Hit>> {
    if terms.is_empty() {
        return Ok(Vec::new());
    }
    set_query_terms(tx, terms)?;
    tx.execute("DELETE FROM bm25_query_accum", [])?;
    let sql = "INSERT INTO bm25_query_accum(chunk_id,score)
        SELECT p.chunk_id,SUM(bm25_score(p.tf,c.token_len,st.doc_count,ts.doc_freq,st.total_tokens,qt.weight))
        FROM bm25_query_terms qt JOIN postings p ON p.term_id=qt.term_id
        JOIN chunks c ON c.id=p.chunk_id JOIN sources s ON s.key=c.source_key AND s.eligible=1
        JOIN stats st ON st.collection=s.collection AND st.kind=s.kind
        JOIN term_stats ts ON ts.collection=s.collection AND ts.kind=s.kind AND ts.term_id=p.term_id
        WHERE s.collection=?1 AND s.kind=?2 AND (?3 IS NULL OR c.agent=?3)
          AND (?4 IS NULL OR c.session_id=?4) AND (?5 IS NULL OR c.timestamp>=?5)
          AND (?6 IS NULL OR c.timestamp<?6) AND bm25_path_matches(s.path)
        GROUP BY p.chunk_id";
    tx.execute(
        sql,
        params![
            filter.collection,
            filter.kind,
            filter.agent,
            filter.session_id,
            filter.after,
            filter.before
        ],
    )?;
    if filter.kind == "session" {
        load_top_session_hits(tx, limit as i64)
    } else {
        load_top_hits(tx, limit as i64)
    }
}

fn corpus_stats(
    tx: &Transaction<'_>,
    filter: &SearchFilter,
    terms: &std::collections::BTreeSet<String>,
) -> Result<ranking::CorpusStats> {
    let (documents, total): (i64, i64) = tx.query_row("SELECT COALESCE(doc_count,0),COALESCE(total_tokens,0) FROM stats WHERE collection=?1 AND kind=?2", params![filter.collection,filter.kind], |r| Ok((r.get(0)?,r.get(1)?))).optional()?.unwrap_or((0,0));
    let mut idf = std::collections::BTreeMap::new();
    let encoded = serde_json::to_string(&terms.iter().collect::<Vec<_>>())?;
    let mut stmt = tx.prepare("SELECT t.term,ts.doc_freq FROM terms t JOIN term_stats ts ON ts.term_id=t.id WHERE ts.collection=?1 AND ts.kind=?2 AND t.term IN (SELECT value FROM json_each(?3))")?;
    let rows = stmt.query_map(params![filter.collection, filter.kind, encoded], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
    })?;
    for row in rows {
        let (term, df) = row?;
        idf.insert(
            term,
            ((documents as f64 - df as f64 + 0.5) / (df as f64 + 0.5) + 1.).ln(),
        );
    }
    Ok(ranking::CorpusStats {
        documents: documents.max(0) as u64,
        average_length: if documents > 0 {
            total as f64 / documents as f64
        } else {
            1.
        },
        idf,
    })
}

fn load_top_hits(tx: &Transaction<'_>, limit: i64) -> Result<Vec<Hit>> {
    let mut stmt = tx.prepare(
        "SELECT c.match_id, a.score, c.source_key, c.source_version, c.ordinal,
                c.text, c.start_line, c.end_line, c.start_byte, c.end_byte,
                c.agent, c.session_id, c.event_id, c.timestamp, c.role, c.tool,
                s.collection, s.path, s.version, s.kind, s.verified_at, c.field_kind
         FROM bm25_query_accum a
         JOIN chunks c ON c.id=a.chunk_id
         JOIN sources s ON s.key=c.source_key
             AND s.eligible=1
         WHERE a.score>0
         ORDER BY a.score DESC, c.match_id ASC LIMIT ?1",
    )?;
    let rows = stmt.query_map(params![limit], hit_from_row)?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

/// Build one search representative per exact logical session event.
///
/// The accumulator still contains every matching chunk.  Session event rows
/// are grouped only after scoring, and the grouping keys live in SQLite temp
/// tables so a broad query does not create an all-corpus Rust set.  A physical
/// event is identified by its source and record bounds; its complete ordered
/// chunk sequence is then compared before two occurrences can be considered
/// replicas.
fn load_top_session_hits(tx: &Transaction<'_>, limit: i64) -> Result<Vec<Hit>> {
    prepare_session_event_tables(tx)?;
    tx.execute(
        "INSERT OR IGNORE INTO bm25_session_occurrence_keys(
             source_key,start_line,end_line,start_byte,end_byte,event_id,agent,session_id)
         SELECT DISTINCT c.source_key,c.start_line,c.end_line,c.start_byte,c.end_byte,
             c.event_id,c.agent,c.session_id
         FROM bm25_query_accum a
         JOIN chunks c ON c.id=a.chunk_id
         JOIN sources s ON s.key=c.source_key AND s.eligible=1
         WHERE a.score>0
           AND NULLIF(TRIM(c.agent),'') IS NOT NULL
           AND NULLIF(TRIM(c.session_id),'') IS NOT NULL
           AND NULLIF(TRIM(c.event_id),'') IS NOT NULL
           AND c.field_kind='message'",
        [],
    )?;
    populate_session_event_chunks(tx)?;

    tx.execute(
        "INSERT INTO bm25_session_occurrences(
             occurrence_id,source_key,agent,session_id,event_id,
             best_chunk_id,best_match_id,best_score)
         WITH ranked AS (
             SELECT e.occurrence_id,c.id,c.match_id,a.score,
                 ROW_NUMBER() OVER (
                     PARTITION BY e.occurrence_id
                     ORDER BY a.score DESC,c.match_id ASC
                 ) AS row_number
             FROM bm25_session_event_chunks e
             JOIN chunks c ON c.id=e.chunk_id
             JOIN bm25_query_accum a ON a.chunk_id=c.id
             WHERE a.score>0
         )
         SELECT r.occurrence_id,k.source_key,k.agent,k.session_id,k.event_id,
             r.id,r.match_id,r.score
         FROM ranked r
         JOIN bm25_session_occurrence_keys k ON k.occurrence_id=r.occurrence_id
         WHERE r.row_number=1",
        [],
    )?;

    // Chunks without a complete logical identity remain independent search
    // rows.  They still participate in the same score order and therefore
    // cannot disappear just because the session finalizer is active.
    tx.execute(
        "INSERT INTO bm25_session_occurrences(
             occurrence_id,source_key,agent,session_id,event_id,
             best_chunk_id,best_match_id,best_score)
         SELECT -c.id,c.source_key,c.agent,c.session_id,c.event_id,
             c.id,c.match_id,a.score
         FROM bm25_query_accum a
         JOIN chunks c ON c.id=a.chunk_id
         JOIN sources s ON s.key=c.source_key AND s.eligible=1
         WHERE a.score>0
           AND (
               NULLIF(TRIM(c.agent),'') IS NULL
               OR NULLIF(TRIM(c.session_id),'') IS NULL
               OR NULLIF(TRIM(c.event_id),'') IS NULL
               OR c.field_kind IS NOT 'message'
           )",
        [],
    )?;

    let same_event = same_session_event_sql("prior.occurrence_id", "candidate.occurrence_id");
    let copy_count = format!(
        "CASE WHEN candidate.occurrence_id <= 0 THEN 1 ELSE (
            SELECT COUNT(*) FROM bm25_session_occurrences copy
            WHERE copy.agent IS candidate.agent
              AND copy.session_id IS candidate.session_id
              AND copy.event_id IS candidate.event_id
              AND {same_event}
        ) END",
        same_event = same_session_event_sql("copy.occurrence_id", "candidate.occurrence_id")
    );
    let sql = format!(
        "WITH results AS (
             SELECT c.match_id,a.score,c.source_key,c.source_version,c.ordinal,
                    c.text,c.start_line,c.end_line,c.start_byte,c.end_byte,
                    c.agent,c.session_id,c.event_id,c.timestamp,c.role,c.tool,
                    s.collection,s.path,s.version,s.kind,s.verified_at,c.field_kind,
                    {copy_count} AS copy_count
             FROM bm25_session_occurrences candidate
             JOIN bm25_session_event_chunks e ON e.occurrence_id=candidate.occurrence_id
             JOIN chunks c ON c.id=e.chunk_id
             JOIN bm25_query_accum a ON a.chunk_id=c.id AND a.score>0
             JOIN sources s ON s.key=c.source_key AND s.eligible=1
             WHERE candidate.occurrence_id>0
               AND c.field_kind='message'
               AND NOT EXISTS (
                   SELECT 1 FROM bm25_session_occurrences prior
                   WHERE (prior.best_score>candidate.best_score
                          OR (prior.best_score=candidate.best_score
                              AND prior.best_match_id<candidate.best_match_id))
                     AND prior.agent IS candidate.agent
                     AND prior.session_id IS candidate.session_id
                     AND prior.event_id IS candidate.event_id
                     AND {same_event}
               )
             UNION ALL
             SELECT c.match_id,a.score,c.source_key,c.source_version,c.ordinal,
                    c.text,c.start_line,c.end_line,c.start_byte,c.end_byte,
                    c.agent,c.session_id,c.event_id,c.timestamp,c.role,c.tool,
                    s.collection,s.path,s.version,s.kind,s.verified_at,c.field_kind,
                    1 AS copy_count
             FROM bm25_session_occurrences candidate
             JOIN chunks c ON c.id=candidate.best_chunk_id
             JOIN bm25_query_accum a ON a.chunk_id=c.id AND a.score>0
             JOIN sources s ON s.key=c.source_key AND s.eligible=1
             WHERE candidate.occurrence_id<0
         )
         SELECT match_id,score,source_key,source_version,ordinal,text,start_line,end_line,
                start_byte,end_byte,agent,session_id,event_id,timestamp,role,tool,
                collection,path,version,kind,verified_at,field_kind,copy_count
         FROM results
         ORDER BY score DESC,match_id ASC
         LIMIT ?1",
        copy_count = copy_count,
        same_event = same_event,
    );
    let mut statement = tx.prepare(&sql)?;
    let rows = statement.query_map(params![limit], session_hit_from_row)?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn prepare_session_event_tables(tx: &Transaction<'_>) -> Result<()> {
    tx.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS bm25_session_occurrence_keys(
             occurrence_id INTEGER PRIMARY KEY AUTOINCREMENT,
             source_key TEXT NOT NULL,
             start_line INTEGER NOT NULL,
             end_line INTEGER NOT NULL,
             start_byte INTEGER NOT NULL,
             end_byte INTEGER NOT NULL,
             event_id TEXT NOT NULL,
             agent TEXT NOT NULL,
             session_id TEXT NOT NULL,
             UNIQUE(source_key,start_line,end_line,start_byte,end_byte,event_id,agent,session_id)
         );
         CREATE INDEX IF NOT EXISTS bm25_session_occurrence_identity
             ON bm25_session_occurrence_keys(agent,session_id,event_id);
         CREATE TEMP TABLE IF NOT EXISTS bm25_session_event_chunks(
             occurrence_id INTEGER NOT NULL,
             position INTEGER NOT NULL,
             chunk_id INTEGER NOT NULL,
             PRIMARY KEY(occurrence_id,position)
         );
         CREATE INDEX IF NOT EXISTS bm25_session_event_chunks_occurrence
             ON bm25_session_event_chunks(occurrence_id,position);
         CREATE TEMP TABLE IF NOT EXISTS bm25_session_occurrences(
             occurrence_id INTEGER PRIMARY KEY,
             source_key TEXT,
             agent TEXT,
             session_id TEXT,
             event_id TEXT,
             best_chunk_id INTEGER NOT NULL,
             best_match_id TEXT NOT NULL,
             best_score REAL NOT NULL
         );
         CREATE INDEX IF NOT EXISTS bm25_session_occurrences_identity
             ON bm25_session_occurrences(agent,session_id,event_id,best_score,best_match_id);",
    )?;
    tx.execute("DELETE FROM bm25_session_event_chunks", [])?;
    tx.execute("DELETE FROM bm25_session_occurrence_keys", [])?;
    tx.execute("DELETE FROM bm25_session_occurrences", [])?;
    Ok(())
}

fn populate_session_event_chunks(tx: &Transaction<'_>) -> Result<()> {
    tx.execute(
        "INSERT INTO bm25_session_event_chunks(occurrence_id,position,chunk_id)
         SELECT k.occurrence_id,
             ROW_NUMBER() OVER (PARTITION BY k.occurrence_id ORDER BY c.ordinal)-1,
             c.id
         FROM bm25_session_occurrence_keys k
         JOIN chunks c ON c.source_key=k.source_key
             AND c.start_line=k.start_line
             AND c.end_line=k.end_line
             AND c.start_byte=k.start_byte
             AND c.end_byte=k.end_byte
             AND c.event_id=k.event_id
             AND c.agent=k.agent
             AND c.session_id=k.session_id
         JOIN sources s ON s.key=c.source_key AND s.eligible=1",
        [],
    )?;
    Ok(())
}

/// Return a SQL predicate for exact equality of two complete physical event
/// sequences.  Comparing rows at their sequence positions preserves repeated
/// fields and prevents a shared prefix from making two events equivalent.
fn same_session_event_sql(left: &str, right: &str) -> String {
    format!(
        "{left}>0 AND {right}>0
         AND (SELECT COUNT(*) FROM bm25_session_event_chunks left_chunk
            WHERE left_chunk.occurrence_id={left})
          = (SELECT COUNT(*) FROM bm25_session_event_chunks right_chunk
            WHERE right_chunk.occurrence_id={right})
         AND NOT EXISTS (
             SELECT 1
             FROM bm25_session_event_chunks left_chunk
             JOIN chunks left_value ON left_value.id=left_chunk.chunk_id
             LEFT JOIN bm25_session_event_chunks right_chunk
                 ON right_chunk.occurrence_id={right}
                AND right_chunk.position=left_chunk.position
             LEFT JOIN chunks right_value ON right_value.id=right_chunk.chunk_id
             WHERE left_chunk.occurrence_id={left}
               AND (right_chunk.chunk_id IS NULL
                    OR left_value.text IS NOT right_value.text
                    OR left_value.role IS NOT right_value.role
                    OR left_value.tool IS NOT right_value.tool
                    OR left_value.field_kind IS NOT right_value.field_kind
                    OR left_value.event_id IS NOT right_value.event_id
                    OR left_value.agent IS NOT right_value.agent
                    OR left_value.session_id IS NOT right_value.session_id)
         )",
        left = left,
        right = right,
    )
}

fn load_session_match_hit(tx: &Transaction<'_>, match_id: &str, collection: &str) -> Result<Hit> {
    tx.query_row(
        "SELECT c.match_id,0.0,c.source_key,c.source_version,c.ordinal,
                c.text,c.start_line,c.end_line,c.start_byte,c.end_byte,
                c.agent,c.session_id,c.event_id,c.timestamp,c.role,c.tool,
                s.collection,s.path,s.version,s.kind,s.verified_at,c.field_kind
         FROM chunks c JOIN sources s ON s.key=c.source_key
         WHERE c.match_id=?1 AND s.collection=?2
           AND s.kind='session' AND s.eligible=1",
        params![match_id, collection],
        hit_from_row,
    )
    .map_err(Into::into)
}

fn meaningful_identity(value: Option<&str>) -> bool {
    value.is_some_and(|value| !value.trim().is_empty())
}

fn session_hit_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Hit> {
    let mut hit = hit_from_row(row)?;
    let copy_count: i64 = row.get(22)?;
    hit.copy_count = usize::try_from(copy_count)
        .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(22, copy_count))?;
    Ok(hit)
}

fn hit_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Hit> {
    let match_id: String = row.get(0)?;
    let score: f64 = row.get(1)?;
    let source_key: String = row.get(2)?;
    let _source_version: String = row.get(3)?;
    let ordinal: i64 = row.get(4)?;
    let chunk = Chunk {
        field_kind: row.get(21)?,
        tokens: None,
        text: row.get(5)?,
        start_line: row.get::<_, i64>(6)? as u64,
        end_line: row.get::<_, i64>(7)? as u64,
        start_byte: row.get::<_, i64>(8)? as u64,
        end_byte: row.get::<_, i64>(9)? as u64,
        agent: row.get(10)?,
        session_id: row.get(11)?,
        event_id: row.get(12)?,
        timestamp: row.get(13)?,
        role: row.get(14)?,
        tool: row.get(15)?,
    };
    let source = Source {
        key: source_key,
        collection: row.get(16)?,
        path: row.get(17)?,
        version: row.get(18)?,
        kind: row.get(19)?,
    };
    let _ = ordinal; // ordinal is part of the opaque ID, not public Hit data.
    Ok(Hit {
        copy_count: 1,
        match_id,
        verified_at: row.get(20)?,
        source,
        chunk,
        score: score as f32,
    })
}

fn context_hit_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Hit> {
    let match_id: String = row.get(0)?;
    let source = Source {
        key: row.get(1)?,
        collection: row.get(15)?,
        path: row.get(16)?,
        version: row.get(17)?,
        kind: row.get(18)?,
    };
    let chunk = Chunk {
        field_kind: row.get(20)?,
        tokens: None,
        text: row.get(4)?,
        start_line: row.get::<_, i64>(5)? as u64,
        end_line: row.get::<_, i64>(6)? as u64,
        start_byte: row.get::<_, i64>(7)? as u64,
        end_byte: row.get::<_, i64>(8)? as u64,
        agent: row.get(9)?,
        session_id: row.get(10)?,
        event_id: row.get(11)?,
        timestamp: row.get(12)?,
        role: row.get(13)?,
        tool: row.get(14)?,
    };
    Ok(Hit {
        copy_count: 1,
        match_id,
        verified_at: row.get(19)?,
        source,
        chunk,
        score: 0.0,
    })
}

fn source_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Source> {
    Ok(Source {
        key: row.get(0)?,
        collection: row.get(1)?,
        path: row.get(2)?,
        version: row.get(3)?,
        kind: row.get(4)?,
    })
}

fn current_generation(conn: &Connection) -> Result<u64> {
    let value: i64 = conn.query_row(
        "SELECT CAST(value AS INTEGER) FROM meta WHERE key='generation'",
        [],
        |row| row.get(0),
    )?;
    u64::try_from(value).context("negative generation in store")
}

fn next_generation(conn: &Connection) -> Result<u64> {
    current_generation(conn)?
        .checked_add(1)
        .ok_or_else(|| anyhow!("generation exhausted"))
}

fn set_generation(conn: &Connection, generation: u64) -> Result<()> {
    let generation = i64::try_from(generation).context("generation does not fit SQLite")?;
    conn.execute(
        "UPDATE meta SET value=?1 WHERE key='generation'",
        params![generation.to_string()],
    )?;
    Ok(())
}

fn checked_i64(value: u64) -> Result<i64> {
    i64::try_from(value).context("chunk coordinate does not fit SQLite")
}

fn match_id(source_key: &str, version: &str, ordinal: i64, generation: u64) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"bm25-mcp-match-v1\0");
    hasher.update(source_key.as_bytes());
    hasher.update([0]);
    hasher.update(version.as_bytes());
    hasher.update([0]);
    hasher.update(ordinal.to_le_bytes());
    hasher.update(generation.to_le_bytes());
    let digest = hasher.finalize();
    let mut id = String::with_capacity(64);
    for byte in digest {
        id.push_str(&format!("{byte:02x}"));
    }
    id
}

fn compile_path_glob(pattern: Option<&str>) -> Result<Option<GlobSet>> {
    let Some(pattern) = pattern else {
        return Ok(None);
    };
    let glob = Glob::new(pattern).with_context(|| format!("invalid path glob {pattern:?}"))?;
    let mut builder = GlobSetBuilder::new();
    builder.add(glob);
    Ok(Some(builder.build()?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn source(key: &str, version: &str) -> Source {
        Source {
            key: key.into(),
            collection: "project".into(),
            path: format!("src/{key}.rs"),
            version: version.into(),
            kind: "code".into(),
        }
    }

    fn chunk(text: &str, line: u64) -> Chunk {
        Chunk {
            text: text.into(),
            start_line: line,
            end_line: line,
            start_byte: line,
            end_byte: line + text.len() as u64,
            ..Chunk::default()
        }
    }

    #[test]
    fn replace_search_delete_and_reopen_are_fresh() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("index.sqlite3");
        let store = Store::open(&path)?;
        let first = source("one", "v1");
        let second = source("two", "v1");
        store.replace_source(
            &first,
            vec![
                Ok(chunk("durable alpha identifier", 1)),
                Ok(chunk("other", 2)),
            ],
        )?;
        store.replace_source(&second, vec![Ok(chunk("durable beta identifier", 1))])?;
        let filter = SearchFilter {
            collection: "project".into(),
            kind: "code".into(),
            ..SearchFilter::default()
        };
        let (_, hits) = store.search("alpha", &filter, 10)?;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].source.key, "one");
        let old_match = hits[0].match_id.clone();

        store.replace_source(&first, vec![Ok(chunk("fresh gamma identifier", 9))])?;
        assert!(store.search("alpha", &filter, 10)?.1.is_empty());
        assert_eq!(store.search("gamma", &filter, 10)?.1[0].chunk.start_line, 9);
        assert!(store.context(&old_match, "project", 1, 1).is_err());

        store.remove_source("one")?;
        assert!(store.search("gamma", &filter, 10)?.1.is_empty());
        drop(store);
        let reopened = Store::open(&path)?;
        assert!(reopened.search("beta", &filter, 10)?.1.len() == 1);
        assert_eq!(reopened.generation()?, 4);
        Ok(())
    }

    #[test]
    fn iterator_error_rolls_back_source_and_generation() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("index.sqlite3");
        let store = Store::open(&path)?;
        let src = source("rollback", "v1");
        store.replace_source(&src, vec![Ok(chunk("committed needle", 1))])?;
        let generation = store.generation()?;
        let error_chunks = vec![
            Ok(chunk("new needle", 2)),
            Err(anyhow!("simulated reader failure")),
        ];
        assert!(store.replace_source(&src, error_chunks).is_err());
        let filter = SearchFilter {
            collection: "project".into(),
            kind: "code".into(),
            ..SearchFilter::default()
        };
        assert_eq!(store.generation()?, generation);
        assert_eq!(store.search("committed", &filter, 10)?.1.len(), 1);
        assert!(store.search("new", &filter, 10)?.1.is_empty());
        Ok(())
    }

    #[test]
    fn invalidation_suppresses_then_verification_restores() -> Result<()> {
        let dir = tempdir()?;
        let store = Store::open(&dir.path().join("index.sqlite3"))?;
        let src = source("one", "v1");
        store.replace_source(&src, vec![Ok(chunk("needle", 1))])?;
        let filter = SearchFilter {
            collection: "project".into(),
            kind: "code".into(),
            ..SearchFilter::default()
        };
        store.invalidate_source("one")?;
        assert!(store.search("needle", &filter, 10)?.1.is_empty());
        store.mark_source_verified("one")?;
        assert_eq!(store.search("needle", &filter, 10)?.1.len(), 1);
        Ok(())
    }

    #[test]
    fn filters_are_applied_before_top_k_and_context_is_scoped() -> Result<()> {
        let dir = tempdir()?;
        let store = Store::open(&dir.path().join("index.sqlite3"))?;
        let mut a = source("a", "v1");
        a.kind = "session".into();
        a.path = "src/a.rs".into();
        let mut b = source("b", "v1");
        b.kind = "session".into();
        b.path = "docs/b.md".into();
        store.replace_source(
            &a,
            vec![Ok(chunk("same needle", 1)), Ok(chunk("same needle", 2))],
        )?;
        store.replace_source(&b, vec![Ok(chunk("same needle", 1))])?;
        let filter = SearchFilter {
            collection: "project".into(),
            kind: "session".into(),
            path_glob: Some("src/**".into()),
            ..SearchFilter::default()
        };
        let (_, hits) = store.search("needle", &filter, 1)?;
        assert_eq!(hits.len(), 1);
        assert!(hits[0].source.path.starts_with("src/"));
        let context = store.context(&hits[0].match_id, "project", 1, 1)?;
        assert!(context.iter().all(|hit| hit.source.key == "a"));
        Ok(())
    }

    #[test]
    fn context_expands_distinct_events_when_one_event_has_multiple_chunks() -> Result<()> {
        let dir = tempdir()?;
        let store = Store::open(&dir.path().join("index.sqlite3"))?;
        let mut src = source("events", "v1");
        src.kind = "session".into();
        let mut first = chunk("before part one", 1);
        first.event_id = Some("before".into());
        let mut first_tail = chunk("before part two", 2);
        first_tail.event_id = Some("before".into());
        let mut target = chunk("needle target", 3);
        target.event_id = Some("target".into());
        let mut target_tail = chunk("target continuation", 4);
        target_tail.event_id = Some("target".into());
        let mut after = chunk("after", 5);
        after.event_id = Some("after".into());
        store.replace_source(
            &src,
            vec![
                Ok(first),
                Ok(first_tail),
                Ok(target),
                Ok(target_tail),
                Ok(after),
            ],
        )?;
        let filter = SearchFilter {
            collection: "project".into(),
            kind: "session".into(),
            ..SearchFilter::default()
        };
        let (_, hits) = store.search("needle", &filter, 1)?;
        let context = store.context(&hits[0].match_id, "project", 1, 1)?;
        assert_eq!(context.len(), 5);
        assert_eq!(context[0].chunk.event_id.as_deref(), Some("before"));
        assert_eq!(context[4].chunk.event_id.as_deref(), Some("after"));
        Ok(())
    }
}
