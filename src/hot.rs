//! Bounded, generation-keyed numeric posting cache above the authoritative store.
use crate::model::{Hit, SearchFilter};
use anyhow::Result;
use globset::GlobSet;
use rusqlite::{OptionalExtension, Transaction, params};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

const CACHE_BYTES: usize = 64 * 1024 * 1024;
const TERM_BYTES: usize = 16 * 1024 * 1024;
const CANDIDATES: usize = 131_072;

#[derive(Clone, Hash, Eq, PartialEq)]
struct Key {
    collection: String,
    kind: String,
    term: i64,
}
struct Posting {
    id: String,
    tf: u32,
    length: u64,
    path: String,
    agent: Option<String>,
    session: Option<String>,
    timestamp: Option<String>,
}
struct Entry {
    postings: Vec<Posting>,
    bytes: usize,
}
#[derive(Default)]
pub(super) struct Cache {
    generation: u64,
    entries: HashMap<Key, Arc<Entry>>,
    order: VecDeque<Key>,
    bytes: usize,
    pressure: bool,
}
impl Cache {
    pub fn pressure(&mut self, pressure: bool) {
        self.pressure = pressure;
        if pressure {
            self.clear();
        }
    }
    fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
        self.bytes = 0;
    }
    fn get(&mut self, generation: u64, key: &Key) -> Option<Arc<Entry>> {
        if self.generation != generation {
            self.clear();
            self.generation = generation;
        }
        self.entries.get(key).cloned()
    }
    fn insert(&mut self, generation: u64, key: Key, entry: Arc<Entry>) {
        if self.pressure || generation != self.generation || self.entries.contains_key(&key) {
            return;
        }
        while self.bytes + entry.bytes > CACHE_BYTES {
            let Some(old) = self.order.pop_front() else {
                break;
            };
            if let Some(entry) = self.entries.remove(&old) {
                self.bytes -= entry.bytes;
            }
        }
        self.bytes += entry.bytes;
        self.order.push_back(key.clone());
        self.entries.insert(key, entry);
    }
}

pub(super) fn search(
    cache: &std::sync::Mutex<Cache>,
    tx: &Transaction<'_>,
    generation: u64,
    terms: &[String],
    filter: &SearchFilter,
    glob: Option<&GlobSet>,
    limit: usize,
) -> Result<Option<Vec<Hit>>> {
    // Session results need whole-event identity and exact sequence comparison
    // before top-k. The bounded posting cache only knows individual chunks;
    // send these queries through the SQLite finalizer instead of returning a
    // prematurely truncated candidate page.
    if cache.lock().unwrap().pressure {
        return Ok(None);
    }
    let stats: Option<(i64, i64)> = tx
        .query_row(
            "SELECT doc_count,total_tokens FROM stats WHERE collection=?1 AND kind=?2",
            params![filter.collection, filter.kind],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let Some((documents, total)) = stats else {
        return Ok(Some(Vec::new()));
    };
    let mut weights = HashMap::<&str, u32>::new();
    for term in terms {
        *weights.entry(term).or_default() += 1;
    }
    let mut scores = HashMap::<String, f64>::new();
    for (term, weight) in weights {
        let identity: Option<(i64, i64)> = tx
            .query_row(
                "SELECT t.id,ts.doc_freq FROM terms t JOIN term_stats ts ON ts.term_id=t.id
             WHERE t.term=?1 AND ts.collection=?2 AND ts.kind=?3",
                params![term, filter.collection, filter.kind],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let Some((term_id, frequency)) = identity else {
            continue;
        };
        if frequency > CANDIDATES as i64 {
            return Ok(None);
        }
        let key = Key {
            collection: filter.collection.clone(),
            kind: filter.kind.clone(),
            term: term_id,
        };
        let cached = cache.lock().unwrap().get(generation, &key);
        let entry = if let Some(entry) = cached {
            entry
        } else {
            let mut statement=tx.prepare(
                "SELECT c.match_id,p.tf,c.token_len,s.path,c.agent,c.session_id,c.timestamp
                 FROM postings p JOIN chunks c ON c.id=p.chunk_id JOIN sources s ON s.key=c.source_key
                 WHERE p.term_id=?1 AND s.collection=?2 AND s.kind=?3 AND s.eligible=1")?;
            let mut rows = statement.query(params![term_id, filter.collection, filter.kind])?;
            let mut postings = Vec::new();
            let mut bytes = 0;
            while let Some(row) = rows.next()? {
                let posting = Posting {
                    id: row.get(0)?,
                    tf: row.get(1)?,
                    length: row.get::<_, i64>(2)? as u64,
                    path: row.get(3)?,
                    agent: row.get(4)?,
                    session: row.get(5)?,
                    timestamp: row.get(6)?,
                };
                bytes += 2 * std::mem::size_of::<Posting>()
                    + posting.id.capacity()
                    + posting.path.capacity()
                    + posting.agent.as_ref().map_or(0, String::capacity)
                    + posting.session.as_ref().map_or(0, String::capacity)
                    + posting.timestamp.as_ref().map_or(0, String::capacity);
                if bytes > TERM_BYTES {
                    return Ok(None);
                }
                postings.push(posting);
            }
            let entry = Arc::new(Entry { postings, bytes });
            cache.lock().unwrap().insert(generation, key, entry.clone());
            entry
        };
        for posting in &entry.postings {
            if glob.is_some_and(|g| !g.is_match(&posting.path))
                || filter
                    .agent
                    .as_ref()
                    .is_some_and(|v| posting.agent.as_ref() != Some(v))
                || filter
                    .session_id
                    .as_ref()
                    .is_some_and(|v| posting.session.as_ref() != Some(v))
                || filter
                    .after
                    .as_ref()
                    .is_some_and(|v| posting.timestamp.as_ref().is_none_or(|t| t < v))
                || filter
                    .before
                    .as_ref()
                    .is_some_and(|v| posting.timestamp.as_ref().is_none_or(|t| t >= v))
            {
                continue;
            }
            let score = super::upstream::scoring::lucene_score(
                posting.tf,
                posting.length,
                total as f64 / documents as f64,
                documents as u64,
                frequency as u64,
            ) * weight as f32;
            *scores.entry(posting.id.clone()).or_default() += f64::from(score);
            if scores.len() > CANDIDATES {
                return Ok(None);
            }
        }
    }
    if filter.kind == "session" {
        super::create_query_tables(tx)?;
        tx.execute("DELETE FROM bm25_query_accum", [])?;
        let mut insert = tx.prepare_cached(
            "INSERT INTO bm25_query_accum(chunk_id,score) SELECT id,?2 FROM chunks WHERE match_id=?1",
        )?;
        for (id, score) in scores {
            insert.execute(params![id, score])?;
        }
        return Ok(Some(super::load_top_session_hits(tx, limit as i64)?));
    }
    let mut scores: Vec<_> = scores.into_iter().collect();
    let compare =
        |(a, sa): &(String, f64), (b, sb): &(String, f64)| sb.total_cmp(sa).then_with(|| a.cmp(b));
    if scores.len() > limit {
        scores.select_nth_unstable_by(limit, compare);
        scores.truncate(limit);
    }
    scores.sort_unstable_by(compare);
    let mut statement=tx.prepare(
        "SELECT c.match_id,?2,c.source_key,c.source_version,c.ordinal,c.text,c.start_line,c.end_line,c.start_byte,c.end_byte,
         c.agent,c.session_id,c.event_id,c.timestamp,c.role,c.tool,s.collection,s.path,s.version,s.kind,s.verified_at,c.field_kind
         FROM chunks c JOIN sources s ON s.key=c.source_key WHERE c.match_id=?1 AND s.eligible=1")?;
    let mut hits = Vec::with_capacity(scores.len());
    for (id, score) in scores {
        hits.push(statement.query_row(params![id, score], super::hit_from_row)?);
    }
    Ok(Some(hits))
}
