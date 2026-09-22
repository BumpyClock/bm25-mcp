use bm25_mcp::{
    model::{Chunk, SearchFilter, Source},
    store::Store,
    text::tokenize,
};
use bm25_turbo::BM25Builder;
use std::collections::HashMap;

#[test]
fn durable_replacements_and_deletions_match_fresh_upstream_builds() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("oracle.sqlite3")).unwrap();
    let mut docs = vec![
        "parseHTTPResponse handles timeout".to_owned(),
        "http server timeout timeout".to_owned(),
        "snake_case argument parser".to_owned(),
    ];
    let filter = SearchFilter {
        collection: "project".into(),
        kind: "project".into(),
        ..Default::default()
    };
    for round in 0..12 {
        for (i, text) in docs.iter().enumerate() {
            store
                .replace_source(
                    &Source {
                        key: i.to_string(),
                        collection: "project".into(),
                        path: i.to_string(),
                        version: format!("r{round}"),
                        kind: "project".into(),
                    },
                    [Ok(Chunk {
                        text: text.clone(),
                        ..Default::default()
                    })],
                )
                .unwrap();
        }
        store.compact().unwrap();
        let tokenized: Vec<_> = docs.iter().map(|s| tokenize(s)).collect();
        let oracle = BM25Builder::new().build_from_tokens(&tokenized).unwrap();
        for query in [
            "timeout",
            "parseHTTPResponse",
            "snake_case",
            "http timeout",
            "timeout timeout",
            "nomatchqvx",
        ] {
            let tokens = tokenize(query);
            let expected = oracle.search_tokens(&tokens, docs.len()).unwrap();
            let expected: HashMap<_, _> = expected
                .doc_ids
                .into_iter()
                .zip(expected.scores)
                .filter(|(_, score)| *score > 0.0)
                .map(|(id, score)| (id.to_string(), score))
                .collect();
            let (_, hot) = store.search(query, &filter, 50).unwrap();
            store.set_memory_pressure(true);
            let (_, actual) = store.search(query, &filter, 50).unwrap();
            store.set_memory_pressure(false);
            assert_eq!(
                hot.iter()
                    .map(|h| (&h.match_id, h.score))
                    .collect::<Vec<_>>(),
                actual
                    .iter()
                    .map(|h| (&h.match_id, h.score))
                    .collect::<Vec<_>>()
            );
            assert_eq!(actual.len(), expected.len(), "round {round}, {query}");
            for hit in actual {
                assert!(
                    (hit.score - expected[&hit.source.key]).abs() < 1e-6,
                    "round {round}, {query}: {} != {}",
                    hit.score,
                    expected[&hit.source.key]
                );
            }
        }
        if round % 3 == 2 {
            let id = docs.len() - 1;
            store.remove_source(&id.to_string()).unwrap();
            docs.pop();
        } else {
            docs.push(format!("newIdentifier{round} http error timeout"));
        }
        docs[0] = format!("parseHTTPResponse timeout round{round}");
    }
}

fn assert_exact_statistics(path: &std::path::Path) {
    let conn = rusqlite::Connection::open(path).unwrap();
    let rows = |sql: &str| {
        conn.prepare(sql)
            .unwrap()
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, i64>(3)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    };
    assert_eq!(
        rows(
            "SELECT collection,kind,doc_count,total_tokens FROM stats WHERE doc_count>0 ORDER BY 1,2"
        ),
        rows(
            "SELECT s.collection,s.kind,COUNT(c.id),SUM(c.token_len) FROM sources s JOIN chunks c ON c.source_key=s.key WHERE s.eligible=1 GROUP BY 1,2 ORDER BY 1,2"
        )
    );
    assert_eq!(
        rows("SELECT collection,kind,term_id,doc_freq FROM term_stats ORDER BY 1,2,3"),
        rows(
            "SELECT s.collection,s.kind,p.term_id,COUNT(*) FROM sources s JOIN chunks c ON c.source_key=s.key JOIN postings p ON p.chunk_id=c.id WHERE s.eligible=1 GROUP BY 1,2,3 ORDER BY 1,2,3"
        )
    );
    assert_eq!(
        rows("SELECT source_key,'',doc_count,total_tokens FROM source_stats ORDER BY 1"),
        rows(
            "SELECT s.key,'',COUNT(c.id),COALESCE(SUM(c.token_len),0) FROM sources s LEFT JOIN chunks c ON c.source_key=s.key GROUP BY s.key ORDER BY 1"
        )
    );
    assert_eq!(
        rows("SELECT source_key,'',term_id,doc_freq FROM source_term_stats ORDER BY 1,3"),
        rows(
            "SELECT c.source_key,'',p.term_id,COUNT(*) FROM chunks c JOIN postings p ON p.chunk_id=c.id GROUP BY 1,3 ORDER BY 1,3"
        )
    );
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |r| r
            .get::<_, i64>(
            0
        ))
        .unwrap(),
        0
    );
}

#[test]
fn source_summaries_preserve_chunk_frequencies_across_mutations_and_migration() -> anyhow::Result<()>
{
    use bm25_mcp::store::SessionCheckpoint;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("summaries.sqlite3");
    let mut store = Store::open(&path)?;
    let mut source = Source {
        key: "history".into(),
        collection: "owner".into(),
        kind: "session".into(),
        path: "history.jsonl".into(),
        version: "0".into(),
    };
    let chunk = || {
        Ok(Chunk {
            text: "repeated repeated shared".into(),
            ..Default::default()
        })
    };
    let checkpoint = SessionCheckpoint {
        offset: 10,
        state: "original".into(),
    };
    store.replace_session(&source, (0..50).map(|_| chunk()), &checkpoint)?;
    let empty = Source {
        key: "empty".into(),
        ..source.clone()
    };
    store.replace_source(&empty, std::iter::empty())?;
    assert_exact_statistics(&path);
    let conn = rusqlite::Connection::open(&path)?;
    assert_eq!(conn.query_row("SELECT doc_freq FROM source_term_stats st JOIN terms t ON t.id=st.term_id WHERE st.source_key='history' AND t.term='repeated'", [], |r| r.get::<_, i64>(0))?, 50);
    drop(conn);

    for round in 1..7 {
        if round % 2 == 0 {
            store.invalidate_source(&source.key)?;
            store.invalidate_source(&source.key)?;
            assert_exact_statistics(&path);
        }
        let previous = source.version.clone();
        source.version = round.to_string();
        store.append_session(&source, &previous, [chunk()], &checkpoint)?;
        assert_exact_statistics(&path);
    }
    store.invalidate_collection("owner", "session")?;
    assert_exact_statistics(&path);
    store.mark_source_verified(&source.key)?;
    assert_exact_statistics(&path);
    store.invalidate_source(&source.key)?;
    drop(store);

    // Simulate an existing v2 cache with both quarantined and empty sources.
    let conn = rusqlite::Connection::open(&path)?;
    conn.execute_batch("DROP TABLE source_term_stats; DROP TABLE source_stats; UPDATE meta SET value='bm25-mcp-store-v2' WHERE key='schema_version'")?;
    let generation: String =
        conn.query_row("SELECT value FROM meta WHERE key='generation'", [], |r| {
            r.get(0)
        })?;
    drop(conn);
    store = Store::open(&path)?;
    assert_exact_statistics(&path);
    assert_eq!(
        store.session_checkpoint(&source.key)?,
        Some(checkpoint.clone())
    );
    assert_eq!(store.generation()?.to_string(), generation);
    store.mark_source_verified(&source.key)?;
    assert_exact_statistics(&path);
    let previous = source.version.clone();
    source.version = "rollback".into();
    assert!(
        store
            .append_session(
                &source,
                &previous,
                [chunk(), Err(anyhow::anyhow!("rollback"))],
                &checkpoint
            )
            .is_err()
    );
    assert_exact_statistics(&path);
    source.collection = "moved".into();
    store.replace_session(
        &source,
        [Ok(Chunk {
            text: "replacement shared".into(),
            ..Default::default()
        })],
        &checkpoint,
    )?;
    assert_exact_statistics(&path);
    store.remove_source(&source.key)?;
    store.remove_source(&empty.key)?;
    store.compact()?;
    assert_exact_statistics(&path);
    Ok(())
}

#[test]
fn source_summary_backfill_failure_rolls_back_and_retries() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("backfill.sqlite3");
    let store = Store::open(&path)?;
    store.replace_source(
        &Source {
            key: "source".into(),
            collection: "owner".into(),
            kind: "project".into(),
            path: "file".into(),
            version: "1".into(),
        },
        [Ok(Chunk {
            text: "retainedmarker".into(),
            ..Default::default()
        })],
    )?;
    drop(store);
    let conn = rusqlite::Connection::open(&path)?;
    conn.execute_batch("UPDATE meta SET value='bm25-mcp-store-v2' WHERE key='schema_version'; DELETE FROM source_term_stats; DELETE FROM source_stats; CREATE TRIGGER reject_backfill BEFORE INSERT ON source_stats BEGIN SELECT RAISE(ABORT,'injected backfill failure'); END;")?;
    assert!(Store::open(&path).is_err());
    assert_eq!(
        conn.query_row(
            "SELECT value FROM meta WHERE key='schema_version'",
            [],
            |r| r.get::<_, String>(0)
        )?,
        "bm25-mcp-store-v2"
    );
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM chunks", [], |r| r.get::<_, i64>(0))?,
        1
    );
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM source_stats", [], |r| r
            .get::<_, i64>(0))?,
        0
    );
    conn.execute_batch("DROP TRIGGER reject_backfill")?;
    let store = Store::open(&path)?;
    assert_exact_statistics(&path);
    assert_eq!(
        store
            .search(
                "retainedmarker",
                &SearchFilter {
                    collection: "owner".into(),
                    kind: "project".into(),
                    ..Default::default()
                },
                10
            )?
            .1
            .len(),
        1
    );
    Ok(())
}
