use bm25_mcp::{
    content_cache::ContentCache,
    ingest,
    model::{Chunk, SearchFilter, Source},
    ranking::RankingOptions,
    store::Store,
    text::fold,
};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension};
use std::path::Path;

fn store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("index.sqlite3")).unwrap();
    (dir, store)
}

fn project(store: &Store, key: &str, path: &str, text: &str) {
    store
        .replace_source(
            &Source {
                key: key.into(),
                collection: "project".into(),
                path: path.into(),
                version: "v1".into(),
                kind: "project".into(),
            },
            [Ok(Chunk {
                text: text.into(),
                start_line: 1,
                end_line: 1,
                ..Default::default()
            })],
        )
        .unwrap();
}

fn filter() -> SearchFilter {
    SearchFilter {
        collection: "project".into(),
        kind: "project".into(),
        ..Default::default()
    }
}

fn boundary_text(token: &str) -> String {
    let prefix = "a ".repeat(8185);
    assert_eq!(prefix.len(), 16_370);
    format!("{prefix}{token}\n")
}

fn indexed_term(path: &Path, term: &str) -> Option<(i64, i64)> {
    Connection::open(path)
        .unwrap()
        .query_row(
            "SELECT p.tf,c.token_len
             FROM postings p
             JOIN terms t ON t.id=p.term_id
             JOIN chunks c ON c.id=p.chunk_id
             WHERE t.term=?1
             ORDER BY c.id
             LIMIT 1",
            [term],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .unwrap()
}

fn project_term_stats(path: &Path, collection: &str) -> Vec<(String, i64)> {
    let connection = Connection::open(path).unwrap();
    let mut statement = connection
        .prepare(
            "SELECT t.term,ts.doc_freq
             FROM terms t
             JOIN term_stats ts ON ts.term_id=t.id
             WHERE ts.collection=?1 AND ts.kind='project'
             ORDER BY t.term",
        )
        .unwrap();
    statement
        .query_map([collection], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
}

#[test]
fn ranked_search_preserves_raw_baseline_score_and_updates() {
    let (_dir, store) = store();
    project(&store, "a", "src/a.rs", "needle needle");
    project(&store, "b", "src/b.rs", "needle other words");
    let f = filter();
    let (_, raw) = store.search("needle", &f, 10).unwrap();
    let ranked = store
        .search_ranked_with("needle", &f, 10, RankingOptions::default())
        .unwrap();
    assert!(!raw.is_empty() && !ranked.hits.is_empty());
    for trace in &ranked.traces {
        let raw_hit = raw
            .iter()
            .find(|hit| hit.match_id == trace.match_id)
            .unwrap();
        assert_eq!(trace.baseline_bm25, raw_hit.score);
    }
    store.remove_source("a").unwrap();
    let (_, hits) = store.search_ranked("needle", &f, 10).unwrap();
    assert!(hits.iter().all(|hit| hit.source.key != "a"));
}

#[test]
fn exact_path_survives_large_fanout_and_glob_filter() {
    let (_dir, store) = store();
    for i in 0..240 {
        project(
            &store,
            &format!("k{i}"),
            &format!("src/{i}.rs"),
            "src/target.rs common token",
        );
    }
    project(&store, "target", "src/target.rs", "unrelated");
    let mut f = filter();
    f.path_glob = Some("src/*.rs".into());
    let ranked = store
        .search_ranked_with("src/target.rs", &f, 10, RankingOptions::default())
        .unwrap();
    assert!(ranked.candidate_count <= 200);
    assert_eq!(ranked.hits[0].source.path, "src/target.rs");
}

#[test]
fn path_literal_is_casefolded_and_sql_wildcards_are_literal() {
    let (_dir, store) = store();
    project(&store, "target", "src/Foo_%Bar.rs", "unrelated");
    project(&store, "wrong", "tests/Foo_%Bar.rs", "unrelated");
    let mut f = filter();
    f.path_glob = Some("src/*.rs".into());
    let ranked = store
        .search_ranked_with("SRC\\foo_%bar.RS", &f, 10, RankingOptions::default())
        .unwrap();
    assert_eq!(ranked.hits.len(), 1);
    assert_eq!(ranked.hits[0].source.key, "target");
}

#[test]
fn expanded_only_candidates_have_zero_original_baseline() {
    let (_dir, store) = store();
    project(&store, "expanded", "src/crash.rs", "crash recovery");
    let ranked = store
        .search_ranked_with("panic", &filter(), 10, RankingOptions::default())
        .unwrap();
    let trace = ranked
        .traces
        .iter()
        .find(|trace| trace.match_id == ranked.hits[0].match_id)
        .unwrap();
    assert_eq!(trace.baseline_bm25, 0.0);
    assert_eq!(trace.original_contribution, 0.0);
    assert!(trace.expanded_contribution > 0.0);
}

#[test]
fn weak_full_original_pool_reserves_space_for_alias_candidates() {
    let (_dir, store) = store();
    for i in 0..200 {
        project(
            &store,
            &format!("panic-{i}"),
            &format!("src/panic-{i}.rs"),
            "panic",
        );
    }
    project(&store, "alias", "src/crash-recovery.rs", "crash recovery");
    let ranked = store
        .search_ranked_with("panic", &filter(), 10, RankingOptions::default())
        .unwrap();
    assert!(ranked.candidate_count <= 200);
    assert!(ranked.hits.iter().any(|hit| hit.source.key == "alias"));
    let alias_trace = ranked
        .traces
        .iter()
        .find(|trace| {
            trace.match_id
                == ranked
                    .hits
                    .iter()
                    .find(|hit| hit.source.key == "alias")
                    .unwrap()
                    .match_id
        })
        .unwrap();
    assert_eq!(alias_trace.baseline_bm25, 0.0);
    assert!(alias_trace.expanded_contribution > 0.0);
}

#[test]
fn tokenizer_v3_store_is_invalidated_before_v4_reindex() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index.sqlite3");
    let old_generation;
    {
        let store = Store::open(&path).unwrap();
        project(&store, "compound", "src/Foo-Bar.rs", "Foo-Bar");
        old_generation = store.generation().unwrap();
    }
    {
        let connection = Connection::open(&path).unwrap();
        connection
            .execute(
                "UPDATE meta SET value=?1 WHERE key='tokenizer_version'",
                ["bm25-mcp-tokenizer-v3"],
            )
            .unwrap();
    }
    let store = Store::open(&path).unwrap();
    assert!(store.generation().unwrap() > old_generation);
    assert!(store.search("foo-bar", &filter(), 10).unwrap().1.is_empty());
    project(&store, "compound", "src/Foo-Bar.rs", "Foo-Bar");
    assert!(!store.search("foo-bar", &filter(), 10).unwrap().1.is_empty());
}

#[test]
fn hot_and_disk_ranked_results_match_under_pressure() {
    let (_dir, store) = store();
    project(&store, "a", "a.rs", "alpha beta");
    project(&store, "b", "b.rs", "alpha");
    let f = filter();
    let first = store.search_ranked("alpha", &f, 10).unwrap();
    store.set_memory_pressure(true);
    let second = store.search_ranked("alpha", &f, 10).unwrap();
    assert_eq!(
        first.1.iter().map(|h| &h.match_id).collect::<Vec<_>>(),
        second.1.iter().map(|h| &h.match_id).collect::<Vec<_>>()
    );
    assert_eq!(
        first.1.iter().map(|h| h.score).collect::<Vec<_>>(),
        second.1.iter().map(|h| h.score).collect::<Vec<_>>()
    );
}

#[test]
fn session_replica_group_reports_copies() {
    let (_dir, store) = store();
    for i in 0..205 {
        store
            .replace_source(
                &Source {
                    key: format!("s{i}"),
                    collection: "owner".into(),
                    path: format!("session-{i}"),
                    version: "v1".into(),
                    kind: "session".into(),
                },
                [Ok(Chunk {
                    text: "replica needle".into(),
                    field_kind: Some("message".into()),
                    agent: Some("codex".into()),
                    session_id: Some("s".into()),
                    event_id: Some("e".into()),
                    start_line: 1,
                    end_line: 1,
                    ..Default::default()
                })],
            )
            .unwrap();
    }
    let f = SearchFilter {
        collection: "owner".into(),
        kind: "session".into(),
        agent: Some("codex".into()),
        session_id: Some("s".into()),
        ..Default::default()
    };
    let result = store.search_ranked("needle", &f, 10).unwrap();
    assert!(!result.1.is_empty());
    assert_eq!(result.1[0].copy_count, 205);
}

#[test]
fn replacing_source_refreshes_token_statistics_without_stale_hits() {
    let (_dir, store) = store();
    project(&store, "a", "a.rs", "legacytoken");
    let f = filter();
    assert!(
        !store
            .search_ranked("legacytoken", &f, 10)
            .unwrap()
            .1
            .is_empty()
    );
    project(&store, "a", "a.rs", "refreshedtoken");
    assert!(
        store
            .search_ranked("legacytoken", &f, 10)
            .unwrap()
            .1
            .is_empty()
    );
    assert!(
        !store
            .search_ranked("refreshedtoken", &f, 10)
            .unwrap()
            .1
            .is_empty()
    );
}

#[test]
fn exact_definition_is_admitted_beyond_raw_top_two_hundred() {
    let (_dir, store) = store();
    let declaration = "pub struct SearchIndex {}";
    let mut chunks = (0..220)
        .map(|index| {
            Ok(Chunk {
                text: format!("mention {index} SearchIndex SearchIndex"),
                start_line: index + 1,
                end_line: index + 1,
                start_byte: index * 64,
                end_byte: index * 64 + 32,
                ..Default::default()
            })
        })
        .collect::<Vec<_>>();
    chunks.push(Ok(Chunk {
        text: declaration.into(),
        start_line: 221,
        end_line: 221,
        start_byte: 220 * 64,
        end_byte: 220 * 64 + declaration.len() as u64,
        ..Default::default()
    }));
    let source = Source {
        key: "definitions".into(),
        collection: "project".into(),
        path: "src/lib.rs".into(),
        version: "v1".into(),
        kind: "project".into(),
    };
    store.replace_source(&source, chunks).unwrap();

    let filter = filter();
    let (_, raw) = store.search("SearchIndex", &filter, 200).unwrap();
    assert_eq!(raw.len(), 200);
    assert!(raw.iter().all(|hit| hit.chunk.text != declaration));

    let ranked = store
        .search_ranked_with("SearchIndex", &filter, 10, RankingOptions::default())
        .unwrap();
    assert!(ranked.candidate_count <= 200);
    assert_eq!(ranked.hits[0].chunk.text, declaration);
}

#[test]
fn declaration_index_lifecycle_and_legacy_rebuild_preserve_raw_stats() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index.sqlite3");
    {
        let store = Store::open(&path).unwrap();
        project(
            &store,
            "declaration",
            "src/lib.rs",
            "pub struct SearchIndex {}",
        );
        let connection = Connection::open(&path).unwrap();
        let stats_before: (i64, i64) = connection
            .query_row(
                "SELECT doc_count,total_tokens FROM stats
                 WHERE collection='project' AND kind='project'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let term_stats_before = project_term_stats(&path, "project");
        assert!(!term_stats_before.is_empty());
        let raw_scores_before = store
            .search("SearchIndex", &filter(), 10)
            .unwrap()
            .1
            .into_iter()
            .map(|hit| hit.score)
            .collect::<Vec<_>>();
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM declarations WHERE symbol='searchindex'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );

        store.invalidate_source("declaration").unwrap();
        assert!(
            store
                .search_ranked("SearchIndex", &filter(), 10)
                .unwrap()
                .1
                .is_empty()
        );
        store.mark_source_verified("declaration").unwrap();
        assert!(
            !store
                .search_ranked("SearchIndex", &filter(), 10)
                .unwrap()
                .1
                .is_empty()
        );
        let restored_raw_scores = store
            .search("SearchIndex", &filter(), 10)
            .unwrap()
            .1
            .into_iter()
            .map(|hit| hit.score)
            .collect::<Vec<_>>();
        assert_eq!(restored_raw_scores, raw_scores_before);
        assert_eq!(project_term_stats(&path, "project"), term_stats_before);
        let stats_after_restore: (i64, i64) = Connection::open(&path)
            .unwrap()
            .query_row(
                "SELECT doc_count,total_tokens FROM stats
                 WHERE collection='project' AND kind='project'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(stats_after_restore, stats_before);
        project(
            &store,
            "declaration",
            "src/lib.rs",
            "pub struct Replacement {}",
        );
        assert!(
            store
                .search_ranked("SearchIndex", &filter(), 10)
                .unwrap()
                .1
                .is_empty()
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM declarations WHERE symbol='replacement'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
        store.remove_source("declaration").unwrap();
        let connection = Connection::open(&path).unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM declarations WHERE symbol='replacement'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        assert!(
            store
                .search_ranked("Replacement", &filter(), 10)
                .unwrap()
                .1
                .is_empty()
        );
    }

    let (stats_before_rebuild, term_stats_before_rebuild, raw_scores_before_rebuild) = {
        let store = Store::open(&path).unwrap();
        project(
            &store,
            "declaration",
            "src/lib.rs",
            "pub struct SearchIndex {}",
        );
        let connection = Connection::open(&path).unwrap();
        let stats: (i64, i64) = connection
            .query_row(
                "SELECT doc_count,total_tokens FROM stats
                 WHERE collection='project' AND kind='project'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let term_stats = project_term_stats(&path, "project");
        let raw_scores = store
            .search("SearchIndex", &filter(), 10)
            .unwrap()
            .1
            .into_iter()
            .map(|hit| hit.score)
            .collect::<Vec<_>>();
        assert_eq!(stats.0, 1);
        connection.execute("DROP TABLE declarations", []).unwrap();
        connection
            .execute("DELETE FROM meta WHERE key='declaration_index_version'", [])
            .unwrap();
        (stats, term_stats, raw_scores)
    };

    let store = Store::open(&path).unwrap();
    let connection = Connection::open(&path).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM declarations WHERE symbol='searchindex'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    let stats_after_rebuild: (i64, i64) = connection
        .query_row(
            "SELECT doc_count,total_tokens FROM stats
             WHERE collection='project' AND kind='project'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(stats_after_rebuild, stats_before_rebuild);
    assert_eq!(
        project_term_stats(&path, "project"),
        term_stats_before_rebuild
    );
    assert_eq!(
        store
            .search("SearchIndex", &filter(), 10)
            .unwrap()
            .1
            .into_iter()
            .map(|hit| hit.score)
            .collect::<Vec<_>>(),
        raw_scores_before_rebuild
    );
    assert_eq!(
        store
            .search_ranked("SearchIndex", &filter(), 10)
            .unwrap()
            .1
            .first()
            .unwrap()
            .chunk
            .text,
        "pub struct SearchIndex {}"
    );
}

#[test]
fn ranked_search_preserves_a_token_completed_after_the_storage_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir(&root).unwrap();
    let token = "needleboundary";
    let prefix = "a ".repeat(8190);
    assert_eq!(prefix.len(), 16_380);
    std::fs::write(root.join("boundary.rs"), format!("{prefix}{token}\n")).unwrap();

    let path = dir.path().join("index.sqlite3");
    let store = Store::open(&path).unwrap();
    let identity = ingest::project_identity(&root).unwrap();
    let report = ingest::scan_project(&root, &store, &identity.collection).unwrap();
    assert_eq!(report.error_count, 0);

    let filter = SearchFilter {
        collection: identity.collection,
        kind: "project".into(),
        ..Default::default()
    };
    let (_, raw) = store.search(token, &filter, 10).unwrap();
    assert!(!raw.is_empty());
    let indexed: i64 = Connection::open(&path)
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM postings p
             JOIN terms t ON t.id=p.term_id
             WHERE t.term=?1",
            [token],
            |row| row.get(0),
        )
        .unwrap();
    assert!(indexed > 0);

    let ranked = store
        .search_ranked_with(token, &filter, 10, RankingOptions::default())
        .unwrap();
    assert!(
        ranked
            .hits
            .iter()
            .any(|hit| hit.chunk.text.ends_with("leboundary\n"))
    );
    let trace = ranked
        .traces
        .iter()
        .find(|trace| ranked.hits.iter().any(|hit| hit.match_id == trace.match_id))
        .unwrap();
    assert!(trace.original_contribution > 0.0);
}

#[test]
fn compound_and_unicode_boundary_evidence_survives_store_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir(&root).unwrap();
    let compound = "SearchIndexBoundary";
    let unicode = "StraßeBoundary";
    std::fs::write(root.join("compound.rs"), boundary_text(compound)).unwrap();
    std::fs::write(root.join("unicode.rs"), boundary_text(unicode)).unwrap();

    let path = dir.path().join("index.sqlite3");
    let mut store = Store::open(&path).unwrap();
    let identity = ingest::project_identity(&root).unwrap();
    let collection = identity.collection.clone();
    let report = ingest::scan_project(&root, &store, &collection).unwrap();
    assert_eq!(report.error_count, 0);
    let filter = SearchFilter {
        collection: collection.clone(),
        kind: "project".into(),
        ..Default::default()
    };

    for (token, source_path) in [(compound, "compound.rs"), (unicode, "unicode.rs")] {
        let normalized = fold(token);
        assert_eq!(indexed_term(&path, &normalized), Some((1, 2)));
        let (_, raw) = store.search(token, &filter, 10).unwrap();
        let raw_hit = raw
            .iter()
            .find(|hit| hit.source.path == source_path && hit.chunk.start_byte >= 16_384)
            .unwrap();
        let ranked = store
            .search_ranked_with(token, &filter, 10, RankingOptions::default())
            .unwrap();
        let hit = ranked
            .hits
            .iter()
            .find(|hit| hit.source.path == source_path && hit.chunk.start_byte >= 16_384)
            .unwrap();
        assert_eq!(hit.chunk.text, raw_hit.chunk.text);
        let trace = ranked
            .traces
            .iter()
            .find(|trace| trace.match_id == hit.match_id)
            .unwrap();
        assert!(trace.original_contribution > 0.0);
    }

    let compound_source = store
        .sources(&collection, "project")
        .unwrap()
        .into_iter()
        .find(|source| source.path == "compound.rs")
        .unwrap();
    let compound_term = fold(compound);
    let mut compound_filter = filter.clone();
    compound_filter.path_glob = Some("compound.rs".into());
    let compound_snapshot = indexed_term(&path, &compound_term).unwrap();
    let raw_before_lifecycle = store
        .search(compound, &compound_filter, 10)
        .unwrap()
        .1
        .into_iter()
        .map(|hit| hit.score)
        .collect::<Vec<_>>();
    let term_stats_before_lifecycle = project_term_stats(&path, &collection);
    assert!(!term_stats_before_lifecycle.is_empty());

    store.invalidate_source(&compound_source.key).unwrap();
    assert!(
        store
            .search(compound, &compound_filter, 10)
            .unwrap()
            .1
            .is_empty()
    );
    assert!(
        store
            .search_ranked(compound, &compound_filter, 10)
            .unwrap()
            .1
            .is_empty()
    );
    store.mark_source_verified(&compound_source.key).unwrap();
    assert!(
        !store
            .search(compound, &compound_filter, 10)
            .unwrap()
            .1
            .is_empty()
    );
    assert_eq!(indexed_term(&path, &compound_term), Some(compound_snapshot));
    assert_eq!(
        store
            .search(compound, &compound_filter, 10)
            .unwrap()
            .1
            .into_iter()
            .map(|hit| hit.score)
            .collect::<Vec<_>>(),
        raw_before_lifecycle
    );
    assert_eq!(
        project_term_stats(&path, &collection),
        term_stats_before_lifecycle
    );
    assert!(
        !store
            .search_ranked(compound, &compound_filter, 10)
            .unwrap()
            .1
            .is_empty()
    );

    let replacement = "ReplacementMarkerToken";
    std::fs::write(root.join("compound.rs"), boundary_text(replacement)).unwrap();
    ingest::scan_project(&root, &store, &collection).unwrap();
    assert!(
        store
            .search(compound, &compound_filter, 10)
            .unwrap()
            .1
            .is_empty()
    );
    assert!(
        !store
            .search(replacement, &compound_filter, 10)
            .unwrap()
            .1
            .is_empty()
    );
    assert_eq!(indexed_term(&path, &compound_term), None);
    assert_eq!(indexed_term(&path, &fold(replacement)), Some((1, 3)));
    assert!(
        store
            .search_ranked(compound, &compound_filter, 10)
            .unwrap()
            .1
            .is_empty()
    );
    assert!(
        !store
            .search_ranked(replacement, &compound_filter, 10)
            .unwrap()
            .1
            .is_empty()
    );

    std::fs::remove_file(root.join("compound.rs")).unwrap();
    ingest::scan_project(&root, &store, &collection).unwrap();
    assert!(
        store
            .search(replacement, &compound_filter, 10)
            .unwrap()
            .1
            .is_empty()
    );
    assert_eq!(indexed_term(&path, &fold(replacement)), None);
    assert!(
        store
            .search_ranked(replacement, &compound_filter, 10)
            .unwrap()
            .1
            .is_empty()
    );

    std::fs::write(root.join("compound.rs"), boundary_text(compound)).unwrap();
    ingest::scan_project(&root, &store, &collection).unwrap();
    {
        let connection = Connection::open(&path).unwrap();
        connection
            .execute(
                "UPDATE meta SET value='bm25-mcp-tokenizer-v3'
                 WHERE key='tokenizer_version'",
                [],
            )
            .unwrap();
    }
    drop(store);
    store = Store::open(&path).unwrap();
    assert!(
        store
            .search(compound, &compound_filter, 10)
            .unwrap()
            .1
            .is_empty()
    );
    assert!(
        store
            .search_ranked(compound, &compound_filter, 10)
            .unwrap()
            .1
            .is_empty()
    );
    ingest::scan_project(&root, &store, &collection).unwrap();
    assert!(
        !store
            .search(compound, &compound_filter, 10)
            .unwrap()
            .1
            .is_empty()
    );
    assert_eq!(indexed_term(&path, &compound_term), Some(compound_snapshot));
    assert!(
        !store
            .search_ranked(compound, &compound_filter, 10)
            .unwrap()
            .1
            .is_empty()
    );
}

#[test]
fn content_cache_restore_rebuilds_definition_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir(&root).unwrap();
    let text = "pub struct CachedDefinition {}\n";
    std::fs::write(root.join("cached.rs"), text).unwrap();
    let path = dir.path().join("index.sqlite3");
    let cache_path = dir.path().join("content-cache");
    let store = Store::open(&path).unwrap();
    let identity = ingest::project_identity(&root).unwrap();
    let collection = identity.collection.clone();
    let mut cache = ContentCache::open(&cache_path).unwrap();
    let first = ingest::scan_project_with_cache(&root, &store, &collection, &mut cache).unwrap();
    assert_eq!(first.diagnostics.get("content_cache_miss"), Some(&1));
    let filter = SearchFilter {
        collection: collection.clone(),
        kind: "project".into(),
        ..Default::default()
    };
    let first_ranked = store
        .search_ranked("CachedDefinition", &filter, 10)
        .unwrap();
    assert_eq!(first_ranked.1[0].source.path, "cached.rs");
    let first_raw_scores = store
        .search("CachedDefinition", &filter, 10)
        .unwrap()
        .1
        .into_iter()
        .map(|hit| hit.score)
        .collect::<Vec<_>>();
    let first_term_stats = project_term_stats(&path, &collection);
    assert!(!first_term_stats.is_empty());
    let source_key = store.sources(&collection, "project").unwrap()[0]
        .key
        .clone();
    store.remove_source(&source_key).unwrap();

    drop(cache);
    let mut cache = ContentCache::open(&cache_path).unwrap();
    let second = ingest::scan_project_with_cache(&root, &store, &collection, &mut cache).unwrap();
    assert_eq!(second.diagnostics.get("content_cache_hit"), Some(&1));
    let restored = store
        .search_ranked("CachedDefinition", &filter, 10)
        .unwrap();
    assert_eq!(restored.1[0].source.path, "cached.rs");
    assert_eq!(
        store
            .search("CachedDefinition", &filter, 10)
            .unwrap()
            .1
            .into_iter()
            .map(|hit| hit.score)
            .collect::<Vec<_>>(),
        first_raw_scores
    );
    assert_eq!(project_term_stats(&path, &collection), first_term_stats);
    assert_eq!(
        Connection::open(&path)
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM declarations WHERE symbol='cacheddefinition'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
}

#[test]
fn declaration_lane_filters_before_limit_and_bounds_deterministically() {
    let (dir, store) = store();
    for index in 0..66 {
        let eligible = index < 45;
        let (collection, path, agent, session_id, timestamp) = if eligible {
            (
                "project",
                format!("src/keep/{index}.rs"),
                "good",
                "session-good",
                "2026-01-15T00:00:00Z",
            )
        } else if index < 48 {
            (
                "project",
                format!("src/drop/{index}.rs"),
                "good",
                "session-good",
                "2026-01-15T00:00:00Z",
            )
        } else if index < 51 {
            (
                "project",
                format!("src/keep/{index}.rs"),
                "bad",
                "session-good",
                "2026-01-15T00:00:00Z",
            )
        } else if index < 54 {
            (
                "project",
                format!("src/keep/{index}.rs"),
                "good",
                "session-bad",
                "2026-01-15T00:00:00Z",
            )
        } else if index < 57 {
            (
                "project",
                format!("src/keep/{index}.rs"),
                "good",
                "session-good",
                "2025-01-15T00:00:00Z",
            )
        } else if index < 60 {
            (
                "other",
                format!("src/keep/{index}.rs"),
                "good",
                "session-good",
                "2026-01-15T00:00:00Z",
            )
        } else if index < 63 {
            (
                "project",
                format!("src/keep/{index}.rs"),
                "good",
                "session-good",
                "2026-02-15T00:00:00Z",
            )
        } else {
            (
                "project",
                format!("src/keep/{index}.rs"),
                "good",
                "session-good",
                "2026-01-15T00:00:00Z",
            )
        };
        store
            .replace_source(
                &Source {
                    key: format!("definition-{index}"),
                    collection: collection.into(),
                    path,
                    version: "v1".into(),
                    kind: "project".into(),
                },
                [Ok(Chunk {
                    text: format!(
                        "pub struct SearchIndex {{ const Marker{index}: usize = {index}; }}"
                    ),
                    agent: Some(agent.into()),
                    session_id: Some(session_id.into()),
                    timestamp: Some(timestamp.into()),
                    ..Default::default()
                })],
            )
            .unwrap();
        if index >= 63 {
            store
                .invalidate_source(&format!("definition-{index}"))
                .unwrap();
        }
    }
    let mut filter = filter();
    filter.path_glob = Some("src/keep/*.rs".into());
    filter.agent = Some("good".into());
    filter.session_id = Some("session-good".into());
    filter.after = Some("2026-01-01T00:00:00Z".into());
    filter.before = Some("2026-02-01T00:00:00Z".into());
    let eligible_declarations: i64 = Connection::open(dir.path().join("index.sqlite3"))
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM declarations d
             JOIN chunks c ON c.id=d.chunk_id
             JOIN sources s ON s.key=c.source_key
             WHERE d.symbol='searchindex' AND s.collection='project'
               AND s.kind='project' AND s.eligible=1
               AND s.path GLOB 'src/keep/*.rs'
               AND c.agent='good' AND c.session_id='session-good'
               AND c.timestamp>=?1 AND c.timestamp<?2",
            ["2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(eligible_declarations, 45);

    let first = store
        .search_ranked_with("SearchIndex", &filter, 200, RankingOptions::default())
        .unwrap();
    assert_eq!(first.admission_counts.definitions, 40);
    assert!(first.candidate_count <= 200);
    assert!(first.hits.iter().all(|hit| {
        hit.source.collection == "project"
            && hit.source.path.starts_with("src/keep/")
            && hit.chunk.agent.as_deref() == Some("good")
            && hit.chunk.session_id.as_deref() == Some("session-good")
            && hit
                .chunk
                .timestamp
                .as_deref()
                .is_some_and(|timestamp| timestamp >= "2026-01-01T00:00:00Z")
            && hit
                .chunk
                .timestamp
                .as_deref()
                .is_some_and(|timestamp| timestamp < "2026-02-01T00:00:00Z")
    }));
    let second = store
        .search_ranked_with("SearchIndex", &filter, 200, RankingOptions::default())
        .unwrap();
    assert_eq!(
        first
            .hits
            .iter()
            .map(|hit| (&hit.match_id, hit.score))
            .collect::<Vec<_>>(),
        second
            .hits
            .iter()
            .map(|hit| (&hit.match_id, hit.score))
            .collect::<Vec<_>>()
    );
}

#[test]
fn ranked_search_with_at_is_repeatable_for_session_decay() {
    let (_dir, store) = store();
    for (key, text, timestamp) in [
        ("old", "needle old", "2020-01-01T00:00:00Z"),
        ("new", "needle new", "2026-01-01T00:00:00Z"),
    ] {
        store
            .replace_source(
                &Source {
                    key: key.into(),
                    collection: "owner".into(),
                    path: key.into(),
                    version: "v1".into(),
                    kind: "session".into(),
                },
                [Ok(Chunk {
                    text: text.into(),
                    timestamp: Some(timestamp.into()),
                    role: Some("assistant".into()),
                    ..Default::default()
                })],
            )
            .unwrap();
    }
    let filter = SearchFilter {
        collection: "owner".into(),
        kind: "session".into(),
        ..Default::default()
    };
    let now: DateTime<Utc> = "2026-09-21T00:00:00Z".parse().unwrap();
    let first = store
        .search_ranked_with_at("needle", &filter, 10, RankingOptions::default(), now)
        .unwrap();
    let second = store
        .search_ranked_with_at("needle", &filter, 10, RankingOptions::default(), now)
        .unwrap();
    assert_eq!(
        first
            .hits
            .iter()
            .map(|hit| (&hit.match_id, hit.score))
            .collect::<Vec<_>>(),
        second
            .hits
            .iter()
            .map(|hit| (&hit.match_id, hit.score))
            .collect::<Vec<_>>()
    );
    assert!(first.hits[0].score > first.hits[1].score);
}
