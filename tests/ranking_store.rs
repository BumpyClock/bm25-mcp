use bm25_mcp::{
    model::{Chunk, SearchFilter, Source},
    ranking::RankingOptions,
    store::Store,
};
use rusqlite::Connection;

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
