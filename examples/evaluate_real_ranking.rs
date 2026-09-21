//! Read-only ranking smoke test over a source tree. The index is disposable.
use anyhow::Result;
use bm25_mcp::{
    ingest::scan_project,
    model::{Hit, SearchFilter},
    store::Store,
};
use chrono::{DateTime, Utc};
use serde_json::json;
use std::{
    env, fs,
    path::{Path, PathBuf},
    time::Instant,
};

const EVALUATION_NOW: &str = "2026-09-21T00:00:00Z";

fn evaluation_now() -> DateTime<Utc> {
    EVALUATION_NOW
        .parse::<DateTime<Utc>>()
        .expect("fixed evaluation timestamp is valid")
}

fn filter(collection: &str) -> SearchFilter {
    SearchFilter {
        collection: collection.into(),
        kind: "project".into(),
        ..Default::default()
    }
}

fn paths(hits: &[Hit]) -> Vec<String> {
    hits.iter().map(|hit| hit.source.path.clone()).collect()
}

fn signature(hits: &[Hit]) -> Vec<(String, u32)> {
    hits.iter()
        .map(|hit| (hit.match_id.clone(), hit.score.to_bits()))
        .collect()
}

fn admission_counts(result: &bm25_mcp::ranking::RankedSearch) -> serde_json::Value {
    json!({
        "lexical": result.admission_counts.lexical,
        "definitions": result.admission_counts.definitions,
        "path": result.admission_counts.path,
        "expansion": result.admission_counts.expansion,
    })
}

fn write_boundary_fixture(root: &Path) -> Result<()> {
    fs::create_dir_all(root.join("mentions"))?;
    fs::create_dir_all(root.join("src"))?;
    fs::write(
        root.join("src/search_index.rs"),
        "pub struct SearchIndex { entries: Vec<String> }\n",
    )?;
    for index in 0..240 {
        fs::write(
            root.join(format!("mentions/{index:03}.rs")),
            format!(
                "// mention saturation {index}\n{}\n",
                "SearchIndex ".repeat(80)
            ),
        )?;
    }

    let mut normal = "p".repeat(16_380);
    normal.push(' ');
    normal.push_str("crossBoundaryIdentifier\n");
    fs::write(root.join("src/boundary-normal.rs"), normal)?;

    let mut compound = "p".repeat(16_380);
    compound.push(' ');
    compound.push_str("Namespace::HTTPServer.rs\n");
    fs::write(root.join("src/boundary-compound.rs"), compound)?;

    let mut unicode = "p".repeat(16_380);
    unicode.push(' ');
    unicode.push_str("StraßeBoundary\n");
    fs::write(root.join("src/boundary-unicode.rs"), unicode)?;

    let multi = format!(
        "multiFirstMarker\n{}\nmultiSecondMarker\n",
        "padding ".repeat(3_000)
    );
    fs::write(root.join("src/multi.rs"), multi)?;
    fs::write(
        root.join("src/qualified.rs"),
        "fn Foo::Bar::bazValue() { refresh_cache(); }\n",
    )?;
    fs::write(root.join("notes.txt"), "why does this happen\n")?;
    fs::write(root.join("src/mutable.rs"), "mutationoldqxz\n")?;
    Ok(())
}

fn run_regressions() -> Result<serde_json::Value> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().join("real-regression");
    let db = dir.path().join("real-regression.sqlite3");
    write_boundary_fixture(&root)?;
    let store = Store::open(&db)?;
    let collection = "real-regression";
    let fixture_filter = filter(collection);
    let report = scan_project(&root, &store, collection)?;

    let raw_top_200 = store.search("SearchIndex", &fixture_filter, 200)?.1;
    let raw_all = store.search("SearchIndex", &fixture_filter, 1_000)?.1;
    let declaration_path = "src/search_index.rs";
    let raw_missing_declaration = !raw_top_200
        .iter()
        .any(|hit| hit.source.path == declaration_path);
    assert!(
        raw_missing_declaration,
        "raw top-200 must miss the low-frequency declaration"
    );
    assert!(
        raw_all
            .iter()
            .any(|hit| hit.source.path == declaration_path),
        "the declaration must still exist outside raw top-200"
    );
    let declaration = store.search_ranked_with_at(
        "SearchIndex",
        &fixture_filter,
        10,
        Default::default(),
        evaluation_now(),
    )?;
    assert_eq!(
        declaration.hits.first().map(|hit| hit.source.path.as_str()),
        Some(declaration_path),
        "definition admission must promote the real declaration"
    );
    let declaration_repeat = store.search_ranked_with_at(
        "SearchIndex",
        &fixture_filter,
        10,
        Default::default(),
        evaluation_now(),
    )?;
    assert_eq!(
        signature(&declaration.hits),
        signature(&declaration_repeat.hits),
        "fixed-clock real fixture ordering and scores must be repeatable"
    );

    let normal_raw = store
        .search("crossBoundaryIdentifier", &fixture_filter, 10)?
        .1;
    let normal_ranked = store.search_ranked_with_at(
        "crossBoundaryIdentifier",
        &fixture_filter,
        10,
        Default::default(),
        evaluation_now(),
    )?;
    assert!(normal_raw.iter().any(|hit| {
        hit.source.path == "src/boundary-normal.rs" && hit.chunk.start_byte > 16_000
    }));
    assert!(
        normal_ranked
            .hits
            .iter()
            .any(|hit| hit.source.path == "src/boundary-normal.rs")
    );

    let compound_raw = store
        .search("Namespace::HTTPServer.rs", &fixture_filter, 10)?
        .1;
    assert!(compound_raw.iter().any(|hit| {
        hit.source.path == "src/boundary-compound.rs" && hit.chunk.start_byte > 16_000
    }));
    let compound_ranked = store.search_ranked_with_at(
        "Namespace::HTTPServer.rs",
        &fixture_filter,
        10,
        Default::default(),
        evaluation_now(),
    )?;
    assert!(
        compound_ranked
            .hits
            .iter()
            .any(|hit| hit.source.path == "src/boundary-compound.rs")
    );
    let unicode_raw = store.search("StraßeBoundary", &fixture_filter, 10)?.1;
    assert!(unicode_raw.iter().any(|hit| {
        hit.source.path == "src/boundary-unicode.rs" && hit.chunk.start_byte > 16_000
    }));
    let unicode_ranked = store.search_ranked_with_at(
        "StraßeBoundary",
        &fixture_filter,
        10,
        Default::default(),
        evaluation_now(),
    )?;
    assert!(
        unicode_ranked
            .hits
            .iter()
            .any(|hit| hit.source.path == "src/boundary-unicode.rs")
    );

    let multi = store.search_ranked_with_at(
        "multiSecondMarker",
        &fixture_filter,
        10,
        Default::default(),
        evaluation_now(),
    )?;
    let multi_hit = multi
        .hits
        .iter()
        .find(|hit| hit.source.path == "src/multi.rs")
        .expect("multi-chunk source should be searchable");
    assert!(multi_hit.chunk.start_byte > 0);

    let qualified = store.search_ranked_with_at(
        "Foo::Bar::bazValue",
        &fixture_filter,
        10,
        Default::default(),
        evaluation_now(),
    )?;
    assert!(
        qualified
            .hits
            .iter()
            .any(|hit| hit.source.path == "src/qualified.rs")
    );

    let zero_raw = store
        .search("why does indexing fail", &fixture_filter, 10)?
        .1;
    let zero_ranked = store.search_ranked_with_at(
        "why does indexing fail",
        &fixture_filter,
        10,
        Default::default(),
        evaluation_now(),
    )?;
    assert!(zero_raw.iter().any(|hit| hit.source.path == "notes.txt"));
    assert!(
        zero_ranked.hits.is_empty(),
        "stopword-only incidental evidence must not survive enhanced eligibility"
    );

    let mutable_path = root.join("src/mutable.rs");
    let mutable_before = store.search("mutationoldqxz", &fixture_filter, 10)?.1;
    assert!(!mutable_before.is_empty());
    fs::write(&mutable_path, "mutationnewqxz\n")?;
    scan_project(&root, &store, collection)?;
    assert!(
        store
            .search("mutationoldqxz", &fixture_filter, 10)?
            .1
            .is_empty()
    );
    assert!(
        !store
            .search("mutationnewqxz", &fixture_filter, 10)?
            .1
            .is_empty()
    );
    let mutable_source = store
        .sources(collection, "project")?
        .into_iter()
        .find(|source| source.path == "src/mutable.rs")
        .expect("mutable source should be indexed");
    store.invalidate_source(&mutable_source.key)?;
    assert!(
        store
            .search("mutationnewqxz", &fixture_filter, 10)?
            .1
            .is_empty()
    );
    scan_project(&root, &store, collection)?;
    assert!(
        !store
            .search("mutationnewqxz", &fixture_filter, 10)?
            .1
            .is_empty()
    );
    fs::remove_file(&mutable_path)?;
    scan_project(&root, &store, collection)?;
    assert!(
        store
            .search("mutationnewqxz", &fixture_filter, 10)?
            .1
            .is_empty()
    );

    assert!(declaration.candidate_count <= 200);

    Ok(json!({
        "collection": collection,
        "sources": report.sources,
        "chunks": report.chunks,
        "raw_top_200": raw_top_200.len(),
        "raw_missing_declaration": raw_missing_declaration,
        "declaration_ranked_paths": paths(&declaration.hits),
        "declaration_candidate_count": declaration.candidate_count,
        "declaration_admission_counts": admission_counts(&declaration),
        "boundary_normal": {"raw": paths(&normal_raw), "ranked": paths(&normal_ranked.hits)},
        "boundary_compound": {
            "raw": paths(&compound_raw),
            "ranked": paths(&compound_ranked.hits)
        },
        "boundary_unicode": {
            "raw": paths(&unicode_raw),
            "ranked": paths(&unicode_ranked.hits)
        },
        "multi_chunk": {"path": multi_hit.source.path, "start_byte": multi_hit.chunk.start_byte},
        "qualified_identity": paths(&qualified.hits),
        "zero_evidence": {"raw_paths": paths(&zero_raw), "ranked_paths": paths(&zero_ranked.hits)},
        "replacement_invalidation_deletion": true,
        "regression_coverage_only": true,
    }))
}

fn main() -> Result<()> {
    let root = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or(env::current_dir()?);
    let dir = tempfile::tempdir()?;
    let db = dir.path().join("real-ranking.sqlite3");
    let store = Store::open(&db)?;
    let started = Instant::now();
    let report = scan_project(&root, &store, "real-ranking")?;
    let indexing_ms = started.elapsed().as_secs_f64() * 1000.0;
    let filter = filter("real-ranking");
    let queries = [
        ("search_ranked", "store.rs"),
        ("RankingOptions", "ranking.rs"),
        ("tokenize_checked", "text.rs"),
        ("session copies", "store.rs"),
        ("BM25 candidate reranking", "ranking.rs"),
        ("store.rs", "store.rs"),
    ];
    let mut output = Vec::new();
    for (query, target_path) in queries {
        let baseline_started = Instant::now();
        let baseline = store.search(query, &filter, 5)?.1;
        let baseline_ms = baseline_started.elapsed().as_secs_f64() * 1000.0;
        let started = Instant::now();
        let ranked =
            store.search_ranked_with_at(query, &filter, 5, Default::default(), evaluation_now())?;
        let repeated =
            store.search_ranked_with_at(query, &filter, 5, Default::default(), evaluation_now())?;
        assert_eq!(
            signature(&ranked.hits),
            signature(&repeated.hits),
            "fixed-clock real smoke ordering and scores must be repeatable"
        );
        let rank = |hits: &[bm25_mcp::model::Hit]| {
            hits.iter()
                .position(|h| h.source.path == target_path)
                .map(|i| i + 1)
        };
        output.push(json!({"query": query, "target_path": target_path,
            "baseline_rank": rank(&baseline), "ranked_rank": rank(&ranked.hits),
            "baseline_ms": baseline_ms, "candidate_count": ranked.candidate_count,
            "latency_ms": started.elapsed().as_secs_f64() * 1000.0,
            "baseline_top": baseline.iter().map(|hit| json!({"path": hit.source.path, "lines": [hit.chunk.start_line, hit.chunk.end_line]})).collect::<Vec<_>>(),
            "results": ranked.hits.iter().map(|hit| json!({"path": hit.source.path,
                "lines": [hit.chunk.start_line, hit.chunk.end_line],
                "symbol_or_excerpt": hit.chunk.text.lines().next().unwrap_or("")})).collect::<Vec<_>>() }));
    }
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"root": root, "evaluation_now": EVALUATION_NOW, "indexing_ms": indexing_ms,
        "database_bytes": db_bytes(&db), "sources": report.sources, "chunks": report.chunks,
        "queries": output, "regressions": run_regressions()?,
        "notes": ["read-only source scan into a disposable index", "regressions use a separate synthetic real-ingest fixture and are not relevance claims", "latency includes local SQLite and concurrent machine workload"]})
        )?
    );
    Ok(())
}

fn db_bytes(path: &Path) -> u64 {
    let wal = path.with_extension("sqlite3-wal");
    let shm = path.with_extension("sqlite3-shm");
    [path, wal.as_path(), shm.as_path()]
        .iter()
        .filter_map(|path| fs::metadata(path).ok())
        .map(|metadata| metadata.len())
        .sum()
}
