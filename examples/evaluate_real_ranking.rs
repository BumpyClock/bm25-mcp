//! Read-only ranking smoke test over a source tree. The index is disposable.
use anyhow::Result;
use bm25_mcp::{ingest::scan_project, model::SearchFilter, store::Store};
use serde_json::json;
use std::{env, path::PathBuf, time::Instant};

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
    let filter = SearchFilter {
        collection: "real-ranking".into(),
        kind: "project".into(),
        ..Default::default()
    };
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
        let ranked = store.search_ranked_with(query, &filter, 5, Default::default())?;
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
        serde_json::to_string_pretty(&json!({"root": root, "indexing_ms": indexing_ms,
        "sources": report.sources, "chunks": report.chunks, "queries": output,
        "notes": ["read-only source scan into a disposable index", "latency includes local SQLite and concurrent machine workload"]}))?
    );
    Ok(())
}
