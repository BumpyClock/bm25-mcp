//! Bounded worst-case lexical reranking probe: 200 candidates with <=16 KiB text.
use anyhow::Result;
use bm25_mcp::{
    model::{Chunk, SearchFilter, Source},
    store::Store,
};
use serde_json::json;
use std::time::Instant;

fn run(unique: bool) -> Result<serde_json::Value> {
    let dir = tempfile::tempdir()?;
    let db = dir.path().join("ranking-stress.sqlite3");
    let store = Store::open(&db)?;
    let started = Instant::now();
    let mut min_bytes = usize::MAX;
    let mut max_bytes = 0;
    for i in 0..220 {
        let suffix = if unique {
            format!(" unique_candidate_{i}")
        } else {
            String::new()
        };
        let body = format!(
            "stress_candidate ranking_probe {suffix}{}",
            "padding ".repeat(2_000)
        );
        min_bytes = min_bytes.min(body.len());
        max_bytes = max_bytes.max(body.len());
        store.replace_source(
            &Source {
                key: format!("stress-{i}"),
                collection: "stress".into(),
                path: format!("src/stress/{i}.rs"),
                version: "v4".into(),
                kind: "project".into(),
            },
            [Ok(Chunk {
                text: body,
                start_line: 1,
                end_line: 900,
                ..Default::default()
            })],
        )?;
    }
    let indexing_ms = started.elapsed().as_secs_f64() * 1000.0;
    let filter = SearchFilter {
        collection: "stress".into(),
        kind: "project".into(),
        ..Default::default()
    };
    let mut samples = Vec::new();
    for _ in 0..4 {
        let started = Instant::now();
        store.search_ranked("ranking_probe", &filter, 20)?;
        samples.push(started.elapsed().as_secs_f64() * 1000.0);
    }
    let diagnostic = store.search_ranked_with("ranking_probe", &filter, 20, Default::default())?;
    samples.sort_by(f64::total_cmp);
    Ok(
        json!({"documents": 220, "text_bytes": {"min": min_bytes, "max": max_bytes}, "candidate_count": diagnostic.candidate_count, "indexing_ms": indexing_ms, "latency_ms": {"p50_ms": samples[1], "p95_ms": samples[3]}}),
    )
}
fn main() -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"cases": {"identical": run(false)?, "distinct": run(true)?}, "notes": ["release-mode bounded stress probe", "220 documents force the 200-candidate cap", "each chunk is below the 16 KiB production bound", "SQLite and machine-local filesystem timing"]})
        )?
    );
    Ok(())
}
