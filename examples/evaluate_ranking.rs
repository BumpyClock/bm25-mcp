//! Reproducible raw-versus-ranked evaluation on the fixture corpus.
#[path = "support/ranking_fixture.rs"]
mod fixture;

use anyhow::Result;
use bm25_mcp::{model::Hit, ranking::RankingOptions, store::Store};
use serde_json::{Value, json};
use std::{collections::HashSet, path::Path, time::Instant};

const WARMUPS: usize = 1;
const SAMPLES: usize = 3;

fn db_bytes(path: &Path) -> u64 {
    let wal = path.with_extension("sqlite3-wal");
    let shm = path.with_extension("sqlite3-shm");
    [path, wal.as_path(), shm.as_path()]
        .iter()
        .filter_map(|p| std::fs::metadata(p).ok())
        .map(|m| m.len())
        .sum()
}
fn percentile(values: &mut [f64], p: f64) -> f64 {
    values.sort_by(f64::total_cmp);
    values[((values.len().saturating_sub(1)) as f64 * p).round() as usize]
}
fn grade(hit: &Hit, case: &fixture::Case) -> f64 {
    if case.must.contains(&hit.source.key.as_str()) {
        3.0
    } else if case.useful.contains(&hit.source.key.as_str()) {
        1.0
    } else {
        0.0
    }
}
fn metrics(hits: &[Hit], case: &fixture::Case) -> Value {
    let rank = hits
        .iter()
        .position(|h| case.must.contains(&h.source.key.as_str()))
        .map(|i| i + 1);
    let recall = |k| {
        case.must
            .iter()
            .filter(|id| hits.iter().take(k).any(|h| h.source.key == **id))
            .count() as f64
            / case.must.len().max(1) as f64
    };
    let dcg = |k| {
        hits.iter()
            .take(k)
            .enumerate()
            .map(|(i, h)| (2.0f64.powf(grade(h, case)) - 1.0) / ((i + 2) as f64).log2())
            .sum::<f64>()
    };
    let mut ideal: Vec<_> = case
        .must
        .iter()
        .map(|_| 3.0)
        .chain(case.useful.iter().map(|_| 1.0))
        .collect();
    ideal.sort_by(f64::total_cmp);
    ideal.reverse();
    let idcg: f64 = ideal
        .into_iter()
        .take(10)
        .enumerate()
        .map(|(i, g)| (2.0f64.powf(g) - 1.0) / ((i + 2) as f64).log2())
        .sum();
    let top: Vec<_> = hits.iter().take(10).collect();
    let unique = top
        .iter()
        .map(|h| h.chunk.text.as_str())
        .collect::<HashSet<_>>()
        .len();
    json!({"rank": rank, "recall10": recall(10), "recall20": recall(20), "mrr": rank.map_or(0.0, |r| 1.0 / r as f64), "ndcg10": if idcg == 0.0 { 0.0 } else { dcg(10) / idcg }, "duplicate_rate10": if top.is_empty() { 0.0 } else { 1.0 - unique as f64 / top.len() as f64 }, "top": top.iter().map(|h| h.source.key.as_str()).collect::<Vec<_>>()})
}
fn options_for(name: &str) -> RankingOptions {
    let mut o = RankingOptions::default();
    match name {
        "no_fields" => o.fields = false,
        "no_classification" => o.classification = false,
        "no_exact" => o.exact = false,
        "no_proximity" => o.proximity = false,
        "no_expansion" => o.expansion = false,
        "no_decay" => o.decay = false,
        "no_dedupe" => o.dedupe = false,
        "no_similarity" => o.weighted_similarity = false,
        "no_mmr" => o.mmr = false,
        _ => {}
    }
    o
}
fn main() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = dir.path().join("ranking.sqlite3");
    let store = Store::open(&db)?;
    let indexing_start = Instant::now();
    let cases = fixture::fixtures(&store)?;
    let indexing_ms = indexing_start.elapsed().as_secs_f64() * 1000.0;
    let modes = [
        "raw",
        "enhanced",
        "no_fields",
        "no_classification",
        "no_exact",
        "no_proximity",
        "no_expansion",
        "no_decay",
        "no_dedupe",
        "no_similarity",
        "no_mmr",
    ];
    let mut records = Vec::new();
    for case in &cases {
        for mode in modes {
            let mut times = Vec::with_capacity(SAMPLES);
            let mut hits = Vec::new();
            let mut candidate_count = 0;
            let mut probes = 0;
            for sample in 0..(WARMUPS + SAMPLES) {
                let start = Instant::now();
                if mode == "raw" {
                    hits = store.search(case.query, &fixture::filter(case.kind), 20)?.1;
                } else {
                    let ranked = store.search_ranked_with(
                        case.query,
                        &fixture::filter(case.kind),
                        20,
                        options_for(mode),
                    )?;
                    candidate_count = ranked.candidate_count;
                    probes = ranked.probes.len();
                    hits = ranked.hits;
                }
                if sample >= WARMUPS {
                    times.push(start.elapsed().as_secs_f64() * 1000.0);
                }
            }
            let mut p = times.clone();
            records.push(json!({"mode": mode, "query": case.query, "kind": case.kind, "candidate_count": candidate_count, "probes": probes, "latency_ms_p50": percentile(&mut p, 0.50), "latency_ms_p95": percentile(&mut p, 0.95), "metrics": metrics(&hits, case)}));
        }
    }
    let update_start = Instant::now();
    fixture::replace_probe(&store)?;
    let update_ms = update_start.elapsed().as_secs_f64() * 1000.0;
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"schema": 2, "tokenizer": "v4", "mode": "raw_and_ranked_fixture", "indexing_ms": indexing_ms, "update_ms": update_ms, "database_bytes": db_bytes(&db), "warmups": WARMUPS, "samples": SAMPLES, "cases": records, "notes": ["raw is Store.search; enhanced and ablations use Store.search_ranked_with", "database_bytes includes SQLite WAL/SHM when present", "fixture is synthetic and does not establish production-wide quality"]})
        )?
    );
    Ok(())
}
