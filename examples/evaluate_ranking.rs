//! Reproducible raw-versus-ranked evaluation on the fixture corpus.
#[path = "support/ranking_fixture.rs"]
mod fixture;

use anyhow::Result;
use bm25_mcp::{
    model::{Chunk, Hit, SearchFilter, Source},
    ranking::RankingOptions,
    store::Store,
};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use std::{collections::HashSet, path::Path, process::Command, time::Instant};

const EVALUATION_NOW: &str = "2026-09-21T00:00:00Z";
const WARMUPS: usize = 5;
const SAMPLES: usize = 50;
const NDCG_TOLERANCE: f64 = 1e-12;

fn evaluation_now() -> DateTime<Utc> {
    EVALUATION_NOW
        .parse::<DateTime<Utc>>()
        .expect("fixed evaluation timestamp is valid")
}

fn rustc_version() -> Option<String> {
    let output = Command::new("rustc").arg("--version").output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn db_file_sizes(path: &Path) -> (u64, u64, u64, u64) {
    let wal = path.with_extension("sqlite3-wal");
    let shm = path.with_extension("sqlite3-shm");
    let main = std::fs::metadata(path).map_or(0, |metadata| metadata.len());
    let wal = std::fs::metadata(wal).map_or(0, |metadata| metadata.len());
    let shm = std::fs::metadata(shm).map_or(0, |metadata| metadata.len());
    (main, wal, shm, main + wal + shm)
}

fn db_file_json(sizes: (u64, u64, u64, u64)) -> Value {
    json!({
        "main": sizes.0,
        "wal": sizes.1,
        "shm": sizes.2,
        "total": sizes.3,
    })
}

fn raw_statistics(
    connection: &rusqlite::Connection,
) -> rusqlite::Result<(i64, i64, i64, i64, i64)> {
    connection.query_row(
        "SELECT
            COALESCE((SELECT SUM(doc_count) FROM stats), 0),
            COALESCE((SELECT SUM(total_tokens) FROM stats), 0),
            (SELECT COUNT(*) FROM terms),
            (SELECT COUNT(*) FROM term_stats),
            (SELECT COUNT(*) FROM postings)",
        [],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        },
    )
}

fn source_shape(connection: &rusqlite::Connection) -> rusqlite::Result<(i64, i64, String)> {
    connection.query_row(
        "SELECT
            (SELECT COUNT(*) FROM sources),
            (SELECT COUNT(*) FROM chunks),
            (SELECT value FROM meta WHERE key='tokenizer_version')",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
}

fn rebuild_declarations(path: &Path, bytes_before_store_close: u64) -> Result<Value> {
    let bytes_after_store_close = db_file_sizes(path);
    let connection = rusqlite::Connection::open(path)?;
    let declarations_before: i64 =
        connection.query_row("SELECT COUNT(*) FROM declarations", [], |row| row.get(0))?;
    let raw_before = raw_statistics(&connection)?;
    let shape_before = source_shape(&connection)?;
    connection.execute_batch(
        "DROP TABLE declarations;
         DELETE FROM meta WHERE key='declaration_index_version';",
    )?;
    drop(connection);

    let started = Instant::now();
    let reopened = Store::open(path)?;
    let rebuild_ms = started.elapsed().as_secs_f64() * 1000.0;
    drop(reopened);

    let verification = rusqlite::Connection::open(path)?;
    let declarations_after: i64 =
        verification.query_row("SELECT COUNT(*) FROM declarations", [], |row| row.get(0))?;
    let declaration_version: String = verification.query_row(
        "SELECT value FROM meta WHERE key='declaration_index_version'",
        [],
        |row| row.get(0),
    )?;
    let raw_after = raw_statistics(&verification)?;
    let shape_after = source_shape(&verification)?;
    assert_eq!(
        raw_before, raw_after,
        "declaration backfill changed raw BM25 statistics"
    );
    assert_eq!(
        shape_before, shape_after,
        "declaration backfill changed source/tokenizer state"
    );
    let after_rebuild = db_file_sizes(path);
    Ok(json!({
        "supported": true,
        "rebuild_ms": rebuild_ms,
        "declarations_before": declarations_before,
        "declarations_after": declarations_after,
        "declaration_index_version": declaration_version,
        "database_bytes_before_store_close": bytes_before_store_close,
        "database_bytes_after_close": bytes_after_store_close.3,
        "database_bytes_after_close_files": db_file_json(bytes_after_store_close),
        "database_bytes_after_rebuild": after_rebuild.3,
        "database_bytes_after_rebuild_files": db_file_json(after_rebuild),
        "database_bytes_growth_after_store_close":
            after_rebuild.3 as i64 - bytes_after_store_close.3 as i64,
        "database_bytes_delta_from_store_open": after_rebuild.3 as i64
            - bytes_before_store_close as i64,
        "raw_statistics_before": raw_before,
        "raw_statistics_after": raw_after,
        "raw_statistics_unchanged": true,
        "source_shape_before": shape_before,
        "source_shape_after": shape_after,
        "source_tokenizer_state_unchanged": true,
    }))
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

fn source_ranking(hits: &[Hit]) -> Vec<&Hit> {
    let mut seen = HashSet::new();
    hits.iter()
        .filter(|hit| seen.insert(hit.source.key.as_str()))
        .collect()
}

fn metrics(hits: &[Hit], case: &fixture::Case) -> Value {
    let source_hits = source_ranking(hits);
    let rank = source_hits
        .iter()
        .position(|h| case.must.contains(&h.source.key.as_str()))
        .map(|i| i + 1);
    let recall = |k| {
        case.must
            .iter()
            .filter(|id| source_hits.iter().take(k).any(|h| h.source.key == **id))
            .count() as f64
            / case.must.len().max(1) as f64
    };
    let dcg = |k| {
        source_hits
            .iter()
            .take(k)
            .enumerate()
            .map(|(i, h)| (2.0f64.powf(grade(h, case)) - 1.0) / ((i + 2) as f64).log2())
            .sum::<f64>()
    };
    let mut judged_sources = HashSet::new();
    let mut ideal = Vec::new();
    for source in case.must {
        if judged_sources.insert(*source) {
            ideal.push(3.0);
        }
    }
    for source in case.useful {
        if judged_sources.insert(*source) {
            ideal.push(1.0);
        }
    }
    ideal.sort_by(f64::total_cmp);
    ideal.reverse();
    let idcg: f64 = ideal
        .into_iter()
        .take(10)
        .enumerate()
        .map(|(i, g)| (2.0f64.powf(g) - 1.0) / ((i + 2) as f64).log2())
        .sum();
    let top_hits: Vec<_> = hits.iter().take(10).collect();
    let unique_chunks = top_hits
        .iter()
        .map(|h| h.chunk.text.as_str())
        .collect::<HashSet<_>>()
        .len();
    let ndcg10 = if idcg == 0.0 { 0.0 } else { dcg(10) / idcg };
    assert!(
        (-NDCG_TOLERANCE..=1.0 + NDCG_TOLERANCE).contains(&ndcg10),
        "source-level nDCG outside [0, 1]: {ndcg10}"
    );
    json!({"rank": rank, "recall10": recall(10), "recall20": recall(20), "mrr": rank.map_or(0.0, |r| 1.0 / r as f64), "ndcg10": ndcg10, "duplicate_rate10": if top_hits.is_empty() { 0.0 } else { 1.0 - unique_chunks as f64 / top_hits.len() as f64 }, "top": source_hits.iter().take(10).map(|h| h.source.key.as_str()).collect::<Vec<_>>()})
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

fn high_fanout_benchmark() -> Result<Value> {
    let dir = tempfile::tempdir()?;
    let db = dir.path().join("high-fanout.sqlite3");
    let store = Store::open(&db)?;
    let indexing_start = Instant::now();
    for index in 0..240 {
        store.replace_source(
            &Source {
                key: format!("mention-{index}"),
                collection: "high-fanout".into(),
                path: format!("mentions/{index:03}.rs"),
                version: "v1".into(),
                kind: "project".into(),
            },
            [Ok(Chunk {
                text: format!(
                    "// mention saturation {index}\n{}",
                    "SearchIndex ".repeat(80)
                ),
                start_line: 1,
                end_line: 2,
                ..Default::default()
            })],
        )?;
    }
    store.replace_source(
        &Source {
            key: "declaration".into(),
            collection: "high-fanout".into(),
            path: "src/search_index.rs".into(),
            version: "v1".into(),
            kind: "project".into(),
        },
        [Ok(Chunk {
            text: "pub struct SearchIndex { value: usize }\n".into(),
            start_line: 1,
            end_line: 1,
            ..Default::default()
        })],
    )?;
    let indexing_ms = indexing_start.elapsed().as_secs_f64() * 1000.0;
    let filter = SearchFilter {
        collection: "high-fanout".into(),
        kind: "project".into(),
        ..Default::default()
    };
    let raw_top = store.search("SearchIndex", &filter, 200)?.1;
    let raw_missed_declaration = !raw_top
        .iter()
        .any(|hit| hit.source.path == "src/search_index.rs");
    assert!(
        raw_missed_declaration,
        "high-fanout raw top-200 must miss the declaration"
    );

    let mut records = Vec::new();
    for mode in ["raw", "enhanced"] {
        let mut times = Vec::with_capacity(SAMPLES);
        let mut expected_signature = None;
        let mut deterministic_checks = 0;
        let mut hits = Vec::new();
        let mut candidate_count = None;
        let mut admission_counts = Value::Null;
        for sample in 0..(WARMUPS + SAMPLES) {
            let started = Instant::now();
            if mode == "raw" {
                hits = store.search("SearchIndex", &filter, 20)?.1;
            } else {
                let ranked = store.search_ranked_with_at(
                    "SearchIndex",
                    &filter,
                    20,
                    RankingOptions::default(),
                    evaluation_now(),
                )?;
                assert!(ranked.candidate_count <= 200);
                candidate_count = Some(ranked.candidate_count);
                admission_counts = json!(ranked.admission_counts);
                hits = ranked.hits;
            }
            let signature: Vec<_> = hits
                .iter()
                .map(|hit| (hit.match_id.clone(), hit.score.to_bits()))
                .collect();
            if let Some(expected) = &expected_signature {
                assert_eq!(expected, &signature);
                deterministic_checks += 1;
            } else {
                expected_signature = Some(signature);
            }
            if sample >= WARMUPS {
                times.push(started.elapsed().as_secs_f64() * 1000.0);
            }
        }
        let mut sorted = times.clone();
        let declaration_rank = hits
            .iter()
            .position(|hit| hit.source.path == "src/search_index.rs")
            .map(|rank| rank + 1);
        records.push(json!({
            "mode": mode,
            "candidate_count": candidate_count,
            "final_pool_size": candidate_count,
            "admission_counts": admission_counts,
            "latency_ms_p50": percentile(&mut sorted, 0.50),
            "latency_ms_p95": percentile(&mut sorted, 0.95),
            "declaration_rank": declaration_rank,
            "deterministic": deterministic_checks == WARMUPS + SAMPLES - 1,
            "deterministic_checks": deterministic_checks,
        }));
    }
    let database_file_sizes = db_file_sizes(&db);
    Ok(json!({
        "separate_from_quality_cases": true,
        "documents": 241,
        "mention_documents": 240,
        "query": "SearchIndex",
        "raw_top_200": raw_top.len(),
        "raw_top_200_missed_declaration": raw_missed_declaration,
        "indexing_ms": indexing_ms,
        "database_bytes": database_file_sizes.3,
        "database_bytes_open": db_file_json(database_file_sizes),
        "database_bytes_stage": "store-open-post-index",
        "warmups": WARMUPS,
        "samples": SAMPLES,
        "cases": records,
        "notes": [
            "Synthetic high-fanout declaration-admission latency workload.",
            "It is separate from the 13 historical quality cases and has no relevance labels."
        ]
    }))
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
            let mut candidate_count = None;
            let mut probes = 0;
            let mut meaningful_retrievals = 0;
            let mut additional_retrievals = 0;
            let mut admission_counts = None;
            let mut expected_signature = None;
            let mut deterministic_checks = 0;
            for sample in 0..(WARMUPS + SAMPLES) {
                let start = Instant::now();
                if mode == "raw" {
                    hits = store.search(case.query, &fixture::filter(case.kind), 20)?.1;
                } else {
                    let ranked = store.search_ranked_with_at(
                        case.query,
                        &fixture::filter(case.kind),
                        20,
                        options_for(mode),
                        evaluation_now(),
                    )?;
                    assert!(
                        ranked.candidate_count <= 200,
                        "bounded candidate pool exceeded 200: {}",
                        ranked.candidate_count
                    );
                    candidate_count = Some(ranked.candidate_count);
                    probes = ranked.probes.len();
                    meaningful_retrievals = ranked.meaningful_retrievals;
                    additional_retrievals = ranked.additional_retrievals;
                    admission_counts = Some(json!(ranked.admission_counts));
                    hits = ranked.hits;
                }
                let signature: Vec<_> = hits
                    .iter()
                    .map(|hit| {
                        (
                            hit.match_id.clone(),
                            hit.source.key.clone(),
                            hit.score.to_bits(),
                        )
                    })
                    .collect();
                if let Some(expected) = &expected_signature {
                    assert_eq!(
                        expected, &signature,
                        "ordering or relevance score changed on repeated evaluation: \
                         query={:?}, kind={}, mode={mode}",
                        case.query, case.kind
                    );
                    deterministic_checks += 1;
                } else {
                    expected_signature = Some(signature);
                }
                if sample >= WARMUPS {
                    times.push(start.elapsed().as_secs_f64() * 1000.0);
                }
            }
            let mut p = times.clone();
            records.push(json!({"mode": mode, "query": case.query, "kind": case.kind, "candidate_count": candidate_count, "final_pool_size": candidate_count, "admission_counts": admission_counts, "probes": probes, "meaningful_retrievals": meaningful_retrievals, "additional_retrievals": additional_retrievals, "latency_ms_p50": percentile(&mut p, 0.50), "latency_ms_p95": percentile(&mut p, 0.95), "deterministic": deterministic_checks == WARMUPS + SAMPLES - 1, "deterministic_checks": deterministic_checks, "metrics": metrics(&hits, case)}));
        }
    }
    let raw_vs_enhanced = cases
        .iter()
        .map(|case| {
            let record = |mode| {
                records
                    .iter()
                    .find(|record| {
                        record["query"] == case.query
                            && record["kind"] == case.kind
                            && record["mode"] == mode
                    })
                    .expect("raw and enhanced records exist")
            };
            let raw = record("raw");
            let enhanced = record("enhanced");
            let delta = |metric| {
                enhanced["metrics"][metric].as_f64().unwrap_or(0.0)
                    - raw["metrics"][metric].as_f64().unwrap_or(0.0)
            };
            json!({
                "query": case.query,
                "kind": case.kind,
                "raw": raw["metrics"].clone(),
                "enhanced": enhanced["metrics"].clone(),
                "delta": {
                    "recall10": delta("recall10"),
                    "recall20": delta("recall20"),
                    "mrr": delta("mrr"),
                    "ndcg10": delta("ndcg10"),
                    "duplicate_rate10": delta("duplicate_rate10"),
                },
            })
        })
        .collect::<Vec<_>>();
    let update_start = Instant::now();
    fixture::replace_probe(&store)?;
    let update_ms = update_start.elapsed().as_secs_f64() * 1000.0;
    let database_file_sizes_before_rebuild = db_file_sizes(&db);
    let database_bytes_before_rebuild = database_file_sizes_before_rebuild.3;
    drop(store);
    let declaration_rebuild = rebuild_declarations(&db, database_bytes_before_rebuild)?;
    let high_fanout = high_fanout_benchmark()?;
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"schema": 3, "revision": "working-tree", "tokenizer": "v4", "mode": "raw_and_ranked_fixture", "evaluation_now": EVALUATION_NOW, "clock_mode": "search_ranked_with_at explicit timestamp", "toolchain": rustc_version(), "platform": {"os": std::env::consts::OS, "arch": std::env::consts::ARCH}, "indexing_ms": indexing_ms, "update_ms": update_ms, "database_bytes": database_bytes_before_rebuild, "database_bytes_open": db_file_json(database_file_sizes_before_rebuild), "database_bytes_stage": "store-open-post-update", "database_bytes_after_close": declaration_rebuild["database_bytes_after_close"].clone(), "database_bytes_after_close_files": declaration_rebuild["database_bytes_after_close_files"].clone(), "database_bytes_after_rebuild": declaration_rebuild["database_bytes_after_rebuild"].clone(), "database_bytes_after_rebuild_files": declaration_rebuild["database_bytes_after_rebuild_files"].clone(), "declaration_rebuild": declaration_rebuild, "high_fanout": high_fanout, "warmups": WARMUPS, "samples": SAMPLES, "cases": records, "raw_vs_enhanced": raw_vs_enhanced, "ablation_modes": &modes[2..], "notes": ["raw is Store.search; enhanced and ablations use Store.search_ranked_with_at with the fixed evaluation clock", "database_bytes_open is captured before closing the store; checkpointed/backfill sizes are reported separately", "declaration_rebuild drops only derived declarations metadata and verifies raw BM25 statistics are unchanged", "the high_fanout workload is separate from historical relevance labels", "relevance metrics are source-level over first-hit source rankings; duplicate_rate10 is measured on the uncollapsed chunk list", "fixture is synthetic and does not establish production-wide quality"]})
        )?
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bm25_mcp::model::{Chunk, Source};

    fn metric_hit(match_id: &str, source_key: &str, text: &str) -> Hit {
        Hit {
            copy_count: 1,
            match_id: match_id.into(),
            verified_at: None,
            source: Source {
                key: source_key.into(),
                collection: "fixture".into(),
                path: format!("{source_key}.rs"),
                version: "v1".into(),
                kind: "project".into(),
            },
            chunk: Chunk {
                text: text.into(),
                start_line: 1,
                end_line: 1,
                end_byte: text.len() as u64,
                ..Chunk::default()
            },
            score: 1.0,
        }
    }

    fn test_case(must: &'static [&'static str], useful: &'static [&'static str]) -> fixture::Case {
        fixture::Case {
            query: "metric",
            kind: "project",
            must,
            useful,
        }
    }

    #[test]
    fn source_grain_metrics_deduplicate_one_source_with_multiple_chunks() {
        let case = test_case(&["source"], &[]);
        let hits = vec![
            metric_hit("source-first", "source", "same chunk"),
            metric_hit("source-second", "source", "same chunk"),
        ];
        let result = metrics(&hits, &case);

        assert_eq!(result["recall10"], 1.0);
        assert_eq!(result["mrr"], 1.0);
        assert!(
            (result["ndcg10"].as_f64().unwrap() - 1.0).abs() < 1e-12,
            "one source at rank one should have ideal nDCG: {result}"
        );
        assert!(
            result["ndcg10"].as_f64().unwrap() <= 1.0 + NDCG_TOLERANCE,
            "source-level nDCG must not award a grade twice: {result}"
        );
        assert_eq!(result["duplicate_rate10"], 0.5);
    }

    #[test]
    fn source_grain_metrics_rank_many_sources_once_each() {
        let case = test_case(&["one", "two"], &["three"]);
        let hits = vec![
            metric_hit("one-chunk", "one", "one"),
            metric_hit("one-duplicate", "one", "one duplicate"),
            metric_hit("two-chunk", "two", "two"),
            metric_hit("three-chunk", "three", "three"),
        ];
        let result = metrics(&hits, &case);

        assert_eq!(result["recall10"], 1.0);
        assert_eq!(result["mrr"], 1.0);
        assert!((result["ndcg10"].as_f64().unwrap() - 1.0).abs() < 1e-12);
        assert!(result["ndcg10"].as_f64().unwrap() <= 1.0 + NDCG_TOLERANCE);
        assert_eq!(
            result["top"],
            json!(["one", "two", "three"]),
            "relevance metrics use the source-deduplicated ranking"
        );
    }

    #[test]
    fn source_grain_metrics_report_zero_for_no_relevant_results() {
        let case = test_case(&["wanted"], &["useful"]);
        let result = metrics(&[metric_hit("noise", "noise", "noise")], &case);

        assert_eq!(result["recall10"], 0.0);
        assert_eq!(result["mrr"], 0.0);
        assert_eq!(result["ndcg10"], 0.0);
    }

    #[test]
    fn source_grain_metrics_report_zero_for_empty_judgments() {
        let case = test_case(&[], &[]);
        let result = metrics(&[metric_hit("noise", "noise", "noise")], &case);

        assert_eq!(result["recall10"], 0.0);
        assert_eq!(result["mrr"], 0.0);
        assert_eq!(result["ndcg10"], 0.0);
    }
}
