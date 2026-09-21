//! Bounded worst-case lexical reranking probe: 200 candidates with <=16 KiB text.
use anyhow::Result;
use bm25_mcp::{
    model::{Chunk, SearchFilter, Source},
    store::Store,
};
use chrono::{DateTime, Utc};
use serde_json::json;
use std::{path::Path, process::Command, time::Instant};

const EVALUATION_NOW: &str = "2026-09-21T00:00:00Z";
const WARMUPS: usize = 5;
const SAMPLES: usize = 30;

fn evaluation_now() -> DateTime<Utc> {
    EVALUATION_NOW
        .parse::<DateTime<Utc>>()
        .expect("fixed evaluation timestamp is valid")
}

fn db_file_sizes(path: &std::path::Path) -> (u64, u64, u64, u64) {
    let wal = path.with_extension("sqlite3-wal");
    let shm = path.with_extension("sqlite3-shm");
    let main = std::fs::metadata(path).map_or(0, |metadata| metadata.len());
    let wal = std::fs::metadata(wal).map_or(0, |metadata| metadata.len());
    let shm = std::fs::metadata(shm).map_or(0, |metadata| metadata.len());
    (main, wal, shm, main + wal + shm)
}

fn db_file_json(sizes: (u64, u64, u64, u64)) -> serde_json::Value {
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

fn rebuild_declarations(path: &Path, bytes_before_store_close: u64) -> Result<serde_json::Value> {
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

fn rss_bytes() -> Option<u64> {
    let pid = sysinfo::get_current_pid().ok()?;
    let mut system = sysinfo::System::new();
    system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
    system.process(pid).map(|process| process.memory())
}

fn rustc_version() -> Option<String> {
    let output = Command::new("rustc").arg("--version").output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

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
    let mut expected_signature = None;
    let mut deterministic_checks = 0;
    let mut admission_counts = None;
    let mut probes = 0;
    let mut candidate_count = 0;
    let rss_before = rss_bytes();
    let mut rss_peak = rss_before;
    for sample in 0..(WARMUPS + SAMPLES) {
        let started = Instant::now();
        let diagnostic = store.search_ranked_with_at(
            "ranking_probe",
            &filter,
            20,
            Default::default(),
            evaluation_now(),
        )?;
        assert!(
            diagnostic.candidate_count <= 200,
            "bounded candidate pool exceeded 200: {}",
            diagnostic.candidate_count
        );
        candidate_count = diagnostic.candidate_count;
        probes = diagnostic.probes.len();
        admission_counts = Some(json!({
            "lexical": diagnostic.admission_counts.lexical,
            "definitions": diagnostic.admission_counts.definitions,
            "path": diagnostic.admission_counts.path,
            "expansion": diagnostic.admission_counts.expansion,
        }));
        let signature: Vec<_> = diagnostic
            .hits
            .iter()
            .map(|hit| (hit.match_id.clone(), hit.score.to_bits()))
            .collect();
        if let Some(expected) = &expected_signature {
            assert_eq!(expected, &signature, "stress ranking is not deterministic");
            deterministic_checks += 1;
        } else {
            expected_signature = Some(signature);
        }
        if sample >= WARMUPS {
            samples.push(started.elapsed().as_secs_f64() * 1000.0);
        }
        if let Some(rss) = rss_bytes() {
            rss_peak = Some(rss_peak.map_or(rss, |peak| peak.max(rss)));
        }
    }
    samples.sort_by(f64::total_cmp);
    let update_started = Instant::now();
    store.replace_source(
        &Source {
            key: "stress-0".into(),
            collection: "stress".into(),
            path: "src/stress/0.rs".into(),
            version: "v2".into(),
            kind: "project".into(),
        },
        [Ok(Chunk {
            text: format!(
                "stress_candidate ranking_probe updated {}",
                "padding ".repeat(2_000)
            ),
            start_line: 1,
            end_line: 900,
            ..Default::default()
        })],
    )?;
    let update_ms = update_started.elapsed().as_secs_f64() * 1000.0;
    let compact_started = Instant::now();
    store.compact()?;
    let compact_ms = compact_started.elapsed().as_secs_f64() * 1000.0;
    let rss_after = rss_bytes();
    let database_file_sizes_before_rebuild = db_file_sizes(&db);
    let database_bytes_before_rebuild = database_file_sizes_before_rebuild.3;
    drop(store);
    let declaration_rebuild = rebuild_declarations(&db, database_bytes_before_rebuild)?;
    Ok(json!({
        "documents": 220,
        "collection": "stress",
        "kind": "project",
        "text_bytes": {"min": min_bytes, "max": max_bytes},
        "candidate_count": candidate_count,
        "final_pool_size": candidate_count,
        "admission_counts": admission_counts,
        "expansion_probes": probes,
        "indexing_ms": indexing_ms,
        "update_ms": update_ms,
        "compact_ms": compact_ms,
        "database_bytes": database_bytes_before_rebuild,
        "database_bytes_open": db_file_json(database_file_sizes_before_rebuild),
        "database_bytes_stage": "store-open-post-compact",
        "database_bytes_after_close": declaration_rebuild["database_bytes_after_close"],
        "database_bytes_after_close_files": declaration_rebuild["database_bytes_after_close_files"],
        "database_bytes_after_rebuild": declaration_rebuild["database_bytes_after_rebuild"],
        "database_bytes_after_rebuild_files": declaration_rebuild["database_bytes_after_rebuild_files"],
        "declaration_rebuild": declaration_rebuild,
        "rss_bytes": {"before": rss_before, "after": rss_after, "observed_peak": rss_peak},
        "warmups": WARMUPS,
        "samples": SAMPLES,
        "deterministic": deterministic_checks == WARMUPS + SAMPLES - 1,
        "deterministic_checks": deterministic_checks,
        "latency_ms": {"p50_ms": samples[(samples.len() - 1) / 2], "p95_ms": samples[((samples.len() - 1) as f64 * 0.95).round() as usize]},
    }))
}
fn main() -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "schema": 2,
            "revision": "working-tree",
            "mode": "bounded_ranking_stress",
            "evaluation_now": EVALUATION_NOW,
            "clock_mode": "search_ranked_with_at explicit timestamp",
            "toolchain": rustc_version(),
            "platform": {"os": std::env::consts::OS, "arch": std::env::consts::ARCH},
            "warmups": WARMUPS,
            "samples": SAMPLES,
            "cases": {"identical": run(false)?, "distinct": run(true)?},
            "notes": [
                "release-mode bounded stress probe",
                "220 documents force the 200-candidate cap",
                "each chunk is below the 16 KiB production bound",
                "SQLite main/WAL/SHM bytes, process RSS, update and compaction costs are reported",
                "declaration_rebuild drops only the derived table/meta key, reopens the index, and verifies raw/source/tokenizer state",
                "timings are machine-local and require same-machine pre/post comparison"
            ]
        }))?
    );
    Ok(())
}
