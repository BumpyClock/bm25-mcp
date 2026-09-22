//! Opt-in, deterministic workloads; timings are evidence, not test thresholds.
use anyhow::Result;
use bm25_mcp::{
    ingest::project_identity,
    model::{Chunk, Source},
    progress::ProgressReporter,
    sessions::{SessionConfig, scan_sessions_observed},
    store::{SessionCheckpoint, Store},
};
use serde_json::json;
use std::{collections::HashSet, fs, io::Write, time::Instant};

#[test]
#[ignore = "release-mode session preparation benchmark"]
fn benchmark_session_preparation() -> Result<()> {
    for (records, width) in [(1024, 4096), (128, 32768), (1, 4 * 1024 * 1024)] {
        for accepted in [true, false] {
            for trial in 0..4 {
                let dir = tempfile::tempdir()?;
                let project = dir.path().join("project");
                let other = dir.path().join("other");
                fs::create_dir(&project)?;
                fs::create_dir(&other)?;
                let config = SessionConfig {
                    codex_home: dir.path().join("codex"),
                    claude_config_dir: dir.path().join("claude"),
                    copilot_home: dir.path().join("copilot"),
                    ..SessionConfig::default()
                };
                fs::create_dir_all(config.codex_home.join("sessions"))?;
                let path = config.codex_home.join("sessions/fixture.jsonl");
                let mut file = fs::File::create(&path)?;
                writeln!(
                    file,
                    "{}",
                    json!({"type":"session_meta","payload":{"cwd":if accepted { &project } else { &other },"id":"bench"}})
                )?;
                let text = "payload ".repeat(width / 8);
                for id in 0..records {
                    writeln!(
                        file,
                        "{}",
                        json!({"type":"response_item","payload":{"type":"message","id":id.to_string(),"role":"user","content":[{"type":"text","text":text}]}})
                    )?;
                }
                drop(file);
                let owner = project_identity(&project)?.owner_key;
                let store = Store::open(&dir.path().join("index.sqlite3"))?;
                let progress = ProgressReporter::new();
                let started = Instant::now();
                let report = scan_sessions_observed(
                    &project,
                    &owner,
                    &store,
                    &config,
                    None,
                    &|| true,
                    &progress,
                )?;
                let elapsed = started.elapsed().as_micros();
                assert_eq!(report.error_count, 0, "{report:?}");
                if trial != 0 {
                    println!(
                        "cold records={records} width={width} accepted={accepted} trial={trial} elapsed_us={elapsed} progress={}",
                        serde_json::to_string(&progress.snapshot())?
                    );
                }
                if accepted {
                    let mut file = fs::OpenOptions::new().append(true).open(&path)?;
                    writeln!(
                        file,
                        "{}",
                        json!({"type":"response_item","payload":{"type":"message","id":"append","role":"user","content":[{"type":"text","text":"appendmarker"}]}})
                    )?;
                    drop(file);
                    let progress = ProgressReporter::new();
                    let started = Instant::now();
                    scan_sessions_observed(
                        &project,
                        &owner,
                        &store,
                        &config,
                        Some(&HashSet::from([path])),
                        &|| true,
                        &progress,
                    )?;
                    if trial != 0 {
                        println!(
                            "append records={records} width={width} trial={trial} elapsed_us={} progress={}",
                            started.elapsed().as_micros(),
                            serde_json::to_string(&progress.snapshot())?
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
#[ignore = "release-mode quarantine/restoration benchmark"]
fn benchmark_source_statistics() -> Result<()> {
    for unique in [false, true] {
        let dir = tempfile::tempdir()?;
        let store = Store::open(&dir.path().join("index.sqlite3"))?;
        let mut source = Source {
            key: "source".into(),
            collection: "owner".into(),
            path: "fixture".into(),
            version: "0".into(),
            kind: "session".into(),
        };
        let checkpoint = SessionCheckpoint {
            offset: 0,
            state: "{}".into(),
        };
        let started = Instant::now();
        store.replace_session(
            &source,
            (0..10_000).map(|i| {
                Ok(Chunk {
                    text: "fixture".into(),
                    tokens: Some(
                        (0..32)
                            .map(|j| format!("term{}", if unique { i * 32 + j } else { j }))
                            .collect(),
                    ),
                    ..Chunk::default()
                })
            }),
            &checkpoint,
        )?;
        println!(
            "stats_import unique={unique} elapsed_us={}",
            started.elapsed().as_micros()
        );
        store.compact()?;
        println!(
            "stats_storage unique={unique} bytes={}",
            fs::metadata(dir.path().join("index.sqlite3"))?.len()
        );
        let connection = rusqlite::Connection::open(dir.path().join("index.sqlite3"))?;
        let queries = [
            (
                "postings",
                "SELECT p.term_id,COUNT(*) FROM chunks c JOIN postings p ON p.chunk_id=c.id WHERE c.source_key='source' GROUP BY p.term_id",
            ),
            (
                "summary",
                "SELECT term_id,doc_freq FROM source_term_stats WHERE source_key='source'",
            ),
        ];
        for (kind, sql) in queries {
            // The same harness also runs against the pre-summary revision.
            if kind == "summary"
                && connection.query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE name='source_term_stats'",
                    [],
                    |r| r.get::<_, i64>(0),
                )? == 0
            {
                continue;
            }
            let mut statement = connection.prepare(sql)?;
            let rows = statement
                .query_map([], |r| r.get::<_, i64>(1))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            assert_eq!(rows.iter().sum::<i64>(), 320_000);
            println!(
                "stats_work unique={unique} query={kind} result_rows={} vm_steps={}",
                rows.len(),
                statement.get_status(rusqlite::StatementStatus::VmStep)
            );
        }
        for trial in 0..6 {
            let started = Instant::now();
            store.invalidate_source(&source.key)?;
            let invalidate = started.elapsed().as_micros();
            let previous = source.version.clone();
            source.version = (trial + 1).to_string();
            let started = Instant::now();
            store.append_session(
                &source,
                &previous,
                [Ok(Chunk {
                    text: "appended".into(),
                    tokens: Some(vec!["term0".into()]),
                    ..Chunk::default()
                })],
                &checkpoint,
            )?;
            if trial != 0 {
                println!(
                    "stats unique={unique} trial={trial} invalidate_us={invalidate} restore_us={}",
                    started.elapsed().as_micros()
                );
            }
        }
    }
    Ok(())
}

#[test]
#[ignore = "release-mode search responsiveness during session import"]
fn benchmark_concurrent_search() -> Result<()> {
    use bm25_mcp::model::SearchFilter;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    for trial in 0..3 {
        let dir = tempfile::tempdir()?;
        let project = dir.path().join("project");
        fs::create_dir(&project)?;
        let owner = project_identity(&project)?.owner_key;
        let store = Store::open(&dir.path().join("index.sqlite3"))?;
        let filter = SearchFilter {
            collection: owner.clone(),
            kind: "project".into(),
            ..Default::default()
        };
        store.replace_source(
            &Source {
                key: "search-source".into(),
                collection: owner.clone(),
                path: "search.txt".into(),
                version: "1".into(),
                kind: "project".into(),
            },
            [Ok(Chunk {
                text: "responsivenessmarker".into(),
                ..Default::default()
            })],
        )?;
        let config = SessionConfig {
            codex_home: dir.path().join("codex"),
            claude_config_dir: dir.path().join("claude"),
            copilot_home: dir.path().join("copilot"),
            ..Default::default()
        };
        fs::create_dir_all(config.codex_home.join("sessions"))?;
        let mut file = fs::File::create(config.codex_home.join("sessions/import.jsonl"))?;
        writeln!(
            file,
            "{}",
            json!({"type":"session_meta","payload":{"cwd":project,"id":"concurrent"}})
        )?;
        for id in 0..1024 {
            writeln!(
                file,
                "{}",
                json!({"type":"response_item","payload":{"type":"message","id":id.to_string(),"role":"user","content":[{"type":"text","text":"payload ".repeat(512)}]}})
            )?;
        }
        drop(file);
        let query = |store: &Store, filter: &SearchFilter| {
            let started = Instant::now();
            assert_eq!(
                store
                    .search("responsivenessmarker", filter, 10)
                    .unwrap()
                    .1
                    .len(),
                1
            );
            started.elapsed().as_micros()
        };
        query(&store, &filter);
        let mut idle = (0..30).map(|_| query(&store, &filter)).collect::<Vec<_>>();
        let stop = Arc::new(AtomicBool::new(false));
        let reader_store = store.clone();
        let reader_stop = stop.clone();
        let reader = std::thread::spawn(move || {
            let mut samples = Vec::new();
            while !reader_stop.load(Ordering::Relaxed) {
                samples.push(query(&reader_store, &filter));
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            samples
        });
        let progress = ProgressReporter::new();
        let started = Instant::now();
        let scan =
            scan_sessions_observed(&project, &owner, &store, &config, None, &|| true, &progress);
        let elapsed = started.elapsed().as_micros();
        stop.store(true, Ordering::Relaxed);
        let mut samples = reader.join().unwrap();
        scan?;
        samples.sort_unstable();
        idle.sort_unstable();
        assert!(!samples.is_empty());
        println!(
            "concurrent trial={trial} queries={} idle_p95_us={} active_p95_us={} active_max_us={} import_us={elapsed} first_progress_ms={:?} first_commit_ms={:?} durable_txn_ms={}",
            samples.len(),
            idle[idle.len() * 95 / 100],
            samples[samples.len() * 95 / 100],
            samples.last().unwrap(),
            progress.snapshot().first_progress_ms,
            progress.snapshot().first_commit_ms,
            progress.snapshot().work.durable_txn_ms
        );
    }
    Ok(())
}
