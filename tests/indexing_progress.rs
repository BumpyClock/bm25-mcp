use bm25_mcp::{
    ingest::{self, PROJECT_SOURCE_KIND, project_identity},
    model::SearchFilter,
    progress::{ProgressPhase, ProgressReporter},
    sessions::{self, SessionConfig},
    store::Store,
};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

fn store_at(root: &Path) -> Store {
    Store::open(&root.join("index.sqlite3")).expect("open store")
}

fn filter(collection: &str, kind: &str) -> SearchFilter {
    SearchFilter {
        collection: collection.to_owned(),
        kind: kind.to_owned(),
        ..SearchFilter::default()
    }
}

fn jsonl(path: &Path, values: impl IntoIterator<Item = Value>) {
    let mut bytes = Vec::new();
    for value in values {
        bytes.extend(serde_json::to_vec(&value).expect("encode JSON"));
        bytes.push(b'\n');
    }
    fs::write(path, bytes).expect("write JSONL");
}

#[test]
fn snapshots_are_bounded_and_polling_does_not_change_work_timestamps() {
    let reporter = ProgressReporter::new();
    reporter.begin_run();
    reporter.set_phase(ProgressPhase::Discovery);
    reporter.record_discovered(3);
    reporter.record_source_bytes(128, 64);
    reporter.record_record();
    reporter.record_prepared_chunks(2);
    reporter.record_committed_chunks(1);
    let first = reporter.snapshot();
    let last_progress_at = first.last_progress_at.clone();
    let polled = reporter.snapshot();
    assert_eq!(polled.last_progress_at, last_progress_at);
    assert_eq!(polled.files_discovered, 3);
    assert_eq!(polled.records_processed, 1);
    assert_eq!(polled.bytes_read, 128);
    assert_eq!(polled.bytes_hashed_for_verification, 64);
    assert_eq!(polled.chunks_prepared, 2);
    assert_eq!(polled.chunks_committed, 1);
    assert!(polled.first_progress_ms.is_some());
    assert!(polled.first_commit_ms.is_some());
    let encoded = serde_json::to_value(&polled).expect("serialize snapshot");
    assert_eq!(encoded["phase"], "discovery");
    assert!(encoded["work"]["discovery_ms"].is_number());
    assert!(encoded["work"]["durable_txn_ms"].is_number());
    assert!(encoded["phase_history"].is_array());

    reporter.finish_run(ProgressPhase::Complete);
    let finished = reporter.snapshot();
    std::thread::sleep(Duration::from_millis(20));
    let finished_later = reporter.snapshot();
    assert_eq!(finished_later.elapsed_ms, finished.elapsed_ms);
    assert_eq!(finished_later.phase_elapsed_ms, finished.phase_elapsed_ms);
    assert_eq!(finished_later.phase_timings_ms, finished.phase_timings_ms);

    reporter.begin_run();
    let restarted = reporter.snapshot();
    assert!(restarted.run_id > finished.run_id);
    assert_eq!(restarted.phase, ProgressPhase::Idle);
    assert_eq!(restarted.files_discovered, 0);
    assert_eq!(restarted.elapsed_ms, 0);

    for index in 0..80 {
        reporter.set_phase(if index % 2 == 0 {
            ProgressPhase::Ownership
        } else {
            ProgressPhase::Normalization
        });
    }
    let bounded = reporter.snapshot();
    assert!(bounded.phase_history.len() <= 64);
    assert!(bounded.phase_transitions_dropped > 0);
}

#[test]
fn precise_project_reconciliation_preserves_unrelated_sources() {
    let dir = tempfile::tempdir().expect("temp directory");
    let root = dir.path().join("project");
    fs::create_dir_all(&root).expect("project directory");
    let first = root.join("first.txt");
    let second = root.join("second.txt");
    fs::write(&first, "zzprojectoldonly").expect("first source");
    fs::write(&second, "zzprojectunrelatedonly").expect("second source");
    let identity = project_identity(&root).expect("project identity");
    let store = store_at(dir.path());

    let initial_progress = ProgressReporter::new();
    ingest::scan_project_observed(
        &root,
        &store,
        &identity.collection,
        None,
        &|| true,
        None,
        &initial_progress,
    )
    .expect("initial project scan");
    assert_eq!(
        store
            .search(
                "zzprojectoldonly",
                &filter(&identity.collection, PROJECT_SOURCE_KIND),
                10,
            )
            .expect("old search")
            .1
            .len(),
        1
    );

    fs::write(&first, "zzprojectnewonly").expect("changed source");
    let changed = HashSet::from([first.clone()]);
    let progress = ProgressReporter::new();
    ingest::scan_project_observed(
        &root,
        &store,
        &identity.collection,
        Some(&changed),
        &|| true,
        None,
        &progress,
    )
    .expect("precise project scan");

    let project_filter = filter(&identity.collection, PROJECT_SOURCE_KIND);
    assert!(
        store
            .search("zzprojectoldonly", &project_filter, 10)
            .expect("old marker search")
            .1
            .is_empty()
    );
    assert_eq!(
        store
            .search("zzprojectnewonly", &project_filter, 10)
            .expect("new marker search")
            .1
            .len(),
        1
    );
    assert_eq!(
        store
            .search("zzprojectunrelatedonly", &project_filter, 10)
            .expect("unrelated marker search")
            .1
            .len(),
        1
    );
    let snapshot = progress.snapshot();
    assert_eq!(snapshot.files_discovered, 1);
    assert_eq!(snapshot.files_completed, 1);
    assert!(snapshot.bytes_read > 0);
    assert!(snapshot.bytes_hashed_for_verification > 0);
    assert!(snapshot.chunks_prepared >= snapshot.chunks_committed);
    assert!(snapshot.chunks_committed > 0);
}

#[test]
fn precise_project_scan_respects_ignore_policy_for_post_ready_env_changes() {
    let dir = tempfile::tempdir().expect("temp directory");
    let root = dir.path().join("project");
    fs::create_dir_all(&root).expect("project directory");
    let init = Command::new("git")
        .args(["-C", root.to_str().expect("root path"), "init", "-q"])
        .status()
        .expect("git installed");
    assert!(init.success());
    fs::write(root.join(".gitignore"), ".env\nnested/\n").expect("ignore file");
    fs::write(root.join("visible.txt"), "visible_after_ready_marker").expect("visible source");
    let identity = project_identity(&root).expect("project identity");
    let store = store_at(dir.path());

    ingest::scan_project_observed(
        &root,
        &store,
        &identity.collection,
        None,
        &|| true,
        None,
        &ProgressReporter::new(),
    )
    .expect("initial project scan");

    let ignored = root.join(".env");
    fs::write(&ignored, "ignored_create_sentinel").expect("ignored create");
    let changed = HashSet::from([ignored.clone()]);
    let first = ingest::scan_project_observed(
        &root,
        &store,
        &identity.collection,
        Some(&changed),
        &|| true,
        None,
        &ProgressReporter::new(),
    )
    .expect("ignored create scan");
    assert_eq!(first.sources, 0);
    assert!(
        store
            .search(
                "ignored_create_sentinel",
                &filter(&identity.collection, PROJECT_SOURCE_KIND),
                10,
            )
            .expect("ignored create search")
            .1
            .is_empty()
    );

    fs::write(&ignored, "ignored_modify_sentinel").expect("ignored modify");
    let second = ingest::scan_project_observed(
        &root,
        &store,
        &identity.collection,
        Some(&changed),
        &|| true,
        None,
        &ProgressReporter::new(),
    )
    .expect("ignored modify scan");
    assert_eq!(second.sources, 0);
    let project_filter = filter(&identity.collection, PROJECT_SOURCE_KIND);
    assert!(
        store
            .search("ignored_create_sentinel", &project_filter, 10)
            .expect("old ignored search")
            .1
            .is_empty()
    );
    assert!(
        store
            .search("ignored_modify_sentinel", &project_filter, 10)
            .expect("new ignored search")
            .1
            .is_empty()
    );
    assert_eq!(
        store
            .search("visible_after_ready_marker", &project_filter, 10)
            .expect("visible source search")
            .1
            .len(),
        1
    );
}

#[test]
fn precise_session_reconciliation_preserves_unrelated_checkpoints() {
    let dir = tempfile::tempdir().expect("temp directory");
    let project = dir.path().join("project");
    let codex = dir.path().join("codex");
    fs::create_dir_all(&project).expect("project directory");
    fs::create_dir_all(codex.join("sessions")).expect("codex sessions");
    let first = codex.join("sessions/first.jsonl");
    let second = codex.join("sessions/second.jsonl");
    let make_session = |id: &str, marker: &str| {
        let cwd = project.to_string_lossy().into_owned();
        [
            json!({"type":"session_meta","payload":{"cwd":cwd,"id":id}}),
            json!({"type":"response_item","payload":{"type":"message","id":format!("{id}-event"),"role":"user","content":[{"type":"text","text":marker}]}}),
        ]
    };
    jsonl(&first, make_session("first", "zzsessionoldonly"));
    jsonl(&second, make_session("second", "zzsessionunrelatedonly"));
    let identity = project_identity(&project).expect("project identity");
    let store = store_at(dir.path());
    let config = SessionConfig {
        codex_home: codex,
        claude_config_dir: dir.path().join("claude"),
        copilot_home: dir.path().join("copilot"),
        ..SessionConfig::default()
    };

    sessions::scan_sessions_observed(
        &project,
        &identity.owner_key,
        &store,
        &config,
        None,
        &|| true,
        &ProgressReporter::new(),
    )
    .expect("initial session scan");

    jsonl(&first, make_session("first", "zzsessionnewonly"));
    let changed = HashSet::from([first.clone()]);
    let progress = ProgressReporter::new();
    sessions::scan_sessions_observed(
        &project,
        &identity.owner_key,
        &store,
        &config,
        Some(&changed),
        &|| true,
        &progress,
    )
    .expect("precise session scan");

    let session_filter = filter(&identity.owner_key, "session");
    assert!(
        store
            .search("zzsessionoldonly", &session_filter, 10)
            .expect("old session marker")
            .1
            .is_empty()
    );
    assert_eq!(
        store
            .search("zzsessionnewonly", &session_filter, 10)
            .expect("new session marker")
            .1
            .len(),
        1
    );
    assert_eq!(
        store
            .search("zzsessionunrelatedonly", &session_filter, 10)
            .expect("unrelated session marker")
            .1
            .len(),
        1
    );
    assert!(
        store
            .sources(&identity.owner_key, "session")
            .expect("sources")
            .len()
            >= 2
    );
    let snapshot = progress.snapshot();
    assert_eq!(snapshot.files_discovered, 1);
    assert_eq!(snapshot.files_completed, 1);
    assert!(snapshot.records_processed > 0);
    assert!(snapshot.records_inspected > 0);
    assert!(snapshot.bytes_hashed_for_verification > 0);
    assert!(snapshot.chunks_prepared >= snapshot.chunks_committed);
    assert!(snapshot.chunks_committed > 0);
}

#[test]
fn precise_session_append_normalizes_only_the_changed_suffix() {
    let dir = tempfile::tempdir().expect("temp directory");
    let project = dir.path().join("project");
    let codex = dir.path().join("codex");
    fs::create_dir_all(&project).expect("project directory");
    fs::create_dir_all(codex.join("sessions")).expect("codex sessions");
    let cwd = project.to_string_lossy().into_owned();
    let history = |id: &str, marker: &str| {
        let mut values = vec![json!({
            "type": "session_meta",
            "payload": {"cwd": cwd, "id": id}
        })];
        for index in 0..24 {
            values.push(json!({
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "id": format!("{id}-{index}"),
                    "role": "user",
                    "content": [{
                        "type": "text",
                        "text": format!("{marker} {}", "historypayload".repeat(512))
                    }]
                }
            }));
        }
        values
    };

    let changed = codex.join("sessions/changed.jsonl");
    jsonl(&changed, history("changed", "zzlargehistorymarker"));
    let unrelated = (0..8)
        .map(|index| {
            let path = codex.join(format!("sessions/unrelated-{index}.jsonl"));
            jsonl(
                &path,
                history(&format!("unrelated-{index}"), "zzunrelatedhistorymarker"),
            );
            path
        })
        .collect::<Vec<_>>();
    let unrelated_bytes = unrelated
        .iter()
        .map(|path| fs::metadata(path).expect("unrelated metadata").len())
        .sum::<u64>();

    let identity = project_identity(&project).expect("project identity");
    let store = store_at(dir.path());
    let config = SessionConfig {
        codex_home: codex,
        claude_config_dir: dir.path().join("claude"),
        copilot_home: dir.path().join("copilot"),
        ..SessionConfig::default()
    };
    sessions::scan_sessions_observed(
        &project,
        &identity.owner_key,
        &store,
        &config,
        None,
        &|| true,
        &ProgressReporter::new(),
    )
    .expect("initial session scan");

    let append_record = json!({
        "type": "response_item",
        "payload": {
            "type": "message",
            "id": "changed-append",
            "role": "assistant",
            "content": [{"type": "text", "text": "zzappendedsuffixmarker"}]
        }
    });
    let suffix_bytes = serde_json::to_vec(&append_record).expect("encode suffix");
    let prefix_bytes = fs::metadata(&changed).expect("prefix metadata").len();
    let mut append = OpenOptions::new()
        .append(true)
        .open(&changed)
        .expect("open changed session");
    append.write_all(&suffix_bytes).expect("append suffix");
    append.write_all(b"\n").expect("terminate suffix");
    drop(append);

    let progress = ProgressReporter::new();
    let changed_paths = HashSet::from([changed.clone()]);
    sessions::scan_sessions_observed(
        &project,
        &identity.owner_key,
        &store,
        &config,
        Some(&changed_paths),
        &|| true,
        &progress,
    )
    .expect("precise append scan");

    let snapshot = progress.snapshot();
    assert_eq!(snapshot.files_discovered, 1);
    assert_eq!(snapshot.files_completed, 1);
    assert_eq!(snapshot.records_inspected, 1);
    assert_eq!(snapshot.records_processed, 1);
    assert!(snapshot.bytes_hashed_for_verification > 0);
    assert!(snapshot.bytes_read > suffix_bytes.len() as u64);
    assert_eq!(
        snapshot.bytes_read,
        prefix_bytes + 2 * (suffix_bytes.len() as u64 + 1)
    );
    assert!(
        snapshot.bytes_read.saturating_mul(2) < unrelated_bytes,
        "precise scan read unrelated bytes: {snapshot:?}, unrelated_bytes={unrelated_bytes}"
    );
    assert_eq!(
        snapshot.work.session_hash_bytes,
        prefix_bytes + 4 * (suffix_bytes.len() as u64 + 1)
    );
    assert_eq!(
        snapshot.work.json_inspection_bytes,
        suffix_bytes.len() as u64 + 1
    );
    assert!(snapshot.chunks_prepared >= snapshot.chunks_committed);
    assert!(snapshot.chunks_committed > 0);

    let session_filter = filter(&identity.owner_key, "session");
    assert_eq!(
        store
            .search("zzappendedsuffixmarker", &session_filter, 10)
            .expect("suffix search")
            .1
            .len(),
        1
    );
    assert_eq!(
        store
            .sources(&identity.owner_key, "session")
            .expect("session sources")
            .len(),
        9
    );
}

#[test]
fn scratch_batch_counters_match_successful_commit_boundaries() {
    let dir = tempfile::tempdir().expect("temp directory");
    let project = dir.path().join("project");
    let codex = dir.path().join("codex");
    fs::create_dir_all(&project).expect("project directory");
    fs::create_dir_all(codex.join("sessions")).expect("codex sessions");
    let cwd = project.to_string_lossy().into_owned();
    let mut values = vec![json!({
        "type": "session_meta",
        "payload": {"cwd": cwd, "id": "batched"}
    })];
    for index in 0..1025 {
        values.push(json!({
            "type": "response_item",
            "payload": {
                "type": "message",
                "id": format!("batched-{index}"),
                "role": "user",
                "content": [{"type": "text", "text": format!("zzbatchmarker{index}")}]
            }
        }));
    }
    jsonl(&codex.join("sessions/batched.jsonl"), values);
    let identity = project_identity(&project).expect("project identity");
    let store = store_at(dir.path());
    let config = SessionConfig {
        codex_home: codex,
        claude_config_dir: dir.path().join("claude"),
        copilot_home: dir.path().join("copilot"),
        ..SessionConfig::default()
    };
    let progress = ProgressReporter::new();
    sessions::scan_sessions_observed(
        &project,
        &identity.owner_key,
        &store,
        &config,
        None,
        &|| true,
        &progress,
    )
    .expect("batched session scan");

    let snapshot = progress.snapshot();
    assert_eq!(snapshot.work.scratch_state_writes, 1025);
    assert_eq!(snapshot.work.scratch_state_transactions, 3);
    assert_eq!(snapshot.work.temp_dedup_writes_count, 1025);
    assert_eq!(snapshot.work.durable_txn_count, 1);
    assert!(snapshot.work.scratch_state_write_bytes >= 1025);
    assert_eq!(snapshot.work.scratch_lookup_count, 1025);
    assert_eq!(snapshot.work.scratch_begin_count, 3);
    assert_eq!(snapshot.work.scratch_insert_count, 1025);
    assert!(snapshot.work.scratch_insert_ms <= snapshot.work.scratch_transaction_lifetime_ms);
    assert!(snapshot.work.temp_dedup_writes_ms <= snapshot.work.scratch_transaction_lifetime_ms);
    assert!(snapshot.work.tokenization_bytes > 0);
    assert!(snapshot.work.temp_file_ops_bytes > 0);
}

#[test]
fn large_json_record_reports_parser_work_before_commit() {
    let dir = tempfile::tempdir().expect("temp directory");
    let project = dir.path().join("project");
    let codex = dir.path().join("codex");
    fs::create_dir_all(&project).expect("project directory");
    fs::create_dir_all(codex.join("sessions")).expect("codex sessions");
    let cwd = project.to_string_lossy().into_owned();
    let large_text = format!(
        "zzlargeparsermarker {}",
        "large-json-payload ".repeat(2 * 1024 * 1024 / 19)
    );
    let path = codex.join("sessions/large.jsonl");
    jsonl(
        &path,
        [
            json!({
                "type": "session_meta",
                "payload": {"cwd": cwd, "id": "large"}
            }),
            json!({
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "id": "large-event",
                    "role": "user",
                    "content": [{"type": "text", "text": large_text}]
                }
            }),
        ],
    );
    let identity = project_identity(&project).expect("project identity");
    let store = store_at(dir.path());
    let config = SessionConfig {
        codex_home: codex,
        claude_config_dir: dir.path().join("claude"),
        copilot_home: dir.path().join("copilot"),
        ..SessionConfig::default()
    };
    let progress = ProgressReporter::new();
    let worker_progress = progress.clone();
    let worker_store = store.clone();
    let worker_config = config.clone();
    let worker_project = project.clone();
    let worker_owner = identity.owner_key.clone();
    let worker = thread::spawn(move || {
        sessions::scan_sessions_observed(
            &worker_project,
            &worker_owner,
            &worker_store,
            &worker_config,
            None,
            &|| true,
            &worker_progress,
        )
    });

    let deadline = Instant::now() + Duration::from_secs(20);
    let mut parser_work_observed = false;
    let mut intermediate = progress.snapshot();
    while Instant::now() < deadline {
        intermediate = progress.snapshot();
        if intermediate.work.json_inspection_bytes > 0 && intermediate.chunks_committed == 0 {
            parser_work_observed = true;
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    let report = worker.join().expect("scan worker").expect("large scan");
    assert!(
        parser_work_observed,
        "parser work was not observable before commit: {intermediate:?}"
    );
    assert_eq!(report.error_count, 0, "{report:?}");
    assert!(progress.snapshot().work.json_inspection_bytes > 0);
    assert!(
        store
            .search(
                "zzlargeparsermarker",
                &filter(&identity.owner_key, "session"),
                10,
            )
            .expect("large marker search")
            .1
            .len()
            == 1
    );
}

#[test]
fn cancelled_scan_never_reports_a_committed_chunk() {
    let dir = tempfile::tempdir().expect("temp directory");
    let root = dir.path().join("project");
    fs::create_dir_all(&root).expect("project directory");
    fs::write(root.join("large.txt"), "cancel_marker ".repeat(4096)).expect("source");
    let identity = project_identity(&root).expect("project identity");
    let store = store_at(dir.path());
    let progress = ProgressReporter::new();
    let result = ingest::scan_project_observed(
        &root,
        &store,
        &identity.collection,
        None,
        &|| false,
        None,
        &progress,
    );
    assert!(result.is_err());
    let snapshot = progress.snapshot();
    assert_eq!(snapshot.phase, ProgressPhase::Cancelled);
    assert_eq!(snapshot.chunks_committed, 0);
}
