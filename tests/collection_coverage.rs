use bm25_mcp::{
    coverage::CollectionCoverage,
    ingest,
    model::{ScanReport, SearchFilter},
    progress::ProgressReporter,
    sessions::{self, SessionConfig},
    store::Store,
};
use serde_json::json;
use std::{
    collections::HashSet,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

struct Fixture {
    _dir: tempfile::TempDir,
    root: PathBuf,
    owner: String,
    config: SessionConfig,
    store: Store,
    a: PathBuf,
    b: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("project");
        let codex = dir.path().join("codex");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(codex.join("sessions")).unwrap();
        fs::create_dir_all(codex.join("archived_sessions")).unwrap();
        let a = codex.join("sessions/a.jsonl");
        let b = codex.join("archived_sessions/b.jsonl");
        let fixture = Self {
            owner: ingest::project_identity(&root).unwrap().owner_key,
            config: SessionConfig {
                codex_home: codex,
                claude_config_dir: dir.path().join("claude"),
                copilot_home: dir.path().join("copilot"),
                ..SessionConfig::default()
            },
            store: Store::open(&dir.path().join("index.sqlite3")).unwrap(),
            root,
            a,
            b,
            _dir: dir,
        };
        fixture.write(&fixture.b, "clean", "unaffectedmarker");
        fixture
    }

    fn event(id: &str) -> String {
        format!(
            "{}\n",
            json!({"type":"response_item","payload":{"type":"message","id":id,"role":"user","content":[{"type":"text","text":id}]}})
        )
    }

    fn write(&self, path: &Path, problem: &str, marker: &str) {
        let header = json!({"type":"session_meta","payload":{"cwd":self.root,"id":marker}});
        let prefix = format!("{header}\n{}", Self::event(marker));
        fs::write(
            path,
            match problem {
                "tail" => format!("{prefix}{{\"type\":"),
                "error" => format!("{prefix}not-json\n"),
                "rejected" => Self::event(marker),
                _ => prefix,
            },
        )
        .unwrap();
    }

    fn scan(
        &self,
        paths: Option<&HashSet<PathBuf>>,
        progress: &ProgressReporter,
        keep_going: &dyn Fn() -> bool,
    ) -> anyhow::Result<ScanReport> {
        sessions::scan_sessions_observed(
            &self.root,
            &self.owner,
            &self.store,
            &self.config,
            paths,
            keep_going,
            progress,
        )
    }

    fn reconcile(
        &self,
        ledger: &mut CollectionCoverage,
        paths: Option<&HashSet<PathBuf>>,
    ) -> ProgressReporter {
        let progress = ProgressReporter::new();
        let report = self.scan(paths, &progress, &|| true);
        assert!(report.is_ok(), "{report:?}");
        ledger.apply(&report);
        progress
    }

    fn hits(&self, marker: &str) -> usize {
        self.store
            .search(
                marker,
                &SearchFilter {
                    collection: self.owner.clone(),
                    kind: "session".into(),
                    ..SearchFilter::default()
                },
                10,
            )
            .unwrap()
            .1
            .len()
    }
}

#[test]
fn repairs_deletions_and_repeated_precise_scans_replace_source_outcomes() {
    for problem in ["tail", "error", "rejected"] {
        let fixture = Fixture::new();
        fixture.write(&fixture.a, problem, "problemmarker");
        let mut ledger = CollectionCoverage::default();
        fixture.reconcile(&mut ledger, None);
        let initial = ledger.snapshot();
        if problem == "tail" {
            assert_eq!(initial.pending_changes, Some(1));
        } else {
            assert!(initial.error_count > 0);
        }
        if problem == "rejected" {
            assert_eq!(
                fixture
                    .store
                    .sources(&fixture.owner, "session")
                    .unwrap()
                    .len(),
                1
            );
        }
        let b_scope = HashSet::from([fixture.b.clone()]);
        for index in 0..3 {
            let mut file = OpenOptions::new().append(true).open(&fixture.b).unwrap();
            write!(file, "{}", Fixture::event(&format!("newmarker{index}"))).unwrap();
            drop(file);
            let progress = fixture.reconcile(&mut ledger, Some(&b_scope)).snapshot();
            let current = ledger.snapshot();
            assert_eq!(current.pending_changes, initial.pending_changes);
            assert_eq!(current.error_count, initial.error_count);
            assert_eq!(current.excluded_count, initial.excluded_count);
            assert_eq!(current.diagnostics, initial.diagnostics);
            assert_eq!(current.errors, initial.errors);
            assert_eq!(progress.files_discovered, 1);
            assert_eq!(progress.files_completed, 1);
            assert_eq!(progress.records_processed, 1);
            assert_eq!(progress.records_inspected, 1);
        }
        let a_scope = HashSet::from([fixture.a.clone()]);
        fixture.write(&fixture.a, "clean", "repairedmarker");
        fixture.reconcile(&mut ledger, Some(&a_scope));
        assert_eq!(ledger.snapshot().pending_changes, Some(0));
        assert_eq!(ledger.snapshot().error_count, 0);
        assert_eq!(ledger.snapshot().excluded_count, 0);
        assert_eq!(fixture.hits("repairedmarker"), 1);

        fixture.write(&fixture.a, problem, "problemmarker");
        fixture.reconcile(&mut ledger, None);
        let rebuilt = ledger.snapshot();
        assert_eq!(rebuilt.pending_changes, initial.pending_changes);
        assert_eq!(rebuilt.error_count, initial.error_count);
        assert_eq!(rebuilt.excluded_count, initial.excluded_count);
        assert_eq!(rebuilt.diagnostics, initial.diagnostics);
        fs::remove_file(&fixture.a).unwrap();
        fixture.reconcile(&mut ledger, Some(&a_scope));
        assert_eq!(ledger.snapshot().pending_changes, Some(0));
        assert_eq!(ledger.snapshot().error_count, 0);
        assert_eq!(ledger.snapshot().excluded_count, 0);
        assert_eq!(fixture.hits("unaffectedmarker"), 1);
    }
}

#[test]
fn failed_discovery_preserves_unseen_outcomes_even_without_source_rows() {
    for problem in ["tail", "rejected"] {
        let fixture = Fixture::new();
        fixture.write(&fixture.a, problem, "discoverymarker");
        let mut ledger = CollectionCoverage::default();
        fixture.reconcile(&mut ledger, None);
        let initial = ledger.snapshot();
        let source_rows = fixture
            .store
            .sources(&fixture.owner, "session")
            .unwrap()
            .len();
        let sessions = fixture.config.codex_home.join("sessions");
        let saved = fixture.config.codex_home.join("saved");
        fs::rename(&sessions, &saved).unwrap();
        fs::write(&sessions, "not a directory").unwrap();
        fixture.reconcile(&mut ledger, None);
        let failed = ledger.snapshot();
        assert_eq!(failed.pending_changes, None);
        assert!(failed.error_count > initial.error_count);
        assert_eq!(failed.excluded_count, initial.excluded_count);
        assert_eq!(
            fixture
                .store
                .sources(&fixture.owner, "session")
                .unwrap()
                .len(),
            source_rows
        );
        let cancelled = fixture.scan(None, &ProgressReporter::new(), &|| false);
        ledger.apply(&cancelled);
        assert_eq!(ledger.snapshot().pending_changes, None);
        assert!(ledger.snapshot().diagnostics.contains_key("discovery"));
        fixture.reconcile(&mut ledger, Some(&HashSet::from([fixture.b.clone()])));
        assert_eq!(ledger.snapshot().pending_changes, None);
        assert!(ledger.snapshot().diagnostics.contains_key("discovery"));
        assert_eq!(ledger.snapshot().excluded_count, initial.excluded_count);
        fs::remove_file(&sessions).unwrap();
        fs::rename(&saved, &sessions).unwrap();
        fixture.reconcile(&mut ledger, None);
        assert_eq!(ledger.snapshot().pending_changes, initial.pending_changes);
        assert_eq!(ledger.snapshot().error_count, initial.error_count);
        assert_eq!(ledger.snapshot().diagnostics, initial.diagnostics);
    }
}

#[test]
fn cancelled_precise_scan_keeps_unvisited_outcomes_and_unaffected_content() {
    let fixture = Fixture::new();
    fixture.write(&fixture.a, "tail", "pendingmarker");
    let c = fixture.config.codex_home.join("archived_sessions/c.jsonl");
    fixture.write(&c, "clean", "thirdmarker");
    let mut ledger = CollectionCoverage::default();
    fixture.reconcile(&mut ledger, None);
    let progress = ProgressReporter::new();
    let changes = HashSet::from([fixture.b.clone(), c.clone()]);
    let observed = std::cell::Cell::new(false);
    let result = fixture.scan(Some(&changes), &progress, &|| {
        if !observed.replace(true) {
            assert_eq!(fixture.hits("pendingmarker"), 1);
        }
        progress.snapshot().files_completed == 0
    });
    assert!(result.as_ref().unwrap().cancelled);
    assert_eq!(progress.snapshot().files_completed, 1);
    assert!(observed.get());
    ledger.apply(&result);
    assert_eq!(ledger.snapshot().pending_changes, None);
    fixture.reconcile(&mut ledger, Some(&HashSet::from([fixture.a.clone()])));
    assert_eq!(ledger.snapshot().pending_changes, None);
    fixture.reconcile(&mut ledger, Some(&changes));
    assert_eq!(ledger.snapshot().pending_changes, Some(1));
    assert_eq!(fixture.hits("unaffectedmarker"), 1);
}

#[test]
fn restart_requires_full_reconstruction_despite_durable_checkpoints() {
    let fixture = Fixture::new();
    fixture.write(&fixture.a, "error", "persistedmarker");
    let mut original = CollectionCoverage::default();
    fixture.reconcile(&mut original, None);
    let original_errors = original.snapshot().error_count;
    let mut restarted = CollectionCoverage::default();
    assert_eq!(restarted.snapshot().pending_changes, None);
    assert_eq!(restarted.snapshot().reconciled_at, None);
    fixture.reconcile(&mut restarted, Some(&HashSet::from([fixture.b.clone()])));
    assert_eq!(restarted.snapshot().pending_changes, None);
    fixture.reconcile(&mut restarted, None);
    assert_eq!(restarted.snapshot().pending_changes, Some(0));
    assert_eq!(restarted.snapshot().error_count, original_errors);
    for _ in 0..2 {
        fixture.reconcile(&mut restarted, None);
        assert_eq!(restarted.snapshot().error_count, original_errors);
    }
}

#[test]
fn legacy_checkpoints_replay_inspection_diagnostics_before_claiming_coverage() {
    let fixture = Fixture::new();
    fs::write(
        &fixture.a,
        format!(
            "{}\n{}",
            json!({"type":"session_meta","payload":{"cwd":fixture.root,"id":"x".repeat(5000)}}),
            Fixture::event("legacymarker"),
        ),
    )
    .unwrap();
    let mut original = CollectionCoverage::default();
    fixture.reconcile(&mut original, None);
    assert!(original.snapshot().diagnostics.contains_key("metadata"));
    let source = fixture
        .store
        .sources(&fixture.owner, "session")
        .unwrap()
        .into_iter()
        .find(|source| source.path.ends_with("a.jsonl"))
        .unwrap();
    let checkpoint = fixture
        .store
        .session_checkpoint(&source.key)
        .unwrap()
        .unwrap();
    let mut legacy: serde_json::Value = serde_json::from_str(&checkpoint.state).unwrap();
    legacy["schema"] = json!(6);
    legacy["diagnostics"]
        .as_object_mut()
        .unwrap()
        .remove("metadata");
    let connection = rusqlite::Connection::open(fixture._dir.path().join("index.sqlite3")).unwrap();
    connection
        .execute(
            "UPDATE session_checkpoints SET state=?1 WHERE source_key=?2",
            rusqlite::params![legacy.to_string(), source.key],
        )
        .unwrap();
    drop(connection);
    let mut restarted = CollectionCoverage::default();
    let progress = fixture.reconcile(&mut restarted, None).snapshot();
    assert_eq!(
        restarted.snapshot().error_count,
        original.snapshot().error_count
    );
    assert_eq!(
        restarted.snapshot().diagnostics,
        original.snapshot().diagnostics
    );
    assert_eq!(
        progress.records_inspected, 2,
        "legacy prefix must be replayed"
    );
    assert_eq!(fixture.hits("legacymarker"), 1);
}

#[test]
fn only_authoritative_full_scans_can_remove_unseen_rejected_sources() {
    let fixture = Fixture::new();
    fixture.write(&fixture.a, "rejected", "rejectedmarker");
    let mut ledger = CollectionCoverage::default();
    fixture.reconcile(&mut ledger, None);
    assert_eq!(ledger.snapshot().excluded_count, 1);
    fs::remove_file(&fixture.a).unwrap();
    let result = fixture.scan(None, &ProgressReporter::new(), &|| false);
    assert!(result.as_ref().unwrap().cancelled);
    ledger.apply(&result);
    assert_eq!(ledger.snapshot().excluded_count, 1);
    assert_eq!(ledger.snapshot().pending_changes, None);
    fixture.reconcile(&mut ledger, None);
    assert_eq!(ledger.snapshot().excluded_count, 0);
    assert_eq!(ledger.snapshot().error_count, 0);
    assert_eq!(ledger.snapshot().pending_changes, Some(0));
}

#[test]
fn unrelated_precise_success_does_not_clear_discovery_or_run_failure() {
    let fixture = Fixture::new();
    fixture.write(&fixture.a, "rejected", "problem");
    let mut ledger = CollectionCoverage::default();
    fixture.reconcile(&mut ledger, None);
    let source_errors = ledger.snapshot().error_count;
    fixture.reconcile(
        &mut ledger,
        Some(&HashSet::from([fixture.root.join("unknown.txt")])),
    );
    assert!(ledger.snapshot().error_count > source_errors);
    let failure_errors = ledger.snapshot().error_count;
    fixture.reconcile(&mut ledger, Some(&HashSet::from([fixture.b.clone()])));
    assert_eq!(ledger.snapshot().pending_changes, None);
    assert_eq!(ledger.snapshot().error_count, failure_errors);
    fixture.reconcile(&mut ledger, None);
    assert_eq!(ledger.snapshot().error_count, source_errors);
    assert_eq!(ledger.snapshot().pending_changes, Some(0));
    ledger.apply(&Err(anyhow::anyhow!("failed durable operation")));
    assert_eq!(ledger.snapshot().pending_changes, None);
    assert_eq!(ledger.snapshot().error_count, source_errors + 1);
    fixture.reconcile(&mut ledger, Some(&HashSet::from([fixture.b.clone()])));
    assert_eq!(ledger.snapshot().pending_changes, None);
    assert_eq!(ledger.snapshot().error_count, source_errors + 1);
    fixture.reconcile(&mut ledger, None);
    assert_eq!(ledger.snapshot().error_count, source_errors);
}

#[test]
fn project_precise_scan_preserves_rejected_text_without_source_rows() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    fs::create_dir_all(&root).unwrap();
    let a = root.join("a.txt");
    let b = root.join("b.txt");
    fs::write(&a, b"\xff\xff\xff").unwrap();
    fs::write(&b, "unaffectedprojectmarker").unwrap();
    let store = Store::open(&dir.path().join("index.sqlite3")).unwrap();
    let collection = ingest::project_identity(&root).unwrap().collection;
    let mut ledger = CollectionCoverage::default();
    let scan = |paths: Option<&HashSet<PathBuf>>| {
        ingest::scan_project_observed(
            &root,
            &store,
            &collection,
            paths,
            &|| true,
            None,
            &ProgressReporter::new(),
        )
    };
    ledger.apply(&scan(None));
    assert_eq!(ledger.snapshot().error_count, 1);
    assert_eq!(ledger.snapshot().excluded_count, 1);
    assert_eq!(
        store
            .sources(&collection, ingest::PROJECT_SOURCE_KIND)
            .unwrap()
            .len(),
        1
    );
    for _ in 0..3 {
        ledger.apply(&scan(Some(&HashSet::from([b.clone()]))));
        assert_eq!(ledger.snapshot().error_count, 1);
        assert_eq!(ledger.snapshot().excluded_count, 1);
        assert_eq!(
            ledger.snapshot().diagnostics.get("unsupported_encoding"),
            Some(&1)
        );
    }
    fs::write(&a, "repairedprojectmarker").unwrap();
    ledger.apply(&scan(Some(&HashSet::from([a.clone()]))));
    assert_eq!(ledger.snapshot().error_count, 0);
    assert_eq!(ledger.snapshot().excluded_count, 0);
    fs::write(&a, b"\xff\xff\xff").unwrap();
    ledger.apply(&scan(None));
    assert_eq!(ledger.snapshot().error_count, 1);
    fs::remove_file(&a).unwrap();
    ledger.apply(&scan(Some(&HashSet::from([a]))));
    assert_eq!(ledger.snapshot().error_count, 0);
    assert_eq!(ledger.snapshot().excluded_count, 0);
}
