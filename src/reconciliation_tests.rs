use super::*;
use crate::{
    coverage::SourceOutcome,
    model::{Chunk, SearchFilter, Source},
    progress::ProgressReporter,
    store::Store,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    sync::mpsc,
    thread,
    time::Duration,
};

fn ticket(controller: &Controller) -> Option<Ticket> {
    controller.begin().map(|mut run| {
        run.finished = true;
        run.ticket.clone()
    })
}

fn report(controller: &Controller, ticket: &Ticket) -> ScanReport {
    let mut report = ScanReport::default();
    report.coverage.full = matches!(ticket.scope, Scope::Full);
    report.coverage.discovery_complete = true;
    report.coverage.publisher = Some(Publisher {
        controller: controller.clone(),
        ticket: ticket.clone(),
    });
    report
}

fn settled(controller: &Controller) {
    let ticket = ticket(controller).unwrap();
    controller.invalidated(&ticket);
    controller.finish(&ticket, &Ok(report(controller, &ticket)));
    assert_eq!(controller.snapshot().status, "ready");
}

// The model tracks unacknowledged event identities, not production revisions,
// safe epochs, ledger deltas or the controller's queue merging algorithm.
#[derive(Default)]
struct Model {
    outstanding: BTreeSet<usize>,
    captured: BTreeSet<usize>,
    sources: BTreeMap<String, (u64, u64)>,
    known: bool,
}

#[test]
fn bounded_transition_sequences_obey_event_obligations_and_source_facts() {
    const EVENTS: usize = 12;
    for encoded in 0..EVENTS.pow(4) {
        let mut controller = Controller::new("collection".into());
        let mut model = Model {
            outstanding: BTreeSet::from([0]),
            ..Default::default()
        };
        let mut active = None;
        let mut older = None;
        let mut read = None;
        let mut code = encoded;
        for step in 1..=4 {
            let event = code % EVENTS;
            code /= EVENTS;
            match event {
                0 | 1 => {
                    controller.observe(
                        if event == 0 {
                            Scope::Sources(HashSet::from([PathBuf::from("a")]))
                        } else {
                            Scope::Full
                        },
                        false,
                    );
                    model.outstanding.insert(step);
                }
                2 if active.is_none() => {
                    active = ticket(&controller);
                    if let Some(ticket) = &active {
                        controller.invalidated(ticket);
                        model.captured = model.outstanding.clone();
                    }
                }
                3..=5 if active.is_some() => {
                    // Commit, reject and confirmed deletion. Each delivery is repeated.
                    let ticket = active.as_ref().unwrap();
                    assert!(controller.matches(&controller.state.lock().unwrap(), ticket));
                    let outcome = if event == 5 {
                        None
                    } else {
                        Some(SourceOutcome {
                            pending: u64::from(event == 3),
                            errors: u64::from(event == 4),
                            ..Default::default()
                        })
                    };
                    for _ in 0..2 {
                        let mut state = controller.state.lock().unwrap();
                        state.ledger.publish_source("a", outcome.as_ref());
                        state.publication += 1;
                    }
                    if let Some(outcome) = outcome {
                        model
                            .sources
                            .insert("a".into(), (outcome.pending, outcome.errors));
                    } else {
                        model.sources.remove("a");
                    }
                }
                6..=8 if active.is_some() => {
                    let ticket = active.take().unwrap();
                    let mut report = report(&controller, &ticket);
                    report.cancelled = event == 7;
                    report.coverage.discovery_complete = event != 8;
                    // Authoritative discovery reports the complete set; no parser is modeled.
                    report.coverage.sources = model
                        .sources
                        .iter()
                        .map(|(key, (pending, errors))| {
                            (
                                key.clone(),
                                Some(SourceOutcome {
                                    pending: *pending,
                                    errors: *errors,
                                    ..Default::default()
                                }),
                            )
                        })
                        .collect();
                    if event == 6 {
                        model
                            .outstanding
                            .retain(|event| !model.captured.contains(event));
                        if report.coverage.full {
                            model.known = true;
                        }
                    } else {
                        model.known = false;
                    }
                    controller.finish(&ticket, &Ok(report));
                    older = Some(ticket);
                }
                9 => {
                    read = Some(controller.snapshot());
                    let before = controller.snapshot();
                    let progress = ProgressReporter::new();
                    for _ in 0..3 {
                        progress.snapshot();
                        controller.snapshot();
                    }
                    assert!(controller.validates(&before), "polling mutated state");
                    if let Some(older) = &older {
                        controller.finish(older, &Ok(report(&controller, older)));
                        assert!(
                            controller.validates(&before),
                            "older completion mutated state"
                        );
                    }
                }
                10 => {
                    controller = Controller::new("collection".into());
                    active = None;
                    model = Model {
                        outstanding: BTreeSet::from([step]),
                        ..Default::default()
                    };
                    if let Some(read) = &read {
                        assert!(!controller.validates(read));
                    }
                }
                11 => {
                    if let Some(old) = &older {
                        let before = controller.snapshot();
                        controller.finish(old, &Ok(report(&controller, old)));
                        assert!(controller.validates(&before));
                    }
                }
                _ => {}
            }
            let snapshot = controller.snapshot();
            assert_eq!(
                snapshot.coverage.error_count,
                model
                    .sources
                    .values()
                    .map(|(_, errors)| errors)
                    .sum::<u64>(),
                "sequence {encoded}, step {step}"
            );
            if active.is_some() || !model.outstanding.is_empty() || !model.known {
                assert_eq!(
                    snapshot.coverage.pending_changes, None,
                    "sequence {encoded}, step {step}"
                );
            } else {
                assert_eq!(
                    snapshot.coverage.pending_changes,
                    Some(model.sources.values().map(|(pending, _)| pending).sum()),
                    "sequence {encoded}, step {step}"
                );
            }
        }
    }
}

#[test]
fn scope_overflow_reconnect_and_stale_or_misscoped_completion() {
    let controller = Controller::new("c".into());
    settled(&controller);
    controller.observe(Scope::Sources(HashSet::from([PathBuf::from("a")])), false);
    let old = ticket(&controller).unwrap();
    controller.invalidated(&old);
    controller.observe(Scope::Sources(HashSet::from([PathBuf::from("b")])), false);
    controller.finish(&old, &Ok(report(&controller, &old)));
    assert_eq!(controller.snapshot().coverage.pending_changes, None);
    assert_eq!(
        controller.state.lock().unwrap().pending,
        Some(Scope::Sources(HashSet::from([PathBuf::from("b")])))
    );
    let next = ticket(&controller).unwrap();
    let mut wrong = next.clone();
    wrong.scope = Scope::Full;
    controller.finish(&wrong, &Ok(report(&controller, &wrong)));
    assert!(controller.active());
    controller.invalidated(&next);
    controller.finish(&next, &Ok(report(&controller, &next)));
    let before = controller.snapshot();
    controller.finish(&old, &Ok(report(&controller, &old)));
    assert!(controller.validates(&before));
    controller.observe(
        Scope::Sources((0..1025).map(|i| PathBuf::from(i.to_string())).collect()),
        false,
    );
    assert_eq!(controller.state.lock().unwrap().pending, Some(Scope::Full));
    controller.observe(Scope::Sources(HashSet::new()), false);
    assert!(matches!(controller.begin().unwrap().scope(), Scope::Full));
}

#[test]
fn watcher_recovery_and_status_precedence_preserve_source_diagnostics() {
    let controller = Controller::new("c".into());
    settled(&controller);
    controller.state.lock().unwrap().ledger.publish_source(
        "bad",
        Some(&SourceOutcome {
            errors: 2,
            ..Default::default()
        }),
    );
    controller.watch_error(Some("watcher_unavailable".into()));
    assert_eq!(controller.snapshot().coverage.error_count, 3);
    controller.watch_error(None);
    assert_eq!(controller.snapshot().status, "degraded");
    assert_eq!(controller.snapshot().coverage.error_count, 2);
    controller.observe(Scope::Full, false);
    assert_eq!(controller.snapshot().status, "refreshing");
    assert_eq!(controller.snapshot().coverage.error_count, 2);
}

struct Fixture {
    _dir: tempfile::TempDir,
    root: PathBuf,
    store: Store,
    controller: Controller,
}
impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("project");
        fs::create_dir(&root).unwrap();
        fs::write(root.join("a.txt"), "changedmarker").unwrap();
        fs::write(root.join("b.txt"), "unaffectedmarker").unwrap();
        let collection = crate::ingest::project_identity(&root).unwrap().collection;
        let fixture = Self {
            store: Store::open(&dir.path().join("index.sqlite3")).unwrap(),
            controller: Controller::new(collection),
            root,
            _dir: dir,
        };
        fixture.scan(&|| true, &ProgressReporter::new()).unwrap();
        fixture
    }
    fn scan(&self, keep: &dyn Fn() -> bool, progress: &ProgressReporter) -> anyhow::Result<bool> {
        self.controller
            .begin()
            .unwrap()
            .project(&self.root, &self.store, keep, None, progress)
    }
    fn source(&self) -> Source {
        self.store
            .sources(&self.controller.collection, "project")
            .unwrap()
            .remove(0)
    }
    fn hits(&self, text: &str) -> usize {
        self.store
            .search(
                text,
                &SearchFilter {
                    collection: self.controller.collection.clone(),
                    kind: "project".into(),
                    ..Default::default()
                },
                10,
            )
            .unwrap()
            .1
            .len()
    }
}

#[test]
fn actual_prepare_change_and_status_then_search_change_are_conservative() {
    let fixture = Fixture::new();
    let before = fixture.controller.snapshot();
    let a = fixture.root.join("a.txt");
    fs::write(&a, "newmarker ".repeat(5000)).unwrap();
    fixture
        .controller
        .observe(Scope::Sources(HashSet::from([a.clone()])), false);
    assert!(!fixture.controller.validates(&before));
    let progress = ProgressReporter::new();
    let changed = std::cell::Cell::new(false);
    fixture
        .scan(
            &|| {
                if progress.snapshot().bytes_read > 0 && !changed.replace(true) {
                    assert_eq!(fixture.hits("unaffectedmarker"), 1);
                    fixture
                        .controller
                        .observe(Scope::Sources(HashSet::from([a.clone()])), false);
                }
                true
            },
            &progress,
        )
        .unwrap();
    assert!(changed.get());
    assert_eq!(fixture.controller.snapshot().coverage.pending_changes, None);
    fixture.scan(&|| true, &ProgressReporter::new()).unwrap();
    assert_eq!(fixture.controller.snapshot().status, "ready");
}

#[test]
fn durable_failure_and_commit_to_metadata_gap_with_restart() {
    let fixture = Fixture::new();
    let initial = fixture.controller.snapshot();
    fixture.controller.observe(
        Scope::Sources(HashSet::from([fixture.root.join("a.txt")])),
        false,
    );
    let mut run = fixture.controller.begin().unwrap();
    crate::ingest::invalidate_project_scope(
        &fixture.root,
        &fixture.store,
        &fixture.controller.collection,
        run.scope().paths(),
    )
    .unwrap();
    fixture.controller.invalidated(&run.ticket);
    let during = fixture.controller.snapshot();
    assert!(during.eligible);
    assert_eq!(during.coverage.pending_changes, None);
    let mut source = fixture.source();
    source.version = "new-version".into();
    let progress = ProgressReporter::new();
    progress.record_prepared_chunks(1);
    let failed = fixture.store.replace_source(
        &source,
        vec![
            Ok(Chunk {
                text: "rolledbackmarker".into(),
                ..Default::default()
            }),
            Err(anyhow::anyhow!("injected before commit")),
        ],
    );
    assert!(failed.is_err());
    assert!(
        fixture
            .store
            .confirm_source_publication(&source.key, Some(&source.version))
            .is_err()
    );
    assert_eq!(progress.snapshot().chunks_committed, 0);
    assert_eq!(fixture.hits("rolledbackmarker"), 0);

    let (committed, observed) = mpsc::channel();
    let (release, resume) = mpsc::channel();
    let store = fixture.store.clone();
    let writer_source = source.clone();
    let writer = thread::spawn(move || {
        store
            .replace_source(
                &writer_source,
                vec![Ok(Chunk {
                    text: "committedmarker".into(),
                    ..Default::default()
                })],
            )
            .unwrap();
        committed.send(()).unwrap();
        resume.recv().unwrap();
    });
    observed.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(fixture.hits("committedmarker"), 1);
    assert!(!fixture.controller.validates(&initial));
    assert_eq!(fixture.controller.snapshot().coverage.pending_changes, None);
    let restarted = Controller::new(fixture.controller.collection.clone());
    assert_eq!(restarted.snapshot().status, "building");
    assert!(!restarted.snapshot().eligible);
    assert!(!restarted.validates(&during));
    release.send(()).unwrap();
    writer.join().unwrap();
    let mut completion = report(&fixture.controller, &run.ticket);
    completion
        .record_source(
            &fixture.store,
            source.key.clone(),
            Some(&source.version),
            ScanReport {
                sources: 1,
                ..Default::default()
            },
        )
        .unwrap();
    assert!(!fixture.controller.validates(&during));
    completion.cancelled = true;
    fixture.controller.finish(&run.ticket, &Ok(completion));
    run.finished = true;
    assert_eq!(
        fixture
            .controller
            .state
            .lock()
            .unwrap()
            .ledger
            .sources_version_for_test(&source.key),
        Some(source.version.as_str())
    );
    assert_eq!(fixture.controller.snapshot().coverage.pending_changes, None);
    assert_eq!(fixture.hits("unaffectedmarker"), 1);
}

#[test]
fn unrelated_collection_database_commit_does_not_expire_a_read() {
    let fixture = Fixture::new();
    let read = fixture.controller.snapshot();
    fixture
        .store
        .replace_source(
            &Source {
                key: "other".into(),
                collection: "other".into(),
                path: "other".into(),
                version: "v".into(),
                kind: "project".into(),
            },
            vec![Ok(Chunk {
                text: "othermarker".into(),
                ..Default::default()
            })],
        )
        .unwrap();
    assert!(fixture.controller.validates(&read));
}

#[test]
fn authoritative_full_and_successful_incremental_reconciliation_converge() {
    let full = Fixture::new();
    let incremental = Fixture::new();
    for fixture in [&full, &incremental] {
        fs::write(fixture.root.join("a.txt"), b"\xff\xff\xff").unwrap();
        fs::write(fixture.root.join("b.txt"), "updatedmarker").unwrap();
    }
    full.controller.observe(Scope::Full, false);
    full.scan(&|| true, &ProgressReporter::new()).unwrap();
    for name in ["a.txt", "b.txt", "b.txt"] {
        incremental.controller.observe(
            Scope::Sources(HashSet::from([incremental.root.join(name)])),
            false,
        );
        incremental
            .scan(&|| true, &ProgressReporter::new())
            .unwrap();
    }
    let a = full.controller.snapshot();
    let b = incremental.controller.snapshot();
    assert_eq!(a.status, b.status);
    assert_eq!(a.coverage.pending_changes, b.coverage.pending_changes);
    assert_eq!(a.coverage.error_count, b.coverage.error_count);
    assert_eq!(a.coverage.excluded_count, b.coverage.excluded_count);
    assert_eq!(a.coverage.diagnostics, b.coverage.diagnostics);
    assert_eq!(
        full.hits("updatedmarker"),
        incremental.hits("updatedmarker")
    );
}

#[test]
fn cancellation_after_repair_retains_committed_outcome_and_unvisited_errors() {
    let fixture = Fixture::new();
    for name in ["a.txt", "b.txt"] {
        fs::write(fixture.root.join(name), b"\xff\xff\xff").unwrap();
    }
    fixture.controller.observe(Scope::Full, false);
    fixture.scan(&|| true, &ProgressReporter::new()).unwrap();
    assert_eq!(fixture.controller.snapshot().coverage.error_count, 2);
    let path = fixture.root.join("a.txt");
    fs::write(&path, "repairedmarker").unwrap();
    fixture
        .controller
        .observe(Scope::Sources(HashSet::from([path])), false);
    let progress = ProgressReporter::new();
    assert!(
        fixture
            .scan(&|| progress.snapshot().chunks_committed == 0, &progress)
            .is_err()
    );
    assert_eq!(progress.snapshot().chunks_committed, 1);
    let coverage = fixture.controller.snapshot().coverage;
    assert_eq!(coverage.error_count, 2); // One untouched source error plus the incomplete run.
    assert_eq!(coverage.diagnostics.get("unsupported_encoding"), Some(&1));
    assert_eq!(coverage.pending_changes, None);
    assert_eq!(fixture.hits("repairedmarker"), 1);
}

#[test]
fn project_spool_finalization_failure_aborts_before_commit_and_retries() {
    use crate::record_spool::{
        FinalizeError,
        testing::{Failure, fail_next_finish},
    };
    for failure in [Failure::Flush, Failure::Open] {
        let fixture = Fixture::new();
        let original_sources = fixture
            .store
            .sources(&fixture.controller.collection, "project")
            .unwrap();
        let changed = fixture.root.join("a.txt");
        fs::write(&changed, "replacementmarker").unwrap();
        fixture
            .controller
            .observe(Scope::Sources(HashSet::from([changed])), false);
        let progress = ProgressReporter::new();
        let _fault = fail_next_finish::<Chunk>(failure);
        let error = fixture.scan(&|| true, &progress).unwrap_err();
        assert!(error.downcast_ref::<FinalizeError>().is_some(), "{error:#}");
        assert_eq!(
            progress.snapshot().phase,
            crate::progress::ProgressPhase::Failed
        );
        assert_eq!(progress.snapshot().chunks_committed, 0);
        assert_eq!(progress.snapshot().work.durable_txn_count, 0);
        assert_eq!(fixture.controller.snapshot().coverage.pending_changes, None);
        assert!(fixture.controller.needs_work());
        let sources = fixture
            .store
            .sources(&fixture.controller.collection, "project")
            .unwrap();
        assert_eq!(sources.len(), original_sources.len());
        for source in sources {
            assert_eq!(
                source.version,
                original_sources
                    .iter()
                    .find(|old| old.key == source.key)
                    .unwrap()
                    .version
            );
        }
        assert_eq!(fixture.hits("replacementmarker"), 0);
        assert_eq!(fixture.hits("unaffectedmarker"), 1);
        assert!(fixture.scan(&|| true, &ProgressReporter::new()).unwrap());
        assert_eq!(fixture.controller.snapshot().status, "ready");
        assert_eq!(fixture.hits("replacementmarker"), 1);
    }
}

#[test]
fn session_spool_finalization_failure_preserves_checkpoint_and_retries() {
    use crate::{
        record_spool::{
            FinalizeError,
            testing::{Failure, fail_next_finish},
        },
        sessions::SessionConfig,
    };
    use std::io::Write;
    for state_updates in [false, true] {
        for failure in [Failure::Flush, Failure::Open] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("project");
            fs::create_dir(&root).unwrap();
            let config = SessionConfig {
                codex_home: dir.path().join("codex"),
                claude_config_dir: dir.path().join("claude"),
                copilot_home: dir.path().join("copilot"),
                own_tool_names: vec![],
                identity_registry_path: None,
            };
            fs::create_dir_all(config.codex_home.join("sessions")).unwrap();
            let path = config.codex_home.join("sessions/events.jsonl");
            let message = |id: &str, text: &str| {
                serde_json::json!({
                    "type": "response_item",
                    "payload": {"type": "message", "id": id, "role": "assistant",
                        "content": [{"type": "text", "text": text}]}
                })
            };
            fs::write(&path, format!("{}\n{}\n",
                serde_json::json!({"type": "session_meta", "payload": {"cwd": root, "id": "session"}}),
                message("original", "originalmarker"),
            )).unwrap();
            let store = Store::open(&dir.path().join("index.sqlite3")).unwrap();
            let controller =
                Controller::new(crate::ingest::project_identity(&root).unwrap().owner_key);
            let scan = |progress: &ProgressReporter| {
                controller
                    .begin()
                    .unwrap()
                    .sessions(&root, &store, &config, &|| true, progress)
            };
            assert!(scan(&ProgressReporter::new()).unwrap());
            let source = store
                .sources(&controller.collection, "session")
                .unwrap()
                .pop()
                .unwrap();
            let checkpoint = store.session_checkpoint(&source.key).unwrap().unwrap();
            let mut append = fs::OpenOptions::new().append(true).open(&path).unwrap();
            writeln!(append, "{}", message("replacement", "replacementmarker")).unwrap();
            drop(append);
            controller.observe(Scope::Sources(HashSet::from([path])), false);
            let _fault = if state_updates {
                fail_next_finish::<(String, String, Option<String>)>(failure)
            } else {
                fail_next_finish::<Chunk>(failure)
            };
            let progress = ProgressReporter::new();
            let error = scan(&progress).unwrap_err();
            assert!(error.downcast_ref::<FinalizeError>().is_some(), "{error:#}");
            assert_eq!(
                progress.snapshot().phase,
                crate::progress::ProgressPhase::Failed
            );
            assert_eq!(progress.snapshot().chunks_committed, 0);
            assert_eq!(progress.snapshot().work.durable_txn_count, 0);
            assert_eq!(
                store.session_checkpoint(&source.key).unwrap(),
                Some(checkpoint.clone())
            );
            assert_eq!(
                store.sources(&controller.collection, "session").unwrap()[0].version,
                source.version
            );
            assert_eq!(controller.snapshot().coverage.pending_changes, None);
            assert!(controller.needs_work());
            let filter = SearchFilter {
                collection: controller.collection.clone(),
                kind: "session".into(),
                ..Default::default()
            };
            assert!(
                store
                    .search("replacementmarker", &filter, 10)
                    .unwrap()
                    .1
                    .is_empty()
            );
            assert!(scan(&ProgressReporter::new()).unwrap());
            assert_eq!(controller.snapshot().status, "ready");
            assert_eq!(
                store
                    .search("replacementmarker", &filter, 10)
                    .unwrap()
                    .1
                    .len(),
                1
            );
            assert_eq!(
                store.search("originalmarker", &filter, 10).unwrap().1.len(),
                1
            );
            assert!(
                store
                    .session_checkpoint(&source.key)
                    .unwrap()
                    .unwrap()
                    .offset
                    > checkpoint.offset
            );
        }
    }
}
