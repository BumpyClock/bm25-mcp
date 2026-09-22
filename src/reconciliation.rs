//! Collection lifecycle and the database-to-memory read boundary.

use crate::{
    coverage::{CollectionCoverage, CoverageSnapshot},
    model::ScanReport,
};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Scope {
    Full,
    Sources(HashSet<PathBuf>),
}

impl Scope {
    pub(crate) fn from_paths(paths: Option<HashSet<PathBuf>>) -> Self {
        paths.map_or(Self::Full, Self::Sources)
    }

    pub(crate) fn paths(&self) -> Option<&HashSet<PathBuf>> {
        match self {
            Self::Full => None,
            Self::Sources(paths) => Some(paths),
        }
    }

    fn merge(&mut self, other: Self) {
        match (&mut *self, other) {
            (Self::Sources(paths), Self::Sources(more)) => {
                paths.extend(more);
                if paths.len() > 1024 {
                    *self = Self::Full;
                }
            }
            _ => *self = Self::Full,
        }
    }
}

struct State {
    observed: u64,
    publication: u64,
    next_run: u64,
    pending: Option<Scope>,
    git_pending: bool,
    active: Option<Ticket>,
    safe_revision: Option<u64>,
    ledger: CollectionCoverage,
    watch_error: Option<String>,
}

#[derive(Clone)]
pub(crate) struct Controller {
    collection: String,
    incarnation: Arc<()>,
    state: Arc<Mutex<State>>,
}

#[derive(Clone)]
struct Ticket {
    incarnation: Arc<()>,
    collection: String,
    run: u64,
    observed: u64,
    scope: Scope,
    git: bool,
}

pub(crate) struct Run<'a> {
    controller: &'a Controller,
    ticket: Ticket,
    finished: bool,
}

/// A bounded direct publication channel tied to one run and controller incarnation.
#[derive(Clone)]
pub(crate) struct Publisher {
    controller: Controller,
    ticket: Ticket,
}

impl std::fmt::Debug for Publisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Publisher")
    }
}

impl Publisher {
    pub(crate) fn source(
        &self,
        publication: &crate::store::SourcePublication,
        outcome: Option<&crate::coverage::SourceOutcome>,
    ) {
        let mut state = self.controller.state.lock().unwrap();
        if self.controller.matches(&state, &self.ticket) {
            state.ledger.publish_source(publication.key(), outcome);
            state.publication += 1;
        }
    }
}

pub(crate) struct ReadSnapshot {
    pub(crate) coverage: CoverageSnapshot,
    pub(crate) status: &'static str,
    pub(crate) eligible: bool,
    token: ReadToken,
}

struct ReadToken {
    incarnation: Arc<()>,
    observed: u64,
    publication: u64,
}

impl Controller {
    pub(crate) fn new(collection: String) -> Self {
        Self {
            collection,
            incarnation: Arc::new(()),
            state: Arc::new(Mutex::new(State {
                observed: 1,
                publication: 0,
                next_run: 0,
                pending: Some(Scope::Full),
                git_pending: false,
                active: None,
                safe_revision: None,
                ledger: CollectionCoverage::default(),
                watch_error: None,
            })),
        }
    }

    pub(crate) fn observe(&self, scope: Scope, git: bool) {
        let mut state = self.state.lock().unwrap();
        state.observed += 1;
        state.git_pending |= git;
        Self::queue(&mut state, scope);
    }

    fn queue(state: &mut State, scope: Scope) {
        let scope = match scope {
            Scope::Sources(paths) if paths.len() > 1024 => Scope::Full,
            scope => scope,
        };
        if let Some(pending) = &mut state.pending {
            pending.merge(scope);
        } else {
            state.pending = Some(scope);
        }
    }

    pub(crate) fn watch_error(&self, error: Option<String>) {
        let mut state = self.state.lock().unwrap();
        if state.watch_error != error {
            state.watch_error = error;
            state.publication += 1;
        }
    }

    pub(crate) fn active(&self) -> bool {
        self.state.lock().unwrap().active.is_some()
    }

    pub(crate) fn needs_work(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.pending.is_some() || state.active.is_some()
    }

    pub(crate) fn begin(&self) -> Option<Run<'_>> {
        let mut state = self.state.lock().unwrap();
        if state.active.is_some() {
            return None;
        }
        let scope = state.pending.take()?;
        state.next_run += 1;
        let run = state.next_run;
        state.publication += 1;
        let ticket = Ticket {
            incarnation: self.incarnation.clone(),
            collection: self.collection.clone(),
            run,
            observed: state.observed,
            scope,
            git: std::mem::take(&mut state.git_pending),
        };
        state.active = Some(ticket.clone());
        Some(Run {
            controller: self,
            ticket,
            finished: false,
        })
    }

    fn matches(&self, state: &State, ticket: &Ticket) -> bool {
        Arc::ptr_eq(&self.incarnation, &ticket.incarnation)
            && self.collection == ticket.collection
            && state.active.as_ref().is_some_and(|active| {
                active.run == ticket.run
                    && active.observed == ticket.observed
                    && active.scope == ticket.scope
            })
    }

    fn invalidated(&self, ticket: &Ticket) {
        let mut state = self.state.lock().unwrap();
        if self.matches(&state, ticket) {
            state.safe_revision = Some(ticket.observed);
            state.publication += 1;
        }
    }

    fn finish(&self, ticket: &Ticket, result: &anyhow::Result<ScanReport>) -> bool {
        let mut state = self.state.lock().unwrap();
        if !self.matches(&state, ticket) {
            return false;
        }
        let invalid_report = Err(anyhow::anyhow!(
            "completion belongs to another run or scope"
        ));
        let result = if result.as_ref().is_ok_and(|report| {
            report.coverage.full != matches!(ticket.scope, Scope::Full)
                || report
                    .coverage
                    .publisher
                    .as_ref()
                    .is_none_or(|publisher| !self.matches(&state, &publisher.ticket))
        }) {
            &invalid_report
        } else {
            result
        };
        let completed = result.as_ref().is_ok_and(|report| {
            !report.cancelled
                && report.coverage.discovery_complete
                && report.coverage.full == matches!(ticket.scope, Scope::Full)
        });
        state.ledger.apply(result);
        state.active = None;
        state.publication += 1;
        if !completed || state.safe_revision != Some(ticket.observed) {
            Self::queue(
                &mut state,
                if result.is_err() {
                    Scope::Full
                } else {
                    ticket.scope.clone()
                },
            );
            state.git_pending |= ticket.git;
            state.safe_revision = None;
        }
        completed
    }

    pub(crate) fn snapshot(&self) -> ReadSnapshot {
        let state = self.state.lock().unwrap();
        let idle = state.active.is_none() && state.pending.is_none();
        let mut coverage = state.ledger.snapshot();
        if !idle {
            coverage.pending_changes = None;
        }
        if let Some(error) = &state.watch_error {
            coverage.error_count += 1;
            coverage.errors.push(error.clone());
            *coverage
                .diagnostics
                .entry("watcher_unavailable".into())
                .or_default() += 1;
        }
        ReadSnapshot {
            status: coverage.status(),
            coverage,
            eligible: state.safe_revision == Some(state.observed),
            token: ReadToken {
                incarnation: self.incarnation.clone(),
                observed: state.observed,
                publication: state.publication,
            },
        }
    }

    pub(crate) fn validates(&self, snapshot: &ReadSnapshot) -> bool {
        let state = self.state.lock().unwrap();
        Arc::ptr_eq(&self.incarnation, &snapshot.token.incarnation)
            && state.observed == snapshot.token.observed
            && state.publication == snapshot.token.publication
    }
}

impl Run<'_> {
    pub(crate) fn scope(&self) -> &Scope {
        &self.ticket.scope
    }
    pub(crate) fn git_pending(&self) -> bool {
        self.ticket.git
    }
    /// Git discovery may widen consumed work; observations arriving meanwhile stay queued.
    pub(crate) fn widen(&mut self, scope: Scope) {
        self.ticket.scope.merge(scope);
        self.controller.state.lock().unwrap().active = Some(self.ticket.clone());
    }

    pub(crate) fn project(
        mut self,
        root: &Path,
        store: &crate::store::Store,
        keep_going: &dyn Fn() -> bool,
        cache: Option<&mut crate::content_cache::ContentCache>,
        progress: &crate::progress::ProgressReporter,
    ) -> anyhow::Result<bool> {
        let result = crate::ingest::invalidate_project_scope(
            root,
            store,
            &self.ticket.collection,
            self.ticket.scope.paths(),
        )
        .and_then(|()| {
            self.controller.invalidated(&self.ticket);
            crate::ingest::scan_project_publishing(
                root,
                store,
                &self.ticket.collection,
                self.ticket.scope.paths(),
                keep_going,
                cache,
                progress,
                Some(Publisher {
                    controller: self.controller.clone(),
                    ticket: self.ticket.clone(),
                }),
            )
        });
        let completed = self.controller.finish(&self.ticket, &result);
        self.finished = true;
        result.map(|_| completed)
    }

    pub(crate) fn sessions(
        mut self,
        root: &Path,
        store: &crate::store::Store,
        config: &crate::sessions::SessionConfig,
        keep_going: &dyn Fn() -> bool,
        progress: &crate::progress::ProgressReporter,
    ) -> anyhow::Result<bool> {
        let result = crate::sessions::invalidate_session_scope(
            &self.ticket.collection,
            store,
            config,
            self.ticket.scope.paths(),
        )
        .and_then(|()| {
            self.controller.invalidated(&self.ticket);
            crate::sessions::scan_sessions_publishing(
                root,
                &self.ticket.collection,
                store,
                config,
                self.ticket.scope.paths(),
                keep_going,
                progress,
                Some(Publisher {
                    controller: self.controller.clone(),
                    ticket: self.ticket.clone(),
                }),
            )
        });
        let completed = self.controller.finish(&self.ticket, &result);
        self.finished = true;
        result.map(|_| completed)
    }
}

impl Drop for Run<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.controller.finish(
                &self.ticket,
                &Err(anyhow::anyhow!("reconciliation abandoned")),
            );
        }
    }
}

#[cfg(test)]
#[path = "reconciliation_tests.rs"]
mod tests;
