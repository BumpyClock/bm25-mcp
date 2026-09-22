//! Current collection outcomes, separate from scan-run work counters.

use crate::model::ScanReport;
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Clone, Debug, Default)]
pub struct SourceOutcome {
    pub(crate) version: Option<String>,
    pub pending: u64,
    pub errors: u64,
    pub excluded: u64,
    pub diagnostics: BTreeMap<String, u64>,
}

impl SourceOutcome {
    pub(crate) fn from_report(report: &ScanReport) -> Self {
        Self {
            version: None,
            pending: report.pending_count,
            errors: report.error_count,
            excluded: report.excluded_count,
            diagnostics: report
                .diagnostics
                .iter()
                .filter(|(category, _)| {
                    !matches!(
                        category.as_str(),
                        "unchanged_index_reuse" | "content_cache_hit" | "content_cache_miss"
                    )
                })
                .map(|(category, count)| (category.clone(), *count))
                .collect(),
        }
    }

    fn add(&mut self, other: &Self) {
        self.pending += other.pending;
        self.errors += other.errors;
        self.excluded += other.excluded;
        for (category, count) in &other.diagnostics {
            *self.diagnostics.entry(category.clone()).or_default() += count;
        }
    }

    fn subtract(&mut self, other: &Self) {
        self.pending -= other.pending;
        self.errors -= other.errors;
        self.excluded -= other.excluded;
        for (category, count) in &other.diagnostics {
            let total = self.diagnostics.get_mut(category).expect("counted outcome");
            *total -= count;
            if *total == 0 {
                self.diagnostics.remove(category);
            }
        }
    }
}

/// Keys include collection identity. `None` means a confirmed durable removal.
#[derive(Clone, Debug, Default)]
pub struct CoverageUpdate {
    pub(crate) publisher: Option<crate::reconciliation::Publisher>,
    pub full: bool,
    pub discovery_complete: bool,
    pub sources: BTreeMap<String, Option<SourceOutcome>>,
    pub discovery: SourceOutcome,
    pub scope: HashSet<String>,
}

/// The controller owns this ledger; snapshots never enumerate source rows.
#[derive(Default)]
pub struct CollectionCoverage {
    sources: HashMap<String, SourceOutcome>,
    totals: SourceOutcome,
    discovery: SourceOutcome,
    scoped_discovery: SourceOutcome,
    reconstructed: bool,
    unfinished: bool,
    scan_failed: bool,
    discovery_uncertain: bool,
    unfinished_sources: HashSet<String>,
    reconciled_at: Option<String>,
}

impl CollectionCoverage {
    pub fn apply(&mut self, result: &anyhow::Result<ScanReport>) {
        let Ok(report) = result else {
            self.reconstructed = false;
            self.unfinished = true;
            self.scan_failed = true;
            return;
        };
        let update = &report.coverage;
        let completed = !report.cancelled && update.discovery_complete;
        self.unfinished_sources.extend(update.scope.iter().cloned());
        if update.full && !completed {
            self.reconstructed = false;
        }
        if !update.discovery_complete {
            self.discovery_uncertain = true;
        } else if update.full && !report.cancelled {
            self.discovery_uncertain = false;
        }
        if update.full && completed {
            self.sources.retain(|key, outcome| {
                if update.sources.contains_key(key) {
                    true
                } else {
                    self.totals.subtract(outcome);
                    false
                }
            });
            self.reconstructed = true;
            self.unfinished_sources.clear();
            self.scoped_discovery = SourceOutcome::default();
            self.scan_failed = false;
        }
        for (key, outcome) in &update.sources {
            self.publish_source(key, outcome.as_ref());
        }
        if update.full && !report.cancelled {
            self.discovery = update.discovery.clone();
        } else if !update.full && !update.discovery_complete && !report.cancelled {
            self.scoped_discovery = update.discovery.clone();
        }
        self.unfinished = !completed;
        if completed {
            self.reconciled_at = Some(chrono::Utc::now().to_rfc3339());
        }
    }

    pub(crate) fn publish_source(&mut self, key: &str, outcome: Option<&SourceOutcome>) {
        self.unfinished_sources.remove(key);
        if let Some(old) = self.sources.remove(key) {
            self.totals.subtract(&old);
        }
        if let Some(outcome) = outcome {
            self.totals.add(outcome);
            self.sources.insert(key.to_owned(), outcome.clone());
        }
    }

    #[cfg(test)]
    pub(crate) fn sources_version_for_test(&self, key: &str) -> Option<&str> {
        self.sources
            .get(key)
            .and_then(|outcome| outcome.version.as_deref())
    }

    pub fn snapshot(&self) -> CoverageSnapshot {
        let mut total = self.totals.clone();
        total.add(&self.discovery);
        total.add(&self.scoped_discovery);
        if self.scan_failed {
            total.errors += 1;
            total.diagnostics.insert("reconciliation_failed".into(), 1);
        }
        CoverageSnapshot {
            pending_changes: (self.reconstructed
                && !self.unfinished
                && !self.scan_failed
                && !self.discovery_uncertain
                && self.unfinished_sources.is_empty())
            .then_some(total.pending),
            error_count: total.errors,
            excluded_count: total.excluded,
            diagnostics: total.diagnostics,
            reconciled_at: self.reconciled_at.clone(),
            ..CoverageSnapshot::default()
        }
    }
}

/// Internal coverage facts. Wire adapters add memory observations and redact errors.
#[derive(Clone, Debug, Default)]
pub struct CoverageSnapshot {
    pub diagnostics: BTreeMap<String, u64>,
    pub reconciled_at: Option<String>,
    pub pending_changes: Option<u64>,
    pub excluded_count: u64,
    pub error_count: u64,
    pub errors: Vec<String>,
}

impl CoverageSnapshot {
    pub(crate) fn status(&self) -> &'static str {
        if self.pending_changes.is_none() {
            if self.reconciled_at.is_none() {
                "building"
            } else {
                "refreshing"
            }
        } else if self.pending_changes.is_some_and(|count| count > 0) {
            "refreshing"
        } else if self.error_count > 0 {
            "degraded"
        } else {
            "ready"
        }
    }
}
