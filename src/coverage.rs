//! Current collection outcomes, separate from scan-run work counters.

use crate::{model::ScanReport, tools::Coverage};
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Clone, Debug, Default)]
pub struct SourceOutcome {
    pub pending: u64,
    pub errors: u64,
    pub excluded: u64,
    pub diagnostics: BTreeMap<String, u64>,
}

impl SourceOutcome {
    pub(crate) fn from_report(report: &ScanReport) -> Self {
        Self {
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
    pub full: bool,
    pub discovery_complete: bool,
    pub sources: BTreeMap<String, Option<SourceOutcome>>,
    pub discovery: SourceOutcome,
    pub scope: HashSet<String>,
}

/// Owned by the collection's single scan worker, never by the status reader.
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
            self.unfinished_sources.remove(key);
            if let Some(old) = self.sources.remove(key) {
                self.totals.subtract(&old);
            }
            if let Some(outcome) = outcome {
                self.totals.add(outcome);
                self.sources.insert(key.clone(), outcome.clone());
            }
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

    pub fn snapshot(&self) -> Coverage {
        let mut total = self.totals.clone();
        total.add(&self.discovery);
        total.add(&self.scoped_discovery);
        if self.scan_failed {
            total.errors += 1;
            total.diagnostics.insert("reconciliation_failed".into(), 1);
        }
        Coverage {
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
            ..Coverage::default()
        }
    }
}
