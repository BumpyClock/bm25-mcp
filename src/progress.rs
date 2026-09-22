//! Bounded, lock-only progress reporting for indexing workers.
//!
//! The reporter deliberately owns no source names, paths, errors, or store
//! handles.  A snapshot is therefore safe to expose while a worker is
//! holding the store write lock, and polling a snapshot never changes the
//! reported progress timestamp.

use chrono::Utc;
use serde::Serialize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const MAX_PHASE_HISTORY: usize = 64;
const PUBLICATION_INTERVAL: Duration = Duration::from_secs(1);

/// The bounded set of phases that may appear in a public progress snapshot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgressPhase {
    #[default]
    Idle,
    Discovery,
    Ownership,
    PrefixVerification,
    JsonInspection,
    Normalization,
    TempFileOps,
    Tokenization,
    ScratchWrites,
    DurableTransaction,
    Complete,
    Cancelled,
    Failed,
}

impl ProgressPhase {
    fn is_terminal(self) -> bool {
        matches!(self, Self::Complete | Self::Cancelled | Self::Failed)
    }
}

/// Fixed work categories used by the D2 timing output.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkKind {
    Discovery,
    Ownership,
    PrefixVerification,
    JsonInspection,
    Normalization,
    TempFileOps,
    Tokenization,
    TempDedupWrites,
    DurableTxn,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ScratchBatchMetrics {
    pub lookup: Duration,
    pub lookup_count: u64,
    pub begin: Duration,
    pub begin_count: u64,
    pub insert: Duration,
    pub insert_count: u64,
    pub commit: Duration,
    pub transaction_lifetime: Duration,
}

/// Aggregate timings and retry/cancellation counts for a run.
#[derive(Clone, Debug, Default, Serialize)]
pub struct WorkMetrics {
    pub discovery_ms: u64,
    pub discovery_count: u64,
    pub ownership_ms: u64,
    pub ownership_count: u64,
    pub prefix_verification_ms: u64,
    pub prefix_verification_count: u64,
    pub json_inspection_ms: u64,
    pub json_inspection_count: u64,
    pub json_inspection_bytes: u64,
    pub normalization_ms: u64,
    pub normalization_count: u64,
    pub temp_file_ops_ms: u64,
    pub temp_file_ops_count: u64,
    pub temp_file_ops_bytes: u64,
    pub tokenization_ms: u64,
    pub tokenization_count: u64,
    pub tokenization_bytes: u64,
    pub temp_dedup_writes_ms: u64,
    pub temp_dedup_writes_count: u64,
    pub scratch_lookup_ms: u64,
    pub scratch_lookup_count: u64,
    pub scratch_begin_ms: u64,
    pub scratch_begin_count: u64,
    pub scratch_insert_ms: u64,
    pub scratch_insert_count: u64,
    pub scratch_commit_ms: u64,
    pub scratch_transaction_lifetime_ms: u64,
    pub durable_txn_ms: u64,
    pub durable_txn_count: u64,
    pub scratch_state_writes: u64,
    pub scratch_state_write_bytes: u64,
    pub scratch_state_transactions: u64,
    pub retries: u64,
    pub cancellations: u64,
}

impl WorkMetrics {
    fn record(&mut self, work: WorkKind, elapsed: Duration) {
        let millis = elapsed.as_millis().min(u128::from(u64::MAX)) as u64;
        let (timing, count) = match work {
            WorkKind::Discovery => (&mut self.discovery_ms, &mut self.discovery_count),
            WorkKind::Ownership => (&mut self.ownership_ms, &mut self.ownership_count),
            WorkKind::PrefixVerification => (
                &mut self.prefix_verification_ms,
                &mut self.prefix_verification_count,
            ),
            WorkKind::JsonInspection => (
                &mut self.json_inspection_ms,
                &mut self.json_inspection_count,
            ),
            WorkKind::Normalization => (&mut self.normalization_ms, &mut self.normalization_count),
            WorkKind::TempFileOps => (&mut self.temp_file_ops_ms, &mut self.temp_file_ops_count),
            WorkKind::Tokenization => (&mut self.tokenization_ms, &mut self.tokenization_count),
            WorkKind::TempDedupWrites => (
                &mut self.temp_dedup_writes_ms,
                &mut self.temp_dedup_writes_count,
            ),
            WorkKind::DurableTxn => (&mut self.durable_txn_ms, &mut self.durable_txn_count),
        };
        *timing = timing.saturating_add(millis);
        *count = count.saturating_add(1);
    }

    fn record_bytes(&mut self, work: WorkKind, bytes: u64) {
        match work {
            WorkKind::TempFileOps => {
                self.temp_file_ops_bytes = self.temp_file_ops_bytes.saturating_add(bytes)
            }
            WorkKind::JsonInspection => {
                self.json_inspection_bytes = self.json_inspection_bytes.saturating_add(bytes)
            }
            WorkKind::Tokenization => {
                self.tokenization_bytes = self.tokenization_bytes.saturating_add(bytes)
            }
            _ => {}
        }
    }

    fn record_scratch_batch(&mut self, metrics: ScratchBatchMetrics) {
        self.scratch_lookup_ms = self
            .scratch_lookup_ms
            .saturating_add(metrics.lookup.as_millis().min(u128::from(u64::MAX)) as u64);
        self.scratch_lookup_count = self
            .scratch_lookup_count
            .saturating_add(metrics.lookup_count);
        self.scratch_begin_ms = self
            .scratch_begin_ms
            .saturating_add(metrics.begin.as_millis().min(u128::from(u64::MAX)) as u64);
        self.scratch_begin_count = self.scratch_begin_count.saturating_add(metrics.begin_count);
        self.scratch_insert_ms = self
            .scratch_insert_ms
            .saturating_add(metrics.insert.as_millis().min(u128::from(u64::MAX)) as u64);
        self.scratch_insert_count = self
            .scratch_insert_count
            .saturating_add(metrics.insert_count);
        self.scratch_commit_ms = self
            .scratch_commit_ms
            .saturating_add(metrics.commit.as_millis().min(u128::from(u64::MAX)) as u64);
        self.scratch_transaction_lifetime_ms = self.scratch_transaction_lifetime_ms.saturating_add(
            metrics
                .transaction_lifetime
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
        );
        self.temp_dedup_writes_ms = self
            .temp_dedup_writes_ms
            .saturating_add(metrics.insert.as_millis().min(u128::from(u64::MAX)) as u64);
        self.temp_dedup_writes_count = self
            .temp_dedup_writes_count
            .saturating_add(metrics.insert_count);
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct PhaseTransition {
    pub sequence: u64,
    pub phase: ProgressPhase,
    pub elapsed_ms: u64,
}

/// A path-free, bounded snapshot suitable for an MCP resource or JSONL row.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ProgressSnapshot {
    pub run_id: u64,
    pub phase: ProgressPhase,
    pub files_discovered: u64,
    pub files_completed: u64,
    pub records_processed: u64,
    #[serde(default)]
    pub records_inspected: u64,
    pub bytes_read: u64,
    pub bytes_hashed_for_verification: u64,
    pub chunks_prepared: u64,
    pub chunks_committed: u64,
    pub current_source_processed_bytes: u64,
    pub last_progress_at: Option<String>,
    pub phase_elapsed_ms: u64,
    pub elapsed_ms: u64,
    pub first_progress_ms: Option<u64>,
    pub first_commit_ms: Option<u64>,
    pub phase_timings_ms: std::collections::BTreeMap<String, u64>,
    pub work: WorkMetrics,
    pub phase_history: Vec<PhaseTransition>,
    pub phase_transitions_dropped: u64,
}

struct ProgressState {
    run_id: u64,
    phase: ProgressPhase,
    started: Option<Instant>,
    phase_started: Option<Instant>,
    finished: Option<Instant>,
    active: bool,
    files_discovered: u64,
    files_completed: u64,
    records_processed: u64,
    records_inspected: u64,
    bytes_read: u64,
    bytes_hashed_for_verification: u64,
    chunks_prepared: u64,
    chunks_committed: u64,
    current_source_processed_bytes: u64,
    last_progress_at: Option<String>,
    last_publication: Option<Instant>,
    first_progress_ms: Option<u64>,
    first_commit_ms: Option<u64>,
    work: WorkMetrics,
    phase_timings_ms: std::collections::BTreeMap<String, u64>,
    phase_history: Vec<PhaseTransition>,
    phase_transitions_dropped: u64,
    phase_sequence: u64,
}

impl Default for ProgressState {
    fn default() -> Self {
        Self {
            run_id: 0,
            phase: ProgressPhase::Idle,
            started: None,
            phase_started: None,
            finished: None,
            active: false,
            files_discovered: 0,
            files_completed: 0,
            records_processed: 0,
            records_inspected: 0,
            bytes_read: 0,
            bytes_hashed_for_verification: 0,
            chunks_prepared: 0,
            chunks_committed: 0,
            current_source_processed_bytes: 0,
            last_progress_at: None,
            last_publication: None,
            first_progress_ms: None,
            first_commit_ms: None,
            work: WorkMetrics::default(),
            phase_timings_ms: std::collections::BTreeMap::new(),
            phase_history: Vec::new(),
            phase_transitions_dropped: 0,
            phase_sequence: 0,
        }
    }
}

impl ProgressState {
    fn reset(&mut self) {
        let next_run = self.run_id.saturating_add(1);
        *self = Self {
            run_id: next_run,
            started: Some(Instant::now()),
            phase_started: Some(Instant::now()),
            active: true,
            ..Self::default()
        };
    }

    fn elapsed_ms(&self, now: Instant) -> u64 {
        let endpoint = self.finished.unwrap_or(now);
        self.started
            .map(|started| {
                endpoint
                    .saturating_duration_since(started)
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64
            })
            .unwrap_or(0)
    }

    fn touch(&mut self, now: Instant, force_timestamp: bool) {
        if self.first_progress_ms.is_none() {
            self.first_progress_ms = Some(self.elapsed_ms(now));
        }
        if force_timestamp
            || self
                .last_publication
                .is_none_or(|published| now.duration_since(published) >= PUBLICATION_INTERVAL)
        {
            self.last_progress_at = Some(Utc::now().to_rfc3339());
            self.last_publication = Some(now);
        }
    }

    fn record_phase_transition(&mut self, phase: ProgressPhase, now: Instant) {
        if self.phase == phase {
            self.touch(now, false);
            return;
        }
        let elapsed = self.elapsed_ms(now);
        if let Some(started) = self.phase_started {
            let duration = now
                .saturating_duration_since(started)
                .as_millis()
                .min(u128::from(u64::MAX)) as u64;
            let key = serde_json::to_string(&self.phase)
                .unwrap_or_else(|_| "\"unknown\"".to_owned())
                .trim_matches('"')
                .to_owned();
            let value = self.phase_timings_ms.entry(key).or_default();
            *value = value.saturating_add(duration);
        }
        self.phase = phase;
        self.phase_started = Some(now);
        self.phase_sequence = self.phase_sequence.saturating_add(1);
        if self.phase_history.len() == MAX_PHASE_HISTORY {
            self.phase_history.remove(0);
            self.phase_transitions_dropped = self.phase_transitions_dropped.saturating_add(1);
        }
        self.phase_history.push(PhaseTransition {
            sequence: self.phase_sequence,
            phase,
            elapsed_ms: elapsed,
        });
        self.touch(now, true);
    }

    fn snapshot(&self, now: Instant) -> ProgressSnapshot {
        let endpoint = self.finished.unwrap_or(now);
        let elapsed_ms = self.elapsed_ms(endpoint);
        let phase_elapsed_ms = self
            .phase_started
            .map(|started| {
                endpoint
                    .saturating_duration_since(started)
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64
            })
            .unwrap_or(0);
        let mut phase_timings_ms = self.phase_timings_ms.clone();
        let current_phase = serde_json::to_string(&self.phase)
            .unwrap_or_else(|_| "\"unknown\"".to_owned())
            .trim_matches('"')
            .to_owned();
        phase_timings_ms
            .entry(current_phase)
            .and_modify(|value| *value = value.saturating_add(phase_elapsed_ms))
            .or_insert(phase_elapsed_ms);
        ProgressSnapshot {
            run_id: self.run_id,
            phase: self.phase,
            files_discovered: self.files_discovered,
            files_completed: self.files_completed,
            records_processed: self.records_processed,
            records_inspected: self.records_inspected,
            bytes_read: self.bytes_read,
            bytes_hashed_for_verification: self.bytes_hashed_for_verification,
            chunks_prepared: self.chunks_prepared,
            chunks_committed: self.chunks_committed,
            current_source_processed_bytes: self.current_source_processed_bytes,
            last_progress_at: self.last_progress_at.clone(),
            phase_elapsed_ms,
            elapsed_ms,
            first_progress_ms: self.first_progress_ms,
            first_commit_ms: self.first_commit_ms,
            phase_timings_ms,
            work: self.work.clone(),
            phase_history: self.phase_history.clone(),
            phase_transitions_dropped: self.phase_transitions_dropped,
        }
    }
}

/// Cloneable reporter backed by one mutex and no store/query resources.
#[derive(Clone)]
pub struct ProgressReporter {
    state: Arc<Mutex<ProgressState>>,
    enabled: bool,
}

impl Default for ProgressReporter {
    fn default() -> Self {
        Self::new()
    }
}

impl ProgressReporter {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ProgressState::default())),
            enabled: true,
        }
    }

    pub fn noop() -> Self {
        Self {
            state: Arc::new(Mutex::new(ProgressState::default())),
            enabled: false,
        }
    }

    /// Start a run. A caller may invoke this before the observed scan; the
    /// scan's own call is idempotent while a non-terminal run is active.
    pub fn begin_run(&self) {
        if !self.enabled {
            return;
        }
        if let Ok(mut state) = self.state.lock()
            && (!state.active || state.phase.is_terminal())
        {
            state.reset();
        }
    }

    /// Mark the terminal phase without changing counters or timestamps.
    pub fn finish_run(&self, phase: ProgressPhase) {
        if !self.enabled {
            return;
        }
        if let Ok(mut state) = self.state.lock() {
            if !state.active {
                state.reset();
            }
            let now = Instant::now();
            state.record_phase_transition(phase, now);
            state.finished = Some(now);
            state.active = false;
        }
    }

    pub fn set_phase(&self, phase: ProgressPhase) {
        if !self.enabled {
            return;
        }
        if let Ok(mut state) = self.state.lock() {
            if !state.active {
                state.reset();
            }
            state.record_phase_transition(phase, Instant::now());
        }
    }

    pub fn record_discovered(&self, count: u64) {
        self.update(|state, now| {
            state.files_discovered = state.files_discovered.saturating_add(count);
            state.touch(now, false);
        });
    }

    pub fn record_source_bytes(&self, read: u64, hashed_for_verification: u64) {
        self.update(|state, now| {
            state.bytes_read = state.bytes_read.saturating_add(read);
            state.bytes_hashed_for_verification = state
                .bytes_hashed_for_verification
                .saturating_add(hashed_for_verification);
            state.current_source_processed_bytes =
                state.current_source_processed_bytes.saturating_add(read);
            if read != 0 || hashed_for_verification != 0 {
                state.touch(now, false);
            }
        });
    }

    pub fn record_record(&self) {
        self.update(|state, now| {
            state.records_processed = state.records_processed.saturating_add(1);
            state.touch(now, false);
        });
    }

    pub fn record_record_inspected(&self) {
        self.update(|state, now| {
            state.records_inspected = state.records_inspected.saturating_add(1);
            state.touch(now, false);
        });
    }

    pub fn record_prepared_chunks(&self, count: u64) {
        self.update(|state, now| {
            state.chunks_prepared = state.chunks_prepared.saturating_add(count);
            state.touch(now, false);
        });
    }

    pub fn record_committed_chunks(&self, count: u64) {
        self.update(|state, now| {
            if state.first_commit_ms.is_none() && count != 0 {
                state.first_commit_ms = Some(state.elapsed_ms(now));
            }
            state.chunks_committed = state.chunks_committed.saturating_add(count);
            if count != 0 {
                state.touch(now, true);
            }
        });
    }

    pub fn record_file_completed(&self) {
        self.update(|state, now| {
            state.files_completed = state.files_completed.saturating_add(1);
            state.touch(now, false);
        });
    }

    pub fn record_retry(&self) {
        self.update(|state, now| {
            state.work.retries = state.work.retries.saturating_add(1);
            state.touch(now, false);
        });
    }

    pub fn record_cancellation(&self) {
        self.update(|state, now| {
            state.work.cancellations = state.work.cancellations.saturating_add(1);
            state.touch(now, true);
        });
    }

    pub fn record_work(&self, work: WorkKind, elapsed: Duration) {
        self.update(|state, now| {
            state.work.record(work, elapsed);
            if !elapsed.is_zero() {
                state.touch(now, false);
            }
        });
    }

    pub fn record_work_bytes(&self, work: WorkKind, bytes: u64) {
        self.update(|state, now| {
            state.work.record_bytes(work, bytes);
            if bytes != 0 {
                state.touch(now, false);
            }
        });
    }

    pub fn record_scratch_state_write(&self, bytes: u64) {
        self.update(|state, now| {
            state.work.scratch_state_writes = state.work.scratch_state_writes.saturating_add(1);
            state.work.scratch_state_write_bytes =
                state.work.scratch_state_write_bytes.saturating_add(bytes);
            state.touch(now, false);
        });
    }

    pub fn record_scratch_state_transaction(&self) {
        self.update(|state, now| {
            state.work.scratch_state_transactions =
                state.work.scratch_state_transactions.saturating_add(1);
            state.touch(now, true);
        });
    }

    pub fn record_scratch_batch(&self, metrics: ScratchBatchMetrics) {
        self.update(|state, now| {
            state.work.record_scratch_batch(metrics);
            state.touch(now, false);
        });
    }

    /// Set cumulative source bytes attempted for the current source. Passing
    /// zero is the source-boundary reset used by scan workers.
    pub fn record_current_source_bytes(&self, bytes: u64) {
        self.update(|state, now| {
            state.current_source_processed_bytes = bytes;
            if bytes != 0 {
                state.touch(now, false);
            }
        });
    }

    pub fn snapshot(&self) -> ProgressSnapshot {
        if !self.enabled {
            return ProgressSnapshot::default();
        }
        self.state
            .lock()
            .map(|state| state.snapshot(Instant::now()))
            .unwrap_or_default()
    }

    fn update(&self, update: impl FnOnce(&mut ProgressState, Instant)) {
        if !self.enabled {
            return;
        }
        if let Ok(mut state) = self.state.lock() {
            if !state.active {
                state.reset();
            }
            update(&mut state, Instant::now());
        }
    }
}
