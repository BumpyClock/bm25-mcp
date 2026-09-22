//! Private retrieval stages. Only Admission constructs an AdmittedPool.

use super::*;
use crate::{
    query::QueryPlan,
    ranking::{AdmissionEvidence, CandidateCounts},
};
use std::{cmp::Ordering, collections::BTreeMap};

#[derive(Clone, Copy)]
pub(super) enum Lane {
    Lexical,
    Meaningful,
    Definition,
    Path,
    Expansion,
}

pub(super) struct Candidate {
    pub(super) hit: Hit,
    pub(super) raw_bm25: Option<f32>,
    evidence: AdmissionEvidence,
}

pub(super) struct Admission {
    filter: SearchFilter,
    path_glob: Option<GlobSet>,
    candidates: BTreeMap<String, Candidate>,
    raw_terms: Vec<String>,
}

pub(super) struct AdmittedPool<'plan> {
    plan: &'plan QueryPlan,
    raw_terms: Vec<String>,
    filter: SearchFilter,
    candidates: Vec<Candidate>,
}

/// Persisted body evidence can only enter the ranker through bounded hydration.
pub(crate) struct IndexedPool {
    entries: Vec<(Hit, ranking::BodyEvidence, AdmissionEvidence)>,
}
impl IndexedPool {
    pub(crate) fn into_entries(self) -> Vec<(Hit, ranking::BodyEvidence, AdmissionEvidence)> {
        self.entries
    }
}

/// Preparation and statistics belong to the same live retrieval transaction.
pub(crate) struct ScorablePool<'snapshot> {
    plan: &'snapshot QueryPlan,
    prepared: Vec<ranking::Prepared>,
    stats: ranking::CorpusStats,
}
impl<'snapshot> ScorablePool<'snapshot> {
    pub(crate) fn into_parts(
        self,
    ) -> (
        Vec<ranking::Prepared>,
        ranking::CorpusStats,
        &'snapshot QueryPlan,
    ) {
        (self.prepared, self.stats, self.plan)
    }
}

impl Admission {
    pub(super) fn new(
        filter: &SearchFilter,
        path_glob: Option<GlobSet>,
        raw_terms: &[String],
    ) -> Self {
        Self {
            filter: filter.clone(),
            raw_terms: raw_terms.to_vec(),
            path_glob,
            candidates: BTreeMap::new(),
        }
    }

    pub(super) fn record(&mut self, lane: Lane, hit: Hit) -> Result<()> {
        // Retrieval applies filters before lane limits. Check scope again at the
        // merge boundary so a new supplemental lane cannot silently bypass it.
        ensure!(
            hit.source.collection == self.filter.collection
                && hit.source.kind == self.filter.kind
                && self
                    .path_glob
                    .as_ref()
                    .is_none_or(|glob| glob.is_match(&hit.source.path))
                && self
                    .filter
                    .agent
                    .as_ref()
                    .is_none_or(|value| hit.chunk.agent.as_ref() == Some(value))
                && self.filter.session_id.as_ref().is_none_or(|value| hit
                    .chunk
                    .session_id
                    .as_ref()
                    == Some(value))
                && self.filter.after.as_ref().is_none_or(|value| hit
                    .chunk
                    .timestamp
                    .as_ref()
                    .is_some_and(|timestamp| timestamp >= value))
                && self.filter.before.as_ref().is_none_or(|value| hit
                    .chunk
                    .timestamp
                    .as_ref()
                    .is_some_and(|timestamp| timestamp < value)),
            "retrieved candidate is outside query scope"
        );
        let score = hit.score;
        let candidate = self
            .candidates
            .entry(hit.match_id.clone())
            .or_insert(Candidate {
                hit,
                raw_bm25: None,
                evidence: AdmissionEvidence::default(),
            });
        match lane {
            Lane::Lexical => {
                candidate.raw_bm25 = Some(score);
                candidate.evidence.lexical = true;
            }
            Lane::Meaningful => candidate.evidence.meaningful_bm25 = Some(score),
            Lane::Definition => candidate.evidence.definition = true,
            Lane::Path => candidate.evidence.path = true,
            Lane::Expansion => {
                candidate.evidence.expansion_bm25 = Some(
                    candidate
                        .evidence
                        .expansion_bm25
                        .map_or(score, |old| old.max(score)),
                )
            }
        }
        Ok(())
    }

    pub(super) fn weak(&self, options: ranking::RankingOptions) -> bool {
        let mut count = 0;
        let mut strongest = 0.0_f32;
        for score in self
            .candidates
            .values()
            .filter_map(|candidate| candidate.raw_bm25)
        {
            count += 1;
            strongest = strongest.max(score);
        }
        options.expansion
            && (count < ranking::THIN_POOL || strongest < ranking::WEAK_BM25_THRESHOLD)
    }

    pub(super) fn counts(&self) -> CandidateCounts {
        let mut counts = CandidateCounts::default();
        for candidate in self.candidates.values() {
            let evidence = &candidate.evidence;
            counts.lexical += usize::from(evidence.lexical);
            counts.meaningful += usize::from(evidence.meaningful_bm25.is_some());
            counts.definitions += usize::from(evidence.definition);
            counts.path += usize::from(evidence.path);
            counts.expansion += usize::from(evidence.expansion_bm25.is_some());
        }
        counts
    }

    pub(super) fn finish(self, plan: &QueryPlan, weak_pool: bool) -> AdmittedPool<'_> {
        let literal = crate::text::fold(&plan.literal.replace('\\', "/"));
        let exact_path = |candidate: &Candidate| {
            let path = crate::text::fold(&candidate.hit.source.path.replace('\\', "/"));
            (path == literal || path.ends_with(&format!("/{literal}"))) as u8
        };
        let compare = |a: &Candidate, b: &Candidate| {
            let exact_order = if matches!(plan.class, query::QueryClass::Path) {
                exact_path(b).cmp(&exact_path(a))
            } else {
                Ordering::Equal
            };
            exact_order
                .then_with(|| b.raw_bm25.is_some().cmp(&a.raw_bm25.is_some()))
                .then_with(|| {
                    b.raw_bm25
                        .unwrap_or(0.)
                        .total_cmp(&a.raw_bm25.unwrap_or(0.))
                })
                .then_with(|| a.hit.match_id.cmp(&b.hit.match_id))
        };
        let has_expansion = self
            .candidates
            .values()
            .any(|candidate| candidate.evidence.expansion_bm25.is_some());
        let mut remaining: Vec<_> = self.candidates.into_values().collect();
        let mut selected = reserve(
            &mut remaining,
            |candidate| candidate.evidence.definition,
            |a, b| {
                b.raw_bm25
                    .unwrap_or(0.)
                    .total_cmp(&a.raw_bm25.unwrap_or(0.))
                    .then_with(|| a.hit.match_id.cmp(&b.hit.match_id))
            },
            DEFINITION_RESERVE.min(ranking::CANDIDATE_LIMIT),
        );
        let meaningful = reserve(
            &mut remaining,
            |candidate| candidate.evidence.meaningful_bm25.is_some(),
            |a, b| {
                b.evidence
                    .meaningful_bm25
                    .unwrap()
                    .total_cmp(&a.evidence.meaningful_bm25.unwrap())
                    .then_with(|| a.hit.match_id.cmp(&b.hit.match_id))
            },
            ranking::MEANINGFUL_RESERVE,
        );
        if weak_pool && has_expansion {
            // Preserve the existing supplemental reservation, including exact
            // paths already retrieved by another lane when expansion is active.
            selected.extend(reserve(
                &mut remaining,
                |candidate| candidate.raw_bm25.is_none(),
                |a, b| {
                    exact_path(b)
                        .cmp(&exact_path(a))
                        .then_with(|| {
                            b.evidence
                                .expansion_bm25
                                .unwrap_or(0.)
                                .total_cmp(&a.evidence.expansion_bm25.unwrap_or(0.))
                        })
                        .then_with(|| a.hit.match_id.cmp(&b.hit.match_id))
                },
                ranking::EXPANSION_RESERVE.min(ranking::CANDIDATE_LIMIT),
            ));
        }
        selected.extend(meaningful);
        remaining.sort_by(compare);
        remaining.truncate(ranking::CANDIDATE_LIMIT.saturating_sub(selected.len()));
        selected.extend(remaining);
        selected.sort_by(compare);
        selected.truncate(ranking::CANDIDATE_LIMIT);
        AdmittedPool {
            plan,
            raw_terms: self.raw_terms,
            filter: self.filter,
            candidates: selected,
        }
    }
}

fn reserve(
    remaining: &mut Vec<Candidate>,
    eligible: impl Fn(&Candidate) -> bool,
    compare: impl Fn(&Candidate, &Candidate) -> Ordering,
    limit: usize,
) -> Vec<Candidate> {
    let (mut selected, mut rest): (Vec<_>, Vec<_>) =
        std::mem::take(remaining).into_iter().partition(eligible);
    selected.sort_by(compare);
    rest.extend(selected.split_off(limit.min(selected.len())));
    *remaining = rest;
    selected
}

impl<'plan> AdmittedPool<'plan> {
    pub(super) fn len(&self) -> usize {
        self.candidates.len()
    }

    pub(super) fn hydrate<'snapshot>(
        mut self,
        tx: &'snapshot Transaction<'_>,
    ) -> Result<ScorablePool<'snapshot>>
    where
        'plan: 'snapshot,
    {
        let plan = self.plan;
        hydrate_baseline_scores(tx, &mut self.candidates, &self.raw_terms)?;
        let mut evidence = hydrate_body_evidence(tx, &self.candidates)?;
        let entries = self
            .candidates
            .into_iter()
            .map(|candidate| {
                let body = evidence.remove(&candidate.hit.match_id).ok_or_else(|| {
                    anyhow!("missing body evidence for {}", candidate.hit.match_id)
                })?;
                Ok((candidate.hit, body, candidate.evidence))
            })
            .collect::<Result<Vec<_>>>()?;
        let prepared = ranking::prepare_admitted(IndexedPool { entries }, plan)?;
        let stat_terms = ranking::statistics_terms(&prepared, plan)?;
        let stats = corpus_stats(tx, &self.filter, &stat_terms)?;
        Ok(ScorablePool {
            plan,
            prepared,
            stats,
        })
    }
}

#[cfg(test)]
#[path = "admission_tests.rs"]
mod tests;
