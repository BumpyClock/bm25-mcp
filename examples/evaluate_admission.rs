//! Fixed-clock regression workload for saturated incidental-stopword pools.
#[path = "support/admission_fixture.rs"]
mod fixture;

use anyhow::Result;
use bm25_mcp::{ranking::RankingOptions, store::Store};
use serde_json::json;
use std::time::Instant;

const WARMUPS: usize = 5;
const SAMPLES: usize = 50;

fn main() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::open(&dir.path().join("admission.sqlite3"))?;
    let mut cases = Vec::new();
    for kind in ["project", "session"] {
        let filter = fixture::populate(&store, kind)?;
        let raw = store.search(fixture::QUERY, &filter, 200)?.1;
        assert_eq!(raw.len(), 200);
        assert!(raw.iter().all(|hit| hit.source.key.starts_with("noise-")));
        assert!(raw[0].score > bm25_mcp::ranking::WEAK_BM25_THRESHOLD);
        for query in [fixture::QUERY, fixture::RETAINED_QUERY, "is", "neutral"] {
            let mut samples = Vec::new();
            let mut expected = None;
            let mut last = None;
            for sample in 0..WARMUPS + SAMPLES {
                let start = Instant::now();
                let result = store.search_ranked_with_at(
                    query,
                    &filter,
                    10,
                    RankingOptions::default(),
                    fixture::NOW.parse()?,
                )?;
                let elapsed = start.elapsed().as_secs_f64() * 1_000.0;
                let signature: Vec<_> = result
                    .hits
                    .iter()
                    .map(|hit| (hit.match_id.clone(), hit.score.to_bits()))
                    .collect();
                if let Some(expected) = &expected {
                    assert_eq!(expected, &signature);
                } else {
                    expected = Some(signature);
                }
                if sample >= WARMUPS {
                    samples.push(elapsed);
                }
                last = Some(result);
            }
            samples.sort_by(f64::total_cmp);
            let result = last.expect("benchmark runs at least once");
            cases.push(json!({
                "kind": kind, "query": query,
                "p50_ms": samples[(SAMPLES - 1) / 2],
                "p95_ms": samples[((SAMPLES - 1) as f64 * 0.95).round() as usize],
                "admission_counts": result.admission_counts,
                "candidate_count": result.candidate_count,
                "optional_probes": result.probes.len(),
                "meaningful_retrievals": result.meaningful_retrievals,
                "additional_retrievals": result.additional_retrievals,
                "original_contributions": result.traces.iter().map(|trace| trace.original_contribution).collect::<Vec<_>>(),
                "admission_evidence": result.traces.iter().map(|trace| &trace.admission).collect::<Vec<_>>(),
                "results": result.hits.iter().map(|hit| json!({
                    "source": hit.source.key, "score": hit.score,
                })).collect::<Vec<_>>(),
                "deterministic": true,
            }));
        }
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "evaluation_now": fixture::NOW, "warmups": WARMUPS, "samples": SAMPLES,
            "sources_per_kind": fixture::NOISE_COUNT + fixture::BACKGROUND_COUNT + 1,
            "target_bytes": fixture::target_text().len(), "cases": cases,
            "notes": ["Synthetic admission regression; not a production relevance claim."],
        }))?
    );
    Ok(())
}
