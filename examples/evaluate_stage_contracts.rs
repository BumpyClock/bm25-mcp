//! Exact fixed-clock differential capture for the architectural consolidation.
#[path = "support/admission_fixture.rs"]
mod admission_fixture;
#[path = "support/ranking_fixture.rs"]
mod fixture;

use anyhow::Result;
use bm25_mcp::{
    model::{Chunk, SearchFilter, Source},
    ranking::RankingOptions,
    store::Store,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

fn chunk_signature(chunk: &Chunk) -> Value {
    let mut value = serde_json::to_value(chunk).expect("serializable chunk");
    value["text"] = json!(format!("{:x}", Sha256::digest(chunk.text.as_bytes())));
    value
}

fn capture(
    store: &Store,
    query: &str,
    filter: &SearchFilter,
    options: RankingOptions,
) -> Result<Value> {
    let raw = store.search(query, filter, 200)?.1;
    let ranked =
        store.search_ranked_with_at(query, filter, 50, options, "2026-09-21T00:00:00Z".parse()?)?;
    let hits = |hits: &[bm25_mcp::model::Hit]| {
        hits.iter()
            .map(|hit| {
                json!({
                    "id": hit.match_id, "source": hit.source, "copy_count": hit.copy_count,
                    "chunk": chunk_signature(&hit.chunk), "score_bits": hit.score.to_bits()
                })
            })
            .collect::<Vec<_>>()
    };
    Ok(
        json!({ "query": query, "kind": filter.kind, "raw": hits(&raw),
        "ranked": hits(&ranked.hits), "traces": ranked.traces,
        "candidate_count": ranked.candidate_count, "counts": ranked.admission_counts,
        "probes": ranked.probes, "meaningful_retrievals": ranked.meaningful_retrievals,
        "additional_retrievals": ranked.additional_retrievals }),
    )
}

fn main() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::open(&dir.path().join("fixture.sqlite3"))?;
    let cases = fixture::fixtures(&store)?;
    let mut records = Vec::new();
    for case in cases {
        // Reuse relevance judgments without changing their grain or labels.
        assert!(!case.must.is_empty() || !case.useful.is_empty());
        for variant in 0..10 {
            let mut options = RankingOptions::default();
            match variant {
                1 => options.fields = false,
                2 => options.classification = false,
                3 => options.exact = false,
                4 => options.proximity = false,
                5 => options.expansion = false,
                6 => options.decay = false,
                7 => options.dedupe = false,
                8 => options.weighted_similarity = false,
                9 => options.mmr = false,
                _ => {}
            }
            records.push(capture(
                &store,
                case.query,
                &fixture::filter(case.kind),
                options,
            )?);
        }
    }
    fixture::replace_probe(&store)?;
    records.push(capture(
        &store,
        "cloud sync",
        &fixture::filter("project"),
        RankingOptions::default(),
    )?);
    for kind in ["project", "session"] {
        let store = Store::open(&dir.path().join(format!("{kind}.sqlite3")))?;
        let filter = admission_fixture::populate(&store, kind)?;
        for query in [
            admission_fixture::QUERY,
            admission_fixture::RETAINED_QUERY,
            "is",
            "why indexing indexing fail",
        ] {
            records.push(capture(&store, query, &filter, RankingOptions::default())?);
        }
        let source = Source {
            key: "target".into(),
            collection: filter.collection.clone(),
            kind: kind.into(),
            path: "target.txt".into(),
            version: "replacement".into(),
        };
        store.replace_source(
            &source,
            [Ok(Chunk {
                text: "indexing fail replacement".into(),
                ..Default::default()
            })],
        )?;
        records.push(capture(
            &store,
            admission_fixture::QUERY,
            &filter,
            RankingOptions::default(),
        )?);
        store.invalidate_source("target")?;
        records.push(capture(
            &store,
            admission_fixture::QUERY,
            &filter,
            RankingOptions::default(),
        )?);
        store.mark_source_verified("target")?;
        records.push(capture(
            &store,
            admission_fixture::QUERY,
            &filter,
            RankingOptions::default(),
        )?);
        store.remove_source("target")?;
        records.push(capture(
            &store,
            admission_fixture::QUERY,
            &filter,
            RankingOptions::default(),
        )?);
    }
    println!("{}", serde_json::to_string(&records)?);
    Ok(())
}
