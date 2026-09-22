use super::*;
use std::collections::BTreeSet;

fn filter() -> SearchFilter {
    SearchFilter {
        collection: "c".into(),
        kind: "project".into(),
        ..Default::default()
    }
}
fn hit(id: usize, score: f32) -> Hit {
    Hit {
        match_id: format!("id-{id:04}"),
        copy_count: 1,
        verified_at: None,
        source: Source {
            key: format!("s-{id}"),
            collection: "c".into(),
            kind: "project".into(),
            path: "source.rs".into(),
            version: "v".into(),
        },
        chunk: Chunk {
            text: "needle".into(),
            ..Default::default()
        },
        score,
    }
}

#[test]
fn protected_reservations_survive_saturation_overlap_and_delivery_order() {
    let plan = QueryPlan::new("why does indexing fail", true).unwrap();
    for rotation in 0..40 {
        for ordinary in [0, 180, 200, 401] {
            let mut admission = Admission::new(&filter(), None, &[]);
            for index in 0..ordinary {
                admission
                    .record(Lane::Lexical, hit(100 + index, (ordinary - index) as f32))
                    .unwrap();
            }
            for index in 0..40 {
                let id = (index + rotation) % 40;
                for _ in 0..2 {
                    admission.record(Lane::Definition, hit(id, 0.)).unwrap();
                    admission
                        .record(Lane::Meaningful, hit(id + 20, id as f32))
                        .unwrap();
                    admission
                        .record(Lane::Expansion, hit(id + 40, id as f32))
                        .unwrap();
                }
            }
            let counts = admission.counts();
            assert_eq!(
                (counts.definitions, counts.meaningful, counts.expansion),
                (40, 40, 40)
            );
            let pool = admission.finish(&plan, true);
            let ids: BTreeSet<_> = pool
                .candidates
                .iter()
                .map(|candidate| candidate.hit.match_id.clone())
                .collect();
            assert_eq!(ids.len(), pool.len());
            assert_eq!(pool.len(), (ordinary + 80).min(ranking::CANDIDATE_LIMIT));
            for protected in 0..80 {
                assert!(ids.contains(&hit(protected, 0.).match_id));
            }
        }
    }
}

#[test]
fn score_provenance_and_zero_raw_scores_are_explicit() {
    let plan = QueryPlan::new("needle", true).unwrap();
    let mut admission = Admission::new(&filter(), None, &[]);
    admission.record(Lane::Lexical, hit(0, 0.)).unwrap();
    admission.record(Lane::Meaningful, hit(0, 42.)).unwrap();
    admission.record(Lane::Expansion, hit(0, 3.)).unwrap();
    admission.record(Lane::Expansion, hit(0, 7.)).unwrap();
    admission.record(Lane::Definition, hit(0, 0.)).unwrap();
    admission.record(Lane::Path, hit(0, 0.)).unwrap();
    admission.record(Lane::Meaningful, hit(1, 99.)).unwrap();
    let pool = admission.finish(&plan, true);
    assert_eq!(pool.len(), 2);
    let lexical = pool
        .candidates
        .iter()
        .find(|candidate| candidate.hit.match_id == hit(0, 0.).match_id)
        .unwrap();
    assert_eq!(lexical.raw_bm25, Some(0.));
    assert!(lexical.evidence.lexical && lexical.evidence.path && lexical.evidence.definition);
    assert_eq!(lexical.evidence.meaningful_bm25, Some(42.));
    assert_eq!(lexical.evidence.expansion_bm25, Some(7.));
    let supplemental = pool
        .candidates
        .iter()
        .find(|candidate| candidate.hit.match_id == hit(1, 0.).match_id)
        .unwrap();
    assert_eq!(supplemental.raw_bm25, None);
    assert_eq!(supplemental.evidence.meaningful_bm25, Some(99.));
}

#[test]
fn every_lane_rejects_candidates_outside_collection_and_filter_scope() {
    let mut scoped = filter();
    scoped.agent = Some("codex".into());
    scoped.session_id = Some("session".into());
    scoped.after = Some("2026-09-01T00:00:00Z".into());
    scoped.before = Some("2026-10-01T00:00:00Z".into());
    scoped.path_glob = Some("src/*.rs".into());
    let mut valid = hit(0, 1.);
    valid.source.path = "src/good.rs".into();
    valid.chunk.agent = scoped.agent.clone();
    valid.chunk.session_id = scoped.session_id.clone();
    valid.chunk.timestamp = Some("2026-09-21T00:00:00Z".into());
    for lane in [
        Lane::Lexical,
        Lane::Definition,
        Lane::Meaningful,
        Lane::Expansion,
        Lane::Path,
    ] {
        for mismatch in 0..8 {
            let mut candidate = valid.clone();
            match mismatch {
                0 => candidate.source.collection = "other".into(),
                1 => candidate.source.kind = "session".into(),
                2 => candidate.source.path = "outside.txt".into(),
                3 => candidate.chunk.agent = None,
                4 => candidate.chunk.session_id = None,
                5 => candidate.chunk.timestamp = None,
                6 => candidate.chunk.timestamp = Some("2026-08-31T00:00:00Z".into()),
                _ => candidate.chunk.timestamp = scoped.before.clone(),
            }
            let mut admission = Admission::new(
                &scoped,
                compile_path_glob(scoped.path_glob.as_deref()).unwrap(),
                &[],
            );
            assert!(admission.record(lane, candidate).is_err());
            assert!(admission.candidates.is_empty());
            admission.record(lane, valid.clone()).unwrap();
        }
    }
}

#[test]
fn admitted_evidence_and_statistics_stay_in_the_retrieval_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("index.sqlite3")).unwrap();
    let mut source = hit(0, 0.).source;
    store
        .replace_source(
            &source,
            [Ok(Chunk {
                text: "nee".into(),
                tokens: Some(vec!["needle".into()]),
                ..Default::default()
            })],
        )
        .unwrap();
    let filter = filter();
    let plan = QueryPlan::new("needle", true).unwrap();
    let terms = tokenize_checked("needle").unwrap();
    let mut conn = store.read_connection().unwrap();
    register_search_functions(&conn, None).unwrap();
    let tx = conn.transaction().unwrap();
    let generation = current_generation(&tx).unwrap();
    let raw = retrieve_terms(&store.hot, &tx, generation, &terms, &filter, None, 200).unwrap();
    assert_eq!(raw.len(), 1);
    let raw_score = raw[0].score;
    let mut admission = Admission::new(&filter, None, &terms);
    for hit in raw {
        admission.record(Lane::Lexical, hit).unwrap();
    }
    let pool = admission.finish(&plan, false);
    source.version = "replacement".into();
    store
        .replace_source(
            &source,
            [Ok(Chunk {
                text: "unrelated".into(),
                ..Default::default()
            })],
        )
        .unwrap();
    let scorable = pool.hydrate(&tx).unwrap();
    let (hits, traces) = ranking::rank_indexed(
        scorable,
        ranking::RankingOptions::default(),
        "2026-09-21T00:00:00Z".parse().unwrap(),
        10,
    )
    .unwrap();
    assert_eq!(
        hits.len(),
        1,
        "persisted evidence survives a fragment boundary"
    );
    assert_eq!(hits[0].source.version, "v");
    assert_eq!(hits[0].chunk.text, "nee");
    assert_eq!(traces[0].baseline_bm25, raw_score);
    assert!(traces[0].original_contribution > 0.);
    tx.commit().unwrap();
    assert!(store.search("needle", &filter, 10).unwrap().1.is_empty());
}
