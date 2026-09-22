#[path = "../examples/support/admission_fixture.rs"]
mod fixture;

use bm25_mcp::{
    ingest,
    model::{Chunk, SearchFilter, Source},
    query::{MAX_PROBES, QueryPlan},
    ranking::{CANDIDATE_LIMIT, MEANINGFUL_RESERVE, RankingOptions, WEAK_BM25_THRESHOLD},
    store::Store,
    text::tokenize_checked,
};
use std::collections::BTreeSet;

fn put(store: &Store, key: &str, text: &str) {
    store
        .replace_source(
            &Source {
                key: key.into(),
                collection: "project".into(),
                path: format!("records/{key}.txt"),
                version: "v1".into(),
                kind: "project".into(),
            },
            [Ok(Chunk {
                text: text.into(),
                ..Default::default()
            })],
        )
        .unwrap();
}

fn saturated_pool_keeps_meaningful_evidence(kind: &str) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("index.sqlite3")).unwrap();
    let filter = fixture::populate(&store, kind).unwrap();
    assert_eq!(fixture::target_text().len(), 16_014);
    let raw = store
        .search(fixture::QUERY, &filter, CANDIDATE_LIMIT)
        .unwrap()
        .1;
    assert_eq!(raw.len(), CANDIDATE_LIMIT);
    assert!(raw.iter().all(|hit| hit.source.key.starts_with("noise-")));
    assert!(raw.iter().all(|hit| hit.copy_count == 1));
    assert!(raw[0].score > WEAK_BM25_THRESHOLD);
    let retained = store
        .search(fixture::RETAINED_QUERY, &filter, 10)
        .unwrap()
        .1;
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].source.key, "target");
    let result = store
        .search_ranked_with_at(
            fixture::QUERY,
            &filter,
            10,
            RankingOptions::default(),
            fixture::NOW.parse().unwrap(),
        )
        .unwrap();
    assert_eq!(
        result.hits.first().map(|hit| hit.source.key.as_str()),
        Some("target"),
        "a strong stopword-only raw pool must not block retained-query evidence"
    );
    assert!(result.traces[0].original_contribution > 0.0);
    assert_eq!(result.traces[0].expanded_contribution, 0.0);
    assert_eq!(result.candidate_count, CANDIDATE_LIMIT);
    assert_eq!(result.admission_counts.lexical, CANDIDATE_LIMIT);
    assert_eq!(result.admission_counts.meaningful, 1);
    assert_eq!(result.meaningful_retrievals, 1);
    assert_eq!(result.additional_retrievals, 1);
    assert!(result.probes.is_empty());
    let trace = &result.traces[0];
    assert!(!trace.admission.lexical);
    assert_eq!(trace.admission.meaningful_bm25, Some(retained[0].score));
    assert_eq!(trace.admission.expansion_bm25, None);
    let raw_target = store
        .search(fixture::QUERY, &filter, 1_202)
        .unwrap()
        .1
        .into_iter()
        .find(|hit| hit.source.key == "target")
        .unwrap();
    assert!(raw_target.score > 0.0);
    assert_eq!(trace.baseline_bm25, raw_target.score);
    let repeated = store
        .search_ranked_with_at(
            fixture::QUERY,
            &filter,
            10,
            RankingOptions::default(),
            fixture::NOW.parse().unwrap(),
        )
        .unwrap();
    assert_eq!(
        result
            .hits
            .iter()
            .map(|hit| (&hit.match_id, hit.score))
            .collect::<Vec<_>>(),
        repeated
            .hits
            .iter()
            .map(|hit| (&hit.match_id, hit.score))
            .collect::<Vec<_>>(),
    );
    for options in [
        RankingOptions {
            expansion: false,
            ..Default::default()
        },
        RankingOptions {
            fields: false,
            ..Default::default()
        },
    ] {
        let ranked = store
            .search_ranked_with_at(
                fixture::QUERY,
                &filter,
                10,
                options,
                fixture::NOW.parse().unwrap(),
            )
            .unwrap();
        assert_eq!(ranked.hits[0].source.key, "target");
        assert!(ranked.hits[0].score > 0.0);
        assert!(ranked.traces[0].original_contribution > 0.0);
    }
    store.set_memory_pressure(true);
    let disk = store
        .search_ranked_with_at(
            fixture::QUERY,
            &filter,
            10,
            RankingOptions::default(),
            fixture::NOW.parse().unwrap(),
        )
        .unwrap();
    assert_eq!(disk.hits[0].match_id, result.hits[0].match_id);
    assert_eq!(disk.hits[0].score, result.hits[0].score);
    store.remove_source("target").unwrap();
    assert!(
        store
            .search_ranked(fixture::QUERY, &filter, 10)
            .unwrap()
            .1
            .is_empty()
    );
}

#[test]
fn saturated_project_pool_keeps_meaningful_evidence() {
    saturated_pool_keeps_meaningful_evidence("project");
}

#[test]
fn saturated_session_pool_keeps_meaningful_evidence() {
    saturated_pool_keeps_meaningful_evidence("session");
}

#[test]
fn meaningful_lane_uses_the_existing_query_reduction_policy() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("index.sqlite3")).unwrap();
    put(
        &store,
        "one",
        "is indexing fail panic crash WhyDoes indexing_fail",
    );
    let filter = SearchFilter {
        collection: "project".into(),
        kind: "project".into(),
        ..Default::default()
    };
    for query in [
        "is",
        "indexing fail",
        "  fail indexing indexing ",
        "why indexing_fail",
        "WhyDoes",
        "src/why.rs",
        "\"why does\"",
    ] {
        assert!(
            !QueryPlan::new(query, true).unwrap().stopwords_removed,
            "{query}"
        );
        let result = store
            .search_ranked_with_at(
                query,
                &filter,
                10,
                Default::default(),
                fixture::NOW.parse().unwrap(),
            )
            .unwrap();
        assert_eq!(result.meaningful_retrievals, 0, "{query}");
        assert_eq!(result.admission_counts.meaningful, 0);
        assert!(result.additional_retrievals <= MAX_PROBES);
        if query == "is" {
            assert_eq!(result.hits.len(), 1);
        }
    }
    let plan = QueryPlan::new(fixture::QUERY, false).unwrap();
    assert!(!plan.stopwords_removed);
    let plan = QueryPlan::new("why delete fix crash serialize terminate", true).unwrap();
    assert!(plan.stopwords_removed);
    assert_eq!(plan.probes.len(), MAX_PROBES - 1);
    assert!(plan.probes.iter().all(|probe| probe.origin != "reduced"));
    let result = store
        .search_ranked_with_at(
            "why delete fix crash serialize terminate",
            &filter,
            10,
            Default::default(),
            fixture::NOW.parse().unwrap(),
        )
        .unwrap();
    assert_eq!(result.meaningful_retrievals, 1);
    assert_eq!(result.additional_retrievals, MAX_PROBES);
    let empty = store
        .search_ranked_with_at(
            "why panic",
            &filter,
            0,
            Default::default(),
            fixture::NOW.parse().unwrap(),
        )
        .unwrap();
    assert!(empty.probes.is_empty());
    assert_eq!(empty.meaningful_retrievals, 0);
    assert_eq!(empty.additional_retrievals, 0);
}

#[test]
fn meaningful_and_expansion_reservations_keep_overlap_and_fill_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("index.sqlite3")).unwrap();
    for index in 0..200 {
        put(
            &store,
            &format!("p{index}"),
            &format!("panic entry {index}"),
        );
    }
    put(&store, "dual", "panic crash");
    put(&store, "alias", "crash recovery");
    let filter = SearchFilter {
        collection: "project".into(),
        kind: "project".into(),
        ..Default::default()
    };
    let result = store
        .search_ranked_with_at(
            "why panic",
            &filter,
            200,
            RankingOptions {
                dedupe: false,
                ..Default::default()
            },
            fixture::NOW.parse().unwrap(),
        )
        .unwrap();
    assert_eq!(result.admission_counts.meaningful, MEANINGFUL_RESERVE);
    assert_eq!(result.candidate_count, CANDIDATE_LIMIT);
    assert_eq!(result.meaningful_retrievals, 1);
    assert_eq!(result.additional_retrievals, 2);
    assert_eq!(result.probes.len(), 1);
    assert_eq!(result.probes[0].origin, "alias");
    assert_eq!(
        result
            .hits
            .iter()
            .map(|hit| &hit.match_id)
            .collect::<BTreeSet<_>>()
            .len(),
        result.hits.len()
    );
    let dual = result
        .hits
        .iter()
        .position(|hit| hit.source.key == "dual")
        .unwrap();
    let evidence = &result.traces[dual];
    assert!(evidence.admission.lexical);
    assert!(evidence.admission.meaningful_bm25.is_some());
    assert!(evidence.admission.expansion_bm25.is_some());
    assert!(evidence.original_contribution > 0.0);
    assert!(evidence.expanded_contribution <= evidence.original_contribution * 0.35);
    let alias = result
        .hits
        .iter()
        .position(|hit| hit.source.key == "alias")
        .unwrap();
    assert_eq!(result.traces[alias].original_contribution, 0.0);
    assert!(result.traces[alias].expanded_contribution > 0.0);
    assert!(result.traces[alias].expanded_contribution <= 0.5);
    assert_eq!(result.traces[alias].baseline_bm25, 0.0);
}

#[test]
fn meaningful_terms_keep_compound_digest_identity_and_raw_query_multiplicity() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("index.sqlite3")).unwrap();
    let filter = fixture::populate(&store, "project").unwrap();
    for (key, term) in [("compound", "foo.bar".into()), ("digest", "x".repeat(300))] {
        put(&store, key, &format!("{term} {}", "padding ".repeat(1_900)));
        let query = format!("why why {term} {term}");
        let plan = QueryPlan::new(&query, true).unwrap();
        assert!(plan.stopwords_removed);
        assert_eq!(plan.original, tokenize_checked(&term).unwrap());
        let result = store
            .search_ranked_with_at(
                &query,
                &filter,
                10,
                Default::default(),
                fixture::NOW.parse().unwrap(),
            )
            .unwrap();
        assert_eq!(result.hits[0].source.key, key);
        assert!(!result.traces[0].admission.lexical);
        let retained_score = store.search(&term, &filter, 10).unwrap().1[0].score;
        assert_eq!(
            result.traces[0].admission.meaningful_bm25,
            Some(retained_score)
        );
        let raw_target = store
            .search(&query, &filter, 2_000)
            .unwrap()
            .1
            .into_iter()
            .find(|hit| hit.source.key == key)
            .unwrap();
        assert_eq!(result.traces[0].baseline_bm25, raw_target.score);
        assert!(result.traces[0].baseline_bm25 > retained_score);
        assert!(result.traces[0].original_contribution > 0.0);
    }
}

#[test]
fn meaningful_lane_filters_before_its_limit_for_projects_and_sessions() {
    for kind in ["project", "session"] {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("index.sqlite3")).unwrap();
        for index in 0..81 {
            let mut source = Source {
                key: format!("row-{index}"),
                collection: "scope".into(),
                path: format!("allowed/{index}.txt"),
                version: "v1".into(),
                kind: kind.into(),
            };
            let mut chunk = Chunk {
                text: if index == 80 {
                    fixture::target_text()
                } else {
                    fixture::RETAINED_QUERY.into()
                },
                agent: Some("codex".into()),
                session_id: Some("wanted".into()),
                event_id: Some(format!("event-{index}")),
                field_kind: Some("message".into()),
                timestamp: Some(fixture::NOW.into()),
                ..Default::default()
            };
            match index / 10 {
                0 => source.collection = "elsewhere".into(),
                1 => {
                    source.kind = if kind == "project" {
                        "session"
                    } else {
                        "project"
                    }
                    .into()
                }
                2 => source.path = format!("excluded/{index}.txt"),
                3 => chunk.agent = Some("claude".into()),
                4 => chunk.session_id = Some("unwanted".into()),
                5 => chunk.timestamp = Some("2025-01-01T00:00:00Z".into()),
                6 => chunk.timestamp = Some("2027-01-01T00:00:00Z".into()),
                _ => {}
            }
            store.replace_source(&source, [Ok(chunk)]).unwrap();
            if (70..80).contains(&index) {
                store.invalidate_source(&source.key).unwrap();
            }
        }
        let filter = SearchFilter {
            collection: "scope".into(),
            kind: kind.into(),
            path_glob: Some("allowed/*.txt".into()),
            agent: Some("codex".into()),
            session_id: Some("wanted".into()),
            after: Some("2026-01-01T00:00:00Z".into()),
            before: Some("2027-01-01T00:00:00Z".into()),
        };
        let result = store
            .search_ranked_with_at(
                fixture::QUERY,
                &filter,
                10,
                Default::default(),
                fixture::NOW.parse().unwrap(),
            )
            .unwrap();
        assert_eq!(result.admission_counts.meaningful, 1, "{kind}");
        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.hits[0].source.key, "row-80");
    }
}

#[test]
fn meaningful_admission_preserves_streamed_boundary_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("corpus");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(
        root.join("part.txt"),
        format!("{}needleboundary\n", "a ".repeat(8_190)),
    )
    .unwrap();
    let store = Store::open(&dir.path().join("index.sqlite3")).unwrap();
    ingest::scan_project(&root, &store, "project").unwrap();
    let filter = SearchFilter {
        collection: "project".into(),
        kind: "project".into(),
        ..Default::default()
    };
    let result = store
        .search_ranked_with_at(
            "why needleboundary",
            &filter,
            10,
            RankingOptions {
                expansion: false,
                ..Default::default()
            },
            fixture::NOW.parse().unwrap(),
        )
        .unwrap();
    assert_eq!(result.meaningful_retrievals, 1);
    assert_eq!(result.hits.len(), 1);
    assert!(result.hits[0].chunk.start_byte >= 16_384);
    assert!(!result.hits[0].chunk.text.contains("needleboundary"));
    assert!(result.traces[0].original_contribution > 0.0);
    assert!(result.traces[0].admission.meaningful_bm25.is_some());
}
