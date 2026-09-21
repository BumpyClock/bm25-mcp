use bm25_mcp::{
    model::{Chunk, Hit, Source},
    query::{QueryClass, QueryPlan, classify},
    ranking::{
        BodyEvidence, CorpusStats, ExactClass, RankingOptions, prepare, prepare_indexed, rerank,
        temporal_factor, weighted_jaccard,
    },
};
use chrono::{Duration, Utc};
use std::collections::BTreeMap;

fn hit(id: &str, kind: &str, path: &str, text: &str, score: f32) -> Hit {
    Hit {
        copy_count: 1,
        match_id: id.into(),
        verified_at: None,
        source: Source {
            key: id.into(),
            collection: "c".into(),
            path: path.into(),
            version: "v".into(),
            kind: kind.into(),
        },
        chunk: Chunk {
            text: text.into(),
            start_line: 1,
            end_line: 3,
            start_byte: 0,
            end_byte: text.len() as u64,
            ..Chunk::default()
        },
        score,
    }
}

#[test]
fn query_classification_and_surface_exactness_are_deterministic() {
    assert_eq!(classify("src/search/index.rs"), QueryClass::Path);
    assert_eq!(classify("SearchIndex::lookup"), QueryClass::Identifier);
    assert_eq!(classify("Foo.bar"), QueryClass::Identifier);
    assert_eq!(classify("console.log"), QueryClass::Identifier);
    assert_eq!(classify("ManagerV2"), QueryClass::Identifier);
    assert_eq!(
        classify("HybridPersistenceCoordinator2"),
        QueryClass::Identifier
    );
    assert_eq!(classify("error[E0425]"), QueryClass::Diagnostic);
    assert_eq!(classify("why does indexing fail"), QueryClass::Natural);

    let plan = QueryPlan::new("SearchIndex::lookup", true).unwrap();
    assert_eq!(plan.literal, "searchindex::lookup");
    assert_eq!(
        QueryPlan::new("Alpha+Beta", true).unwrap().literal,
        "alpha+beta"
    );
    assert_eq!(
        QueryPlan::new("Foo(bar)", true).unwrap().literal,
        "foo(bar)"
    );
    let candidates = prepare(
        vec![hit(
            "symbol",
            "project",
            "src/search.rs",
            "pub fn SearchIndex::lookup() {}",
            0.1,
        )],
        &plan,
    )
    .unwrap();
    let (hits, traces) = rerank(
        candidates,
        &plan,
        &CorpusStats::default(),
        RankingOptions::default(),
        Utc::now(),
        10,
    )
    .unwrap();
    assert_eq!(hits[0].match_id, "symbol");
    assert_eq!(traces[0].exact_class, ExactClass::QualifiedSymbol);
}

#[test]
fn all_stopword_queries_keep_a_lexical_term() {
    let plan = QueryPlan::new("is", true).unwrap();
    assert_eq!(plan.original, vec!["is"]);
}

#[test]
fn all_stopword_fallback_remains_eligible() {
    let plan = QueryPlan::new("is", true).unwrap();
    let candidates = prepare(vec![hit("stopword", "project", "is.txt", "is", 0.0)], &plan).unwrap();
    let (hits, _) = rerank(
        candidates,
        &plan,
        &CorpusStats::default(),
        RankingOptions::default(),
        Utc::now(),
        1,
    )
    .unwrap();
    assert_eq!(hits.len(), 1);
}

#[test]
fn historical_qualified_symbols_require_the_complete_surface() {
    let plan = QueryPlan::new("Foo::Bar::bazValue", true).unwrap();
    let exact = hit(
        "exact",
        "session",
        "history.jsonl",
        "Previously fixed Foo::Bar::bazValue in the cache.",
        0.0,
    );
    let partial = hit(
        "partial",
        "session",
        "history2.jsonl",
        "Previously fixed Foo::Bar::bazValueExtra in the cache.",
        10.0,
    );
    let candidates = prepare(vec![partial, exact], &plan).unwrap();
    let (hits, traces) = rerank(
        candidates,
        &plan,
        &CorpusStats::default(),
        RankingOptions::default(),
        Utc::now(),
        2,
    )
    .unwrap();
    assert_eq!(hits[0].match_id, "exact");
    assert_eq!(traces[0].exact_class, ExactClass::HistoricalIdentity);
    assert_eq!(traces[1].exact_class, ExactClass::None);
}

#[test]
fn project_qualified_references_keep_the_complete_surface_tier() {
    let plan = QueryPlan::new("Foo::Bar::bazValue", true).unwrap();
    let candidates = prepare(
        vec![
            hit(
                "qualified-reference",
                "project",
                "src/use.rs",
                "use Foo::Bar::bazValue;",
                0.0,
            ),
            hit(
                "qualified-prefix",
                "project",
                "src/other.rs",
                "use Foo::Bar::bazValueExtra;",
                0.0,
            ),
        ],
        &plan,
    )
    .unwrap();
    let (hits, traces) = rerank(
        candidates,
        &plan,
        &CorpusStats::default(),
        RankingOptions::default(),
        Utc::now(),
        2,
    )
    .unwrap();
    assert_eq!(hits[0].match_id, "qualified-reference");
    assert_eq!(traces[0].exact_class, ExactClass::HistoricalIdentity);
    assert_eq!(traces[1].exact_class, ExactClass::None);
}

#[test]
fn path_basename_and_diagnostic_matches_get_exact_tiers() {
    let path_plan = QueryPlan::new("index.rs", true).unwrap();
    let candidates = prepare(
        vec![hit(
            "path",
            "project",
            "src/search/index.rs",
            "fn unrelated() {}",
            0.1,
        )],
        &path_plan,
    )
    .unwrap();
    let (_, traces) = rerank(
        candidates,
        &path_plan,
        &CorpusStats::default(),
        RankingOptions::default(),
        Utc::now(),
        10,
    )
    .unwrap();
    assert_eq!(traces[0].exact_class, ExactClass::Basename);

    let diagnostic_plan = QueryPlan::new("error[E0425]", true).unwrap();
    let candidates = prepare(
        vec![hit(
            "diagnostic",
            "session",
            "session",
            "compiler reported error[E0425] here",
            0.1,
        )],
        &diagnostic_plan,
    )
    .unwrap();
    let (_, traces) = rerank(
        candidates,
        &diagnostic_plan,
        &CorpusStats::default(),
        RankingOptions::default(),
        Utc::now(),
        10,
    )
    .unwrap();
    assert_eq!(traces[0].exact_class, ExactClass::Diagnostic);
}

#[test]
fn session_decay_has_floor_and_exact_exemption() {
    let now = Utc::now();
    let old = (now - Duration::days(365)).to_rfc3339();
    let floor = temporal_factor(Some(&old), now, false, false);
    assert!((0.25..0.30).contains(&floor));
    assert_eq!(temporal_factor(Some(&old), now, true, true), 1.0);
    assert!(temporal_factor(Some(&old), now, false, true) > floor);
}

#[test]
fn weighted_jaccard_is_symmetric_and_mmr_keeps_distinct_content() {
    let mut a = BTreeMap::new();
    a.insert("alpha".into(), 2.0);
    a.insert("beta".into(), 1.0);
    let mut b = BTreeMap::new();
    b.insert("alpha".into(), 1.0);
    b.insert("gamma".into(), 1.0);
    assert_eq!(weighted_jaccard(&a, &b), weighted_jaccard(&b, &a));

    let plan = QueryPlan::new("alpha", true).unwrap();
    let candidates = prepare(
        vec![
            hit("one", "project", "a.rs", "alpha alpha alpha", 1.0),
            hit("two", "project", "a.rs", "alpha alpha alpha", 0.9),
            hit("three", "project", "b.rs", "alpha beta", 0.8),
        ],
        &plan,
    )
    .unwrap();
    let (hits, traces) = rerank(
        candidates,
        &plan,
        &CorpusStats::default(),
        RankingOptions::default(),
        Utc::now(),
        3,
    )
    .unwrap();
    assert_eq!(
        hits.len(),
        2,
        "identical project chunks should structurally dedupe"
    );
    assert_eq!(hits[0].match_id, "one");
    assert_eq!(hits[1].match_id, "three");
    assert!(traces[0].collapsed_ids.contains(&"two".to_string()));
}

#[test]
fn exact_symbol_beats_repeated_prose_and_project_time_is_ignored() {
    let plan = QueryPlan::new("SearchIndex", true).unwrap();
    let mut prose = hit(
        "prose",
        "project",
        "docs/design.md",
        &"SearchIndex ".repeat(80),
        9.0,
    );
    prose.chunk.timestamp = Some("2000-01-01T00:00:00Z".into());
    let declaration = hit(
        "decl",
        "project",
        "src/index.rs",
        "pub struct SearchIndex { entries: Vec<String> }",
        0.01,
    );
    let candidates = prepare(vec![prose, declaration], &plan).unwrap();
    let (hits, traces) = rerank(
        candidates,
        &plan,
        &CorpusStats::default(),
        RankingOptions::default(),
        Utc::now(),
        2,
    )
    .unwrap();
    assert_eq!(hits[0].match_id, "decl");
    assert_eq!(traces[0].exact_class, ExactClass::Symbol);

    let old = hit(
        "old",
        "project",
        "old.rs",
        "fn lookup() { SearchIndex(); }",
        0.9,
    );
    let recent = hit(
        "recent",
        "project",
        "new.rs",
        "fn lookup() { SearchIndex(); return 1; }",
        0.9,
    );
    let candidates = prepare(vec![old, recent], &plan).unwrap();
    let (_, traces) = rerank(
        candidates,
        &plan,
        &CorpusStats::default(),
        RankingOptions::default(),
        Utc::now(),
        2,
    )
    .unwrap();
    assert_eq!(traces[0].decay_factor, 1.0);
    assert_eq!(traces[1].decay_factor, 1.0);
}

#[test]
fn expansion_is_bounded_and_keeps_provenance() {
    let plan = QueryPlan::new("panic", true).unwrap();
    assert!(
        plan.probes
            .iter()
            .any(|p| p.query == "crash" && p.origin == "alias" && p.weight == 0.4)
    );
    assert!(plan.probes.len() <= 6);
    let candidates = prepare(
        vec![hit(
            "expanded",
            "project",
            "crash.rs",
            "fn crash_recovery() { restart(); }",
            0.0,
        )],
        &plan,
    )
    .unwrap();
    let (_, traces) = rerank(
        candidates,
        &plan,
        &CorpusStats::default(),
        RankingOptions::default(),
        Utc::now(),
        1,
    )
    .unwrap();
    assert!(traces[0].original_contribution.abs() < f64::EPSILON);
    assert!(traces[0].expanded_contribution <= 0.5);

    let candidates = prepare(
        vec![hit(
            "expanded-disabled",
            "project",
            "crash.rs",
            "fn crash_recovery() { restart(); }",
            0.0,
        )],
        &plan,
    )
    .unwrap();
    let options = RankingOptions {
        expansion: false,
        ..RankingOptions::default()
    };
    let (hits, _) = rerank(
        candidates,
        &plan,
        &CorpusStats::default(),
        options,
        Utc::now(),
        1,
    )
    .unwrap();
    assert!(hits.is_empty());
}

#[test]
fn proximity_bonus_is_relative_and_capped() {
    let plan = QueryPlan::new("alpha beta", true).unwrap();
    let near = hit("near", "project", "near.rs", "alpha beta", 0.0);
    let far = hit(
        "far",
        "project",
        "far.rs",
        "alpha filler filler filler filler beta",
        0.0,
    );
    let candidates = prepare(vec![far, near], &plan).unwrap();
    let (hits, traces) = rerank(
        candidates,
        &plan,
        &CorpusStats::default(),
        RankingOptions::default(),
        Utc::now(),
        2,
    )
    .unwrap();
    assert_eq!(hits[0].match_id, "near");
    assert!(traces[0].proximity_bonus > traces[1].proximity_bonus);
    for trace in traces {
        assert!(trace.proximity_bonus <= trace.lexical_score * 0.15 + f64::EPSILON);
    }
}

#[test]
fn old_exact_session_survives_decay_while_recent_nonexact_wins_over_tool_spam() {
    let now = Utc::now();
    let plan = QueryPlan::new("error[E0425]", true).unwrap();
    let mut old = hit(
        "old",
        "session",
        "old.jsonl",
        &"error[E0425] ".repeat(40),
        0.0,
    );
    old.chunk.timestamp = Some("2000-01-01T00:00:00Z".into());
    old.chunk.role = Some("assistant".into());
    let mut recent = hit(
        "recent",
        "session",
        "recent.jsonl",
        "parser fixed the missing error diagnostic",
        0.0,
    );
    recent.chunk.timestamp = Some((now - Duration::days(1)).to_rfc3339());
    recent.chunk.role = Some("assistant".into());
    let options = RankingOptions::default();
    let candidates = prepare(vec![old, recent], &plan).unwrap();
    let (hits, traces) =
        rerank(candidates, &plan, &CorpusStats::default(), options, now, 2).unwrap();
    assert_eq!(hits[0].match_id, "old");
    assert_eq!(traces[0].decay_factor, 1.0);
    assert!(traces[1].decay_factor < 1.0);
}

#[test]
fn concise_assistant_reasoning_beats_repeated_tool_output() {
    let plan = QueryPlan::new("checkpoint decision", true).unwrap();
    let mut tool = hit(
        "tool",
        "session",
        "tool.jsonl",
        &"checkpoint ".repeat(600),
        0.0,
    );
    tool.chunk.role = Some("tool".into());
    tool.chunk.field_kind = Some("tool_result".into());
    let mut assistant = hit(
        "assistant",
        "session",
        "answer.jsonl",
        "Architectural decision: checkpoint commits are atomic.",
        0.0,
    );
    assistant.chunk.role = Some("assistant".into());
    assistant.chunk.field_kind = Some("message".into());
    let candidates = prepare(vec![tool, assistant], &plan).unwrap();
    let (hits, _) = rerank(
        candidates,
        &plan,
        &CorpusStats::default(),
        RankingOptions::default(),
        Utc::now(),
        2,
    )
    .unwrap();
    assert_eq!(hits[0].match_id, "assistant");
}

#[test]
fn mmr_reduces_redundant_results_when_dedupe_is_disabled() {
    let plan = QueryPlan::new("alpha", true).unwrap();
    let options = RankingOptions {
        dedupe: false,
        ..RankingOptions::default()
    };
    let candidates = prepare(
        vec![
            hit("a", "project", "a.rs", "alpha shared shared shared", 1.0),
            hit("b", "project", "b.rs", "alpha shared shared shared", 0.99),
            hit("c", "project", "c.rs", "alpha distinct token", 0.8),
        ],
        &plan,
    )
    .unwrap();
    let (hits, traces) = rerank(
        candidates,
        &plan,
        &CorpusStats::default(),
        options,
        Utc::now(),
        2,
    )
    .unwrap();
    assert_eq!(hits.len(), 2);
    let ids: std::collections::BTreeSet<_> = hits.iter().map(|hit| hit.match_id.as_str()).collect();
    assert!(ids.contains("a") && ids.contains("c"));
    assert!(!ids.contains("b"));
    assert!(traces[1].mmr_penalty > 0.0);
}

#[test]
fn declaration_components_contribute_symbol_field_evidence() {
    let plan = QueryPlan::new("search index", true).unwrap();
    let candidate = hit(
        "search-index",
        "project",
        "src/index.rs",
        "pub struct SearchIndex {}",
        0.0,
    );
    let candidates = prepare(vec![candidate], &plan).unwrap();
    let (_, traces) = rerank(
        candidates,
        &plan,
        &CorpusStats::default(),
        RankingOptions::default(),
        Utc::now(),
        1,
    )
    .unwrap();
    assert!(
        traces[0]
            .matched_fields
            .get("symbol")
            .copied()
            .unwrap_or_default()
            > 0.0
    );

    let exact_plan = QueryPlan::new("SearchIndex", true).unwrap();
    let candidates = prepare(
        vec![hit(
            "search-index",
            "project",
            "src/index.rs",
            "pub struct SearchIndex {}",
            0.0,
        )],
        &exact_plan,
    )
    .unwrap();
    let (_, traces) = rerank(
        candidates,
        &exact_plan,
        &CorpusStats::default(),
        RankingOptions::default(),
        Utc::now(),
        1,
    )
    .unwrap();
    assert!(
        traces[0]
            .matched_fields
            .get("symbol")
            .copied()
            .unwrap_or_default()
            > 0.0
    );
    assert_eq!(traces[0].exact_class, ExactClass::Symbol);
}

#[test]
fn declaration_components_do_not_become_exact_definitions() {
    let plan = QueryPlan::new("SearchIndex", true).unwrap();
    let candidates = prepare(
        vec![hit(
            "partial-definition",
            "project",
            "src/index.rs",
            "pub struct SearchIndexExtra {}",
            0.0,
        )],
        &plan,
    )
    .unwrap();
    let (_, traces) = rerank(
        candidates,
        &plan,
        &CorpusStats::default(),
        RankingOptions::default(),
        Utc::now(),
        1,
    )
    .unwrap();
    assert_eq!(traces[0].exact_class, ExactClass::None);

    let plan = QueryPlan::new("Alpha+Beta", true).unwrap();
    let candidates = prepare(
        vec![hit(
            "multi-surface-query",
            "project",
            "src/alpha.rs",
            "pub struct Alpha {}",
            0.0,
        )],
        &plan,
    )
    .unwrap();
    let (_, traces) = rerank(
        candidates,
        &plan,
        &CorpusStats::default(),
        RankingOptions::default(),
        Utc::now(),
        1,
    )
    .unwrap();
    assert_eq!(traces[0].exact_class, ExactClass::None);
}

#[test]
fn acronym_declarations_contribute_each_symbol_component() {
    let plan = QueryPlan::new("http response parser", true).unwrap();
    let candidates = prepare(
        vec![hit(
            "http-response-parser",
            "project",
            "src/http.rs",
            "pub struct HTTPResponseParser {}",
            0.0,
        )],
        &plan,
    )
    .unwrap();
    let (_, traces) = rerank(
        candidates,
        &plan,
        &CorpusStats::default(),
        RankingOptions::default(),
        Utc::now(),
        1,
    )
    .unwrap();
    assert!(
        traces[0]
            .matched_fields
            .get("symbol")
            .copied()
            .unwrap_or_default()
            > 0.0
    );
}

#[test]
fn dotted_historical_identity_is_exact_and_exempt_from_decay() {
    let now = Utc::now();
    let plan = QueryPlan::new("Foo.bar", true).unwrap();
    let mut candidate = hit(
        "dotted",
        "session",
        "history.jsonl",
        "Previously fixed Foo.bar in the parser.",
        0.0,
    );
    candidate.chunk.timestamp = Some("2000-01-01T00:00:00Z".into());
    let candidates = prepare(vec![candidate], &plan).unwrap();
    let (_, traces) = rerank(
        candidates,
        &plan,
        &CorpusStats::default(),
        RankingOptions::default(),
        now,
        1,
    )
    .unwrap();
    assert_eq!(traces[0].exact_class, ExactClass::HistoricalIdentity);
    assert_eq!(traces[0].decay_factor, 1.0);
}

#[test]
fn unsupported_stopword_incidental_match_is_not_eligible() {
    let plan = QueryPlan::new("why does indexing fail", true).unwrap();
    let candidates = prepare(
        vec![hit(
            "unsupported",
            "session",
            "unrelated.jsonl",
            "why does this happen",
            0.0,
        )],
        &plan,
    )
    .unwrap();
    let (hits, _) = rerank(
        candidates,
        &plan,
        &CorpusStats::default(),
        RankingOptions::default(),
        Utc::now(),
        1,
    )
    .unwrap();
    assert!(hits.is_empty());
}

#[test]
fn unsupported_stopword_match_stays_ineligible_without_fields_or_exact() {
    let plan = QueryPlan::new("why does indexing fail", true).unwrap();
    for options in [
        RankingOptions {
            fields: false,
            ..RankingOptions::default()
        },
        RankingOptions {
            exact: false,
            ..RankingOptions::default()
        },
        RankingOptions {
            fields: false,
            exact: false,
            ..RankingOptions::default()
        },
    ] {
        let candidates = prepare(
            vec![hit(
                "unsupported-ablation",
                "session",
                "unrelated.jsonl",
                "why does this happen",
                0.0,
            )],
            &plan,
        )
        .unwrap();
        let (hits, _) = rerank(
            candidates,
            &plan,
            &CorpusStats::default(),
            options,
            Utc::now(),
            1,
        )
        .unwrap();
        assert!(hits.is_empty());
    }

    let fallback_plan = QueryPlan::new("why", true).unwrap();
    let candidates = prepare(
        vec![hit("stopword-fallback", "project", "notes.txt", "why", 0.0)],
        &fallback_plan,
    )
    .unwrap();
    let (hits, _) = rerank(
        candidates,
        &fallback_plan,
        &CorpusStats::default(),
        RankingOptions {
            fields: false,
            exact: false,
            ..RankingOptions::default()
        },
        Utc::now(),
        1,
    )
    .unwrap();
    assert_eq!(hits.len(), 1);
}

#[test]
fn indexed_body_evidence_drives_fragment_boundary_scoring() {
    let plan = QueryPlan::new("needleboundary", true).unwrap();
    let mut terms = BTreeMap::new();
    terms.insert("needleboundary".into(), 1);
    let candidates = prepare_indexed(
        vec![(
            hit("boundary", "project", "boundary.rs", "leboundary", 0.0),
            BodyEvidence { terms, length: 1 },
        )],
        &plan,
    )
    .unwrap();
    let (hits, traces) = rerank(
        candidates,
        &plan,
        &CorpusStats::default(),
        RankingOptions::default(),
        Utc::now(),
        1,
    )
    .unwrap();
    assert_eq!(hits.len(), 1);
    assert!(
        traces[0]
            .matched_fields
            .get("body")
            .copied()
            .unwrap_or_default()
            > 0.0
    );
    assert!(traces[0].original_contribution > 0.0);

    let mut terms = BTreeMap::new();
    terms.insert("needleboundary".into(), 1);
    let candidates = prepare_indexed(
        vec![(
            hit(
                "session-boundary",
                "session",
                "history.jsonl",
                "leboundary",
                0.0,
            ),
            BodyEvidence { terms, length: 1 },
        )],
        &plan,
    )
    .unwrap();
    let (_, traces) = rerank(
        candidates,
        &plan,
        &CorpusStats::default(),
        RankingOptions::default(),
        Utc::now(),
        1,
    )
    .unwrap();
    assert!(
        traces[0]
            .matched_fields
            .get("assistant_prose")
            .copied()
            .unwrap_or_default()
            > 0.0
    );

    let mut terms = BTreeMap::new();
    terms.insert("needleboundary".into(), 1);
    let candidates = prepare_indexed(
        vec![(
            hit(
                "boundary-no-fields",
                "project",
                "boundary.rs",
                "leboundary",
                0.0,
            ),
            BodyEvidence { terms, length: 1 },
        )],
        &plan,
    )
    .unwrap();
    let options = RankingOptions {
        fields: false,
        ..RankingOptions::default()
    };
    let (hits, traces) = rerank(
        candidates,
        &plan,
        &CorpusStats::default(),
        options,
        Utc::now(),
        1,
    )
    .unwrap();
    assert_eq!(hits.len(), 1);
    assert!(traces[0].original_contribution > 0.0);
}

#[test]
fn dotted_identity_requires_the_complete_surface() {
    let plan = QueryPlan::new("Foo.bar", true).unwrap();
    let candidates = prepare(
        vec![hit(
            "partial-dotted",
            "session",
            "history.jsonl",
            "Foo.barExtra was mentioned.",
            0.0,
        )],
        &plan,
    )
    .unwrap();
    let (_, traces) = rerank(
        candidates,
        &plan,
        &CorpusStats::default(),
        RankingOptions::default(),
        Utc::now(),
        1,
    )
    .unwrap();
    assert_eq!(traces[0].exact_class, ExactClass::None);

    let component_plan = QueryPlan::new("Foo", true).unwrap();
    let candidates = prepare(
        vec![hit(
            "dotted-component",
            "session",
            "history.jsonl",
            "Foo.bar was mentioned.",
            0.0,
        )],
        &component_plan,
    )
    .unwrap();
    let (_, traces) = rerank(
        candidates,
        &component_plan,
        &CorpusStats::default(),
        RankingOptions::default(),
        Utc::now(),
        1,
    )
    .unwrap();
    assert_eq!(traces[0].exact_class, ExactClass::None);
}

#[test]
fn unqualified_camel_identity_survives_surrounding_punctuation() {
    let plan = QueryPlan::new("SearchIndex", true).unwrap();
    let candidates = prepare(
        vec![hit(
            "camel-punctuation",
            "session",
            "history.jsonl",
            "Previously used SearchIndex.",
            0.0,
        )],
        &plan,
    )
    .unwrap();
    let (_, traces) = rerank(
        candidates,
        &plan,
        &CorpusStats::default(),
        RankingOptions::default(),
        Utc::now(),
        1,
    )
    .unwrap();
    assert_eq!(traces[0].exact_class, ExactClass::HistoricalIdentity);
}

#[test]
fn dotted_identity_uses_normalized_tokenizer_surface_with_punctuation() {
    let plan = QueryPlan::new("foo.bar!", true).unwrap();
    assert_eq!(plan.literal, "foo.bar");
    let candidates = prepare(
        vec![hit(
            "dotted-punctuation",
            "session",
            "history.jsonl",
            "Previously fixed FOO.BAR, in the parser.",
            0.0,
        )],
        &plan,
    )
    .unwrap();
    let (_, traces) = rerank(
        candidates,
        &plan,
        &CorpusStats::default(),
        RankingOptions::default(),
        Utc::now(),
        1,
    )
    .unwrap();
    assert_eq!(traces[0].exact_class, ExactClass::HistoricalIdentity);
}
