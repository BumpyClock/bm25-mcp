use bm25_mcp::{
    model::{Chunk, SearchFilter, Source},
    store::Store,
    tools::{Coverage, dispatch, public_coverage_value},
};
use serde_json::{Value, json};

fn source(store: &Store, collection: &str, key: &str, path: &str, text: &str) {
    store
        .replace_source(
            &Source {
                key: key.into(),
                collection: collection.into(),
                path: path.into(),
                version: "v1".into(),
                kind: "project".into(),
            },
            [Ok(Chunk {
                text: text.into(),
                start_line: 1,
                end_line: 1,
                ..Default::default()
            })],
        )
        .unwrap();
}
fn search(store: &Store, args: Value) -> Value {
    dispatch(
        store,
        "project",
        "owner",
        "search_project",
        args,
        "ready",
        Coverage::default(),
        true,
    )
    .unwrap()
}

#[test]
fn public_coverage_omits_raw_errors_and_unknown_diagnostic_names() {
    let mut coverage = Coverage::default();
    coverage
        .errors
        .push("/private/project/secret-marker".into());
    coverage
        .diagnostics
        .insert("provider:/private/project/secret-marker".into(), 2);
    coverage.diagnostics.insert("unsupported_record".into(), 3);

    let value = public_coverage_value(&coverage);
    let text = value.to_string();
    assert!(!text.contains("secret-marker"));
    assert!(value.get("errors").is_none());
    assert_eq!(value["diagnostics"]["unsupported_record"], 3);
    assert_eq!(value["diagnostics"]["other"], 2);
}

#[test]
fn enhanced_tools_suppress_unsupported_hits_without_changing_raw_search() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("evidence.sqlite3")).unwrap();
    for (kind, collection, tool) in [
        ("project", "project", "search_project"),
        ("session", "owner", "search_sessions"),
    ] {
        let source = Source {
            key: kind.into(),
            collection: collection.into(),
            path: "notes.txt".into(),
            version: "v1".into(),
            kind: kind.into(),
        };
        store
            .replace_source(
                &source,
                [Ok(Chunk {
                    text: "why does this happen".into(),
                    ..Default::default()
                })],
            )
            .unwrap();
        let filter = SearchFilter {
            collection: collection.into(),
            kind: kind.into(),
            ..Default::default()
        };
        let raw = store
            .search("why does indexing fail", &filter, 10)
            .unwrap()
            .1;
        assert_eq!(raw.len(), 1);
        assert!(raw[0].score > 0.0);
        let result = dispatch(
            &store,
            "project",
            "owner",
            tool,
            json!({"query":"why does indexing fail"}),
            "ready",
            Coverage::default(),
            true,
        )
        .unwrap();
        assert_eq!(result["results"], json!([]), "{kind}: {result}");

        store
            .replace_source(
                &Source {
                    version: "v2".into(),
                    ..source
                },
                [Ok(Chunk {
                    text: "is".into(),
                    ..Default::default()
                })],
            )
            .unwrap();
        let fallback = dispatch(
            &store,
            "project",
            "owner",
            tool,
            json!({"query":"is"}),
            "ready",
            Coverage::default(),
            true,
        )
        .unwrap();
        assert_eq!(fallback["results"].as_array().unwrap().len(), 1);
        assert!(fallback["results"][0]["score"].as_f64().unwrap() > 0.0);
    }
}

#[test]
fn filters_are_applied_before_top_k_and_collections_are_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("test.sqlite3")).unwrap();
    source(&store, "project", "a", "high.rs", "needle needle needle");
    source(
        &store,
        "project",
        "b",
        "low.txt",
        "needle with several other tokens",
    );
    source(&store, "elsewhere", "c", "secret.txt", "needle secret");
    let result = search(
        &store,
        json!({"query":"needle","path_glob":"*.txt","limit":1}),
    );
    assert_eq!(result["results"].as_array().unwrap().len(), 1);
    assert_eq!(result["results"][0]["relative_path"], "low.txt");
}
#[test]
fn responses_obey_utf8_budgets_and_reject_invalid_modes() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("test.sqlite3")).unwrap();
    source(
        &store,
        "project",
        "a",
        "file.txt",
        &format!("needle {}", "🦀".repeat(4000)),
    );
    let result = search(&store, json!({"query":"needle","max_response_bytes":1024}));
    assert!(serde_json::to_vec(&result).unwrap().len() <= 1024);
    assert_eq!(result["truncated"], true);
    assert!(!result["results"].as_array().unwrap().is_empty());
    assert!(
        dispatch(
            &store,
            "project",
            "owner",
            "search_project",
            json!({"query":"needle","mode":"context"}),
            "ready",
            Coverage::default(),
            true
        )
        .is_err()
    );
    assert!(
        dispatch(
            &store,
            "project",
            "owner",
            "search_sessions",
            json!({"query":"needle","before":"invalid"}),
            "ready",
            Coverage::default(),
            true
        )
        .is_err()
    );
}

#[test]
fn default_search_keeps_ten_long_hits_with_compact_excerpts() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("compact.sqlite3")).unwrap();
    for index in 0..10 {
        source(
            &store,
            "project",
            &format!("source-{index}"),
            &format!("file-{index}.txt"),
            &format!("needle hit-{index} {}", "context ".repeat(200)),
        );
    }

    let result = search(&store, json!({"query":"needle"}));
    let results = result["results"].as_array().unwrap();
    assert_eq!(results.len(), 10);
    assert!(!result["truncated"].as_bool().unwrap());
    assert!(serde_json::to_vec(&result).unwrap().len() <= 16384);
    for item in results {
        let excerpt = item["excerpt"].as_str().unwrap();
        let offset = item["excerpt_byte_offset"].as_u64().unwrap() as usize;
        assert!(excerpt.len() <= 640);
        assert!(item["excerpt_truncated"].as_bool().unwrap());
        assert!(excerpt.contains("needle"));
        assert_eq!(item["start_byte"], 0);
        assert_eq!(item["end_byte"], 0);
        assert!(offset + excerpt.len() <= 2_000);
    }
}

#[test]
fn compact_excerpt_prefers_late_whole_identifier_over_early_component() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("identifier.sqlite3")).unwrap();
    let text = format!("{}target_marker tail", "target ".repeat(500));
    source(&store, "project", "identifier", "identifier.txt", &text);

    for query in ["target_marker", "target marker"] {
        let result = search(&store, json!({"query":query}));
        let item = &result["results"][0];
        let excerpt = item["excerpt"].as_str().unwrap();
        let offset = item["excerpt_byte_offset"].as_u64().unwrap() as usize;
        assert!(excerpt.contains("target_marker"));
        assert_eq!(excerpt, &text[offset..offset + excerpt.len()]);
        assert!(offset > "target ".len());
    }
}

#[test]
fn compact_excerpt_maps_casefold_expansion_to_contiguous_source_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("casefold.sqlite3")).unwrap();
    let text = format!("{}Straße suffix", "prefix ".repeat(500));
    store
        .replace_source(
            &Source {
                key: "casefold".into(),
                collection: "project".into(),
                path: "casefold.txt".into(),
                version: "v1".into(),
                kind: "project".into(),
            },
            [Ok(Chunk {
                text: text.clone(),
                start_byte: 100,
                end_byte: 100 + text.len() as u64,
                ..Default::default()
            })],
        )
        .unwrap();

    let result = search(&store, json!({"query":"STRASSE"}));
    let item = &result["results"][0];
    let excerpt = item["excerpt"].as_str().unwrap();
    let offset = item["excerpt_byte_offset"].as_u64().unwrap() as usize;
    assert!(excerpt.contains("Straße"));
    assert_eq!(excerpt, &text[offset..offset + excerpt.len()]);
    assert_eq!(item["start_byte"], 100);
    assert_eq!(item["end_byte"], 100 + text.len() as u64);
    assert!(item["excerpt_truncated"].as_bool().unwrap());
}

#[test]
fn session_context_paginates_utf8_without_losing_text() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("test.sqlite3")).unwrap();
    let text = format!("needle {}", "🦀".repeat(4000));
    store
        .replace_source(
            &Source {
                key: "session".into(),
                collection: "owner".into(),
                path: "codex:test.jsonl".into(),
                version: "v1".into(),
                kind: "session".into(),
            },
            [Ok(Chunk {
                text: text.clone(),
                event_id: Some("event".into()),
                agent: Some("codex".into()),
                ..Default::default()
            })],
        )
        .unwrap();
    let search = dispatch(
        &store,
        "project",
        "owner",
        "search_sessions",
        json!({"query":"needle"}),
        "ready",
        Coverage::default(),
        true,
    )
    .unwrap();
    let id = search["results"][0]["match_id"].clone();
    let mut cursor = None;
    let mut collected = String::new();
    for _ in 0..100 {
        let mut args = json!({"mode":"context","match_id":id,"max_response_bytes":1024});
        if let Some(value) = cursor {
            args["cursor"] = value;
        }
        let response = dispatch(
            &store,
            "project",
            "owner",
            "search_sessions",
            args,
            "ready",
            Coverage::default(),
            true,
        )
        .unwrap();
        assert!(serde_json::to_vec(&response).unwrap().len() <= 1024);
        for event in response["context"].as_array().unwrap() {
            collected.push_str(event["excerpt"].as_str().unwrap());
        }
        cursor = response.get("cursor").cloned();
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(collected, text);
    source(&store, "owner", "project-file", "code.rs", "needle");
    let project = dispatch(
        &store,
        "owner",
        "owner",
        "search_project",
        json!({"query":"needle"}),
        "ready",
        Coverage::default(),
        true,
    )
    .unwrap();
    assert!(
        dispatch(
            &store,
            "owner",
            "owner",
            "search_sessions",
            json!({"mode":"context","match_id":project["results"][0]["match_id"]}),
            "ready",
            Coverage::default(),
            true
        )
        .is_err()
    );
}

#[test]
fn budgeted_excerpt_keeps_a_late_matching_term_and_decoded_offset() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("excerpt.sqlite3")).unwrap();
    let text = format!("{} lateuniquemarker end", "prefix ".repeat(2000));
    source(&store, "project", "late", "late.txt", &text);
    let result = search(
        &store,
        json!({"query":"lateuniquemarker","max_response_bytes":1024}),
    );
    let item = &result["results"][0];
    let excerpt = item["excerpt"].as_str().unwrap();
    let offset = item["excerpt_byte_offset"].as_u64().unwrap() as usize;
    assert!(excerpt.contains("lateuniquemarker"));
    assert_eq!(excerpt, &text[offset..offset + excerpt.len()]);
    assert!(serde_json::to_vec(&result).unwrap().len() <= 1024);
}

fn session_request(store: &Store, args: Value) -> anyhow::Result<Value> {
    dispatch(
        store,
        "project",
        "owner",
        "search_sessions",
        args,
        "ready",
        Coverage::default(),
        true,
    )
}

fn session_source(store: &Store, key: &str, chunks: Vec<Chunk>) {
    store
        .replace_source(
            &Source {
                key: key.into(),
                collection: "owner".into(),
                path: format!("codex:{key}.jsonl"),
                version: "v1".into(),
                kind: "session".into(),
            },
            chunks.into_iter().map(Ok),
        )
        .unwrap();
}

fn raw_session(store: &Store, limit: usize) -> Vec<bm25_mcp::model::Hit> {
    store
        .search(
            "needle",
            &SearchFilter {
                collection: "owner".into(),
                kind: "session".into(),
                ..Default::default()
            },
            limit,
        )
        .unwrap()
        .1
}

fn message(event: Option<&str>, text: &str, line: u64) -> Chunk {
    Chunk {
        text: text.into(),
        agent: Some("codex".into()),
        session_id: Some("session".into()),
        event_id: event.map(str::to_owned),
        role: Some("user".into()),
        field_kind: Some("message".into()),
        timestamp: Some("2026-09-01T00:00:00.000Z".into()),
        start_line: line,
        end_line: line,
        start_byte: line * 100,
        end_byte: line * 100 + 90,
        ..Default::default()
    }
}

#[test]
fn copied_session_events_refill_results_and_preserve_each_source_context() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("copies.sqlite3")).unwrap();
    for index in 0..31 {
        session_source(
            &store,
            &format!("copy-{index:02}"),
            vec![
                message(
                    Some(&format!("before-{index}")),
                    &format!("before copy-{index:02}"),
                    1,
                ),
                message(Some("copied-event"), "needle", 2),
                message(
                    Some(&format!("after-{index}")),
                    &format!("after copy-{index:02}"),
                    3,
                ),
            ],
        );
    }
    for index in 0..12 {
        session_source(
            &store,
            &format!("other-{index}"),
            vec![message(
                Some(&format!("other-{index}")),
                "needle additional context",
                1,
            )],
        );
    }
    let raw = raw_session(&store, 10);
    assert_eq!(raw.len(), 10);
    assert!(raw.iter().any(|hit| hit.copy_count == 31));
    let result = session_request(&store, json!({"query":"needle","limit":10})).unwrap();
    let hits = result["results"].as_array().unwrap();
    assert_eq!(
        hits.len(),
        2,
        "ranked search collapses identical session content"
    );
    assert_eq!(
        hits.iter()
            .filter(|h| h["event_id"] == "copied-event")
            .count(),
        1
    );
    let copied = hits
        .iter()
        .find(|h| h["event_id"] == "copied-event")
        .unwrap();
    assert_eq!(copied["copy_count"], 31);
    let anchor = copied["match_id"].as_str().unwrap();
    let mut cursor = None;
    let mut ids = std::collections::HashSet::new();
    let mut paths = std::collections::HashSet::new();
    loop {
        let mut args =
            json!({"mode":"copies","match_id":anchor,"limit":7,"max_response_bytes":2048});
        if let Some(c) = cursor.take() {
            args["cursor"] = c;
        }
        let page = session_request(&store, args).unwrap();
        assert!(serde_json::to_vec(&page).unwrap().len() <= 2048);
        for copy in page["copies"].as_array().unwrap() {
            assert!(ids.insert(copy["match_id"].as_str().unwrap().to_owned()));
            assert!(
                paths.insert(
                    copy["source_reference"]["path"]
                        .as_str()
                        .unwrap()
                        .to_owned()
                )
            );
            let context = session_request(&store, json!({"mode":"context","match_id":copy["match_id"],"before_events":1,"after_events":1})).unwrap();
            let rows = context["context"].as_array().unwrap();
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[1]["excerpt"], "needle");
            assert!(
                rows.iter()
                    .all(|row| row["source_reference"]["path"] == copy["source_reference"]["path"])
            );
        }
        match page.get("cursor") {
            Some(c) => cursor = Some(c.clone()),
            None => break,
        }
    }
    assert_eq!(ids.len(), 31);
    store.set_memory_pressure(true);
    let pressure = session_request(&store, json!({"query":"needle","limit":10})).unwrap();
    let mut expected = result.clone();
    let mut actual = pressure.clone();
    for value in [&mut expected, &mut actual] {
        for row in value["results"].as_array_mut().unwrap() {
            row.as_object_mut().unwrap().remove("score");
        }
    }
    assert_eq!(actual, expected);
    for (left, right) in result["results"]
        .as_array()
        .unwrap()
        .iter()
        .zip(pressure["results"].as_array().unwrap())
    {
        assert!((left["score"].as_f64().unwrap() - right["score"].as_f64().unwrap()).abs() < 1e-5);
    }
    store
        .invalidate_source(
            copied["source_reference"]["path"]
                .as_str()
                .unwrap()
                .strip_prefix("codex:")
                .unwrap()
                .strip_suffix(".jsonl")
                .unwrap(),
        )
        .unwrap();
    let refreshed = session_request(&store, json!({"query":"needle","limit":10})).unwrap();
    let replacement = refreshed["results"]
        .as_array()
        .unwrap()
        .iter()
        .find(|h| h["event_id"] == "copied-event")
        .unwrap();
    assert_eq!(replacement["copy_count"], 30);
    assert_ne!(replacement["match_id"], copied["match_id"]);
    assert!(session_request(&store, json!({"mode":"copies","match_id":anchor})).is_err());
}

#[test]
fn session_dedup_preserves_missing_ids_conflicting_content_and_repeated_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("distinct.sqlite3")).unwrap();
    for key in ["repeat-a", "repeat-b"] {
        session_source(
            &store,
            key,
            vec![
                message(Some("repeat"), "needle repeated", 1),
                message(Some("repeat"), "needle repeated", 1),
            ],
        );
    }
    for (key, tail) in [
        ("conflict-a", "first ending"),
        ("conflict-b", "different ending"),
    ] {
        session_source(
            &store,
            key,
            vec![
                message(Some("conflict"), "needle shared prefix", 1),
                message(Some("conflict"), tail, 1),
            ],
        );
    }
    for index in 0..2 {
        session_source(
            &store,
            &format!("missing-{index}"),
            vec![message(None, "needle repeated", 1)],
        );
        session_source(
            &store,
            &format!("distinct-{index}"),
            vec![message(
                Some(&format!("distinct-{index}")),
                "needle repeated",
                1,
            )],
        );
        let mut chunk = message(Some("different-role"), "needle repeated", 1);
        chunk.role = Some(if index == 0 { "user" } else { "assistant" }.into());
        session_source(&store, &format!("role-{index}"), vec![chunk]);
    }
    let raw = raw_session(&store, 10);
    assert_eq!(raw.len(), 10);
    let raw_repeat: Vec<_> = raw
        .iter()
        .filter(|hit| hit.chunk.event_id.as_deref() == Some("repeat"))
        .collect();
    assert_eq!(raw_repeat.len(), 2);
    assert!(raw_repeat.iter().all(|hit| hit.copy_count == 2));
    assert_eq!(raw_repeat[0].source.path, raw_repeat[1].source.path);
    let r = session_request(&store, json!({"query":"needle","limit":50})).unwrap();
    let hits = r["results"].as_array().unwrap();
    assert_eq!(hits.len(), 2);
    let repeated: Vec<_> = hits.iter().filter(|h| h["event_id"] == "repeat").collect();
    assert_eq!(
        repeated.len(),
        0,
        "ranked search collapses identical repeated content"
    );
    assert!(repeated.is_empty());
    assert!(
        hits.iter()
            .filter(|h| h["event_id"] != "repeat")
            .all(|h| h["copy_count"] == 1)
    );
}

#[test]
fn copies_pagination_rejects_stale_or_mismatched_cursors() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("cursor.sqlite3")).unwrap();
    for index in 0..3 {
        session_source(
            &store,
            &format!("copy-{index}"),
            vec![message(Some("event"), "needle", 1)],
        );
    }
    let result = session_request(&store, json!({"query":"needle"})).unwrap();
    let id = result["results"][0]["match_id"].clone();
    let page = session_request(&store, json!({"mode":"copies","match_id":id,"limit":1})).unwrap();
    assert!(page.get("cursor").is_some());
    assert!(
        session_request(
            &store,
            json!({"mode":"context","match_id":id,"cursor":page["cursor"]})
        )
        .is_err()
    );
    session_source(
        &store,
        "new-copy",
        vec![message(Some("event"), "needle", 1)],
    );
    assert!(
        session_request(
            &store,
            json!({"mode":"copies","match_id":id,"cursor":page["cursor"]})
        )
        .is_err()
    );
}

#[test]
fn session_copy_groups_respect_provider_session_time_and_owner_filters() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("filters.sqlite3")).unwrap();
    for agent in ["codex", "claude", "copilot"] {
        for index in 0..2 {
            let mut chunk = message(Some("shared-id"), "needle", 1);
            chunk.agent = Some(agent.into());
            session_source(&store, &format!("{agent}-{index}"), vec![chunk]);
        }
    }
    let mut later = message(Some("shared-id"), "needle", 1);
    later.timestamp = Some("2026-09-02T00:00:00.000Z".into());
    session_source(&store, "later", vec![later]);
    let mut other_session = message(Some("shared-id"), "needle", 1);
    other_session.session_id = Some("other-session".into());
    session_source(&store, "other-session", vec![other_session]);
    let mut missing_session = message(Some("shared-id"), "needle", 1);
    missing_session.session_id = None;
    session_source(&store, "missing-session-a", vec![missing_session.clone()]);
    session_source(&store, "missing-session-b", vec![missing_session]);
    store
        .replace_source(
            &Source {
                key: "foreign".into(),
                collection: "foreign-owner".into(),
                path: "codex:foreign.jsonl".into(),
                version: "v1".into(),
                kind: "session".into(),
            },
            [Ok(message(Some("shared-id"), "needle", 1))],
        )
        .unwrap();
    let raw = raw_session(&store, 50);
    assert_eq!(raw.len(), 6);
    let all = session_request(&store, json!({"query":"needle","limit":50})).unwrap();
    assert_eq!(all["results"].as_array().unwrap().len(), 1);
    let filtered = session_request(&store, json!({"query":"needle","agent":"codex","session_id":"session","before":"2026-09-02T00:00:00Z"})).unwrap();
    assert_eq!(filtered["results"].as_array().unwrap().len(), 1);
    assert_eq!(filtered["results"][0]["copy_count"], 2);
    let copies = session_request(
        &store,
        json!({"mode":"copies","match_id":filtered["results"][0]["match_id"]}),
    )
    .unwrap();
    assert_eq!(copies["copies"].as_array().unwrap().len(), 3);
    assert!(
        copies["copies"]
            .as_array()
            .unwrap()
            .iter()
            .any(|copy| copy["timestamp"] == "2026-09-02T00:00:00.000Z")
    );
    assert!(
        copies["copies"]
            .as_array()
            .unwrap()
            .iter()
            .all(|copy| copy["source_reference"]["path"] != "codex:foreign.jsonl")
    );
    let after = session_request(
        &store,
        json!({"query":"needle","after":"2026-09-02T00:00:00Z"}),
    )
    .unwrap();
    assert_eq!(after["results"].as_array().unwrap().len(), 1);
    assert_eq!(after["results"][0]["copy_count"], 1);
}

#[test]
fn message_copy_grouping_keeps_tool_and_legacy_chunks_once_per_occurrence() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("mixed.sqlite3")).unwrap();
    for index in 0..2 {
        let mut argument = message(Some("mixed"), "needle argument", 1);
        argument.field_kind = Some("tool_argument".into());
        argument.tool = Some("bash".into());
        argument.role = Some("assistant".into());
        session_source(
            &store,
            &format!("mixed-{index}"),
            vec![message(Some("mixed"), "needle message", 1), argument],
        );
        let mut legacy = message(Some("legacy"), "needle legacy", 1);
        legacy.field_kind = None;
        session_source(&store, &format!("legacy-{index}"), vec![legacy]);
    }
    let raw = raw_session(&store, 50);
    assert_eq!(raw.len(), 5);
    assert_eq!(
        raw.iter()
            .filter(|hit| hit.chunk.field_kind.as_deref() == Some("tool_argument"))
            .count(),
        2
    );
    assert_eq!(
        raw.iter()
            .filter(|hit| hit.chunk.event_id.as_deref() == Some("legacy"))
            .count(),
        2
    );
    let result = session_request(&store, json!({"query":"needle","limit":50})).unwrap();
    let hits = result["results"].as_array().unwrap();
    assert_eq!(hits.len(), 3);
    assert_eq!(
        hits.iter()
            .filter(|h| h["event_kind"] == "message" && h["event_id"] == "mixed")
            .count(),
        1
    );
    assert_eq!(
        hits.iter()
            .filter(|h| h["event_kind"] == "tool_argument")
            .count(),
        1
    );
    assert_eq!(hits.iter().filter(|h| h["event_id"] == "legacy").count(), 1);
}

#[test]
fn same_file_replayed_message_contexts_remain_separate() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("same-file.sqlite3")).unwrap();
    session_source(
        &store,
        "replays",
        vec![
            message(Some("before-first"), "first before", 1),
            message(Some("replayed"), "needle", 2),
            message(Some("after-first"), "first after", 3),
            message(Some("before-second"), "second before", 4),
            message(Some("replayed"), "needle", 5),
            message(Some("after-second"), "second after", 6),
        ],
    );
    let result = session_request(&store, json!({"query":"needle"})).unwrap();
    assert_eq!(result["results"].as_array().unwrap().len(), 1);
    assert_eq!(result["results"][0]["copy_count"], 2);
    let copies = session_request(
        &store,
        json!({"mode":"copies","match_id":result["results"][0]["match_id"]}),
    )
    .unwrap();
    assert_eq!(copies["copies"].as_array().unwrap().len(), 2);
    for copy in copies["copies"].as_array().unwrap() {
        let line = copy["source_reference"]["start_line"].as_u64().unwrap();
        let context = session_request(&store, json!({"mode":"context","match_id":copy["match_id"],"before_events":1,"after_events":1})).unwrap();
        let actual: Vec<_> = context["context"]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| h["source_reference"]["start_line"].as_u64().unwrap())
            .collect();
        assert_eq!(actual, vec![line - 1, line, line + 1]);
    }
}

#[test]
fn adjacent_same_id_message_contexts_remain_separate() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("adjacent.sqlite3")).unwrap();
    session_source(
        &store,
        "adjacent",
        vec![
            message(Some("adjacent"), "needle", 1),
            message(Some("adjacent"), "needle", 2),
        ],
    );
    let result = session_request(&store, json!({"query":"needle"})).unwrap();
    assert_eq!(result["results"].as_array().unwrap().len(), 1);
    assert_eq!(result["results"][0]["copy_count"], 2);
    let copies = session_request(
        &store,
        json!({"mode":"copies","match_id":result["results"][0]["match_id"]}),
    )
    .unwrap();
    assert_eq!(copies["copies"].as_array().unwrap().len(), 2);
    for copy in copies["copies"].as_array().unwrap() {
        let line = copy["source_reference"]["start_line"].as_u64().unwrap();
        let context = session_request(
            &store,
            json!({
                "mode":"context",
                "match_id":copy["match_id"],
                "before_events":0,
                "after_events":0
            }),
        )
        .unwrap();
        assert_eq!(context["context"].as_array().unwrap().len(), 1);
        assert_eq!(
            context["context"][0]["source_reference"]["start_line"],
            line
        );
    }
}
