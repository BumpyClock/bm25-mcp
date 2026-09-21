use bm25_mcp::{
    model::{Chunk, SearchFilter, Source},
    store::Store,
};

pub struct Case {
    pub query: &'static str,
    pub kind: &'static str,
    pub must: &'static [&'static str],
    pub useful: &'static [&'static str],
}

pub fn fixtures(store: &Store) -> anyhow::Result<Vec<Case>> {
    let docs = [
        (
            "definition",
            "project",
            "src/memory/manager-search.ts",
            "export class HybridPersistenceCoordinator {\n  subscriptionActiveStateGate() { return restoreSubscriptions(); }\n}\n// Restores subscriptions after cloud sync.",
            "",
            "",
        ),
        (
            "test",
            "project",
            "tests/persistence.ts",
            "test('persistence coordinator restores subscriptions after cloud sync', () => {\n const coordinator = new HybridPersistenceCoordinator();\n});",
            "",
            "",
        ),
        (
            "caller",
            "project",
            "src/sync.ts",
            "function synchronizeCloud() {\n return new HybridPersistenceCoordinator().subscriptionActiveStateGate();\n}",
            "",
            "",
        ),
        (
            "path",
            "project",
            "src/empty-manager.ts",
            "export const unrelated = 42;",
            "",
            "",
        ),
        (
            "qualified",
            "project",
            "src/cache.cpp",
            "void Foo::Bar::bazValue() { refreshCache(); }",
            "",
            "",
        ),
        (
            "expansion",
            "project",
            "src/cleanup.rs",
            "fn delete_stale_entries() { database.remove_expired(); }",
            "",
            "",
        ),
        (
            "literal",
            "project",
            "src/fix.rs",
            "fn fixCache() { repair_cache(); }",
            "",
            "",
        ),
        (
            "alias",
            "project",
            "src/crash.rs",
            "fn crashRecovery() { restart(); }",
            "",
            "",
        ),
        (
            "diagnostic",
            "session",
            "history/old.jsonl",
            "Zone Not Found: stale CloudKit zone identity. Recreate the zone before replaying subscriptions.",
            "2024-01-01T00:00:00Z",
            "assistant",
        ),
        (
            "reasoning",
            "session",
            "history/fix.jsonl",
            "Subscriptions disappear after cloud sync because a stale zone overwrites active state. Fixed by preserving the subscription active state gate during reconciliation.",
            "2026-09-01T00:00:00Z",
            "assistant",
        ),
        (
            "decision",
            "session",
            "history/design.jsonl",
            "We chose SQLite transactions for checkpoint ownership because interrupted imports must never expose partial sessions. Architectural decision: atomic checkpoint commits.",
            "2025-12-01T00:00:00Z",
            "assistant",
        ),
        (
            "old-symbol",
            "session",
            "history/symbol.jsonl",
            "HybridPersistenceCoordinator resolved the old checkpoint corruption.",
            "2023-01-01T00:00:00Z",
            "assistant",
        ),
        (
            "new-noise",
            "session",
            "history/noise.jsonl",
            "Fresh status: checkpoint pending. Today we discussed routine formatting.",
            "2026-09-21T00:00:00Z",
            "user",
        ),
    ];
    for (id, kind, path, text, time, role) in docs {
        add(store, id, kind, path, text, time, role)?;
    }
    for i in 0..18 {
        add(
            store,
            &format!("repeat-{i}"),
            "project",
            &format!("generated/repeat-{i}.txt"),
            &"HybridPersistenceCoordinator persistence coordinator manager-search.ts ".repeat(20),
            "",
            "",
        )?;
        add(
            store,
            &format!("tool-{i}"),
            "session",
            &format!("history/tool-{i}.jsonl"),
            &"subscriptions cloud sync active state checkpoint ".repeat(200),
            "2026-09-21T00:00:00Z",
            "tool",
        )?;
    }
    Ok(vec![
        Case {
            query: "HybridPersistenceCoordinator",
            kind: "project",
            must: &["definition"],
            useful: &["test", "caller"],
        },
        Case {
            query: "persistence coordinator",
            kind: "project",
            must: &["definition"],
            useful: &["test", "caller"],
        },
        Case {
            query: "manager-search.ts",
            kind: "project",
            must: &["definition"],
            useful: &[],
        },
        Case {
            query: "src/empty-manager.ts",
            kind: "project",
            must: &["path"],
            useful: &[],
        },
        Case {
            query: "Foo::Bar::bazValue",
            kind: "project",
            must: &["qualified"],
            useful: &[],
        },
        Case {
            query: "\"Zone Not Found\"",
            kind: "session",
            must: &["diagnostic"],
            useful: &[],
        },
        Case {
            query: "why did subscriptions disappear after cloud sync?",
            kind: "session",
            must: &["reasoning"],
            useful: &["diagnostic"],
        },
        Case {
            query: "why did subscriptions disappear after cloud sync?",
            kind: "project",
            must: &["definition"],
            useful: &["test", "caller"],
        },
        Case {
            query: "architectural decision atomic checkpoint",
            kind: "session",
            must: &["decision"],
            useful: &[],
        },
        Case {
            query: "HybridPersistenceCoordinator",
            kind: "session",
            must: &["old-symbol"],
            useful: &[],
        },
        Case {
            query: "subscription active state",
            kind: "project",
            must: &["definition"],
            useful: &["test"],
        },
        Case {
            query: "panic",
            kind: "project",
            must: &["alias"],
            useful: &[],
        },
        Case {
            query: "fixCache",
            kind: "project",
            must: &["literal"],
            useful: &[],
        },
    ])
}

fn add(
    store: &Store,
    id: &str,
    kind: &str,
    path: &str,
    text: &str,
    time: &str,
    role: &str,
) -> anyhow::Result<()> {
    store.replace_source(
        &Source {
            key: id.into(),
            collection: "fixture".into(),
            path: path.into(),
            version: "v1".into(),
            kind: kind.into(),
        },
        [Ok(Chunk {
            text: text.into(),
            start_line: 1,
            end_line: text.lines().count() as u64,
            start_byte: 0,
            end_byte: text.len() as u64,
            timestamp: (!time.is_empty()).then(|| time.into()),
            role: (!role.is_empty()).then(|| role.into()),
            agent: (kind == "session").then(|| "codex".into()),
            session_id: (kind == "session").then(|| id.into()),
            event_id: (kind == "session").then(|| id.into()),
            field_kind: (kind == "session").then(|| {
                if role == "tool" {
                    "tool_result"
                } else {
                    "message"
                }
                .into()
            }),
            ..Default::default()
        })],
    )
}

/// A small replacement used by the harness to measure the incremental write
/// path without changing the query cases or exporting fixture contents.
pub fn replace_probe(store: &Store) -> anyhow::Result<()> {
    add(
        store,
        "update-probe",
        "project",
        "src/update-probe.rs",
        "fn update_probe() { refreshed_index(); }",
        "",
        "",
    )
}

pub fn filter(kind: &str) -> SearchFilter {
    SearchFilter {
        collection: "fixture".into(),
        kind: kind.into(),
        ..Default::default()
    }
}
