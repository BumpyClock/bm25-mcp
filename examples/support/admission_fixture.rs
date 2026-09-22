use anyhow::Result;
use bm25_mcp::{
    model::{Chunk, SearchFilter, Source},
    store::Store,
};

pub const QUERY: &str = "why does indexing fail";
pub const RETAINED_QUERY: &str = "indexing fail";
pub const NOW: &str = "2026-09-21T00:00:00Z";
pub const NOISE_COUNT: usize = 201;
pub const BACKGROUND_COUNT: usize = 1_000;

pub fn target_text() -> String {
    format!("indexing fail {}", "padding ".repeat(2_000))
}

pub fn populate(store: &Store, kind: &str) -> Result<SearchFilter> {
    for index in 0..NOISE_COUNT + BACKGROUND_COUNT + 1 {
        let (key, text) = if index < NOISE_COUNT {
            (format!("noise-{index}"), "why does this happen".into())
        } else if index < NOISE_COUNT + BACKGROUND_COUNT {
            (format!("background-{index}"), "neutral ".repeat(20))
        } else {
            ("target".into(), target_text())
        };
        store.replace_source(
            &Source {
                key: key.clone(),
                collection: kind.into(),
                path: format!("records/{index}.txt"),
                version: "v1".into(),
                kind: kind.into(),
            },
            [Ok(Chunk {
                text,
                agent: (kind == "session").then(|| "codex".into()),
                session_id: (kind == "session").then(|| "admission".into()),
                event_id: (kind == "session").then_some(key),
                field_kind: (kind == "session").then(|| "message".into()),
                role: (kind == "session").then(|| "assistant".into()),
                timestamp: Some(NOW.into()),
                ..Default::default()
            })],
        )?;
    }
    Ok(SearchFilter {
        collection: kind.into(),
        kind: kind.into(),
        ..Default::default()
    })
}
