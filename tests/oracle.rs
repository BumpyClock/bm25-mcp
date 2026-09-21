use bm25_mcp::{
    model::{Chunk, SearchFilter, Source},
    store::Store,
    text::tokenize,
};
use bm25_turbo::BM25Builder;
use std::collections::HashMap;

#[test]
fn durable_replacements_and_deletions_match_fresh_upstream_builds() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("oracle.sqlite3")).unwrap();
    let mut docs = vec![
        "parseHTTPResponse handles timeout".to_owned(),
        "http server timeout timeout".to_owned(),
        "snake_case argument parser".to_owned(),
    ];
    let filter = SearchFilter {
        collection: "project".into(),
        kind: "project".into(),
        ..Default::default()
    };
    for round in 0..12 {
        for (i, text) in docs.iter().enumerate() {
            store
                .replace_source(
                    &Source {
                        key: i.to_string(),
                        collection: "project".into(),
                        path: i.to_string(),
                        version: format!("r{round}"),
                        kind: "project".into(),
                    },
                    [Ok(Chunk {
                        text: text.clone(),
                        ..Default::default()
                    })],
                )
                .unwrap();
        }
        store.compact().unwrap();
        let tokenized: Vec<_> = docs.iter().map(|s| tokenize(s)).collect();
        let oracle = BM25Builder::new().build_from_tokens(&tokenized).unwrap();
        for query in [
            "timeout",
            "parseHTTPResponse",
            "snake_case",
            "http timeout",
            "timeout timeout",
            "nomatchqvx",
        ] {
            let tokens = tokenize(query);
            let expected = oracle.search_tokens(&tokens, docs.len()).unwrap();
            let expected: HashMap<_, _> = expected
                .doc_ids
                .into_iter()
                .zip(expected.scores)
                .filter(|(_, score)| *score > 0.0)
                .map(|(id, score)| (id.to_string(), score))
                .collect();
            let (_, hot) = store.search(query, &filter, 50).unwrap();
            store.set_memory_pressure(true);
            let (_, actual) = store.search(query, &filter, 50).unwrap();
            store.set_memory_pressure(false);
            assert_eq!(
                hot.iter()
                    .map(|h| (&h.match_id, h.score))
                    .collect::<Vec<_>>(),
                actual
                    .iter()
                    .map(|h| (&h.match_id, h.score))
                    .collect::<Vec<_>>()
            );
            assert_eq!(actual.len(), expected.len(), "round {round}, {query}");
            for hit in actual {
                assert!(
                    (hit.score - expected[&hit.source.key]).abs() < 1e-6,
                    "round {round}, {query}: {} != {}",
                    hit.score,
                    expected[&hit.source.key]
                );
            }
        }
        if round % 3 == 2 {
            let id = docs.len() - 1;
            store.remove_source(&id.to_string()).unwrap();
            docs.pop();
        } else {
            docs.push(format!("newIdentifier{round} http error timeout"));
        }
        docs[0] = format!("parseHTTPResponse timeout round{round}");
    }
}
