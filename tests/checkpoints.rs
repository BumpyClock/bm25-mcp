use anyhow::{Result, anyhow};
use bm25_mcp::{
    model::{Chunk, SearchFilter, Source},
    store::{SessionCheckpoint, Store},
};

#[test]
fn session_chunks_and_offsets_commit_and_roll_back_together() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("index.sqlite3");
    let store = Store::open(&path)?;
    let mut source = Source {
        key: "session".into(),
        collection: "repo".into(),
        path: "events.jsonl".into(),
        version: "first".into(),
        kind: "session".into(),
    };
    let first = SessionCheckpoint {
        offset: 100,
        state: "parser-one".into(),
    };
    store.replace_session(
        &source,
        [Ok(Chunk {
            text: "firstmarker".into(),
            event_id: Some("first".into()),
            ..Chunk::default()
        })],
        &first,
    )?;
    let filter = SearchFilter {
        collection: "repo".into(),
        kind: "session".into(),
        ..SearchFilter::default()
    };
    let id = store.search("firstmarker", &filter, 1)?.1[0]
        .match_id
        .clone();
    source.version = "second".into();
    let next = SessionCheckpoint {
        offset: 200,
        state: "parser-two".into(),
    };
    assert!(
        store
            .append_session(
                &source,
                "first",
                [
                    Ok(Chunk {
                        text: "rolledback".into(),
                        ..Chunk::default()
                    }),
                    Err(anyhow!("interrupted"))
                ],
                &next
            )
            .is_err()
    );
    assert_eq!(store.session_checkpoint("session")?, Some(first));
    assert!(store.search("rolledback", &filter, 1)?.1.is_empty());
    store.append_session(
        &source,
        "first",
        [Ok(Chunk {
            text: "secondmarker".into(),
            event_id: Some("second".into()),
            ..Chunk::default()
        })],
        &next,
    )?;
    assert_eq!(store.context(&id, "repo", 0, 1)?.len(), 2);
    assert!(
        store
            .append_session(&source, "first", std::iter::empty(), &next)
            .is_err()
    );
    drop(store);
    let store = Store::open(&path)?;
    assert_eq!(store.session_checkpoint("session")?, Some(next));
    assert_eq!(store.search("secondmarker", &filter, 1)?.1.len(), 1);
    store.remove_source("session")?;
    assert_eq!(store.session_checkpoint("session")?, None);
    Ok(())
}

#[test]
fn crash_during_transaction_preserves_last_committed_offset() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("crash.sqlite3");
    let store = Store::open(&path)?;
    let source = Source {
        key: "crash".into(),
        collection: "repo".into(),
        path: "events.jsonl".into(),
        version: "old".into(),
        kind: "session".into(),
    };
    let checkpoint = SessionCheckpoint {
        offset: 42,
        state: "committed".into(),
    };
    store.replace_session(
        &source,
        [Ok(Chunk {
            text: "durablemarker".into(),
            ..Chunk::default()
        })],
        &checkpoint,
    )?;
    let generation = store.generation()?;
    drop(store);
    let status = std::process::Command::new(std::env::current_exe()?)
        .args(["--exact", "crash_writer_helper", "--nocapture"])
        .env("BM25_TEST_CRASH_DATABASE", &path)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()?;
    assert!(!status.success());
    let store = Store::open(&path)?;
    assert_eq!(store.generation()?, generation);
    assert_eq!(store.session_checkpoint("crash")?, Some(checkpoint));
    let filter = SearchFilter {
        collection: "repo".into(),
        kind: "session".into(),
        ..SearchFilter::default()
    };
    assert_eq!(store.search("durablemarker", &filter, 1)?.1.len(), 1);
    assert!(store.search("uncommittedmarker", &filter, 1)?.1.is_empty());
    Ok(())
}

#[test]
fn crash_writer_helper() -> Result<()> {
    let Some(path) = std::env::var_os("BM25_TEST_CRASH_DATABASE") else {
        return Ok(());
    };
    let store = Store::open(std::path::Path::new(&path))?;
    let source = Source {
        key: "crash".into(),
        collection: "repo".into(),
        path: "events.jsonl".into(),
        version: "new".into(),
        kind: "session".into(),
    };
    let mut n = 0;
    let chunks = std::iter::from_fn(|| {
        n += 1;
        if n == 2 {
            std::process::exit(73);
        }
        Some(Ok(Chunk {
            text: "uncommittedmarker".into(),
            ..Chunk::default()
        }))
    });
    store.append_session(
        &source,
        "old",
        chunks,
        &SessionCheckpoint {
            offset: 100,
            state: "uncommitted".into(),
        },
    )?;
    panic!("crash helper did not exit during transaction")
}

#[test]
fn tokenizer_upgrade_discards_only_derived_index_state() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("upgrade.sqlite3");
    let store = Store::open(&path)?;
    let source = Source {
        key: "old".into(),
        collection: "repo".into(),
        path: "untouched.txt".into(),
        version: "old".into(),
        kind: "project".into(),
    };
    store.replace_source(
        &source,
        [Ok(Chunk {
            text: "oldtoken".into(),
            ..Chunk::default()
        })],
    )?;
    drop(store);
    let db = rusqlite::Connection::open(&path)?;
    db.execute(
        "UPDATE meta SET value='previous-tokenizer' WHERE key='tokenizer_version'",
        [],
    )?;
    drop(db);
    let store = Store::open(&path)?;
    assert!(store.sources("repo", "project")?.is_empty());
    store.replace_source(
        &source,
        [Ok(Chunk {
            text: "newtoken".into(),
            ..Chunk::default()
        })],
    )?;
    assert_eq!(store.sources("repo", "project")?.len(), 1);
    Ok(())
}

#[test]
fn disk_parser_state_is_atomic_with_chunks_and_checkpoint() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::open(&dir.path().join("state.sqlite3"))?;
    let source = Source {
        key: "state".into(),
        collection: "repo".into(),
        path: "session.jsonl".into(),
        version: "one".into(),
        kind: "session".into(),
    };
    let checkpoint = SessionCheckpoint {
        offset: 1,
        state: "metadata".into(),
    };
    store.replace_session_with_state(
        &source,
        std::iter::empty(),
        &checkpoint,
        [Ok((
            "own_call".into(),
            "call-id".into(),
            Some("tool-name".into()),
        ))],
    )?;
    let reader = store.session_state_reader()?;
    assert_eq!(
        reader.get("state", "own_call", "call-id")?,
        Some("tool-name".into())
    );
    let next = Source {
        version: "two".into(),
        ..source.clone()
    };
    let next_checkpoint = SessionCheckpoint {
        offset: 2,
        state: "new".into(),
    };
    let updates = [
        Ok(("own_call".into(), "call-id".into(), None)),
        Err(anyhow!("state spool interrupted")),
    ];
    assert!(
        store
            .append_session_with_state(&next, "one", std::iter::empty(), &next_checkpoint, updates)
            .is_err()
    );
    assert_eq!(
        reader.get("state", "own_call", "call-id")?,
        Some("tool-name".into())
    );
    assert_eq!(store.session_checkpoint("state")?, Some(checkpoint));
    store.replace_session_with_state(
        &next,
        std::iter::empty(),
        &next_checkpoint,
        std::iter::empty(),
    )?;
    assert_eq!(reader.get("state", "own_call", "call-id")?, None);
    Ok(())
}
