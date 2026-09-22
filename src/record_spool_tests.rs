use super::*;
use crate::{
    model::{Chunk, SearchFilter, Source},
    store::{SessionCheckpoint, Store},
};
use std::{sync::Barrier, thread};
use testing::{Failure, fail_next_finish};

#[test]
fn borrowed_records_round_trip_with_independent_replay() -> Result<()> {
    let mut spool = RecordSpool::new()?;
    let path = spool.file.0.clone();
    for record in [("first\n雪", Some("value")), ("second", None)] {
        spool.push(&record)?;
    }
    let mut records = spool.finish::<(String, Option<String>)>()?;
    let replay = records.reopen()?;
    assert_eq!(
        records.next().transpose()?,
        Some(("first\n雪".into(), Some("value".into())))
    );
    drop(records);
    assert!(path.exists());
    assert_eq!(
        replay.collect::<Result<Vec<_>>>()?,
        vec![
            ("first\n雪".into(), Some("value".into())),
            ("second".into(), None)
        ]
    );
    assert!(!path.exists());
    Ok(())
}

#[test]
fn empty_and_exhausted_readers_can_replay_until_dropped() -> Result<()> {
    let mut records = RecordSpool::new()?.finish::<Chunk>()?;
    let path = records.file.0.clone();
    assert!(records.next().is_none());
    assert!(records.next().is_none());
    assert!(records.reopen()?.next().is_none());
    assert!(path.exists());
    drop(records);
    assert!(!path.exists());
    Ok(())
}

#[test]
fn abandoned_writer_and_partial_reader_remove_their_files() -> Result<()> {
    let mut spool = RecordSpool::new()?;
    let path = spool.file.0.clone();
    spool.push(&"unpublished")?;
    drop(spool);
    assert!(!path.exists());

    let mut spool = RecordSpool::new()?;
    let path = spool.file.0.clone();
    spool.push(&1)?;
    spool.push(&2)?;
    let mut records = spool.finish::<u64>()?;
    assert_eq!(records.next().transpose()?, Some(1));
    drop(records);
    assert!(!path.exists());
    Ok(())
}

#[test]
fn simultaneous_last_readers_remove_the_file() -> Result<()> {
    let mut spool = RecordSpool::new()?;
    spool.push(&"record")?;
    let records = spool.finish::<String>()?;
    let path = records.file.0.clone();
    let replay = records.reopen()?;
    let barrier = Arc::new(Barrier::new(2));
    let workers: Vec<_> = [records, replay]
        .into_iter()
        .map(|reader| {
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                drop(reader);
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    assert!(!path.exists());
    Ok(())
}

#[test]
fn flush_and_open_failures_are_reported_by_finish_and_clean_up() -> Result<()> {
    for failure in [Failure::Flush, Failure::Open] {
        let mut spool = RecordSpool::new()?;
        let path = spool.file.0.clone();
        spool.push(&"buffered record")?;
        let _fault = fail_next_finish::<String>(failure);
        let error = spool.finish::<String>().unwrap_err();
        assert!(std::error::Error::source(&error).is_some());
        assert!(!path.exists(), "{failure:?} left a temporary file");
    }
    Ok(())
}

#[test]
fn malformed_record_stops_iteration_and_cleans_up() -> Result<()> {
    let spool = RecordSpool::new()?;
    let path = spool.file.0.clone();
    fs::write(&path, b"1\nnot-json\n2\n")?;
    let mut records = spool.finish::<u64>()?;
    assert_eq!(records.next().transpose()?, Some(1));
    assert!(records.next().unwrap().is_err());
    assert!(records.next().is_none());
    drop(records);
    assert!(!path.exists());
    Ok(())
}

#[cfg(unix)]
#[test]
fn temporary_records_are_private() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let spool = RecordSpool::new()?;
    assert_eq!(fs::metadata(&spool.file.0)?.permissions().mode() & 0o077, 0);
    Ok(())
}

#[test]
fn malformed_spooled_updates_roll_back_chunks_state_and_checkpoint() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::open(&dir.path().join("index.sqlite3"))?;
    let mut source = Source {
        key: "session".into(),
        collection: "collection".into(),
        kind: "session".into(),
        path: "session.jsonl".into(),
        version: "original".into(),
    };
    let checkpoint = SessionCheckpoint {
        offset: 1,
        state: "original".into(),
    };
    store.replace_session_with_state(
        &source,
        [Ok(Chunk {
            text: "originalmarker".into(),
            ..Default::default()
        })],
        &checkpoint,
        [Ok(("kind".into(), "key".into(), Some("original".into())))],
    )?;
    let filter = SearchFilter {
        collection: source.collection.clone(),
        kind: source.kind.clone(),
        ..Default::default()
    };
    let original = store.search("originalmarker", &filter, 10)?;
    let mut chunks = RecordSpool::new()?;
    chunks.push(&Chunk {
        text: "rolledbackmarker".into(),
        ..Default::default()
    })?;
    let updates = RecordSpool::new()?;
    let path = updates.file.0.clone();
    fs::write(&path, b"[\"kind\",\"key\",\"replacement\"]\nmalformed\n")?;
    source.version = "replacement".into();
    assert!(
        store
            .append_session_with_state(
                &source,
                "original",
                chunks.finish::<Chunk>()?,
                &SessionCheckpoint {
                    offset: 2,
                    state: "replacement".into()
                },
                updates.finish::<(String, String, Option<String>)>()?,
            )
            .is_err()
    );
    assert_eq!(store.session_checkpoint(&source.key)?, Some(checkpoint));
    assert_eq!(
        store
            .session_state_reader()?
            .get(&source.key, "kind", "key")?,
        Some("original".into())
    );
    let after = store.search("originalmarker", &filter, 10)?;
    assert_eq!(after.0, original.0);
    assert_eq!(after.1[0].match_id, original.1[0].match_id);
    assert!(store.search("rolledbackmarker", &filter, 10)?.1.is_empty());
    assert!(!path.exists());
    Ok(())
}
