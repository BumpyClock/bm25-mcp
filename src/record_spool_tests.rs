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

#[test]
fn byte_ranges_replay_across_spill_reset_and_owner_drop() -> Result<()> {
    for length in [
        0,
        INLINE_BYTES - 1,
        INLINE_BYTES,
        INLINE_BYTES + 1,
        INLINE_BYTES * 3,
    ] {
        let progress = ProgressReporter::new();
        let mut spool = ByteSpool::observed(&progress);
        let expected: Vec<u8> = (0..length).map(|i| (i % 251) as u8).collect();
        spool.write_all(&expected)?;
        let snapshot = spool.range(0, length as u64)?;
        let path = spool.file.as_ref().map(|file| file.0.clone());
        assert_eq!(path.is_some(), length > INLINE_BYTES);
        spool.reset();
        spool.write_all(b"next record")?;
        let next = spool.finish()?;
        let mut actual = Vec::new();
        snapshot.reader()?.read_to_end(&mut actual)?;
        assert_eq!(actual, expected);
        let work = progress.snapshot().work;
        assert_eq!(
            work.spool_write_bytes,
            if path.is_some() { length as u64 } else { 0 }
        );
        assert_eq!(work.spool_read_bytes, work.spool_write_bytes);
        assert!(snapshot.range(1, 0).is_err());
        assert!(snapshot.range(0, length as u64 + 1).is_err());
        let mut reader = snapshot.reader()?;
        drop(snapshot);
        if let Some(path) = &path {
            assert!(path.exists());
        }
        actual.clear();
        reader.read_to_end(&mut actual)?;
        assert_eq!(actual, expected);
        drop(reader);
        if let Some(path) = &path {
            assert!(!path.exists());
        }
        actual.clear();
        next.reader()?.read_to_end(&mut actual)?;
        assert_eq!(actual, b"next record");
    }
    Ok(())
}

#[test]
fn spilled_byte_flush_failure_is_a_finalization_error() -> Result<()> {
    let mut spool = ByteSpool::default();
    spool.write_all(&vec![0; INLINE_BYTES + 1])?;
    let path = spool.file.as_ref().unwrap().0.clone();
    spool.writer.as_mut().unwrap().get_mut().file = File::open(&path)?;
    spool.write_all(b"buffered")?;
    assert!(
        spool
            .finish()
            .unwrap_err()
            .downcast_ref::<FinalizeError>()
            .is_some()
    );
    assert!(!path.exists());
    Ok(())
}

#[test]
fn spilled_byte_reader_open_failure_is_a_finalization_error() -> Result<()> {
    let mut spool = ByteSpool::default();
    spool.write_all(&vec![0; INLINE_BYTES + 1])?;
    let path = spool.file.as_ref().unwrap().0.clone();
    let bytes = spool.finish()?;
    fs::remove_file(path)?;
    let error = bytes.reader().err().expect("missing spool must fail");
    assert!(error.downcast_ref::<FinalizeError>().is_some());
    Ok(())
}
