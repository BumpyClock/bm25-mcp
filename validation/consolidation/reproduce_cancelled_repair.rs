use bm25_mcp::{coverage::CollectionCoverage, ingest, progress::ProgressReporter, store::Store};
use std::{collections::HashSet, fs};
fn main() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().join("project");
    fs::create_dir(&root)?;
    for name in ["a.txt", "b.txt"] { fs::write(root.join(name), b"\xff\xff\xff")?; }
    let store = Store::open(&dir.path().join("index.sqlite3"))?;
    let collection = ingest::project_identity(&root)?.collection;
    let mut ledger = CollectionCoverage::default();
    ledger.apply(&ingest::scan_project(&root, &store, &collection));
    assert_eq!(ledger.snapshot().error_count, 2);
    let path = root.join("a.txt");
    fs::write(&path, "repairedmarker")?;
    let progress = ProgressReporter::new();
    let result = ingest::scan_project_observed(&root, &store, &collection, Some(&HashSet::from([path])), &|| progress.snapshot().chunks_committed == 0, None, &progress);
    assert!(result.is_err());
    ledger.apply(&result);
    println!("committed_chunks={} source_errors_plus_run_error={} pending={:?}", progress.snapshot().chunks_committed, ledger.snapshot().error_count, ledger.snapshot().pending_changes);
    assert_eq!(progress.snapshot().chunks_committed, 1);
    assert_eq!(ledger.snapshot().error_count, 3);
    Ok(())
}
