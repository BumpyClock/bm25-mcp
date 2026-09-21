use bm25_turbo::{BM25Builder, Tokenizer, persistence, wal::WriteAheadLog};
use serde_json::{Value, json};

fn result(value: bm25_turbo::types::Results) -> Value {
    json!({"ids": value.doc_ids, "scores": value.scores})
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempfile::tempdir()?;
    let base = ["alpha beta", "beta", "alpha alpha gamma"];
    let mut index = BM25Builder::new().build_from_corpus(&base)?;
    let path = tmp.path().join("base.bm25");
    persistence::save(&index, &path)?;
    let loaded = persistence::load(&path)?;
    let before = result(index.search("alpha", 10)?);
    let after = result(loaded.search("alpha", 10)?);
    println!("{}", json!({"check":"default_snapshot_roundtrip", "passed": before == after, "before":before,"after":after}));

    let mut wal = WriteAheadLog::new();
    wal.initialize(&index)?;
    wal.append_documents(&["alpha delta delta delta delta delta delta delta"])?;
    wal.delete_documents(&[1])?;
    let fresh = BM25Builder::new().build_from_corpus(&[base[0],base[2],"alpha delta delta delta delta delta delta delta"])?;
    let overlay = result(wal.search_exact(&index, "alpha", 10)?);
    wal.compact(&mut index)?;
    let compacted = result(index.search("alpha",10)?);
    let rebuilt = result(fresh.search("alpha",10)?);
    println!("{}",json!({"check":"compaction_fresh_build_equivalence", "passed":compacted==rebuilt, "compacted":compacted,"fresh":rebuilt,"exact_overlay_before_compaction":overlay}));

    let custom = Tokenizer::builder().lowercase(false).build()?;
    let custom_index = BM25Builder::new().tokenizer(custom).build_from_corpus(&["CamelCase", "other"])?;
    let custom_path = tmp.path().join("custom.bm25");
    persistence::save(&custom_index,&custom_path)?;
    let restored = persistence::load(&custom_path)?;
    let before = result(custom_index.search("CamelCase",10)?);
    let after = result(restored.search("CamelCase",10)?);
    println!("{}",json!({"check":"custom_tokenizer_roundtrip","passed":before==after,"before":before,"after":after}));
    let mut custom_wal = WriteAheadLog::new();
    custom_wal.initialize(&custom_index)?;
    custom_wal.append_documents(&["CamelCase"])?;
    let overlay = custom_wal.search(&custom_index,"CamelCase",10)?;
    let found = overlay.doc_ids.contains(&2) && overlay.doc_ids.contains(&0);
    println!("{}",json!({"check":"custom_tokenizer_wal_consistency","passed":found,"results":result(overlay)}));

    let mut disk_index = BM25Builder::new().build_from_corpus(&base)?;
    let disk_base = tmp.path().join("disk.bm25");
    let disk_wal = tmp.path().join("disk.wal");
    persistence::save(&disk_index,&disk_base)?;
    let mut log = WriteAheadLog::with_path(disk_wal.clone())?;
    log.initialize(&disk_index)?;
    log.append_documents(&["uniquenewterm"])?;
    drop(log);
    let mut replay = WriteAheadLog::with_path(disk_wal.clone())?;
    replay.initialize(&disk_index)?;
    println!("{}",json!({"check":"wal_append_restart","passed":replay.search(&disk_index,"uniquenewterm",10)?.doc_ids.contains(&3)}));
    replay.compact(&mut disk_index)?;
    drop(replay);
    let old_base = persistence::load(&disk_base)?;
    let mut recovery = WriteAheadLog::with_path(disk_wal)?;
    recovery.initialize(&old_base)?;
    println!("{}",json!({"check":"recovery_after_compaction_before_base_save","passed":recovery.search(&old_base,"uniquenewterm",10)?.doc_ids.contains(&3),"note":"Simulated process stop between compaction and saving the new base; not a power-loss test."}));

    let partial = tmp.path().join("partial.wal");
    let mut log = WriteAheadLog::with_path(partial.clone())?;
    log.initialize(&old_base)?;
    log.append_documents(&["durablefirst"])?;
    log.append_documents(&["partialsecond"])?;
    drop(log);
    let file = std::fs::OpenOptions::new().write(true).open(&partial)?;
    file.set_len(file.metadata()?.len()-3)?;
    let recovery = WriteAheadLog::with_path(partial);
    println!("{}",json!({"check":"truncated_final_wal_record_recovery","passed":recovery.is_ok(),"error":recovery.err().map(|e|e.to_string())}));

    let shared = std::sync::Arc::new(fresh);
    let expected = result(shared.search("alpha",10)?);
    let handles: Vec<_> = (0..4).map(|_| {
        let shared = shared.clone();
        std::thread::spawn(move || (0..100).map(|_| result(shared.search("alpha",10).unwrap())).collect::<Vec<_>>())
    }).collect();
    let passed = handles.into_iter().all(|h| h.join().unwrap().iter().all(|r|r==&expected));
    println!("{}",json!({"check":"concurrent_immutable_queries","passed":passed,"threads":4,"queries_per_thread":100}));
    let tokens = vec![vec!["CamelCase".to_owned()],vec!["other".to_owned()]];
    let token_index = BM25Builder::new().build_from_tokens(&tokens)?;
    let token_path = tmp.path().join("tokens.bm25");
    persistence::save(&token_index,&token_path)?;
    let token_loaded = persistence::load(&token_path)?;
    let query = vec!["CamelCase".to_owned()];
    let before = result(token_index.search_tokens(&query,10)?);
    let after = result(token_loaded.search_tokens(&query,10)?);
    println!("{}",json!({"check":"external_tokenization_snapshot_roundtrip","passed":before==after && before["ids"]==json!([0]),"before":before,"after":after}));
    Ok(())
}
