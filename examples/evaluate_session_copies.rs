use anyhow::{Context, Result, ensure};
use bm25_mcp::{
    store::Store,
    tools::{Coverage, dispatch},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, io::Write, path::PathBuf};

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    ensure!(
        args.len() == 3,
        "usage: evaluate_session_copies MANIFEST PRIVATE_CACHE OUTPUT_JSONL"
    );
    let manifest: Value = serde_json::from_slice(&std::fs::read(&args[0])?)?;
    let private = PathBuf::from(&args[1]);
    let mut output = std::io::BufWriter::new(std::fs::File::create(&args[2])?);
    let mut families = BTreeMap::<String, Vec<&Value>>::new();
    for project in manifest["projects"]
        .as_array()
        .context("projects missing")?
    {
        families
            .entry(project["family"].as_str().context("family missing")?.into())
            .or_default()
            .push(project);
    }
    for (family, projects) in families {
        if !projects.iter().any(|p| {
            p["queries"]
                .as_array()
                .unwrap()
                .iter()
                .any(|q| q["kind"] == "sessions")
        }) {
            continue;
        }
        let owner = format!("{:x}", Sha256::digest(family.as_bytes()));
        let source = private
            .join("cache-after")
            .join(&owner)
            .join(&owner)
            .join("index.sqlite3");
        let scratch = tempfile::tempdir()?;
        let db = scratch.path().join("evaluation.sqlite3");
        let original = rusqlite::Connection::open_with_flags(
            &source,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        original.execute(
            "VACUUM INTO ?1",
            [db.to_str().context("database path is not UTF-8")?],
        )?;
        drop(original);
        let store = Store::open(&db)?;
        let verification =
            rusqlite::Connection::open_with_flags(&db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        for project in projects {
            let name = project["name"].as_str().unwrap();
            for q in project["queries"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|q| q["kind"] == "sessions")
            {
                let id = q["id"].as_str().unwrap();
                let key = format!(
                    "{:x}.json",
                    Sha256::digest(format!("{name}{id}").as_bytes())
                );
                let baseline: Value = serde_json::from_slice(&std::fs::read(
                    private.join("responses-after").join(key),
                )?)?;
                let coverage: Coverage = serde_json::from_value(baseline["coverage"].clone())?;
                let start = std::time::Instant::now();
                let response = dispatch(
                    &store,
                    "project",
                    &owner,
                    "search_sessions",
                    json!({"query":q["query"],"agent":q["agent"],"limit":10}),
                    baseline["status"].as_str().unwrap(),
                    coverage,
                    true,
                )?;
                let millis = start.elapsed().as_secs_f64() * 1000.0;
                let mut source_checks = 0;
                for hit in response["results"].as_array().unwrap() {
                    let (text, path, start_line, end_line, start_byte, end_byte): (String, String, i64, i64, i64, i64) = verification.query_row(
                        "SELECT c.text,s.path,c.start_line,c.end_line,c.start_byte,c.end_byte FROM chunks c JOIN sources s ON s.key=c.source_key WHERE c.match_id=?1 AND s.eligible=1",
                        [hit["match_id"].as_str().unwrap()],
                        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)))?;
                    let offset = hit["excerpt_byte_offset"].as_u64().unwrap() as usize;
                    let excerpt = hit["excerpt"].as_str().unwrap();
                    ensure!(
                        text.get(offset..offset + excerpt.len()) == Some(excerpt),
                        "excerpt does not match indexed chunk"
                    );
                    let reference = &hit["source_reference"];
                    ensure!(
                        reference["path"] == path
                            && reference["start_line"] == start_line
                            && reference["end_line"] == end_line
                            && reference["start_byte"] == start_byte
                            && reference["end_byte"] == end_byte,
                        "source reference mismatch"
                    );
                    source_checks += 1;
                }
                ensure!(
                    serde_json::to_vec(&response)?.len() <= 16384,
                    "response exceeds default budget"
                );
                let mut logical_target_rank = None;
                let mut baseline_logical_target_rank = None;
                let mut logical_groups = std::collections::HashSet::new();
                let mut copies_enumerated = 0;
                for (index, hit) in response["results"].as_array().unwrap().iter().enumerate() {
                    let mut cursor = None;
                    let mut references = Vec::new();
                    let mut is_target = false;
                    loop {
                        let mut request =
                            json!({"mode":"copies","match_id":hit["match_id"],"limit":50});
                        if let Some(c) = cursor.take() {
                            request["cursor"] = c;
                        }
                        let page = dispatch(
                            &store,
                            "project",
                            &owner,
                            "search_sessions",
                            request,
                            "ready",
                            Coverage::default(),
                            true,
                        )?;
                        for copy in page["copies"].as_array().unwrap() {
                            copies_enumerated += 1;
                            let reference = &copy["source_reference"];
                            references.push((
                                reference["path"].as_str().unwrap().to_owned(),
                                reference["start_byte"].as_u64().unwrap(),
                                reference["end_byte"].as_u64().unwrap(),
                            ));
                            if reference["path"] == q["path"]
                                && reference["start_line"].as_u64() <= q["line"].as_u64()
                                && reference["end_line"].as_u64() >= q["line"].as_u64()
                            {
                                logical_target_rank.get_or_insert(index + 1);
                                is_target = true;
                            }
                        }
                        match page.get("cursor") {
                            Some(c) => cursor = Some(c.clone()),
                            None => break,
                        }
                    }
                    references.sort();
                    references.dedup();
                    logical_groups.insert(references.clone());
                    if is_target {
                        for (old_index, old) in
                            baseline["results"].as_array().unwrap().iter().enumerate()
                        {
                            let r = &old["source_reference"];
                            let key = (
                                r["path"].as_str().unwrap().to_owned(),
                                r["start_byte"].as_u64().unwrap(),
                                r["end_byte"].as_u64().unwrap(),
                            );
                            if references.contains(&key) {
                                let rank = old_index + 1;
                                baseline_logical_target_rank = Some(
                                    baseline_logical_target_rank
                                        .map_or(rank, |previous: usize| previous.min(rank)),
                                );
                            }
                        }
                    }
                }
                writeln!(
                    output,
                    "{}",
                    json!({"project":name,"id":id,"latency_ms":millis,"logical_target_rank":logical_target_rank,"baseline_logical_target_rank":baseline_logical_target_rank,"distinct_logical_events":logical_groups.len(),"copies_enumerated":copies_enumerated,"indexed_source_checks":source_checks,"response":response})
                )?;
                output.flush()?;
                eprintln!("{name} {id} complete");
            }
        }
    }
    Ok(())
}
