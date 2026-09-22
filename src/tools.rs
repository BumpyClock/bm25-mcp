use crate::{model::SearchFilter, store::Store};
use anyhow::{Result, bail, ensure};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;

/// Search responses keep each hit useful without allowing one large chunk to
/// crowd every other result out of the response budget. This is a byte limit
/// because the MCP response budget is defined in serialized UTF-8 bytes.
const SEARCH_EXCERPT_BYTES: usize = 640;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Arguments {
    query: Option<String>,
    mode: Option<String>,
    limit: Option<usize>,
    max_response_bytes: Option<usize>,
    path_glob: Option<String>,
    agent: Option<String>,
    session_id: Option<String>,
    after: Option<String>,
    before: Option<String>,
    match_id: Option<String>,
    before_events: Option<usize>,
    after_events: Option<usize>,
    cursor: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct Coverage {
    #[serde(default)]
    pub diagnostics: std::collections::BTreeMap<String, u64>,
    pub reconciled_at: Option<String>,
    pub pending_changes: Option<u64>,
    pub excluded_count: u64,
    pub error_count: u64,
    pub errors: Vec<String>,
    pub memory: Option<MemoryCoverage>,
}

impl serde::Serialize for Coverage {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        public_coverage_value(self).serialize(serializer)
    }
}

#[derive(Clone, Debug, serde::Serialize, Deserialize)]
pub struct MemoryCoverage {
    pub target_bytes: u64,
    pub observed_rss_bytes: Option<u64>,
    pub peak_observed_rss_bytes: u64,
    pub pressure: bool,
}

const PUBLIC_DIAGNOSTICS: &[&str] = &[
    "binary_excluded",
    "content_cache_hit",
    "content_cache_miss",
    "content_cache_write_error",
    "git_metadata_excluded",
    "non_file_change",
    "outside_root",
    "ownership_excluded",
    "symlink_excluded",
    "unsupported_encoding",
    "unsupported_record",
    "watcher_unavailable",
];

/// Project coverage is also returned by the owner status resource. Keep this
/// projection separate from the internal report so path-bearing error samples
/// never cross the MCP boundary.
pub fn public_coverage_value(coverage: &Coverage) -> Value {
    let mut diagnostics = BTreeMap::new();
    for (key, count) in &coverage.diagnostics {
        let category = if PUBLIC_DIAGNOSTICS.contains(&key.as_str()) {
            key.as_str()
        } else {
            "other"
        };
        let entry = diagnostics.entry(category.to_owned()).or_insert(0u64);
        *entry = (*entry).saturating_add(*count);
    }
    json!({
        "reconciled_at": coverage.reconciled_at,
        "pending_changes": coverage.pending_changes,
        "excluded_count": coverage.excluded_count,
        "error_count": coverage.error_count,
        "diagnostics": diagnostics,
        "memory": coverage.memory,
    })
}

#[allow(clippy::too_many_arguments)]
pub fn dispatch(
    store: &Store,
    collection: &str,
    owner_key: &str,
    name: &str,
    arguments: Value,
    status: &str,
    coverage: Coverage,
    eligible: bool,
) -> Result<Value> {
    ensure!(
        matches!(name, "search_project" | "search_sessions"),
        "unknown_tool"
    );
    let a: Arguments = serde_json::from_value(arguments)?;
    let limit = a.limit.unwrap_or(10);
    ensure!((1..=50).contains(&limit), "limit must be between 1 and 50");
    let budget = a.max_response_bytes.unwrap_or(16384);
    ensure!(
        (1024..=65536).contains(&budget),
        "max_response_bytes must be between 1024 and 65536"
    );
    let is_session = name == "search_sessions";
    let mode = a.mode.as_deref().unwrap_or("search");
    ensure!(
        matches!(mode, "search" | "context" | "copies"),
        "invalid mode"
    );
    if !is_session {
        ensure!(
            a.mode.is_none()
                && a.agent.is_none()
                && a.session_id.is_none()
                && a.after.is_none()
                && a.before.is_none(),
            "session arguments cannot be used for project search"
        );
    } else {
        ensure!(
            a.path_glob.is_none(),
            "path_glob is only supported for project search"
        );
    }
    let mut response = json!({"status":status,"generation":store.generation()?,"coverage":public_coverage_value(&coverage),
        "truncated":false,"results":[]});
    if mode == "copies" {
        ensure!(
            is_session
                && a.query.is_none()
                && a.agent.is_none()
                && a.session_id.is_none()
                && a.after.is_none()
                && a.before.is_none()
                && a.before_events.is_none()
                && a.after_events.is_none(),
            "invalid copies arguments"
        );
        let id = a
            .match_id
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("match_id is required"))?;
        let page: Option<(String, String, u64, usize)> = a
            .cursor
            .map(|cursor| serde_json::from_str(&cursor))
            .transpose()?;
        if let Some((mode, anchor, _, _)) = &page {
            ensure!(
                mode == "copies" && anchor == id,
                "cursor does not match copies request"
            );
        }
        response.as_object_mut().unwrap().remove("results");
        response["copies"] = json!([]);
        if !eligible {
            return Ok(response);
        }
        let offset = page.as_ref().map_or(0, |page| page.3);
        let (generation, hits, more) =
            store.session_copies_snapshot(id, owner_key, offset, limit)?;
        ensure!(
            page.as_ref().is_none_or(|page| page.2 == generation),
            "match_expired: copies cursor generation changed"
        );
        ensure!(
            offset == 0 || !hits.is_empty(),
            "match_expired: copies cursor is outside the current group"
        );
        response["generation"] = json!(generation);
        for (index, hit) in hits.iter().enumerate() {
            let item = session_hit(hit);
            response["copies"].as_array_mut().unwrap().push(json!({
                "match_id": hit.match_id,
                "timestamp": hit.chunk.timestamp,
                "source_reference": item["source_reference"]
            }));
            let next = offset
                .checked_add(index + 1)
                .ok_or_else(|| anyhow::anyhow!("invalid copies cursor offset"))?;
            if index + 1 < hits.len() || more {
                response["cursor"] =
                    json!(serde_json::to_string(&("copies", id, generation, next))?);
                response["truncated"] = json!(true);
            } else {
                response.as_object_mut().unwrap().remove("cursor");
                response["truncated"] = json!(false);
            }
            if serde_json::to_vec(&response)?.len() > budget {
                response["copies"].as_array_mut().unwrap().pop();
                ensure!(
                    !response["copies"].as_array().unwrap().is_empty(),
                    "response_budget_too_small: increase max_response_bytes"
                );
                response["cursor"] = json!(serde_json::to_string(&(
                    "copies",
                    id,
                    generation,
                    next - 1
                ))?);
                response["truncated"] = json!(true);
                break;
            }
        }
        ensure!(
            serde_json::to_vec(&response)?.len() <= budget,
            "response_budget_too_small: increase max_response_bytes"
        );
        return Ok(response);
    }
    if mode == "context" {
        ensure!(
            is_session
                && a.query.is_none()
                && a.limit.is_none()
                && a.agent.is_none()
                && a.session_id.is_none()
                && a.after.is_none()
                && a.before.is_none(),
            "invalid context arguments"
        );
        let id = a
            .match_id
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("match_id is required"))?;
        let before = a.before_events.unwrap_or(2);
        let after = a.after_events.unwrap_or(2);
        ensure!(
            before <= 10 && after <= 10,
            "context windows must be at most 10 events"
        );
        let (offset, byte_offset) = if let Some(cursor) = a.cursor {
            let c: (String, usize, usize, usize, usize) = serde_json::from_str(&cursor)?;
            ensure!(
                c.0 == id && c.1 == before && c.2 == after,
                "cursor does not match context window"
            );
            (c.3, c.4)
        } else {
            (0, 0)
        };
        if !eligible {
            bail!("source_changed: session reconciliation is pending");
        }
        let (generation, hits, more) =
            store.context_page_snapshot(id, owner_key, before, after, offset, 32)?;
        response["generation"] = json!(generation);
        ensure!(
            offset == 0 || !hits.is_empty(),
            "match_expired: cursor is outside the current window"
        );
        response.as_object_mut().unwrap().remove("results");
        response["context"] = json!([]);
        for (local_index, hit) in hits.iter().enumerate() {
            let index = offset + local_index;
            let start = if index == offset { byte_offset } else { 0 };
            ensure!(
                start <= hit.chunk.text.len() && hit.chunk.text.is_char_boundary(start),
                "invalid context cursor offset"
            );
            let mut item = session_hit(hit);
            item["excerpt"] = json!(&hit.chunk.text[start..]);
            item["excerpt_byte_offset"] = json!(start);
            response["context"].as_array_mut().unwrap().push(item);
            let continuation_reserve = if local_index + 1 < hits.len() || more {
                serde_json::to_vec(
                    &json!({"cursor":serde_json::to_string(&(id,before,after,index+1,0))?}),
                )?
                .len()
                    + 16
            } else {
                0
            };
            if serde_json::to_vec(&response)?.len() + continuation_reserve > budget {
                let mut item = response["context"].as_array_mut().unwrap().pop().unwrap();
                response["truncated"] = json!(true);
                if !response["context"].as_array().unwrap().is_empty() {
                    response["cursor"] =
                        json!(serde_json::to_string(&(id, before, after, index, start))?);
                    break;
                }
                let remaining = &hit.chunk.text[start..];
                let mut length = remaining.len();
                loop {
                    length /= 2;
                    while !remaining.is_char_boundary(length) {
                        length -= 1;
                    }
                    ensure!(
                        length > 0,
                        "response_budget_too_small: increase max_response_bytes"
                    );
                    item["excerpt"] = json!(&remaining[..length]);
                    response["context"] = json!([item]);
                    response["cursor"] = json!(serde_json::to_string(&(
                        id,
                        before,
                        after,
                        index,
                        start + length
                    ))?);
                    if serde_json::to_vec(&response)?.len() <= budget {
                        break;
                    }
                    item = response["context"].as_array_mut().unwrap().pop().unwrap();
                }
                break;
            }
        }
        if more && response.get("cursor").is_none() {
            response["truncated"] = json!(true);
            response["cursor"] = json!(serde_json::to_string(&(
                id,
                before,
                after,
                offset + hits.len(),
                0
            ))?);
        }
        return Ok(response);
    }
    ensure!(
        a.match_id.is_none()
            && a.before_events.is_none()
            && a.after_events.is_none()
            && a.cursor.is_none(),
        "context arguments require context mode"
    );
    let query = a
        .query
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("query is required"))?;
    ensure!(
        !query.trim().is_empty() && query.len() <= 8192,
        "query must contain 1..8192 UTF-8 bytes"
    );
    let normalize_date = |date: Option<String>| -> Result<Option<String>> {
        date.map(|d| {
            chrono::DateTime::parse_from_rfc3339(&d)
                .map(|t| {
                    t.with_timezone(&chrono::Utc)
                        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                })
                .map_err(Into::into)
        })
        .transpose()
    };
    let after = normalize_date(a.after)?;
    let before = normalize_date(a.before)?;
    if let (Some(a), Some(b)) = (&after, &before) {
        ensure!(a < b, "after must precede before");
    }
    if let Some(agent) = &a.agent {
        ensure!(
            matches!(agent.as_str(), "codex" | "claude" | "copilot"),
            "unsupported agent"
        );
    }
    if let Some(glob) = &a.path_glob {
        globset::Glob::new(glob)?;
    }
    if eligible {
        let filter = SearchFilter {
            collection: if is_session { owner_key } else { collection }.into(),
            kind: if is_session { "session" } else { "project" }.into(),
            path_glob: a.path_glob,
            agent: a.agent,
            session_id: a.session_id,
            after,
            before,
        };
        let ranked = store.search_ranked(query, &filter, limit)?;
        let (generation, hits) = ranked;
        response["generation"] = json!(generation);
        for hit in hits {
            let mut item = if is_session {
                session_hit(&hit)
            } else {
                json!({"match_id":hit.match_id,"relative_path":hit.source.path,
                    "start_line":hit.chunk.start_line,"end_line":hit.chunk.end_line,
                    "start_byte":hit.chunk.start_byte,"end_byte":hit.chunk.end_byte,
                    "excerpt":hit.chunk.text,"score":hit.score,"source_version":hit.source.version,"verified_at":hit.verified_at})
            };
            if is_session {
                item["copy_count"] = json!(hit.copy_count);
            }
            compact_search_excerpt(&mut item, query, SEARCH_EXCERPT_BYTES);
            response["results"].as_array_mut().unwrap().push(item);
        }
    }
    fit_results(&mut response, budget, query)?;
    Ok(response)
}

fn session_hit(hit: &crate::model::Hit) -> Value {
    json!({"match_id":hit.match_id,"agent":hit.chunk.agent,"session_id":hit.chunk.session_id,
        "timestamp":hit.chunk.timestamp,"event_id":hit.chunk.event_id,"role":hit.chunk.role,
        "tool":hit.chunk.tool,"event_kind":hit.chunk.field_kind.as_deref().unwrap_or(if hit.chunk.tool.is_some(){"tool"}else{"message"}),"excerpt":hit.chunk.text,"score":hit.score,
        "source_reference":{"path":hit.source.path,"version":hit.source.version,"verified_at":hit.verified_at,
            "start_line":hit.chunk.start_line,"end_line":hit.chunk.end_line,
            "start_byte":hit.chunk.start_byte,"end_byte":hit.chunk.end_byte}})
}

fn compact_search_excerpt(item: &mut Value, query: &str, max_bytes: usize) {
    let text = item["excerpt"].as_str().unwrap_or("");
    let previous_offset = item["excerpt_byte_offset"].as_u64().unwrap_or(0);
    let previous_truncated = item["excerpt_truncated"].as_bool().unwrap_or(false);
    let (offset, excerpt) = excerpt_window(text, query, max_bytes);
    let truncated = previous_truncated || offset != 0 || excerpt.len() != text.len();
    let excerpt = excerpt.to_owned();
    item["excerpt"] = json!(excerpt);
    item["excerpt_byte_offset"] = json!(previous_offset.saturating_add(offset as u64));
    item["excerpt_truncated"] = json!(truncated);
}

fn fit_results(response: &mut Value, budget: usize, query: &str) -> Result<()> {
    while serde_json::to_vec(response)?.len() > budget {
        response["truncated"] = json!(true);
        let results = response["results"].as_array_mut().unwrap();
        if results.len() > 1 {
            results.pop();
            continue;
        }
        if let Some(item) = results.first_mut() {
            let text = item["excerpt"].as_str().unwrap_or("");
            if text.len() > 32 {
                let (start, excerpt) = excerpt_window(text, query, text.len() / 2);
                let excerpt = excerpt.to_owned();
                let offset = item["excerpt_byte_offset"]
                    .as_u64()
                    .unwrap_or(0)
                    .saturating_add(start as u64);
                item["excerpt"] = json!(excerpt);
                item["excerpt_byte_offset"] = json!(offset);
                item["excerpt_truncated"] = json!(true);
                continue;
            }
            results.pop();
        } else {
            bail!("response_budget_too_small");
        }
    }
    Ok(())
}

fn excerpt_window<'a>(text: &'a str, query: &str, bytes: usize) -> (usize, &'a str) {
    use unicode_casefold::UnicodeCaseFold;

    if text.len() <= bytes {
        return (0, text);
    }

    let mut folded = String::new();
    let mut positions = Vec::new();
    for (offset, character) in text.char_indices() {
        for folded_character in character.case_fold() {
            folded.push(folded_character);
            positions.extend(std::iter::repeat_n(offset, folded_character.len_utf8()));
        }
    }

    // The complete query and complete identifier forms carry more meaning
    // than an early component such as `set` or `error`. Search candidates by
    // descending byte length so a later exact identifier wins over an earlier
    // common component. The folded full query is considered as one candidate
    // first, which also handles ordinary multi-word phrase matches.
    let mut candidates = Vec::new();
    let folded_query: String = query
        .chars()
        .flat_map(|character| character.case_fold())
        .collect();
    let exact_center = if folded_query.is_empty() {
        None
    } else {
        folded.find(&folded_query)
    };
    if !folded_query.is_empty() {
        candidates.push(folded_query.clone());
    }
    if let Ok(terms) = crate::text::tokenize_checked(query) {
        for term in terms {
            let folded_term: String = term
                .chars()
                .flat_map(|character| character.case_fold())
                .collect();
            if !folded_term.is_empty()
                && !candidates.iter().any(|candidate| candidate == &folded_term)
            {
                candidates.push(folded_term);
            }
        }
    }
    let query_runs = lexical_query_runs(query);
    let phrase_center = find_lexical_phrase(&folded, &query_runs);
    candidates.sort_by_key(|candidate| std::cmp::Reverse(candidate.len()));
    let center = exact_center
        .or(phrase_center)
        .or_else(|| {
            candidates
                .iter()
                .find_map(|candidate| folded.find(candidate))
        })
        .and_then(|position| positions.get(position).copied())
        .unwrap_or(0);
    let mut start = center
        .saturating_sub(bytes / 3)
        .min(text.len().saturating_sub(bytes));
    while !text.is_char_boundary(start) {
        start -= 1;
    }
    let mut end = (start + bytes).min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    (start, &text[start..end])
}

fn lexical_query_runs(query: &str) -> Vec<String> {
    use unicode_casefold::UnicodeCaseFold;

    let mut runs = Vec::new();
    let mut current = String::new();
    for character in query.chars() {
        if character.is_alphanumeric() || character == '_' {
            current.push(character);
        } else if !current.is_empty() {
            runs.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        runs.push(current);
    }
    runs.into_iter()
        .map(|run| {
            run.chars()
                .flat_map(|character| character.case_fold())
                .collect()
        })
        .collect()
}

fn find_lexical_phrase(text: &str, terms: &[String]) -> Option<usize> {
    let first = terms.first()?.as_str();
    if terms.len() < 2 || first.is_empty() {
        return None;
    }
    let mut search_from = 0;
    while search_from < text.len() {
        let relative = text[search_from..].find(first)?;
        let start = search_from + relative;
        let mut cursor = start + first.len();
        let mut matched = true;
        for term in &terms[1..] {
            while cursor < text.len() {
                let character = text[cursor..].chars().next().unwrap();
                if character.is_alphanumeric() {
                    break;
                }
                cursor += character.len_utf8();
            }
            if !text[cursor..].starts_with(term) {
                matched = false;
                break;
            }
            cursor += term.len();
        }
        if matched {
            return Some(start);
        }
        search_from = start + text[start..].chars().next().unwrap().len_utf8();
    }
    None
}

pub fn tool_definitions() -> Vec<rmcp::model::Tool> {
    let common = json!({"limit":{"type":"integer","minimum":1,"maximum":50,"default":10},
        "max_response_bytes":{"type":"integer","minimum":1024,"maximum":65536,"default":16384}});
    let mut project = common.clone();
    project["query"] =
        json!({"type":"string","description":"Plain lexical query, at most 8192 UTF-8 bytes."});
    project["path_glob"] = json!({"type":"string"});
    let mut sessions = common;
    for key in [
        "query",
        "session_id",
        "after",
        "before",
        "match_id",
        "cursor",
    ] {
        sessions[key] = json!({"type":"string"});
    }
    sessions["agent"] = json!({"type":"string","enum":["codex","claude","copilot"]});
    sessions["mode"] =
        json!({"type":"string","enum":["search","context","copies"],"default":"search"});
    for key in ["before_events", "after_events"] {
        sessions[key] = json!({"type":"integer","minimum":0,"maximum":10,"default":2});
    }
    [("search_project", "Search current project text with strict BM25 lexical ranking.",project,true),
     ("search_sessions", "Search this project's Codex, Claude Code and Copilot CLI history, expand a match in context mode, or list its source copies in copies mode.",sessions,false)]
        .into_iter().map(|(name, description, properties, query_required)| {
            let mut schema = json!({"type":"object","properties":properties,"additionalProperties":false});
            if query_required { schema["required"]=json!(["query"]); }
            else {schema["oneOf"]=json!([
                {"required":["query"],"properties":{"mode":{"const":"search"}}},
                {"required":["mode","match_id"],"properties":{"mode":{"enum":["context","copies"]}},"not":{"required":["query"]}}
            ]);}
            let mut tool = rmcp::model::Tool::default();
            tool.name=name.into(); tool.description=Some(description.into());
            tool.input_schema=std::sync::Arc::new(schema.as_object().unwrap().clone());
            tool.output_schema=Some(std::sync::Arc::new(json!({"type":"object","oneOf":[
                {"required":["status","generation","coverage","truncated"],"properties":{
                    "status":{"enum":["building","ready","refreshing","degraded"]},
                    "generation":{"type":"integer","minimum":0},"coverage":{"type":"object"},
                    "truncated":{"type":"boolean"},"results":{"type":"array","items":{"type":"object"}},
                    "context":{"type":"array","items":{"type":"object"}},"copies":{"type":"array","items":{"type":"object"}},"cursor":{"type":"string"}}},
                {"required":["error"],"properties":{"error":{"type":"string"}}}
            ]}).as_object().unwrap().clone()));
            tool.annotations=Some(rmcp::model::ToolAnnotations::new().read_only(true).destructive(false).idempotent(true).open_world(false));
            tool
        }).collect()
}
