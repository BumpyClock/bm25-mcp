//! Streaming adapters for local Codex, Claude Code, and Copilot CLI logs.
//!
//! Provider parsing lives in [`session_stream`]. This module owns the public
//! configuration and scan entry points; the stream adapter keeps physical
//! records and parser state on disk so a provider's large history cannot turn
//! into an unbounded process allocation.

use crate::progress::{ProgressPhase, ProgressReporter};
use crate::store::Store;
use anyhow::{Context, Result, anyhow};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

#[path = "session_json.rs"]
mod session_json;
#[path = "session_stream.rs"]
mod session_stream;

pub(crate) const SESSION_KIND: &str = "session";

/// Locations and explicit MCP tool identities used by session discovery.
#[derive(Clone, Debug)]
pub struct SessionConfig {
    pub codex_home: PathBuf,
    pub claude_config_dir: PathBuf,
    pub copilot_home: PathBuf,
    pub own_tool_names: Vec<String>,
    /// Shared owner-cache registry used to associate removed or nested cwd
    /// paths without guessing ownership.
    pub identity_registry_path: Option<PathBuf>,
}

impl Default for SessionConfig {
    fn default() -> Self {
        let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
        let mut own_tool_names = vec![
            "mcp__bm25-mcp__search_project".to_owned(),
            "mcp__bm25-mcp__search_sessions".to_owned(),
            "mcp__bm25_mcp__search_project".to_owned(),
            "mcp__bm25_mcp__search_sessions".to_owned(),
            "bm25-mcp::search_project".to_owned(),
            "bm25-mcp::search_sessions".to_owned(),
        ];
        if let Some(extra) = std::env::var_os("BM25_MCP_OWN_TOOL_NAMES")
            && let Ok(extra) = parse_own_tool_names(&extra.to_string_lossy())
        {
            own_tool_names.extend(extra);
        }
        Self {
            codex_home: std::env::var_os("CODEX_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join(".codex")),
            claude_config_dir: std::env::var_os("CLAUDE_CONFIG_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join(".claude")),
            copilot_home: std::env::var_os("COPILOT_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join(".copilot")),
            own_tool_names,
            identity_registry_path: None,
        }
    }
}

/// Build configuration from environment and reject malformed own-tool JSON.
/// `Default` remains available to callers that do not need startup errors;
/// scans validate the environment independently before touching the store.
pub fn session_config_from_env() -> Result<SessionConfig> {
    if let Some(raw) = std::env::var_os("BM25_MCP_OWN_TOOL_NAMES") {
        let extra = parse_own_tool_names(&raw.to_string_lossy())
            .context("BM25_MCP_OWN_TOOL_NAMES must be a JSON array of strings")?;
        let mut config = SessionConfig::default();
        for name in extra {
            if !config.own_tool_names.contains(&name) {
                config.own_tool_names.push(name);
            }
        }
        Ok(config)
    } else {
        Ok(SessionConfig::default())
    }
}

fn parse_own_tool_names(raw: &str) -> Result<Vec<String>> {
    let names = serde_json::from_str::<Vec<String>>(raw)
        .context("BM25_MCP_OWN_TOOL_NAMES must be a JSON array of strings")?;
    if names.iter().any(|name| name.is_empty()) {
        return Err(anyhow!(
            "BM25_MCP_OWN_TOOL_NAMES cannot contain empty names"
        ));
    }
    Ok(names)
}

fn validate_own_tool_names_env() -> Result<()> {
    if let Some(raw) = std::env::var_os("BM25_MCP_OWN_TOOL_NAMES") {
        parse_own_tool_names(&raw.to_string_lossy())?;
    }
    Ok(())
}

/// Discover and reconcile session logs associated with a repository owner.
pub fn scan_sessions(
    root: &Path,
    owner_key: &str,
    store: &crate::store::Store,
    config: &SessionConfig,
) -> Result<crate::model::ScanReport> {
    scan_sessions_observed(
        root,
        owner_key,
        store,
        config,
        None,
        &|| true,
        &ProgressReporter::noop(),
    )
}

/// Reconcile sessions while allowing the owner to stop between records/files.
/// A canceled scan leaves existing sources invalidated for the next pass so
/// stale rows cannot be returned as current.
pub fn scan_sessions_controlled(
    root: &Path,
    owner_key: &str,
    store: &crate::store::Store,
    config: &SessionConfig,
    should_continue: &dyn Fn() -> bool,
) -> Result<crate::model::ScanReport> {
    scan_sessions_observed(
        root,
        owner_key,
        store,
        config,
        None,
        should_continue,
        &ProgressReporter::noop(),
    )
}

/// Invalidate only the session sources represented by an exact set of
/// canonical provider-file paths. `None` is the conservative full fallback
/// used for registry, topology, overflow, or otherwise ambiguous events.
pub fn invalidate_session_scope(
    owner_key: &str,
    store: &Store,
    config: &SessionConfig,
    changes: Option<&HashSet<PathBuf>>,
) -> Result<()> {
    session_stream::invalidate_scope(owner_key, store, config, changes)
}

/// Reconcile sessions while publishing bounded progress snapshots.
pub fn scan_sessions_observed(
    root: &Path,
    owner_key: &str,
    store: &Store,
    config: &SessionConfig,
    changes: Option<&HashSet<PathBuf>>,
    should_continue: &dyn Fn() -> bool,
    progress: &ProgressReporter,
) -> Result<crate::model::ScanReport> {
    progress.begin_run();
    let result = session_stream::scan_observed(
        root,
        owner_key,
        store,
        config,
        changes,
        should_continue,
        progress,
    );
    match &result {
        Ok(report) if report.cancelled => progress.finish_run(ProgressPhase::Cancelled),
        Ok(_) => progress.finish_run(ProgressPhase::Complete),
        Err(error) if error.to_string().contains("session scan cancelled") => {
            progress.record_cancellation();
            progress.finish_run(ProgressPhase::Cancelled);
        }
        Err(_) => progress.finish_run(ProgressPhase::Failed),
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::project_identity;
    use crate::model::SearchFilter;
    use crate::store::Store;
    use serde_json::Value;
    use std::fs;
    use std::process::Command;
    use tempfile::TempDir;

    fn git(root: &Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(args)
            .output()
            .expect("git installed");
        assert!(
            output.status.success(),
            "git {:?}: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn jsonl(path: &Path, values: &[Value]) {
        let mut bytes = Vec::new();
        for value in values {
            bytes.extend(serde_json::to_vec(value).unwrap());
            bytes.push(b'\n');
        }
        fs::write(path, bytes).unwrap();
    }

    #[test]
    fn discovers_and_normalizes_all_supported_providers() {
        let project = TempDir::new().unwrap();
        git(project.path(), &["init", "-q"]);
        let identity = project_identity(project.path()).unwrap();
        let home = TempDir::new().unwrap();
        let codex = home.path().join("codex");
        let claude = home.path().join("claude");
        let copilot = home.path().join("copilot");
        fs::create_dir_all(codex.join("sessions")).unwrap();
        fs::create_dir_all(claude.join("projects")).unwrap();
        fs::create_dir_all(copilot.join("session-state")).unwrap();
        jsonl(
            &codex.join("sessions/a.jsonl"),
            &[
                serde_json::json!({"type":"session_meta","payload":{"cwd":project.path(),"id":"s1"}}),
                serde_json::json!({"type":"response_item","payload":{"type":"message","id":"c1","role":"user","content":[{"type":"text","text":"codex unique message"}],"timestamp":"2026-01-01T01:02:03Z"}}),
                serde_json::json!({"type":"response_item","payload":{"type":"function_call","id":"c2","name":"mcp__bm25-mcp__search_project","call_id":"call-1","arguments":"query"}}),
                serde_json::json!({"type":"response_item","payload":{"type":"function_call_output","id":"c3","call_id":"call-1","output":"own result must not be indexed"}}),
            ],
        );
        jsonl(
            &claude.join("projects/a.jsonl"),
            &[
                serde_json::json!({"type":"user","cwd":project.path(),"uuid":"u1","message":{"role":"user","content":[{"type":"text","text":"claude unique message"}]},"timestamp":"2026-01-01T01:02:03.120+00:00"}),
            ],
        );
        jsonl(
            &copilot.join("session-state/events.jsonl"),
            &[
                serde_json::json!({"type":"session.start","data":{"context":{"cwd":project.path()},"sessionId":"p1"}}),
                serde_json::json!({"type":"user.message","id":"p2","data":{"content":"copilot unique message"},"timestamp":"2026-01-01T01:02:03.123Z"}),
            ],
        );
        let store_path = std::env::temp_dir().join(format!(
            "bm25-mcp-session-test-{}.sqlite3",
            std::process::id()
        ));
        let store = Store::open(&store_path).unwrap();
        let config = SessionConfig {
            codex_home: codex,
            claude_config_dir: claude,
            copilot_home: copilot,
            own_tool_names: SessionConfig::default().own_tool_names,
            identity_registry_path: None,
        };
        let report = scan_sessions(project.path(), &identity.owner_key, &store, &config).unwrap();
        assert_eq!(report.sources, 3);
        assert!(report.chunks >= 3);
        let filter = SearchFilter {
            collection: identity.owner_key.clone(),
            kind: SESSION_KIND.to_owned(),
            ..SearchFilter::default()
        };
        let (_, hits) = store.search("unique message", &filter, 20).unwrap();
        assert!(
            hits.iter()
                .any(|hit| hit.chunk.agent.as_deref() == Some("codex"))
        );
        assert!(
            hits.iter()
                .any(|hit| hit.chunk.agent.as_deref() == Some("claude"))
        );
        assert!(
            hits.iter()
                .any(|hit| hit.chunk.agent.as_deref() == Some("copilot"))
        );
        assert!(
            !hits
                .iter()
                .any(|hit| hit.chunk.text.contains("own result must not be indexed"))
        );
    }

    #[test]
    fn incomplete_trailing_record_is_pending_and_does_not_publish() {
        let project = TempDir::new().unwrap();
        git(project.path(), &["init", "-q"]);
        let identity = project_identity(project.path()).unwrap();
        let home = TempDir::new().unwrap();
        let codex = home.path().join("codex");
        fs::create_dir_all(codex.join("sessions")).unwrap();
        let path = codex.join("sessions/tail.jsonl");
        fs::write(
            &path,
            format!(
                "{}\n{{\"type\":\"response_item\"",
                serde_json::json!({"type":"session_meta","payload":{"cwd":project.path()}})
            ),
        )
        .unwrap();
        let store_path = std::env::temp_dir().join(format!(
            "bm25-mcp-session-tail-test-{}.sqlite3",
            std::process::id()
        ));
        let store = Store::open(&store_path).unwrap();
        let config = SessionConfig {
            codex_home: codex,
            claude_config_dir: home.path().join("claude"),
            copilot_home: home.path().join("copilot"),
            ..SessionConfig::default()
        };
        let report = scan_sessions(project.path(), &identity.owner_key, &store, &config).unwrap();
        assert_eq!(report.pending_count, 1);
        assert_eq!(report.sources, 1);
    }

    #[test]
    fn controlled_scan_stops_before_reconciling_unvisited_files() {
        let project = TempDir::new().unwrap();
        git(project.path(), &["init", "-q"]);
        let identity = project_identity(project.path()).unwrap();
        let home = TempDir::new().unwrap();
        let codex = home.path().join("codex");
        fs::create_dir_all(codex.join("sessions")).unwrap();
        jsonl(
            &codex.join("sessions/cancel.jsonl"),
            &[serde_json::json!({"type":"session_meta","payload":{"cwd":project.path()}})],
        );
        let store_path = std::env::temp_dir().join(format!(
            "bm25-mcp-session-cancel-test-{}.sqlite3",
            std::process::id()
        ));
        let store = Store::open(&store_path).unwrap();
        let config = SessionConfig {
            codex_home: codex,
            claude_config_dir: home.path().join("claude"),
            copilot_home: home.path().join("copilot"),
            ..SessionConfig::default()
        };
        let report = scan_sessions_controlled(
            project.path(),
            &identity.owner_key,
            &store,
            &config,
            &|| false,
        )
        .unwrap();
        assert_eq!(report.sources, 0);
        assert_eq!(report.pending_count, 1);
    }

    #[test]
    fn canceled_scan_suppresses_verified_sources_until_reconciled() {
        let project = TempDir::new().unwrap();
        git(project.path(), &["init", "-q"]);
        let identity = project_identity(project.path()).unwrap();
        let home = TempDir::new().unwrap();
        let codex = home.path().join("codex");
        fs::create_dir_all(codex.join("sessions")).unwrap();
        jsonl(
            &codex.join("sessions/preserved.jsonl"),
            &[
                serde_json::json!({
                    "type":"session_meta",
                    "payload":{"cwd":project.path(),"id":"preserved"}
                }),
                serde_json::json!({
                    "type":"response_item",
                    "payload":{"type":"message","role":"user","content":[
                        {"type":"text","text":"preserved during refresh"}
                    ]}
                }),
            ],
        );
        let store_dir = TempDir::new().unwrap();
        let store = Store::open(&store_dir.path().join("index.sqlite3")).unwrap();
        let config = SessionConfig {
            codex_home: codex,
            claude_config_dir: home.path().join("claude"),
            copilot_home: home.path().join("copilot"),
            ..SessionConfig::default()
        };
        scan_sessions(project.path(), &identity.owner_key, &store, &config).unwrap();
        let filter = SearchFilter {
            collection: identity.owner_key.clone(),
            kind: SESSION_KIND.to_owned(),
            ..SearchFilter::default()
        };
        let (_, before) = store.search("preserved refresh", &filter, 10).unwrap();
        assert_eq!(before.len(), 1);

        let report = scan_sessions_controlled(
            project.path(),
            &identity.owner_key,
            &store,
            &config,
            &|| false,
        )
        .unwrap();
        assert!(report.cancelled);
        let (_, after) = store.search("preserved refresh", &filter, 10).unwrap();
        assert!(after.is_empty());
    }

    #[test]
    fn rejects_nested_and_unrelated_git_cwds() {
        let project = TempDir::new().unwrap();
        git(project.path(), &["init", "-q"]);
        let nested = project.path().join("nested");
        fs::create_dir(&nested).unwrap();
        git(&nested, &["init", "-q"]);
        let unrelated = TempDir::new().unwrap();
        git(unrelated.path(), &["init", "-q"]);
        let identity = project_identity(project.path()).unwrap();
        let home = TempDir::new().unwrap();
        let codex = home.path().join("codex");
        fs::create_dir_all(codex.join("sessions")).unwrap();
        jsonl(
            &codex.join("sessions/nested.jsonl"),
            &[serde_json::json!({"type":"session_meta","payload":{"cwd":nested}})],
        );
        jsonl(
            &codex.join("sessions/unrelated.jsonl"),
            &[serde_json::json!({"type":"session_meta","payload":{"cwd":unrelated.path()}})],
        );
        let store_path = std::env::temp_dir().join(format!(
            "bm25-mcp-session-ownership-test-{}.sqlite3",
            std::process::id()
        ));
        let store = Store::open(&store_path).unwrap();
        let config = SessionConfig {
            codex_home: codex,
            claude_config_dir: home.path().join("claude"),
            copilot_home: home.path().join("copilot"),
            ..SessionConfig::default()
        };
        let report = scan_sessions(project.path(), &identity.owner_key, &store, &config).unwrap();
        assert_eq!(report.sources, 0);
        assert_eq!(report.excluded_count, 2);
    }

    #[test]
    fn malformed_own_tool_configuration_is_rejected() {
        assert!(parse_own_tool_names("{not-json").is_err());
        assert!(parse_own_tool_names("[\"\"]").is_err());
        assert_eq!(
            parse_own_tool_names("[\"server::search_project\"]").unwrap(),
            ["server::search_project"]
        );
    }
}
