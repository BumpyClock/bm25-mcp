use serde_json::{Value, json};
use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Write},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

struct Client {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    id: u64,
}
impl Client {
    fn start(root: &std::path::Path, cache: &std::path::Path, homes: &std::path::Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_bm25-mcp"))
            .args(["serve", "--project"])
            .arg(root)
            .arg("--cache-dir")
            .arg(cache)
            .env("CODEX_HOME", homes.join("codex"))
            .env("CLAUDE_CONFIG_DIR", homes.join("claude"))
            .env("COPILOT_HOME", homes.join("copilot"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let output = BufReader::new(child.stdout.take().unwrap());
        let mut client = Self {
            child,
            input,
            output,
            id: 0,
        };
        let init=client.rpc("initialize",json!({"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"integration","version":"1"}}));
        assert_eq!(init["result"]["serverInfo"]["name"], "bm25-mcp");
        assert_eq!(
            init["result"]["capabilities"]["resources"]["subscribe"],
            Value::Null
        );
        writeln!(
            client.input,
            "{}",
            json!({"jsonrpc":"2.0","method":"notifications/initialized"})
        )
        .unwrap();
        client.input.flush().unwrap();
        client
    }
    fn rpc(&mut self, method: &str, params: Value) -> Value {
        self.id += 1;
        writeln!(
            self.input,
            "{}",
            json!({"jsonrpc":"2.0","id":self.id,"method":method,"params":params})
        )
        .unwrap();
        self.input.flush().unwrap();
        loop {
            let mut line = String::new();
            assert!(
                self.output.read_line(&mut line).unwrap() > 0,
                "server exited"
            );
            let value: Value = serde_json::from_str(&line).unwrap();
            if value["id"] == self.id {
                return value;
            }
        }
    }
    fn search(&mut self, query: &str) -> Value {
        let value = self.rpc(
            "tools/call",
            json!({"name":"search_project","arguments":{"query":query}}),
        );
        assert_ne!(value["result"]["isError"], true, "{value}");
        value["result"]["structuredContent"].clone()
    }
    fn search_sessions(&mut self, query: &str) -> Value {
        let value = self.rpc(
            "tools/call",
            json!({"name":"search_sessions","arguments":{"query":query}}),
        );
        assert_ne!(value["result"]["isError"], true, "{value}");
        value["result"]["structuredContent"].clone()
    }
    fn status(&mut self) -> Value {
        let value = self.rpc("resources/read", json!({"uri":"bm25://indexing/status"}));
        assert!(
            value["result"]["contents"][0]["text"].is_string(),
            "{value}"
        );
        serde_json::from_str(value["result"]["contents"][0]["text"].as_str().unwrap()).unwrap()
    }
    fn until_status(&mut self, predicate: impl Fn(&Value) -> bool) -> Value {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let value = self.status();
            if predicate(&value) {
                return value;
            }
            assert!(
                Instant::now() < deadline,
                "status did not converge: {value}"
            );
            thread::sleep(Duration::from_millis(50));
        }
    }
    fn pipeline(&mut self, requests: &[(&str, Value)]) -> Vec<(u64, Value)> {
        let mut ids = Vec::with_capacity(requests.len());
        for (method, params) in requests {
            self.id += 1;
            let id = self.id;
            ids.push(id);
            writeln!(
                self.input,
                "{}",
                json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
            )
            .unwrap();
        }
        self.input.flush().unwrap();

        let mut pending = ids
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        let mut responses = HashMap::with_capacity(ids.len());
        while !pending.is_empty() {
            let mut line = String::new();
            assert!(
                self.output.read_line(&mut line).unwrap() > 0,
                "server exited"
            );
            let value: Value = serde_json::from_str(&line).unwrap();
            let Some(id) = value["id"].as_u64() else {
                continue;
            };
            if pending.remove(&id) {
                responses.insert(id, value);
            }
        }
        ids.into_iter()
            .map(|id| (id, responses.remove(&id).unwrap()))
            .collect()
    }
    fn until(&mut self, query: &str, predicate: impl Fn(&Value) -> bool) -> Value {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let value = self.search(query);
            if predicate(&value) {
                return value;
            }
            assert!(
                Instant::now() < deadline,
                "search did not converge: {value}"
            );
            thread::sleep(Duration::from_millis(50));
        }
    }
}
impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn mcp_search_refresh_and_multiple_clients() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("main.rs"), "fn lexicalneedleqvx() {}\n").unwrap();
    std::fs::write(root.join(".gitignore"), "ignored.txt\n").unwrap();
    std::fs::write(root.join("ignored.txt"), "excludedqvx").unwrap();
    let mut one = Client::start(&root, &dir.path().join("cache"), &dir.path().join("homes"));
    let tools = one.rpc("tools/list", json!({}));
    assert_eq!(tools["result"]["tools"].as_array().unwrap().len(), 2);
    let resources = one.rpc("resources/list", json!({}));
    assert_eq!(
        resources["result"]["resources"].as_array().unwrap().len(),
        1
    );
    assert_eq!(
        resources["result"]["resources"][0]["uri"],
        "bm25://indexing/status"
    );
    assert_eq!(
        resources["result"]["resources"][0]["mimeType"],
        "application/json"
    );
    let initial_status = one.status();
    assert!(initial_status["project"]["progress"].is_object());
    assert!(initial_status["sessions"]["progress"].is_object());
    assert!(
        initial_status["project"]["coverage"]["pending_changes"].is_null()
            || initial_status["project"]["coverage"]["pending_changes"].is_number()
    );
    let status_text = initial_status.to_string();
    assert!(!status_text.contains(root.to_string_lossy().as_ref()));
    assert!(!status_text.contains("excludedqvx"));
    let unknown = one.rpc("resources/read", json!({"uri":"file:///not-forwarded"}));
    assert!(unknown["error"]["code"].is_number(), "{unknown}");
    let found = one.until("lexicalneedleqvx", |v| {
        v["results"].as_array().is_some_and(|a| !a.is_empty())
    });
    assert_eq!(found["results"][0]["relative_path"], "main.rs");
    assert_eq!(one.search("excludedqvx")["results"], json!([]));
    let mut two = Client::start(&root, &dir.path().join("cache"), &dir.path().join("homes"));
    std::fs::write(root.join("main.rs"), "fn changedneedleqvx() {}\n").unwrap();
    two.until("changedneedleqvx", |v| {
        v["status"] == "ready" && !v["results"].as_array().unwrap().is_empty()
    });
    assert_eq!(two.search("lexicalneedleqvx")["results"], json!([]));
    drop(one);
    std::fs::write(root.join("second.rs"), "survivingclientneedleqvx").unwrap();
    two.until("survivingclientneedleqvx", |v| {
        !v["results"].as_array().unwrap().is_empty()
    });
    let invalid = two.rpc(
        "tools/call",
        json!({"name":"search_project","arguments":{"query":"x","limit":0}}),
    );
    assert_eq!(invalid["result"]["isError"], true);
}

#[test]
fn post_ready_ignored_project_sources_stay_excluded() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join(".gitignore"), ".env\n").unwrap();
    std::fs::write(root.join("main.rs"), "post_ready_baseline_marker").unwrap();
    let mut client = Client::start(&root, &dir.path().join("cache"), &dir.path().join("homes"));
    client.until("post_ready_baseline_marker", |value| {
        value["status"] == "ready"
    });

    let mut baseline = client.status();
    for marker in ["postreadycreateqvx", "postreadymodifyqvx"] {
        std::fs::write(root.join(".env"), marker).unwrap();
        let previous = baseline["project"]["coverage"]["reconciled_at"].clone();
        baseline = client.until_status(|value| {
            value["project"]["status"] == "ready"
                && value["project"]["coverage"]["reconciled_at"] != previous
        });
        assert_eq!(client.search(marker)["results"], json!([]));
    }
}

#[test]
fn deleting_a_jsonl_named_session_directory_falls_back_safely() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    let homes = dir.path().join("homes");
    let nested = homes.join("codex/sessions/folder.jsonl");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::write(
        nested.join("child.jsonl"),
        format!(
            "{}\n{}\n",
            json!({"type":"session_meta","payload":{"id":"nested","cwd":root}}),
            json!({"type":"response_item","payload":{"type":"message","id":"nested-event","role":"user","content":[{"type":"input_text","text":"nested_delete_marker_qvx"}]}})
        ),
    )
    .unwrap();
    std::fs::create_dir_all(&root).unwrap();
    let mut client = Client::start(&root, &dir.path().join("cache"), &homes);
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let result = client.search_sessions("nested_delete_marker_qvx");
        if !result["results"]
            .as_array()
            .is_none_or(|results| results.is_empty())
        {
            break;
        }
        assert!(Instant::now() < deadline, "nested session did not index");
        thread::sleep(Duration::from_millis(50));
    }
    std::fs::remove_dir_all(&nested).unwrap();
    let _ = client.until_status(|value| {
        value["sessions"]["status"] == "ready"
            && value["sessions"]["coverage"]["pending_changes"] == 0
    });
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if client.search_sessions("nested_delete_marker_qvx")["results"] == json!([]) {
            break;
        }
        assert!(Instant::now() < deadline, "deleted nested session remained");
        thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn status_resource_can_be_pipelined_behind_a_search_request() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("small.txt"), "pipeline_small_marker").unwrap();
    let mut client = Client::start(&root, &dir.path().join("cache"), &dir.path().join("homes"));
    client.until("pipeline_small_marker", |value| value["status"] == "ready");

    std::fs::write(
        root.join("large.txt"),
        format!("pipeline_large_marker {}", "padding ".repeat(500_000)),
    )
    .unwrap();
    let refresh_deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let status = client.status();
        if status["project"]["status"] == "refreshing"
            && status["project"]["progress"]["bytes_read"]
                .as_u64()
                .unwrap_or(0)
                > 0
        {
            break;
        }
        assert!(Instant::now() < refresh_deadline, "refresh did not start");
        thread::sleep(Duration::from_millis(20));
    }

    let responses = client.pipeline(&[
        (
            "tools/call",
            json!({"name":"search_project","arguments":{"query":"pipeline_large_marker"}}),
        ),
        ("resources/read", json!({"uri":"bm25://indexing/status"})),
    ]);
    assert_eq!(responses.len(), 2);
    let status = responses
        .iter()
        .find(|(_, value)| value["result"]["contents"][0]["text"].is_string())
        .map(|(_, value)| {
            serde_json::from_str::<Value>(value["result"]["contents"][0]["text"].as_str().unwrap())
                .unwrap()
        })
        .expect("pipelined status response");
    assert!(
        status["project"]["progress"]["bytes_read"]
            .as_u64()
            .unwrap_or(0)
            > 0
    );
    assert!(responses.iter().any(|(_, value)| {
        value["result"]["structuredContent"].is_object() || value["result"]["isError"] == true
    }));
}

#[test]
fn mcp_search_excludes_incidental_stopwords_but_keeps_lexical_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("notes.txt"), "why does this happen").unwrap();
    let mut client = Client::start(&root, &dir.path().join("cache"), &dir.path().join("homes"));
    let supported = client.until("happen", |value| {
        value["status"] == "ready"
            && value["results"]
                .as_array()
                .is_some_and(|results| !results.is_empty())
    });
    assert_eq!(supported["results"][0]["relative_path"], "notes.txt");
    assert_eq!(
        client.search("why does indexing fail")["results"],
        json!([])
    );
    let fallback = client.search("why");
    assert_eq!(fallback["results"].as_array().unwrap().len(), 1);
    assert_eq!(fallback["results"][0]["relative_path"], "notes.txt");
    assert!(fallback["results"][0]["score"].as_f64().unwrap() > 0.0);
}

#[test]
fn mcp_saturated_stopwords_do_not_hide_meaningful_results() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir(&root).unwrap();
    for index in 0..1_201 {
        let text = if index < 201 {
            "why does this happen".into()
        } else {
            "neutral ".repeat(20)
        };
        std::fs::write(root.join(format!("{index}.txt")), text).unwrap();
    }
    std::fs::write(
        root.join("1201.txt"),
        format!("indexing fail {}", "padding ".repeat(2_000)),
    )
    .unwrap();
    let mut client = Client::start(&root, &dir.path().join("cache"), &dir.path().join("homes"));
    let ready = client.until("indexing fail", |value| {
        value["status"] == "ready"
            && value["coverage"]["reconciled_at"].is_string()
            && value["results"]
                .as_array()
                .is_some_and(|results| !results.is_empty())
    });
    assert_eq!(ready["coverage"]["error_count"], 0);
    assert_eq!(ready["results"][0]["relative_path"], "1201.txt");
    let result = client.search("why does indexing fail");
    assert_eq!(result["status"], "ready");
    assert_eq!(result["results"].as_array().unwrap().len(), 1);
    assert_eq!(result["results"][0]["relative_path"], "1201.txt");
    assert!(result["results"][0]["score"].as_f64().unwrap() > 0.0);
}

#[test]
fn codex_history_is_scoped_and_context_uses_the_same_tool() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir(&root).unwrap();
    let homes = dir.path().join("homes");
    let sessions = homes.join("codex/sessions");
    std::fs::create_dir_all(&sessions).unwrap();
    let records = [
        json!({"type":"session_meta","payload":{"id":"test-session","cwd":root}}),
        json!({"type":"response_item","timestamp":"2026-09-20T12:00:00Z","payload":{"type":"message","id":"event1","role":"user","content":[{"type":"input_text","text":"Earlier discussion about historicalNeedle"}]}}),
        json!({"type":"response_item","timestamp":"2026-09-20T12:00:01Z","payload":{"type":"message","id":"event2","role":"assistant","content":[{"type":"output_text","text":"The answer is preserved here."}]}}),
    ];
    let text = records.iter().map(|r| format!("{r}\n")).collect::<String>();
    std::fs::write(sessions.join("test.jsonl"), text).unwrap();
    let mut client = Client::start(&root, &dir.path().join("cache"), &homes);
    let deadline = Instant::now() + Duration::from_secs(15);
    let result = loop {
        let value = client.rpc(
            "tools/call",
            json!({"name":"search_sessions","arguments":{"query":"historicalNeedle"}}),
        );
        assert_ne!(value["result"]["isError"], true, "{value}");
        let result = value["result"]["structuredContent"].clone();
        if result["results"].as_array().is_some_and(|r| !r.is_empty()) {
            break result;
        }
        assert!(
            Instant::now() < deadline,
            "sessions did not converge: {value}"
        );
        thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(result["results"][0]["agent"], "codex");
    let context=client.rpc("tools/call",json!({"name":"search_sessions","arguments":{"mode":"context","match_id":result["results"][0]["match_id"]}}));
    assert_ne!(context["result"]["isError"], true, "{context}");
    assert!(
        context["result"]["structuredContent"]["context"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["excerpt"]
                .as_str()
                .unwrap_or("")
                .contains("preserved here"))
    );
}

#[test]
fn claude_and_copilot_discovery_filters_and_context_reach_mcp() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir(&root).unwrap();
    let homes = dir.path().join("homes");
    let claude = homes.join("claude/projects/project");
    let copilot = homes.join("copilot/session-state/session");
    std::fs::create_dir_all(&claude).unwrap();
    std::fs::create_dir_all(&copilot).unwrap();
    let timestamp = "2026-09-20T12:00:00Z";
    let fixtures = [
        (
            claude.join("conversation.jsonl"),
            vec![
                json!({"type":"user","cwd":root,"sessionId":"claude-session","uuid":"c1","timestamp":timestamp,"message":{"role":"user","content":[{"type":"text","text":"claudehistoryneedle"}]}}),
                json!({"type":"assistant","cwd":root,"sessionId":"claude-session","uuid":"c2","timestamp":timestamp,"message":{"role":"assistant","content":[{"type":"text","text":"claudecontextanswer"}]}}),
            ],
        ),
        (
            copilot.join("events.jsonl"),
            vec![
                json!({"type":"session.start","data":{"context":{"cwd":root},"sessionId":"copilot-session"}}),
                json!({"type":"user.message","id":"p1","timestamp":timestamp,"data":{"content":"copilothistoryneedle"}}),
                json!({"type":"assistant.message","id":"p2","timestamp":timestamp,"data":{"content":"copilotcontextanswer"}}),
            ],
        ),
    ];
    for (path, records) in fixtures {
        std::fs::write(
            path,
            records.iter().map(|r| format!("{r}\n")).collect::<String>(),
        )
        .unwrap();
    }
    let mut client = Client::start(&root, &dir.path().join("cache"), &homes);
    for agent in ["claude", "copilot"] {
        let deadline = Instant::now() + Duration::from_secs(15);
        let query = format!("{agent}historyneedle");
        let result = loop {
            let response = client.rpc("tools/call", json!({"name":"search_sessions","arguments":{"query":query,"agent":agent,"after":"2026-09-20T00:00:00Z","before":"2026-09-21T00:00:00Z"}}));
            assert_ne!(response["result"]["isError"], true, "{response}");
            let result = response["result"]["structuredContent"].clone();
            if result["results"]
                .as_array()
                .is_some_and(|hits| !hits.is_empty())
            {
                break result;
            }
            assert!(
                Instant::now() < deadline,
                "{agent} did not converge: {result}"
            );
            thread::sleep(Duration::from_millis(50));
        };
        assert_eq!(result["results"][0]["agent"], agent);
        let context = client.rpc("tools/call", json!({"name":"search_sessions","arguments":{"mode":"context","match_id":result["results"][0]["match_id"]}}));
        assert_ne!(context["result"]["isError"], true, "{context}");
        let answer = format!("{agent}contextanswer");
        assert!(
            context["result"]["structuredContent"]["context"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["excerpt"].as_str().unwrap_or("").contains(&answer)),
            "{context}"
        );
        let excluded = client.rpc(
            "tools/call",
            json!({"name":"search_sessions","arguments":{"query":query,"agent":"codex"}}),
        );
        assert_eq!(
            excluded["result"]["structuredContent"]["results"],
            json!([])
        );
    }
}

#[test]
fn copied_messages_and_copy_context_are_available_over_mcp() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir(&root).unwrap();
    let homes = dir.path().join("homes");
    let logs = homes.join("codex/sessions");
    std::fs::create_dir_all(&logs).unwrap();
    for index in 0..3 {
        let records = [
            json!({"type":"session_meta","payload":{"id":"copied-session","cwd":root}}),
            json!({"type":"response_item","timestamp":format!("2026-09-20T12:00:0{index}Z"),"payload":{"type":"message","id":"copied-message","role":"user","content":[{"type":"input_text","text":"copiedmessageneedle"}]}}),
            json!({"type":"response_item","payload":{"type":"message","id":format!("answer-{index}"),"role":"assistant","content":[{"type":"output_text","text":format!("context answer {index}")}]}}),
        ];
        std::fs::write(
            logs.join(format!("rollout-copy-{index}.jsonl")),
            records.iter().map(|r| format!("{r}\n")).collect::<String>(),
        )
        .unwrap();
    }
    let mut client = Client::start(&root, &dir.path().join("cache"), &homes);
    let deadline = Instant::now() + Duration::from_secs(15);
    let result = loop {
        let response = client.rpc(
            "tools/call",
            json!({"name":"search_sessions","arguments":{"query":"copiedmessageneedle"}}),
        );
        assert_ne!(response["result"]["isError"], true, "{response}");
        let r = response["result"]["structuredContent"].clone();
        if r["coverage"]["pending_changes"] == 0 && r["results"][0]["copy_count"] == 3 {
            break r;
        }
        assert!(
            Instant::now() < deadline,
            "copied sessions did not reconcile: {r}"
        );
        thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(result["results"].as_array().unwrap().len(), 1);
    let copies = client.rpc("tools/call", json!({"name":"search_sessions","arguments":{"mode":"copies","match_id":result["results"][0]["match_id"]}}));
    assert_ne!(copies["result"]["isError"], true, "{copies}");
    let copies = copies["result"]["structuredContent"]["copies"]
        .as_array()
        .unwrap();
    assert_eq!(copies.len(), 3);
    for copy in copies {
        let context = client.rpc("tools/call", json!({"name":"search_sessions","arguments":{"mode":"context","match_id":copy["match_id"],"before_events":0,"after_events":1}}));
        assert_ne!(context["result"]["isError"], true, "{context}");
        let events = context["result"]["structuredContent"]["context"]
            .as_array()
            .unwrap();
        assert_eq!(events.len(), 2);
        assert!(
            events
                .iter()
                .all(|h| h["source_reference"]["path"] == copy["source_reference"]["path"])
        );
    }
}
