use bm25_mcp::ingest::project_identity;
use serde_json::{Value, json};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

struct Client {
    child: Child,
    input: ChildStdin,
    output: BufReader<std::process::ChildStdout>,
    next_id: u64,
}

impl Client {
    fn start(root: &Path, cache_dir: &Path, provider_home: &Path) -> Self {
        let binary = env!("CARGO_BIN_EXE_bm25-mcp");
        let mut command = Command::new(binary);
        command
            .args(["serve", "--project"])
            .arg(root)
            .args(["--cache-dir"])
            .arg(cache_dir)
            .env("CODEX_HOME", provider_home.join("codex"))
            .env("CLAUDE_CONFIG_DIR", provider_home.join("claude"))
            .env("COPILOT_HOME", provider_home.join("copilot"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        let mut child = command.spawn().expect("spawn server");
        let input = child.stdin.take().expect("server stdin");
        let output = BufReader::new(child.stdout.take().expect("server stdout"));
        let mut client = Self {
            child,
            input,
            output,
            next_id: 1,
        };

        let initialized = client.rpc(
            "initialize",
            json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "recovery-test", "version": "1"}
            }),
        );
        assert!(
            initialized.get("result").is_some(),
            "initialize failed: {initialized}"
        );
        client.notify("notifications/initialized", json!({}));
        client
    }

    fn rpc(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let request = json!({"jsonrpc":"2.0", "id": id, "method": method, "params": params});
        writeln!(self.input, "{request}").expect("write request");
        self.input.flush().expect("flush request");
        loop {
            let mut line = String::new();
            self.output.read_line(&mut line).expect("read response");
            assert!(!line.is_empty(), "server exited while waiting for {method}");
            let response: Value = serde_json::from_str(&line).expect("JSON-RPC response");
            if response.get("id") == Some(&json!(id)) {
                return response;
            }
        }
    }

    fn notify(&mut self, method: &str, params: Value) {
        let request = json!({"jsonrpc":"2.0", "method": method, "params": params});
        writeln!(self.input, "{request}").expect("write notification");
        self.input.flush().expect("flush notification");
    }

    fn tool(&mut self, name: &str, arguments: Value) -> Value {
        self.rpc("tools/call", json!({"name": name, "arguments": arguments}))
    }

    fn search_project(&mut self, query: &str) -> Value {
        self.tool("search_project", json!({"query": query, "limit": 10}))
    }

    fn search_sessions(&mut self, query: &str) -> Value {
        self.tool("search_sessions", json!({"query": query, "limit": 10}))
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn temp_workspace() -> (TempDir, PathBuf, PathBuf, PathBuf) {
    let temp = tempfile::tempdir().expect("temporary workspace");
    let root = temp.path().join("project");
    let cache = temp.path().join("cache");
    let homes = temp.path().join("provider-homes");
    fs::create_dir_all(&root).expect("project directory");
    fs::create_dir_all(&cache).expect("cache directory");
    fs::create_dir_all(&homes).expect("provider home directory");
    (temp, root, cache, homes)
}

fn wait_for<T>(description: &str, mut check: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(value) = check() {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {description}"
        );
        thread::sleep(Duration::from_millis(100));
    }
}

fn result_items(response: &Value) -> Vec<Value> {
    response["result"]["content"][0]["text"]
        .as_str()
        .and_then(|text| serde_json::from_str(text).ok())
        .and_then(|payload: Value| payload["results"].as_array().cloned())
        .unwrap_or_default()
}

fn owner_directory(cache_dir: &Path, root: &Path) -> PathBuf {
    let identity = project_identity(root).expect("project identity");
    cache_dir.join(identity.owner_key)
}

fn wait_owner_stopped(owner_dir: &Path) {
    let endpoint = owner_dir.join("endpoint.json");
    let lock_path = owner_dir.join("owner.lock");
    wait_for("owner endpoint removal and lock release", || {
        if endpoint.exists() {
            return None;
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)
            .ok()?;
        if lock.try_lock().is_ok() {
            Some(())
        } else {
            None
        }
    });
}

fn run_git(root: &Path, args: &[&str]) {
    let output = Command::new("git")
        .args(args)
        .current_dir(root)
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn owner_releases_endpoint_and_lock_after_last_client_disconnects() {
    let (_temp, root, cache, homes) = temp_workspace();
    fs::write(root.join("main.rs"), "owner_cleanup_marker").expect("source fixture");
    let mut client = Client::start(&root, &cache, &homes);
    wait_for("initial project result", || {
        let items = result_items(&client.search_project("owner_cleanup_marker"));
        (!items.is_empty()).then_some(())
    });

    let owner_dir = owner_directory(&cache, &root);
    assert!(
        owner_dir.join("endpoint.json").exists(),
        "owner endpoint missing"
    );
    drop(client);
    wait_owner_stopped(&owner_dir);
}

#[test]
fn restarted_owner_recovers_persisted_index_after_offline_edit() {
    let (_temp, root, cache, homes) = temp_workspace();
    let source = root.join("main.rs");
    fs::write(&source, "recoveryolduniquealpha").expect("initial source fixture");

    let mut first = Client::start(&root, &cache, &homes);
    wait_for("initial persisted result", || {
        let items = result_items(&first.search_project("recoveryolduniquealpha"));
        (!items.is_empty()).then_some(())
    });
    let owner_dir = owner_directory(&cache, &root);
    drop(first);
    wait_owner_stopped(&owner_dir);

    fs::write(&source, "recoverynewuniquebeta").expect("offline source edit");
    let mut second = Client::start(&root, &cache, &homes);
    wait_for("recovered offline edit", || {
        let items = result_items(&second.search_project("recoverynewuniquebeta"));
        (!items.is_empty()).then_some(())
    });
    assert!(
        result_items(&second.search_project("recoveryolduniquealpha")).is_empty(),
        "stale term survived the offline replacement"
    );
}

#[test]
fn git_worktrees_share_sessions_but_keep_code_collections_separate() {
    let (_temp, primary, cache, homes) = temp_workspace();
    run_git(&primary, &["init", "-q"]);
    run_git(&primary, &["config", "user.email", "recovery@example.test"]);
    run_git(&primary, &["config", "user.name", "Recovery Test"]);
    fs::write(primary.join("README.md"), "initial").expect("initial git fixture");
    run_git(&primary, &["add", "."]);
    run_git(&primary, &["commit", "-qm", "initial"]);
    let worktree = primary
        .parent()
        .expect("workspace parent")
        .join("secondary-worktree");
    run_git(
        &primary,
        &[
            "worktree",
            "add",
            "-q",
            "-b",
            "recovery-secondary",
            worktree.to_str().expect("worktree path"),
        ],
    );
    fs::write(primary.join("primaryalpha.rs"), "primaryalpha").expect("primary worktree fixture");
    fs::write(
        worktree.join("secondary_worktree_marker.rs"),
        "secondarybeta",
    )
    .expect("secondary worktree fixture");

    let sessions = homes.join("codex").join("sessions");
    fs::create_dir_all(&sessions).expect("session fixture directory");
    let session_path = sessions.join("shared.jsonl");
    let session_meta = json!({
        "type": "session_meta",
        "payload": {"id": "shared-recovery-session", "cwd": primary}
    });
    let session_message = json!({
        "type": "response_item",
        "payload": {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "shared_worktree_session_marker"}]
        }
    });
    fs::write(
        session_path,
        format!("{}\n{}\n", session_meta, session_message),
    )
    .expect("session fixture");

    let mut primary_client = Client::start(&primary, &cache, &homes);
    wait_for("primary worktree code result", || {
        let items = result_items(&primary_client.search_project("primaryalpha"));
        (!items.is_empty()).then_some(())
    });
    let primary_secondary = primary_client.search_project("secondarybeta");
    assert!(
        result_items(&primary_secondary).is_empty(),
        "primary worktree received secondary code"
    );

    let mut secondary_client = Client::start(&worktree, &cache, &homes);
    wait_for("secondary worktree code result", || {
        let items = result_items(&secondary_client.search_project("secondarybeta"));
        (!items.is_empty()).then_some(())
    });
    assert!(
        result_items(&secondary_client.search_project("primaryalpha")).is_empty(),
        "secondary worktree received primary code"
    );
    wait_for("shared session result", || {
        let items =
            result_items(&secondary_client.search_sessions("shared_worktree_session_marker"));
        (!items.is_empty()).then_some(())
    });
}

#[test]
fn live_frontend_recovers_after_its_owner_process_crashes() {
    let (_temp, root, cache, homes) = temp_workspace();
    fs::write(root.join("source.txt"), "beforeownerkillmarker").unwrap();
    let mut owner = Command::new(env!("CARGO_BIN_EXE_bm25-mcp"))
        .arg("owner")
        .arg("--project")
        .arg(&root)
        .arg("--cache-dir")
        .arg(&cache)
        .env("CODEX_HOME", homes.join("codex"))
        .env("CLAUDE_CONFIG_DIR", homes.join("claude"))
        .env("COPILOT_HOME", homes.join("copilot"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let owner_dir = owner_directory(&cache, &root);
    wait_for("explicit owner startup", || {
        owner_dir.join("endpoint.json").exists().then_some(())
    });
    let mut client = Client::start(&root, &cache, &homes);
    wait_for("pre-crash source", || {
        (!result_items(&client.search_project("beforeownerkillmarker")).is_empty()).then_some(())
    });
    owner.kill().unwrap();
    owner.wait().unwrap();
    fs::write(root.join("source.txt"), "afterownerkillmarker").unwrap();
    wait_for("automatic owner replacement", || {
        (!result_items(&client.search_project("afterownerkillmarker")).is_empty()).then_some(())
    });
    assert!(result_items(&client.search_project("beforeownerkillmarker")).is_empty());
}

#[test]
fn concurrent_frontends_replace_a_stale_endpoint_and_share_one_owner() {
    let (_temp, root, cache, homes) = temp_workspace();
    fs::write(root.join("source.txt"), "concurrentattachmarker").unwrap();
    let owner_dir = owner_directory(&cache, &root);
    fs::create_dir_all(&owner_dir).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    fs::write(
        owner_dir.join("endpoint.json"),
        json!({"protocol":1,"address":address,"capability":"stale"}).to_string(),
    )
    .unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let handles: Vec<_> = (0..2)
        .map(|_| {
            let (root, cache, homes, barrier) =
                (root.clone(), cache.clone(), homes.clone(), barrier.clone());
            thread::spawn(move || {
                barrier.wait();
                Client::start(&root, &cache, &homes)
            })
        })
        .collect();
    let mut clients: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    for client in &mut clients {
        wait_for("shared owner result", || {
            let items = result_items(&client.search_project("concurrentattachmarker"));
            (items.len() == 1).then_some(())
        });
    }
    let endpoint: Value =
        serde_json::from_slice(&fs::read(owner_dir.join("endpoint.json")).unwrap()).unwrap();
    assert_ne!(endpoint["capability"], "stale");
    drop(clients.remove(0));
    assert_eq!(
        result_items(&clients[0].search_project("concurrentattachmarker")).len(),
        1
    );
}
