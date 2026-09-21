use crate::{
    ingest,
    model::ScanReport,
    store::Store,
    tools::{self, Coverage},
};
use anyhow::{Context, Result, bail, ensure};
use notify::{RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

const PROTOCOL: u32 = 1;
const FRAME_LIMIT: usize = 262144;

#[derive(Serialize, Deserialize)]
struct Endpoint {
    protocol: u32,
    address: std::net::SocketAddr,
    capability: String,
}
#[derive(Serialize, Deserialize)]
struct Hello {
    protocol: u32,
    capability: String,
    project: PathBuf,
}
#[derive(Serialize, Deserialize)]
struct Request {
    name: String,
    arguments: Value,
}

pub struct OwnerClient {
    reader: BufReader<TcpStream>,
    root: PathBuf,
    cache: PathBuf,
}
impl OwnerClient {
    pub fn connect(root: &Path, cache: &Path) -> Result<Self> {
        let identity = ingest::project_identity(root)?;
        let dir = cache.join(&identity.owner_key);
        private_directory(&dir)?;
        crate::identity::IdentityRegistry::for_cache_dir(cache)?.remember_verified(&identity)?;
        let mut child = None;
        for attempt in 0..100 {
            if let Ok(bytes) = fs::read(dir.join("endpoint.json"))
                && let Ok(endpoint) = serde_json::from_slice::<Endpoint>(&bytes)
                && endpoint.address.ip().is_loopback()
                && let Ok(stream) =
                    TcpStream::connect_timeout(&endpoint.address, Duration::from_millis(100))
            {
                ensure!(
                    endpoint.protocol == PROTOCOL,
                    "incompatible live owner protocol; stop older clients first"
                );
                stream.set_read_timeout(Some(Duration::from_secs(30)))?;
                stream.set_write_timeout(Some(Duration::from_secs(30)))?;
                let mut client = Self {
                    reader: BufReader::new(stream),
                    root: identity.root.clone(),
                    cache: cache.to_path_buf(),
                };
                let hello = Hello {
                    protocol: PROTOCOL,
                    capability: endpoint.capability,
                    project: identity.root.clone(),
                };
                if write_json(client.reader.get_mut(), &hello).is_ok()
                    && read_json::<Value>(&mut client.reader)
                        .ok()
                        .is_some_and(|v| v.get("ok") == Some(&json!(true)))
                {
                    return Ok(client);
                }
            }
            if attempt % 20 == 0 {
                let mut command = Command::new(std::env::current_exe()?);
                command
                    .arg("owner")
                    .arg("--project")
                    .arg(&identity.root)
                    .arg("--cache-dir")
                    .arg(cache)
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::inherit());
                #[cfg(windows)]
                {
                    use std::os::windows::process::CommandExt;
                    command.creation_flags(0x00000200 | 0x08000000);
                }
                child = Some(command.spawn().context("start repository owner")?);
            }
            if let Some(c) = child.as_mut() {
                let _ = c.try_wait();
            }
            thread::sleep(Duration::from_millis(50));
        }
        bail!(
            "owner unavailable after startup retries; run the owner command in a terminal for diagnostics"
        )
    }
    pub fn call(&mut self, name: &str, arguments: Value) -> Result<Value> {
        match self.call_once(name, arguments.clone()) {
            Ok(value) => Ok(value),
            Err(error)
                if error.downcast_ref::<std::io::Error>().is_some()
                    || error.to_string() == "connection closed" =>
            {
                *self = Self::connect(&self.root, &self.cache)?;
                self.call_once(name, arguments)
            }
            Err(error) => Err(error),
        }
    }
    fn call_once(&mut self, name: &str, arguments: Value) -> Result<Value> {
        write_json(
            self.reader.get_mut(),
            &Request {
                name: name.into(),
                arguments,
            },
        )?;
        let response: Value = read_json(&mut self.reader)?;
        if let Some(error) = response.get("error") {
            bail!("{}", error.as_str().unwrap_or("owner_error"));
        }
        Ok(response["result"].clone())
    }
    pub fn heartbeat(&mut self) -> Result<()> {
        self.call("__ping", json!({})).map(|_| ())
    }
}

struct SessionState {
    registry_path: PathBuf,
    in_flight: AtomicBool,
    epoch: AtomicU64,
    scanned: AtomicU64,
    safe_epoch: AtomicU64,
    coverage: Mutex<Coverage>,
    watcher: Mutex<Option<notify::RecommendedWatcher>>,
    watch_error: Mutex<Option<String>>,
}
impl SessionState {
    fn new(registry_path: PathBuf) -> Self {
        Self {
            in_flight: AtomicBool::new(false),
            registry_path,
            epoch: AtomicU64::new(1),
            scanned: AtomicU64::new(0),
            safe_epoch: AtomicU64::new(0),
            coverage: Mutex::new(Coverage::default()),
            watcher: Mutex::new(None),
            watch_error: Mutex::new(None),
        }
    }
}

struct RootState {
    root: PathBuf,
    collection: String,
    lifecycle: Mutex<()>,
    leases: AtomicUsize,
    epoch: AtomicU64,
    scanned: AtomicU64,
    safe_epoch: AtomicU64,
    changed_paths: Mutex<Option<HashSet<PathBuf>>>,
    git_pending: AtomicBool,
    git_snapshot: Mutex<Option<crate::git_changes::Snapshot>>,
    coverage: Mutex<Coverage>,
    sessions: Arc<SessionState>,
    watcher: Mutex<Option<notify::RecommendedWatcher>>,
    watch_error: Mutex<Option<String>>,
}
impl RootState {
    fn new(root: PathBuf, collection: String, sessions: Arc<SessionState>) -> Arc<Self> {
        Arc::new(Self {
            root,
            collection,
            lifecycle: Mutex::new(()),
            leases: AtomicUsize::new(0),
            epoch: AtomicU64::new(1),
            scanned: AtomicU64::new(0),
            safe_epoch: AtomicU64::new(0),
            changed_paths: Mutex::new(None),
            git_pending: AtomicBool::new(false),
            git_snapshot: Mutex::new(None),
            coverage: Mutex::new(Coverage::default()),
            sessions,
            watcher: Mutex::new(None),
            watch_error: Mutex::new(None),
        })
    }
    fn watch(self: &Arc<Self>) -> Result<()> {
        let external_ignores = external_ignore_paths(&self.root);
        let watched_ignores = external_ignores.clone();
        let weak = Arc::downgrade(self);
        let mut watcher =
            notify::recommended_watcher(move |result: notify::Result<notify::Event>| {
                if let Some(state) = weak.upgrade() {
                    let mut pending = state.changed_paths.lock().unwrap();
                    match result {
                        Ok(event) if matches!(event.kind, notify::EventKind::Access(_)) => return,
                        Ok(event) if !event.need_rescan() && !event.paths.is_empty() => {
                            let mut relevant = false;
                            for path in event.paths {
                                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                                let external_ignore = watched_ignores.contains(&path);
                                let metadata = !path.starts_with(&state.root)
                                    || path.components().any(|part| part.as_os_str() == ".git");
                                if metadata
                                    && !external_ignore
                                    && !matches!(name, "HEAD" | "index" | "exclude" | "config")
                                {
                                    continue;
                                }
                                relevant = true;
                                if metadata && matches!(name, "HEAD" | "index") {
                                    state.git_pending.store(true, Ordering::SeqCst);
                                } else if metadata || matches!(name, ".gitignore" | ".ignore") {
                                    *pending = None;
                                } else if let Some(paths) = pending.as_mut() {
                                    paths.insert(path);
                                    if paths.len() > 1024 {
                                        *pending = None;
                                    }
                                }
                            }
                            if !relevant {
                                return;
                            }
                        }
                        _ => *pending = None,
                    }
                    state.epoch.fetch_add(1, Ordering::SeqCst);
                }
            })?;
        watcher.watch(&self.root, RecursiveMode::Recursive)?;
        let mut metadata_dirs = HashSet::new();
        if let Some(common) = ingest::project_identity(&self.root)?.git_common_dir {
            metadata_dirs.insert(common.clone());
            metadata_dirs.insert(common.join("info"));
        }
        if let Some(git_dir) = git_path(&self.root, &["rev-parse", "--absolute-git-dir"]) {
            metadata_dirs.insert(git_dir);
        }
        for ignored in external_ignores {
            if let Some(parent) = ignored.parent() {
                metadata_dirs.insert(parent.to_path_buf());
            }
        }
        for path in metadata_dirs {
            if path.is_dir() {
                watcher.watch(&path, RecursiveMode::NonRecursive)?;
            }
        }
        *self.watcher.lock().unwrap() = Some(watcher);
        let mut session_watcher = self.sessions.watcher.lock().unwrap();
        let weak = Arc::downgrade(&self.sessions);
        let mut watcher =
            notify::recommended_watcher(move |result: notify::Result<notify::Event>| {
                if let Some(state) = weak.upgrade()
                    && result.map_or(true, |event| {
                        !matches!(event.kind, notify::EventKind::Access(_))
                    })
                {
                    state.epoch.fetch_add(1, Ordering::SeqCst);
                }
            })?;
        let config = crate::sessions::SessionConfig::default();
        for path in [
            &config.codex_home,
            &config.claude_config_dir,
            &config.copilot_home,
        ] {
            if path.is_dir() {
                watcher.watch(path, RecursiveMode::Recursive)?;
            }
        }
        if let Some(parent) = self.sessions.registry_path.parent() {
            watcher.watch(parent, RecursiveMode::NonRecursive)?;
        }
        *session_watcher = Some(watcher);
        Ok(())
    }
}

fn git_path(root: &Path, args: &[&str]) -> Option<PathBuf> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .env("GIT_OPTIONAL_LOCKS", "0")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let path = PathBuf::from(value.trim());
    if path.as_os_str().is_empty() {
        return None;
    }
    Some(if path.is_absolute() {
        path
    } else {
        root.join(path)
    })
}

fn external_ignore_paths(root: &Path) -> HashSet<PathBuf> {
    let mut paths = HashSet::new();
    if let Some(path) = git_path(root, &["config", "--path", "--get", "core.excludesFile"]) {
        paths.insert(path);
    } else if let Some(home) = dirs::home_dir() {
        let config = std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".config"));
        paths.insert(config.join("git").join("ignore"));
    }
    paths
}

struct MemoryState {
    target: u64,
    observed: AtomicU64,
    peak: AtomicU64,
}
impl MemoryState {
    fn snapshot(&self) -> tools::MemoryCoverage {
        let observed = self.observed.load(Ordering::Relaxed);
        tools::MemoryCoverage {
            target_bytes: self.target,
            observed_rss_bytes: (observed != 0).then_some(observed),
            peak_observed_rss_bytes: self.peak.load(Ordering::Relaxed),
            pressure: observed > self.target,
        }
    }
}

pub fn run_owner(root: &Path, cache: &Path) -> Result<()> {
    let identity = ingest::project_identity(root)?;
    let dir = cache.join(&identity.owner_key);
    private_directory(&dir)?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.join("owner.lock"))?;
    if lock.try_lock().is_err() {
        return Ok(());
    }
    let store = Store::open(&dir.join("index.sqlite3"))?;
    let memory_mib = std::env::var("BM25_MCP_MEMORY_MIB")
        .ok()
        .map(|v| v.parse::<u64>())
        .transpose()?
        .unwrap_or(512);
    ensure!(
        memory_mib > 0 && memory_mib <= u64::MAX / (1024 * 1024),
        "invalid BM25_MCP_MEMORY_MIB"
    );
    let memory = Arc::new(MemoryState {
        target: memory_mib * 1024 * 1024,
        observed: AtomicU64::new(0),
        peak: AtomicU64::new(0),
    });
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
    listener.set_nonblocking(true)?;
    let endpoint = Endpoint {
        protocol: PROTOCOL,
        address: listener.local_addr()?,
        capability: (0..4)
            .map(|_| format!("{:016x}", rand::random::<u64>()))
            .collect(),
    };
    let endpoint_path = dir.join("endpoint.json");
    write_private(&endpoint_path, &serde_json::to_vec(&endpoint)?)?;
    let roots: Arc<Mutex<HashMap<String, Arc<RootState>>>> = Arc::new(Mutex::new(HashMap::new()));
    let sessions = Arc::new(SessionState::new(cache.join("identity-registry.json")));
    let live = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let monitor_memory = memory.clone();
    let monitor_store = store.clone();
    let monitor_stop = stop.clone();
    let monitor = thread::spawn(move || {
        let mut system = sysinfo::System::new();
        let Ok(pid) = sysinfo::get_current_pid() else {
            return;
        };
        while !monitor_stop.load(Ordering::SeqCst) {
            system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
            if let Some(process) = system.process(pid) {
                let rss = process.memory();
                monitor_memory.observed.store(rss, Ordering::Relaxed);
                monitor_memory.peak.fetch_max(rss, Ordering::Relaxed);
                monitor_store.set_memory_pressure(rss > monitor_memory.target);
            }
            thread::sleep(Duration::from_millis(500));
        }
    });
    let worker_memory = memory.clone();
    let worker_sessions = sessions.clone();
    let worker_roots = roots.clone();
    let worker_stop = stop.clone();
    let worker_store = store.clone();
    let content_cache_path = dir.join("content-cache");
    let worker = thread::spawn(move || {
        let mut content_cache = crate::content_cache::ContentCache::open(&content_cache_path).ok();
        let mut last_full = Instant::now();
        while !worker_stop.load(Ordering::SeqCst) {
            let states: Vec<_> = worker_roots.lock().unwrap().values().cloned().collect();
            if worker_sessions.in_flight.load(Ordering::SeqCst) {
                last_full = Instant::now();
            }
            let force = last_full.elapsed() > Duration::from_secs(30);
            if force {
                last_full = Instant::now();
                worker_sessions.epoch.fetch_add(1, Ordering::SeqCst);
            }
            for state in states {
                if worker_stop.load(Ordering::SeqCst) || state.leases.load(Ordering::SeqCst) == 0 {
                    continue;
                }
                if force {
                    *state.changed_paths.lock().unwrap() = None;
                    state.epoch.fetch_add(1, Ordering::SeqCst);
                }
                let epoch = state.epoch.load(Ordering::SeqCst);
                if epoch != state.scanned.load(Ordering::SeqCst) {
                    let mut paths = state.changed_paths.lock().unwrap().replace(HashSet::new());
                    if state.git_pending.swap(false, Ordering::SeqCst) && paths.is_some() {
                        let next = crate::git_changes::Snapshot::capture(&state.root);
                        let git_paths =
                            state
                                .git_snapshot
                                .lock()
                                .unwrap()
                                .as_ref()
                                .and_then(|previous| {
                                    next.as_ref()
                                        .and_then(|next| previous.changed_paths(&state.root, next))
                                });
                        if let Some(changes) = git_paths {
                            paths.as_mut().unwrap().extend(changes);
                        } else {
                            paths = None;
                        }
                    }
                    let git_before = crate::git_changes::Snapshot::capture(&state.root);
                    let should_continue = || {
                        !worker_stop.load(Ordering::SeqCst)
                            && state.leases.load(Ordering::SeqCst) > 0
                    };
                    let invalidation = if paths.is_none() {
                        worker_store
                            .invalidate_collection(&state.collection, ingest::PROJECT_SOURCE_KIND)
                    } else {
                        worker_store
                            .sources(&state.collection, ingest::PROJECT_SOURCE_KIND)
                            .and_then(|sources| {
                                for source in sources {
                                    if paths.as_ref().is_some_and(|paths| {
                                        paths
                                            .iter()
                                            .any(|p| state.root.join(&source.path).starts_with(p))
                                    }) {
                                        worker_store.invalidate_source(&source.key)?;
                                    }
                                }
                                Ok(())
                            })
                    };
                    if invalidation.is_ok() {
                        state.safe_epoch.store(epoch, Ordering::SeqCst);
                    }
                    let full_scan = paths.is_none();
                    let result = invalidation.and_then(|()| {
                        ingest::scan_project_controlled_with_cache(
                            &state.root,
                            &worker_store,
                            &state.collection,
                            paths.as_ref(),
                            &should_continue,
                            content_cache.as_mut(),
                        )
                    });
                    let completed = result.as_ref().is_ok_and(|report| !report.cancelled);
                    if state.epoch.load(Ordering::SeqCst) == epoch && result.is_ok() {
                        let git_after = crate::git_changes::Snapshot::capture(&state.root);
                        if state.epoch.load(Ordering::SeqCst) == epoch
                            && git_before
                                .as_ref()
                                .zip(git_after.as_ref())
                                .is_some_and(|(a, b)| a.same_revision(b))
                        {
                            *state.git_snapshot.lock().unwrap() = git_after;
                        }
                    }
                    if full_scan {
                        if should_continue() {
                            let _lifecycle = state.lifecycle.lock().unwrap();
                            match state.watch() {
                                Ok(()) => {
                                    *state.watch_error.lock().unwrap() = None;
                                    *state.sessions.watch_error.lock().unwrap() = None;
                                }
                                Err(error) => {
                                    let error = format!("watcher_unavailable: {error}");
                                    *state.watch_error.lock().unwrap() = Some(error.clone());
                                    *state.sessions.watch_error.lock().unwrap() = Some(error);
                                }
                            }
                        }
                        if result.is_ok() && should_continue() {
                            let _ = worker_store.compact();
                        }
                        if completed {
                            last_full = Instant::now();
                        }
                    }
                    *state.coverage.lock().unwrap() =
                        with_watch_error(coverage(result), &state.watch_error);
                    if completed && state.safe_epoch.load(Ordering::SeqCst) == epoch {
                        state.scanned.store(epoch, Ordering::SeqCst);
                    } else {
                        *state.changed_paths.lock().unwrap() = None;
                    }
                }
            }
            thread::sleep(Duration::from_millis(
                if worker_memory.snapshot().pressure {
                    500
                } else {
                    100
                },
            ));
        }
    });
    let session_store = store.clone();
    let session_roots = roots.clone();
    let session_stop = stop.clone();
    let session_state = sessions.clone();
    let session_memory = memory.clone();
    let owner_key = identity.owner_key.clone();
    let session_config = crate::sessions::SessionConfig {
        identity_registry_path: Some(cache.join("identity-registry.json")),
        ..crate::sessions::SessionConfig::default()
    };
    let session_worker = thread::spawn(move || {
        while !session_stop.load(Ordering::SeqCst) {
            let states: Vec<_> = session_roots.lock().unwrap().values().cloned().collect();
            let active = states.iter().find(|s| s.leases.load(Ordering::SeqCst) > 0);
            if let Some(state) = active {
                let epoch = session_state.epoch.load(Ordering::SeqCst);
                let can_continue = || {
                    !session_stop.load(Ordering::SeqCst)
                        && states.iter().any(|s| s.leases.load(Ordering::SeqCst) > 0)
                        && states.iter().all(|s| {
                            s.leases.load(Ordering::SeqCst) == 0
                                || s.epoch.load(Ordering::SeqCst)
                                    == s.scanned.load(Ordering::SeqCst)
                        })
                };
                if epoch != session_state.scanned.load(Ordering::SeqCst) && can_continue() {
                    session_state.in_flight.store(true, Ordering::SeqCst);
                    let invalidation = session_store.invalidate_collection(&owner_key, "session");
                    if invalidation.is_ok() {
                        session_state.safe_epoch.store(epoch, Ordering::SeqCst);
                    }
                    let result = invalidation.and_then(|()| {
                        crate::sessions::scan_sessions_controlled(
                            &state.root,
                            &owner_key,
                            &session_store,
                            &session_config,
                            &can_continue,
                        )
                    });
                    let completed = result.as_ref().is_ok_and(|report| !report.cancelled);
                    if can_continue() {
                        *session_state.coverage.lock().unwrap() =
                            with_watch_error(coverage(result), &session_state.watch_error);
                        if completed
                            && session_state.epoch.load(Ordering::SeqCst) == epoch
                            && session_state.safe_epoch.load(Ordering::SeqCst) == epoch
                        {
                            session_state.scanned.store(epoch, Ordering::SeqCst);
                        }
                    }
                    session_state.in_flight.store(false, Ordering::SeqCst);
                }
            }
            thread::sleep(Duration::from_millis(
                if session_memory.snapshot().pressure {
                    500
                } else {
                    100
                },
            ));
        }
    });
    let mut idle = Instant::now();
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                let memory = memory.clone();
                let sessions = sessions.clone();
                let roots = roots.clone();
                let live = live.clone();
                let store = store.clone();
                let capability = endpoint.capability.clone();
                let owner_key = identity.owner_key.clone();
                live.fetch_add(1, Ordering::SeqCst);
                thread::spawn(move || {
                    struct ConnectionCount(Arc<AtomicUsize>);
                    impl Drop for ConnectionCount {
                        fn drop(&mut self) {
                            self.0.fetch_sub(1, Ordering::SeqCst);
                        }
                    }
                    let _connection_count = ConnectionCount(live);
                    let result = serve_connection(
                        stream,
                        &capability,
                        &owner_key,
                        &store,
                        &roots,
                        &memory,
                        &sessions,
                    );
                    if let Err(error) = result
                        && error.to_string() != "connection closed"
                    {
                        let _ = writeln!(std::io::stderr(), "client connection ended: {error:#}");
                    }
                });
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => return Err(e.into()),
        }
        if live.load(Ordering::SeqCst) > 0 {
            idle = Instant::now();
        }
        if idle.elapsed() > Duration::from_secs(3) {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    stop.store(true, Ordering::SeqCst);
    let _ = worker.join();
    let _ = session_worker.join();
    let _ = monitor.join();
    fs::remove_file(endpoint_path).ok();
    drop(lock);
    Ok(())
}

fn serve_connection(
    stream: TcpStream,
    capability: &str,
    owner_key: &str,
    store: &Store,
    roots: &Mutex<HashMap<String, Arc<RootState>>>,
    memory: &MemoryState,
    sessions: &Arc<SessionState>,
) -> Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(15)))?;
    stream.set_write_timeout(Some(Duration::from_secs(15)))?;
    let mut reader = BufReader::new(stream);
    let hello: Hello = read_json(&mut reader)?;
    ensure!(
        hello.protocol == PROTOCOL && hello.capability == capability,
        "invalid owner capability or protocol"
    );
    let identity = ingest::project_identity(&hello.project)?;
    ensure!(
        identity.owner_key == owner_key,
        "project belongs to another owner"
    );
    let state = {
        let mut guard = roots.lock().unwrap();
        guard
            .entry(identity.collection.clone())
            .or_insert_with(|| RootState::new(identity.root, identity.collection, sessions.clone()))
            .clone()
    };
    {
        let _lifecycle = state.lifecycle.lock().unwrap();
        if state.leases.fetch_add(1, Ordering::SeqCst) == 0 {
            *state.changed_paths.lock().unwrap() = None;
            state.sessions.epoch.fetch_add(1, Ordering::SeqCst);
            state.epoch.fetch_add(1, Ordering::SeqCst);
            if let Err(error) = state.watch() {
                let error = format!("watcher_unavailable: {error}");
                *state.watch_error.lock().unwrap() = Some(error.clone());
                *state.sessions.watch_error.lock().unwrap() = Some(error);
            } else {
                *state.watch_error.lock().unwrap() = None;
                *state.sessions.watch_error.lock().unwrap() = None;
            }
        }
    }
    struct Lease(Arc<RootState>);
    impl Drop for Lease {
        fn drop(&mut self) {
            let _lifecycle = self.0.lifecycle.lock().unwrap();
            if self.0.leases.fetch_sub(1, Ordering::SeqCst) == 1 {
                self.0.watcher.lock().unwrap().take();
            }
        }
    }
    let _lease = Lease(state.clone());
    write_json(reader.get_mut(), &json!({"ok":true}))?;
    loop {
        let request: Request = read_json(&mut reader)?;
        if request.name == "__ping" {
            write_json(reader.get_mut(), &json!({"result":{}}))?;
            continue;
        }
        let sessions = request.name == "search_sessions";
        let start_epoch = state.epoch.load(Ordering::SeqCst);
        let session_epoch = state.sessions.epoch.load(Ordering::SeqCst);
        let ready = if sessions {
            session_epoch == state.sessions.scanned.load(Ordering::SeqCst)
        } else {
            start_epoch == state.scanned.load(Ordering::SeqCst)
        };
        let mut coverage = if sessions {
            state.sessions.coverage.lock().unwrap().clone()
        } else {
            state.coverage.lock().unwrap().clone()
        };
        coverage.memory = Some(memory.snapshot());
        if !ready {
            coverage.pending_changes = None;
        }
        let status = search_status(ready, &coverage);
        let result = tools::dispatch(
            store,
            &state.collection,
            owner_key,
            &request.name,
            request.arguments,
            status,
            coverage,
            ready
                || if sessions {
                    session_epoch == state.sessions.safe_epoch.load(Ordering::SeqCst)
                } else {
                    start_epoch == state.safe_epoch.load(Ordering::SeqCst)
                },
        );
        let result = result.map(|mut value| {
            if (!sessions && state.epoch.load(Ordering::SeqCst) != start_epoch)
                || (sessions && state.sessions.epoch.load(Ordering::SeqCst) != session_epoch)
            {
                if value.get("copies").is_some() {
                    value["copies"] = json!([]);
                    value.as_object_mut().unwrap().remove("cursor");
                } else if value.get("context").is_some() {
                    value["context"] = json!([]);
                    value.as_object_mut().unwrap().remove("cursor");
                } else {
                    value["results"] = json!([]);
                }
                value["status"] = json!("refreshing");
                value["coverage"]["pending_changes"] = Value::Null;
            }
            value
        });
        let response = match result {
            Ok(value) => json!({"result":value}),
            Err(error) => json!({"error":format!("{error:#}")}),
        };
        write_json(reader.get_mut(), &response)?;
    }
}

fn with_watch_error(mut coverage: Coverage, error: &Mutex<Option<String>>) -> Coverage {
    if let Some(error) = error.lock().unwrap().as_ref() {
        coverage.error_count += 1;
        coverage.errors.push(error.clone());
        *coverage
            .diagnostics
            .entry("watcher_unavailable".into())
            .or_default() += 1;
    }
    coverage
}

fn search_status(ready: bool, coverage: &Coverage) -> &'static str {
    if !ready {
        if coverage.reconciled_at.is_none() {
            "building"
        } else {
            "refreshing"
        }
    } else if coverage.pending_changes.is_some_and(|count| count > 0) {
        "refreshing"
    } else if coverage.error_count > 0 {
        "degraded"
    } else {
        "ready"
    }
}

fn coverage(result: Result<ScanReport>) -> Coverage {
    match result {
        Ok(report) => Coverage {
            diagnostics: report.diagnostics,
            reconciled_at: Some(chrono::Utc::now().to_rfc3339()),
            pending_changes: Some(report.pending_count),
            excluded_count: report.excluded_count,
            error_count: report.error_count,
            errors: report.errors,
            memory: None,
        },
        Err(error) => Coverage {
            error_count: 1,
            errors: vec![format!("{error:#}")],
            ..Coverage::default()
        },
    }
}

fn private_directory(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
fn write_json(writer: &mut impl Write, value: &impl Serialize) -> Result<()> {
    serde_json::to_writer(&mut *writer, value)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}
fn read_json<T: serde::de::DeserializeOwned>(reader: &mut impl BufRead) -> Result<T> {
    let mut bytes = Vec::new();
    loop {
        let available = reader.fill_buf()?;
        ensure!(!available.is_empty(), "connection closed");
        let n = available
            .iter()
            .position(|b| *b == b'\n')
            .map(|i| i + 1)
            .unwrap_or(available.len());
        ensure!(bytes.len() + n <= FRAME_LIMIT, "IPC frame too large");
        bytes.extend_from_slice(&available[..n]);
        reader.consume(n);
        if bytes.last() == Some(&b'\n') {
            return Ok(serde_json::from_slice(&bytes)?);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_ready_status_takes_precedence_over_diagnostics() {
        let mut coverage = Coverage {
            error_count: 1,
            ..Coverage::default()
        };
        assert_eq!(search_status(false, &coverage), "building");
        coverage.reconciled_at = Some("2026-09-21T00:00:00Z".to_owned());
        assert_eq!(search_status(false, &coverage), "refreshing");
        assert_eq!(search_status(true, &coverage), "degraded");
        coverage.pending_changes = Some(1);
        assert_eq!(search_status(true, &coverage), "refreshing");
    }
}
