use crate::{
    coverage::CollectionCoverage,
    ingest,
    progress::ProgressReporter,
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
    pub fn status(&mut self) -> Result<Value> {
        self.call("__status", json!({}))
    }
}

struct SessionState {
    registry_path: PathBuf,
    in_flight: AtomicBool,
    epoch: AtomicU64,
    scanned: AtomicU64,
    safe_epoch: AtomicU64,
    changed_paths: Mutex<Option<HashSet<PathBuf>>>,
    force_full: AtomicBool,
    coverage: Mutex<Coverage>,
    progress: ProgressReporter,
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
            changed_paths: Mutex::new(None),
            force_full: AtomicBool::new(false),
            coverage: Mutex::new(Coverage::default()),
            progress: ProgressReporter::new(),
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
    progress: ProgressReporter,
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
            progress: ProgressReporter::new(),
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
                            if !project_event_is_precise(&event, &state.root) {
                                *pending = None;
                            }
                            if matches!(
                                event.kind,
                                notify::EventKind::Create(notify::event::CreateKind::Folder)
                                    | notify::EventKind::Remove(notify::event::RemoveKind::Folder)
                            ) {
                                *pending = None;
                            }
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
                                if path.is_dir() {
                                    *pending = None;
                                } else if metadata && matches!(name, "HEAD" | "index") {
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
                        Err(_) => {
                            *state.watch_error.lock().unwrap() = Some("watcher_unavailable".into());
                            *pending = None;
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
        let config = crate::sessions::SessionConfig::default();
        let provider_roots = [
            normalize_watch_path(&config.codex_home),
            normalize_watch_path(&config.claude_config_dir),
            normalize_watch_path(&config.copilot_home),
        ];
        let registry_path = normalize_watch_path(&self.sessions.registry_path);
        let mut watcher =
            notify::recommended_watcher(move |result: notify::Result<notify::Event>| {
                let Some(state) = weak.upgrade() else {
                    return;
                };
                let scope = match result {
                    Ok(event) if matches!(event.kind, notify::EventKind::Access(_)) => return,
                    Ok(event) if is_registry_auxiliary_event(&event, &registry_path) => return,
                    Ok(event) if is_owner_cache_directory_event(&event, &registry_path) => return,
                    Ok(event) => session_event_scope(&event, &provider_roots, &registry_path),
                    Err(_) => {
                        *state.watch_error.lock().unwrap() = Some("watcher_unavailable".into());
                        None
                    }
                };
                let mut pending = state.changed_paths.lock().unwrap();
                match scope {
                    Some(changed) => {
                        if let Some(paths) = pending.as_mut() {
                            paths.extend(changed);
                            if paths.len() > 1024 {
                                *pending = None;
                            }
                        }
                    }
                    None => *pending = None,
                }
                state.epoch.fetch_add(1, Ordering::SeqCst);
            })?;
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

fn project_rename_is_precise(event: &notify::Event, root: &Path) -> bool {
    let notify::EventKind::Modify(notify::event::ModifyKind::Name(mode)) = event.kind else {
        return true;
    };
    matches!(mode, notify::event::RenameMode::Both)
        && event.paths.len() == 2
        && event.paths.iter().any(|path| path.is_file())
        && event.paths.iter().all(|path| {
            path.starts_with(root)
                && !path.is_dir()
                && !path.components().any(|part| part.as_os_str() == ".git")
        })
}

fn project_event_is_precise(event: &notify::Event, root: &Path) -> bool {
    if !project_rename_is_precise(event, root) {
        return false;
    }
    for path in &event.paths {
        if !path.starts_with(root) || path.components().any(|part| part.as_os_str() == ".git") {
            continue;
        }
        match event.kind {
            notify::EventKind::Create(
                notify::event::CreateKind::Any | notify::event::CreateKind::File,
            )
            | notify::EventKind::Modify(
                notify::event::ModifyKind::Any
                | notify::event::ModifyKind::Data(_)
                | notify::event::ModifyKind::Metadata(_),
            ) if !path.is_file() => return false,
            notify::EventKind::Remove(notify::event::RemoveKind::Any) if !path.is_file() => {
                return false;
            }
            _ => {}
        }
    }
    true
}

fn is_registry_auxiliary_event(event: &notify::Event, registry_path: &Path) -> bool {
    if event.paths.is_empty() {
        return false;
    }
    let lock_path = registry_path.with_extension("json.lock");
    let temp_prefix = registry_path
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| format!(".{name}-tmp-"));
    event.paths.iter().all(|path| {
        let path = normalize_watch_path(path);
        path == lock_path
            || temp_prefix.as_ref().is_some_and(|prefix| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(prefix))
            })
    })
}

fn is_owner_cache_directory_event(event: &notify::Event, registry_path: &Path) -> bool {
    let Some(parent) = registry_path.parent() else {
        return false;
    };
    let directory_event = matches!(
        event.kind,
        notify::EventKind::Create(notify::event::CreateKind::Folder)
            | notify::EventKind::Modify(notify::event::ModifyKind::Metadata(_))
            | notify::EventKind::Remove(notify::event::RemoveKind::Folder)
    );
    directory_event
        && !event.paths.is_empty()
        && event.paths.iter().all(|path| {
            let path = normalize_watch_path(path);
            path.parent() == Some(parent)
                && path != registry_path
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.len() == 64 && name.bytes().all(|byte| byte.is_ascii_hexdigit())
                    })
        })
}

fn session_event_scope(
    event: &notify::Event,
    provider_roots: &[PathBuf],
    registry_path: &Path,
) -> Option<HashSet<PathBuf>> {
    if event.need_rescan() || event.paths.is_empty() {
        return None;
    }
    let registry_parent = registry_path.parent();
    let normalized_paths = event
        .paths
        .iter()
        .map(|path| (path, normalize_watch_path(path)))
        .collect::<Vec<_>>();
    if normalized_paths.iter().any(|(_, path)| {
        path == registry_path
            || registry_parent.is_some_and(|parent| path == parent)
            || path.starts_with(registry_path)
    }) {
        return None;
    }
    let precise_rename = matches!(
        event.kind,
        notify::EventKind::Modify(notify::event::ModifyKind::Name(
            notify::event::RenameMode::Both
        ))
    ) && event.paths.len() == 2
        && normalized_paths
            .iter()
            .any(|(original, path)| original.is_file() || path.is_file());
    if matches!(
        event.kind,
        notify::EventKind::Modify(notify::event::ModifyKind::Name(_))
    ) && !precise_rename
    {
        return None;
    }
    let mut paths = HashSet::new();
    for (original, path) in normalized_paths {
        let provider_file = provider_roots
            .iter()
            .any(|root| path.starts_with(root) && path.as_path() != root.as_path())
            && path
                .extension()
                .is_some_and(|extension| extension == "jsonl");
        if !provider_file || original.is_dir() || path.is_dir() {
            return None;
        }
        let existing_file = original.is_file() || path.is_file();
        let precise = match event.kind {
            notify::EventKind::Create(
                notify::event::CreateKind::Any | notify::event::CreateKind::File,
            )
            | notify::EventKind::Modify(
                notify::event::ModifyKind::Any
                | notify::event::ModifyKind::Data(_)
                | notify::event::ModifyKind::Metadata(_),
            ) => existing_file,
            notify::EventKind::Modify(notify::event::ModifyKind::Name(
                notify::event::RenameMode::Both,
            )) => precise_rename,
            notify::EventKind::Remove(notify::event::RemoveKind::File) => true,
            notify::EventKind::Remove(notify::event::RemoveKind::Any) => existing_file,
            _ => false,
        };
        if !precise {
            return None;
        }
        paths.insert(path);
    }
    Some(paths)
}

fn normalize_watch_path(path: &Path) -> PathBuf {
    if let Ok(path) = fs::canonicalize(path) {
        return path;
    }
    let mut suffix = Vec::new();
    let mut probe = path.to_path_buf();
    while !probe.exists() {
        let Some(name) = probe.file_name() else {
            break;
        };
        suffix.push(name.to_os_string());
        if !probe.pop() {
            break;
        }
    }
    if let Ok(mut canonical) = fs::canonicalize(&probe) {
        for component in suffix.iter().rev() {
            canonical.push(component);
        }
        return canonical;
    }
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        use std::path::Component;
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            component => normalized.push(component.as_os_str()),
        }
    }
    crate::identity::canonical_or_normalized(&normalized)
}

fn merge_pending_scope(
    pending: &Mutex<Option<HashSet<PathBuf>>>,
    consumed: Option<&HashSet<PathBuf>>,
) {
    let mut pending = pending.lock().unwrap();
    let Some(consumed) = consumed else {
        *pending = None;
        return;
    };
    let Some(paths) = pending.as_mut() else {
        return;
    };
    paths.extend(consumed.iter().cloned());
    if paths.len() > 1024 {
        *pending = None;
    }
}

fn take_pending_scope(
    pending: &Mutex<Option<HashSet<PathBuf>>>,
    force_full: bool,
) -> Option<HashSet<PathBuf>> {
    let mut pending = pending.lock().unwrap();
    if force_full {
        *pending = None;
    }
    let consumed = pending.take();
    *pending = Some(HashSet::new());
    if force_full { None } else { consumed }
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
        let mut coverage_by_collection = HashMap::<String, CollectionCoverage>::new();
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
                *worker_sessions.changed_paths.lock().unwrap() = None;
                worker_sessions.force_full.store(true, Ordering::SeqCst);
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
                    let mut paths = take_pending_scope(&state.changed_paths, force);
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
                    let full_scan = paths.is_none();
                    let invalidation = ingest::invalidate_project_scope(
                        &state.root,
                        &worker_store,
                        &state.collection,
                        paths.as_ref(),
                    );
                    if invalidation.is_ok() {
                        // Unaffected verified sources remain searchable while
                        // the observed scanner rebuilds this invalidated scope.
                        state.safe_epoch.store(epoch, Ordering::SeqCst);
                    }
                    let result = invalidation.and_then(|()| {
                        ingest::scan_project_observed(
                            &state.root,
                            &worker_store,
                            &state.collection,
                            paths.as_ref(),
                            &should_continue,
                            content_cache.as_mut(),
                            &state.progress,
                        )
                    });
                    let completed = result.as_ref().is_ok_and(|report| {
                        !report.cancelled && report.coverage.discovery_complete
                    });
                    if state.epoch.load(Ordering::SeqCst) == epoch && completed {
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
                    let ledger = coverage_by_collection
                        .entry(state.collection.clone())
                        .or_default();
                    ledger.apply(&result);
                    let mut published = state.coverage.lock().unwrap();
                    *published = ledger.snapshot();
                    if completed && state.safe_epoch.load(Ordering::SeqCst) == epoch {
                        state.scanned.store(epoch, Ordering::SeqCst);
                    } else {
                        state
                            .safe_epoch
                            .store(state.scanned.load(Ordering::SeqCst), Ordering::SeqCst);
                        merge_pending_scope(
                            &state.changed_paths,
                            if result.is_err() {
                                None
                            } else {
                                paths.as_ref()
                            },
                        );
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
        let mut ledger = CollectionCoverage::default();
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
                    let force_full = session_state.force_full.swap(false, Ordering::SeqCst);
                    let paths = take_pending_scope(&session_state.changed_paths, force_full);
                    session_state.in_flight.store(true, Ordering::SeqCst);
                    let invalidation = crate::sessions::invalidate_session_scope(
                        &owner_key,
                        &session_store,
                        &session_config,
                        paths.as_ref(),
                    );
                    if invalidation.is_ok() {
                        session_state.safe_epoch.store(epoch, Ordering::SeqCst);
                    }
                    let result = invalidation.and_then(|()| {
                        crate::sessions::scan_sessions_observed(
                            &state.root,
                            &owner_key,
                            &session_store,
                            &session_config,
                            paths.as_ref(),
                            &can_continue,
                            &session_state.progress,
                        )
                    });
                    let completed = result.as_ref().is_ok_and(|report| {
                        !report.cancelled && report.coverage.discovery_complete
                    });
                    ledger.apply(&result);
                    let mut published = session_state.coverage.lock().unwrap();
                    *published = ledger.snapshot();
                    if completed
                        && session_state.epoch.load(Ordering::SeqCst) == epoch
                        && session_state.safe_epoch.load(Ordering::SeqCst) == epoch
                    {
                        session_state.scanned.store(epoch, Ordering::SeqCst);
                    } else {
                        session_state.safe_epoch.store(
                            session_state.scanned.load(Ordering::SeqCst),
                            Ordering::SeqCst,
                        );
                        merge_pending_scope(
                            &session_state.changed_paths,
                            if result.is_err() {
                                None
                            } else {
                                paths.as_ref()
                            },
                        );
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
            *state.sessions.changed_paths.lock().unwrap() = None;
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
        if request.name == "__status" {
            write_json(
                reader.get_mut(),
                &json!({"result":status_snapshot(&state, memory, sessions)}),
            )?;
            continue;
        }
        let sessions = request.name == "search_sessions";
        let start_epoch = state.epoch.load(Ordering::SeqCst);
        let session_epoch = state.sessions.epoch.load(Ordering::SeqCst);
        let (ready, mut coverage) = if sessions {
            coverage_snapshot(
                &state.sessions.coverage,
                &state.sessions.epoch,
                &state.sessions.scanned,
                &state.sessions.watch_error,
            )
        } else {
            coverage_snapshot(
                &state.coverage,
                &state.epoch,
                &state.scanned,
                &state.watch_error,
            )
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
    if !ready || coverage.pending_changes.is_none() {
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

fn status_snapshot(state: &RootState, memory: &MemoryState, sessions: &SessionState) -> Value {
    let (project_ready, mut project_coverage) = coverage_snapshot(
        &state.coverage,
        &state.epoch,
        &state.scanned,
        &state.watch_error,
    );
    let (session_ready, mut session_coverage) = coverage_snapshot(
        &sessions.coverage,
        &sessions.epoch,
        &sessions.scanned,
        &sessions.watch_error,
    );
    project_coverage.memory = Some(memory.snapshot());
    session_coverage.memory = Some(memory.snapshot());
    if !project_ready {
        project_coverage.pending_changes = None;
    }
    if !session_ready {
        session_coverage.pending_changes = None;
    }
    json!({
        "project": {
            "status": search_status(project_ready, &project_coverage),
            "coverage": tools::public_coverage_value(&project_coverage),
            "progress": state.progress.snapshot(),
        },
        "sessions": {
            "status": search_status(session_ready, &session_coverage),
            "coverage": tools::public_coverage_value(&session_coverage),
            "progress": sessions.progress.snapshot(),
        },
    })
}

fn coverage_snapshot(
    coverage: &Mutex<Coverage>,
    epoch: &AtomicU64,
    scanned: &AtomicU64,
    watch_error: &Mutex<Option<String>>,
) -> (bool, Coverage) {
    let published = coverage.lock().unwrap();
    let ready = epoch.load(Ordering::SeqCst) == scanned.load(Ordering::SeqCst);
    let mut snapshot = with_watch_error(published.clone(), watch_error);
    if !ready {
        snapshot.pending_changes = None;
    }
    (ready, snapshot)
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

    fn session_coverage_after_unrelated_update(problem: &str) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("project");
        let home = dir.path().join("codex");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(home.join("sessions")).unwrap();
        let config = crate::sessions::SessionConfig {
            codex_home: home.clone(),
            claude_config_dir: dir.path().join("claude"),
            copilot_home: dir.path().join("copilot"),
            ..crate::sessions::SessionConfig::default()
        };
        let store = Store::open(&dir.path().join("index.sqlite3")).unwrap();
        let owner = ingest::project_identity(&root).unwrap().owner_key;
        let a = home.join("sessions/a.jsonl");
        let b = home.join("sessions/b.jsonl");
        let header = json!({"type":"session_meta","payload":{"cwd":root,"id":"coverage"}});
        let event = json!({"type":"response_item","payload":{"type":"message","id":"event","role":"user","content":[{"type":"text","text":"coverageprefix"}]}});
        let prefix = format!("{header}\n{event}\n");
        fs::write(
            &a,
            match problem {
                "tail" => format!("{prefix}{{\"type\":"),
                "malformed" => format!("{prefix}not-json\n"),
                _ => format!("{event}\n"),
            },
        )
        .unwrap();
        fs::write(&b, &prefix).unwrap();
        let scan = |changes: Option<&HashSet<PathBuf>>| {
            crate::sessions::scan_sessions_observed(
                &root,
                &owner,
                &store,
                &config,
                changes,
                &|| true,
                &ProgressReporter::new(),
            )
        };
        let mut ledger = CollectionCoverage::default();
        ledger.apply(&scan(None));
        let initial = ledger.snapshot();
        if problem == "tail" {
            assert_eq!(initial.pending_changes, Some(1));
        } else {
            assert!(initial.error_count > 0);
        }
        if problem == "rejected" {
            assert_eq!(store.sources(&owner, "session").unwrap().len(), 1);
            assert_eq!(initial.excluded_count, 1);
        }
        fs::write(&b, format!("{prefix}{event}\n")).unwrap();
        ledger.apply(&scan(Some(&HashSet::from([b]))));
        let updated = ledger.snapshot();
        assert_eq!(
            updated.pending_changes, initial.pending_changes,
            "{problem}"
        );
        assert_eq!(updated.error_count, initial.error_count, "{problem}");
        assert_eq!(updated.excluded_count, initial.excluded_count, "{problem}");
        assert_eq!(updated.diagnostics, initial.diagnostics, "{problem}");
    }

    #[test]
    fn collection_coverage_preserves_untouched_tail() {
        session_coverage_after_unrelated_update("tail");
    }

    #[test]
    fn collection_coverage_preserves_untouched_error() {
        session_coverage_after_unrelated_update("malformed");
    }

    #[test]
    fn collection_coverage_preserves_rejected_source_without_rows() {
        session_coverage_after_unrelated_update("rejected");
    }

    #[test]
    fn watcher_recovery_does_not_clear_source_diagnostics() {
        let coverage = Mutex::new(Coverage {
            reconciled_at: Some("completed".into()),
            pending_changes: Some(0),
            error_count: 2,
            diagnostics: std::collections::BTreeMap::from([("unsupported_record".into(), 2)]),
            ..Coverage::default()
        });
        let epoch = AtomicU64::new(1);
        let scanned = AtomicU64::new(1);
        let watch_error = Mutex::new(Some("watcher_unavailable".into()));
        let (_, failed) = coverage_snapshot(&coverage, &epoch, &scanned, &watch_error);
        assert_eq!(failed.error_count, 3);
        assert_eq!(failed.diagnostics.get("watcher_unavailable"), Some(&1));
        *watch_error.lock().unwrap() = None;
        let (ready, recovered) = coverage_snapshot(&coverage, &epoch, &scanned, &watch_error);
        assert_eq!(search_status(ready, &recovered), "degraded");
        assert_eq!(recovered.error_count, 2);
        assert!(!recovered.diagnostics.contains_key("watcher_unavailable"));
        epoch.store(2, Ordering::SeqCst);
        let (ready, active) = coverage_snapshot(&coverage, &epoch, &scanned, &watch_error);
        assert_eq!(search_status(ready, &active), "refreshing");
        assert_eq!(active.pending_changes, None);
        assert_eq!(active.error_count, 2);
    }

    #[test]
    fn status_snapshot_does_not_wait_for_a_durable_index_write() {
        use std::sync::mpsc;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("index.sqlite3")).unwrap();
        let sessions = Arc::new(SessionState::new(dir.path().join("registry.json")));
        let state = RootState::new(dir.path().to_owned(), "collection".into(), sessions.clone());
        let memory = MemoryState {
            target: 512 * 1024 * 1024,
            observed: AtomicU64::new(0),
            peak: AtomicU64::new(0),
        };
        let (entered, blocked) = mpsc::channel();
        let (release, resume) = mpsc::channel();
        let writer = thread::spawn(move || {
            store.replace_source(
                &crate::model::Source {
                    key: "source".into(),
                    collection: "collection".into(),
                    path: "source.txt".into(),
                    version: "version".into(),
                    kind: "project".into(),
                },
                std::iter::once_with(|| {
                    entered.send(()).unwrap();
                    resume.recv().unwrap();
                    Ok(crate::model::Chunk {
                        text: "marker".into(),
                        ..Default::default()
                    })
                }),
            )
        });
        blocked.recv_timeout(Duration::from_secs(5)).unwrap();
        let (observed, response) = mpsc::channel();
        let reader = thread::spawn(move || {
            observed
                .send(status_snapshot(&state, &memory, &sessions))
                .unwrap();
        });
        let snapshot = response.recv_timeout(Duration::from_secs(2));
        release.send(()).unwrap();
        writer.join().unwrap().unwrap();
        reader.join().unwrap();
        let snapshot = snapshot.expect("status must not wait for the writer");
        assert_eq!(snapshot["project"]["status"], "building");
        assert_eq!(
            snapshot["project"]["coverage"]["pending_changes"],
            Value::Null
        );
    }

    #[test]
    fn not_ready_status_takes_precedence_over_diagnostics() {
        let mut coverage = Coverage {
            error_count: 1,
            pending_changes: Some(0),
            ..Coverage::default()
        };
        assert_eq!(search_status(false, &coverage), "building");
        coverage.reconciled_at = Some("2026-09-21T00:00:00Z".to_owned());
        assert_eq!(search_status(false, &coverage), "refreshing");
        assert_eq!(search_status(true, &coverage), "degraded");
        coverage.pending_changes = Some(1);
        assert_eq!(search_status(true, &coverage), "refreshing");
    }

    #[test]
    fn rename_routing_requires_both_endpoints() {
        let root = Path::new("/project");
        let provider_roots = [PathBuf::from("/provider")];
        let registry = Path::new("/cache/registry.json");
        for mode in [
            notify::event::RenameMode::From,
            notify::event::RenameMode::To,
            notify::event::RenameMode::Any,
            notify::event::RenameMode::Other,
        ] {
            let event = notify::Event {
                kind: notify::EventKind::Modify(notify::event::ModifyKind::Name(mode)),
                paths: vec![PathBuf::from("/provider/sessions/source.jsonl")],
                attrs: Default::default(),
            };
            assert!(!project_rename_is_precise(&event, root));
            assert!(session_event_scope(&event, &provider_roots, registry).is_none());
        }

        let project_temp = tempfile::tempdir().unwrap();
        let project_root = project_temp.path();
        std::fs::write(project_root.join("new.jsonl"), "new").unwrap();
        let event = notify::Event {
            kind: notify::EventKind::Modify(notify::event::ModifyKind::Name(
                notify::event::RenameMode::Both,
            )),
            paths: vec![
                project_root.join("old.jsonl"),
                project_root.join("new.jsonl"),
            ],
            attrs: Default::default(),
        };
        assert!(project_rename_is_precise(&event, project_root));
        let session_temp = tempfile::tempdir().unwrap();
        let session_provider = session_temp.path().join("provider/sessions");
        std::fs::create_dir_all(&session_provider).unwrap();
        std::fs::write(session_provider.join("new.jsonl"), "new").unwrap();
        let session_provider_roots = [normalize_watch_path(&session_temp.path().join("provider"))];
        let session_registry = session_temp.path().join("registry.json");
        let session_event = notify::Event {
            kind: event.kind,
            paths: vec![
                session_provider.join("old.jsonl"),
                session_provider.join("new.jsonl"),
            ],
            attrs: Default::default(),
        };
        let session_scope =
            session_event_scope(&session_event, &session_provider_roots, &session_registry);
        assert!(session_scope.is_some(), "{session_event:?}");
        assert_eq!(session_scope.unwrap().len(), 2);

        let deleted = notify::Event {
            kind: notify::EventKind::Remove(notify::event::RemoveKind::Any),
            paths: vec![PathBuf::from("/provider/sessions/deleted.jsonl")],
            attrs: Default::default(),
        };
        assert!(session_event_scope(&deleted, &provider_roots, registry).is_none());
        let removed_file = notify::Event {
            kind: notify::EventKind::Remove(notify::event::RemoveKind::File),
            paths: vec![PathBuf::from("/provider/sessions/deleted.jsonl")],
            attrs: Default::default(),
        };
        assert_eq!(
            session_event_scope(&removed_file, &provider_roots, registry)
                .unwrap()
                .len(),
            1
        );
        let temp = tempfile::tempdir().unwrap();
        let provider = temp.path().join("provider/sessions");
        std::fs::create_dir_all(provider.join("folder.jsonl")).unwrap();
        std::fs::write(provider.join("folder.jsonl/child.jsonl"), "child").unwrap();
        let registry = temp.path().join("registry.json");
        let directory_event = notify::Event {
            kind: notify::EventKind::Remove(notify::event::RemoveKind::Any),
            paths: vec![provider.join("folder.jsonl")],
            attrs: Default::default(),
        };
        assert!(
            session_event_scope(
                &directory_event,
                &[normalize_watch_path(&temp.path().join("provider"))],
                &registry,
            )
            .is_none()
        );
        let ambiguous = notify::Event {
            kind: notify::EventKind::Modify(notify::event::ModifyKind::Any),
            paths: vec![PathBuf::from("/provider/sessions/missing.jsonl")],
            attrs: Default::default(),
        };
        assert!(session_event_scope(&ambiguous, &provider_roots, &registry).is_none());
        let project_ambiguous = notify::Event {
            kind: ambiguous.kind,
            paths: vec![PathBuf::from("/project/missing.rs")],
            attrs: Default::default(),
        };
        assert!(!project_event_is_precise(&project_ambiguous, root));
    }

    #[test]
    fn forced_scope_discards_pending_candidates() {
        let pending = Mutex::new(Some(HashSet::from([PathBuf::from("/source.jsonl")])));
        assert!(take_pending_scope(&pending, true).is_none());
        assert!(
            pending
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(HashSet::is_empty)
        );
    }

    #[test]
    fn registry_lock_and_temp_events_do_not_trigger_session_reconcile() {
        let registry = Path::new("/cache/identity-registry.json");
        for path in [
            "/cache/identity-registry.json.lock",
            "/cache/.identity-registry.json-tmp-123-456-0",
        ] {
            let event = notify::Event {
                kind: notify::EventKind::Create(notify::event::CreateKind::File),
                paths: vec![PathBuf::from(path)],
                attrs: Default::default(),
            };
            assert!(is_registry_auxiliary_event(&event, registry));
        }
        let event = notify::Event {
            kind: notify::EventKind::Create(notify::event::CreateKind::File),
            paths: vec![registry.to_path_buf()],
            attrs: Default::default(),
        };
        assert!(!is_registry_auxiliary_event(&event, registry));
    }
}
