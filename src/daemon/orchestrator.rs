//! Long-standing orchestrator daemon that manages per-session MCP worker
//! child processes over a private Unix domain socket.

use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde_json::Value;

use crate::daemon::{
    CONTROL_IO_TIMEOUT, DaemonPaths, DaemonRequest, DaemonResponse, DaemonStateFile, DaemonStatus,
    MAX_CONTROL_LINE_BYTES, MAX_MCP_FRAME_BYTES, PROTOCOL_VERSION, STARTUP_IO_TIMEOUT, SessionInfo,
    SessionState, StartSessionRequest, WorkerReady, ensure_private_dir, unix_now_secs,
    verify_peer_credentials, write_json_line,
};
use crate::lifecycle::ProcessOwner;
use crate::maintenance::FileLease;

const DEFAULT_MAX_SESSIONS: usize = 128;
const MAX_RECENT_SESSIONS: usize = 50;
const MAX_SESSION_STDERR_LINES: usize = 64;
const MAX_STDERR_BYTES: usize = 16 * 1024;
const MAX_STDERR_LINE_BYTES: usize = 2048;
const MAX_PENDING_CONNECTIONS: usize = 32;
const SESSION_PAGE_SIZE: usize = 8;
const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(2);
const GRACEFUL_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const FORCED_EOF_GRACE: Duration = Duration::from_millis(800);
const SIGTERM_GRACE: Duration = Duration::from_secs(2);
const POLL_WAIT_STEP: Duration = Duration::from_millis(25);

static SIGNAL_STOP: AtomicBool = AtomicBool::new(false);
static SIGNAL_WAKE_FD: AtomicI32 = AtomicI32::new(-1);

extern "C" fn handle_termination_signal(_sig: libc::c_int) {
    SIGNAL_STOP.store(true, Ordering::Release);
    let fd = SIGNAL_WAKE_FD.load(Ordering::Acquire);
    if fd >= 0 {
        let byte: u8 = 1;
        unsafe {
            let _ = libc::write(fd, (&byte as *const u8).cast(), 1);
        }
    }
}

const SUPERVISOR_SIGNALS: [libc::c_int; 3] = [libc::SIGTERM, libc::SIGINT, libc::SIGCHLD];

fn supervisor_signal_set() -> libc::sigset_t {
    let mut set = unsafe { std::mem::zeroed() };
    unsafe {
        libc::sigemptyset(&mut set);
        for signal in SUPERVISOR_SIGNALS {
            libc::sigaddset(&mut set, signal);
        }
    }
    set
}

fn default_signal_action() -> libc::sigaction {
    let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
    action.sa_sigaction = libc::SIG_DFL;
    unsafe { libc::sigemptyset(&mut action.sa_mask) };
    action
}

struct SignalGuard {
    previous_actions: Vec<(libc::c_int, libc::sigaction)>,
    previous_mask: libc::sigset_t,
    _wake_holder: UnixStream,
}

impl SignalGuard {
    fn install(wake_writer: UnixStream) -> Result<Self> {
        wake_writer.set_nonblocking(true)?;
        let signals = supervisor_signal_set();
        let mut previous_mask = unsafe { std::mem::zeroed() };
        // A launcher can ignore or block these signals across exec. Install
        // dispositions before unblocking, while new threads still inherit
        // this one normalized mask.
        let rc = unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, &signals, &mut previous_mask) };
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc))
                .context("failed to block supervisor signals");
        }
        SIGNAL_STOP.store(false, Ordering::Release);
        SIGNAL_WAKE_FD.store(wake_writer.as_raw_fd(), Ordering::Release);
        let mut guard = Self {
            previous_actions: Vec::with_capacity(SUPERVISOR_SIGNALS.len()),
            previous_mask,
            _wake_holder: wake_writer,
        };
        for signal in SUPERVISOR_SIGNALS {
            let mut action = default_signal_action();
            if signal != libc::SIGCHLD {
                action.sa_sigaction = handle_termination_signal as *const () as usize;
                action.sa_flags = libc::SA_RESTART;
            }
            // SIG_DFL with SA_NOCLDWAIT cleared preserves the waitable worker
            // leader. Group shutdown depends on that PID remaining reserved.
            let mut previous = unsafe { std::mem::zeroed() };
            if unsafe { libc::sigaction(signal, &action, &mut previous) } != 0 {
                return Err(io::Error::last_os_error())
                    .context("failed to install supervisor signal disposition");
            }
            guard.previous_actions.push((signal, previous));
        }
        let rc =
            unsafe { libc::pthread_sigmask(libc::SIG_UNBLOCK, &signals, std::ptr::null_mut()) };
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc))
                .context("failed to unblock supervisor signals");
        }
        Ok(guard)
    }
}

impl Drop for SignalGuard {
    fn drop(&mut self) {
        // Also rolls back partial setup. Restore the launcher's mask only
        // after returning every disposition to the state it supplied.
        let signals = supervisor_signal_set();
        unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, &signals, std::ptr::null_mut()) };
        SIGNAL_WAKE_FD.store(-1, Ordering::Release);
        for (signal, previous) in self.previous_actions.iter().rev() {
            unsafe { libc::sigaction(*signal, previous, std::ptr::null_mut()) };
        }
        SIGNAL_STOP.store(false, Ordering::Release);
        unsafe {
            libc::pthread_sigmask(libc::SIG_SETMASK, &self.previous_mask, std::ptr::null_mut())
        };
    }
}

struct SessionHandle {
    info: Mutex<SessionInfo>,
    owner: Option<ProcessOwner>,
    completed: Condvar,
    watcher_registered: AtomicBool,
    forced_stop: AtomicBool,
    stop_reason: Mutex<Option<String>>,
    stderr_lines: Mutex<VecDeque<String>>,
}

impl SessionHandle {
    fn snapshot(&self) -> SessionInfo {
        let mut info = self.info.lock().unwrap_or_else(|e| e.into_inner()).clone();
        info.recent_stderr = self
            .stderr_lines
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .cloned()
            .collect();
        info
    }

    fn request_stop(&self, reason: &str) {
        {
            let mut slot = self.stop_reason.lock().unwrap_or_else(|e| e.into_inner());
            if slot.is_none() {
                *slot = Some(reason.to_string());
            }
        }
        // Publish the reason before releasing the cancellation flag, so a
        // supervisor that observes cancellation always has its causal outcome.
        self.forced_stop.store(true, Ordering::Release);
        // The session poll loop observes cancellation within one poll turn.
        // Keep the data stream open until reaping and outcome publication, so
        // clients can distinguish a clean stop from an unexpected daemon death.
    }

    fn record_stderr(&self, line: String) {
        let mut buf = self.stderr_lines.lock().unwrap_or_else(|e| e.into_inner());
        let mut line = line;
        truncate_text(&mut line, MAX_STDERR_LINE_BYTES);
        while buf.len() >= MAX_SESSION_STDERR_LINES
            || buf.iter().map(String::len).sum::<usize>() + line.len() > MAX_STDERR_BYTES
        {
            buf.pop_front();
        }
        buf.push_back(line);
    }
}

struct DaemonCore {
    paths: DaemonPaths,
    worker_executable: std::path::PathBuf,
    worker_groups: bool,
    instance_id: String,
    pending_connections: AtomicUsize,
    outcome_watchers: AtomicUsize,
    started_at: u64,
    started_instant: Instant,
    max_sessions: usize,
    next_session_seq: AtomicU64,
    total_sessions_started: AtomicU64,
    shutdown: AtomicBool,
    wake_writer: Mutex<UnixStream>,
    sweep_cv: Condvar,
    sweep_lock: Mutex<()>,
    active_sessions: Mutex<HashMap<String, Arc<SessionHandle>>>,
    recent_sessions: Mutex<VecDeque<SessionInfo>>,
    active_threads: Mutex<usize>,
    threads_cv: Condvar,
}

impl DaemonCore {
    fn new(paths: DaemonPaths, wake_writer: UnixStream, worker_groups: bool) -> Result<Self> {
        let max_sessions = std::env::var("TSINDEX_MAX_SESSIONS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|&value| value > 0)
            .unwrap_or(DEFAULT_MAX_SESSIONS);
        let worker_executable = crate::daemon::service::invocation_binary()
            .context("failed to locate worker executable")?;
        // A random boot nonce prevents a stale destructive session ID from
        // selecting a different session after a daemon restart.
        let mut nonce = [0u8; 16];
        fs::File::open("/dev/urandom")?.read_exact(&mut nonce)?;
        let instance_id = nonce.iter().map(|b| format!("{b:02x}")).collect();
        Ok(Self {
            paths,
            worker_executable,
            worker_groups,
            instance_id,
            pending_connections: AtomicUsize::new(0),
            outcome_watchers: AtomicUsize::new(0),
            started_at: unix_now_secs(),
            started_instant: Instant::now(),
            max_sessions,
            next_session_seq: AtomicU64::new(1),
            total_sessions_started: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
            wake_writer: Mutex::new(wake_writer),
            sweep_cv: Condvar::new(),
            sweep_lock: Mutex::new(()),
            active_sessions: Mutex::new(HashMap::new()),
            recent_sessions: Mutex::new(VecDeque::new()),
            active_threads: Mutex::new(0),
            threads_cv: Condvar::new(),
        })
    }

    fn is_shutting_down(&self) -> bool {
        self.shutdown.load(Ordering::Acquire) || SIGNAL_STOP.load(Ordering::Acquire)
    }

    fn request_shutdown(&self) {
        if !self.shutdown.swap(true, Ordering::AcqRel) {
            if let Ok(mut writer) = self.wake_writer.lock() {
                let _ = writer.write_all(&[1]);
            }
            self.sweep_cv.notify_all();
        }
    }

    fn status(&self) -> DaemonStatus {
        let active_sessions = self
            .active_sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len();
        DaemonStatus {
            daemon_pid: std::process::id(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            protocol: PROTOCOL_VERSION,
            started_at: self.started_at,
            uptime_secs: self.started_instant.elapsed().as_secs(),
            socket: self.paths.socket_path.clone(),
            active_sessions,
            total_sessions_started: self.total_sessions_started.load(Ordering::Acquire),
        }
    }

    fn list_sessions(&self, include_recent: bool) -> (Vec<SessionInfo>, Vec<SessionInfo>) {
        let mut active: Vec<SessionInfo> = self
            .active_sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .map(|session| session.snapshot())
            .collect();
        active.sort_by_key(|session| (session.started_at, session.session_id.clone()));

        let recent = if include_recent {
            self.recent_sessions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .cloned()
                .collect()
        } else {
            Vec::new()
        };
        (active, recent)
    }

    fn kill_session(&self, session_id: &str, reason: &str) -> Result<Vec<String>> {
        let target = self
            .active_sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(session_id)
            .cloned();
        let Some(handle) = target else {
            bail!("unknown active session {session_id:?}");
        };
        handle.request_stop(reason);
        self.wait_sessions_stopped(&[session_id.to_string()])?;
        Ok(vec![session_id.to_string()])
    }

    fn kill_all(&self, reason: &str) -> Vec<String> {
        let targets: Vec<(String, Arc<SessionHandle>)> = self
            .active_sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(id, handle)| (id.clone(), Arc::clone(handle)))
            .collect();
        let mut killed = Vec::with_capacity(targets.len());
        for (id, handle) in targets {
            handle.request_stop(reason);
            killed.push(id);
        }
        killed.sort();
        killed
    }

    fn wait_sessions_stopped(&self, ids: &[String]) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(4);
        loop {
            if !ids.iter().any(|id| {
                self.active_sessions
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .contains_key(id)
            }) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!("termination requested; worker cleanup is still in progress");
            }
            thread::sleep(POLL_WAIT_STEP);
        }
    }

    fn watch_session(
        self: &Arc<Self>,
        session_id: &str,
        peer_pid: Option<u32>,
        writer: &mut UnixStream,
        pending: &mut Option<PendingConnection>,
        watcher_lease: &mut Option<WatcherLease>,
    ) -> Result<()> {
        let active = self
            .active_sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(session_id)
            .cloned();
        let session = if let Some(handle) = active {
            {
                let info = handle.info.lock().unwrap_or_else(|e| e.into_inner());
                if peer_pid != Some(info.client_pid) {
                    bail!("session outcome watcher must be the original proxy process");
                }
            }
            let lease = WatcherLease::try_acquire(self)
                .context("maximum concurrent outcome watchers reached")?;
            if handle.watcher_registered.swap(true, Ordering::AcqRel) {
                bail!("session already has an outcome watcher");
            }
            // A separate global lease survives worker-registry removal and
            // remains with the outer handler through terminal/error writes.
            *watcher_lease = Some(lease);
            // A verified session now owns this single long-lived watcher.
            // Invalid watchers leave ownership with the outer handler through
            // its error reply, including any bounded write wait.
            drop(pending.take());
            write_json_line(writer, &DaemonResponse::Watching)?;
            let mut info = handle.info.lock().unwrap_or_else(|e| e.into_inner());
            while info.state != SessionState::Exited {
                info = handle
                    .completed
                    .wait(info)
                    .unwrap_or_else(|e| e.into_inner());
            }
            info.clone()
        } else {
            let session = self
                .recent_sessions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .find(|info| info.session_id == session_id)
                .cloned()
                .context("unknown session for outcome watcher")?;
            if peer_pid != Some(session.client_pid) {
                bail!("session outcome watcher must be the original proxy process");
            }
            write_json_line(writer, &DaemonResponse::Watching)?;
            session
        };
        let mut session = session;
        // Outcome delivery needs the reason and status, keeping diagnostics in
        // the separately bounded SessionDetail response.
        summarize_session(&mut session);
        write_json_line(writer, &DaemonResponse::Session { session })
    }

    fn finish_session(&self, session_id: &str, mut final_reason: String) {
        truncate_text(&mut final_reason, 1024);
        let mut active = self
            .active_sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(handle) = active.get(session_id) {
            {
                let mut info = handle.info.lock().unwrap_or_else(|e| e.into_inner());
                info.state = SessionState::Exited;
                info.ended_at = Some(unix_now_secs());
                info.exit_reason = Some(final_reason);
            }
            let snapshot = handle.snapshot();
            let mut recent = self
                .recent_sessions
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if recent.len() >= MAX_RECENT_SESSIONS {
                recent.pop_front();
            }
            recent.push_back(snapshot);
            handle.completed.notify_all();
            // Publish history before removing the active handle. A watcher
            // registering concurrently can always find one complete record.
            active.remove(session_id);
        }
    }

    fn wait_for_threads(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        let mut count = self
            .active_threads
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        while *count > 0 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let (next, _) = self
                .threads_cv
                .wait_timeout(count, remaining)
                .unwrap_or_else(|e| e.into_inner());
            count = next;
        }
    }
}

struct ActiveThreadGuard {
    core: Arc<DaemonCore>,
}

impl ActiveThreadGuard {
    fn try_enter(core: Arc<DaemonCore>) -> Option<Self> {
        let ceiling = MAX_PENDING_CONNECTIONS.saturating_add(core.max_sessions.saturating_mul(2));
        let mut count = core
            .active_threads
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        // This outer admission check covers cleanup and logging tails after
        // a worker registry entry or inner connection permit has been released.
        // Check and increment atomically under the existing completion mutex.
        if *count >= ceiling {
            return None;
        }
        *count += 1;
        drop(count);
        Some(Self { core })
    }
}

impl Drop for ActiveThreadGuard {
    fn drop(&mut self) {
        let mut count = self
            .core
            .active_threads
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        *count = count.saturating_sub(1);
        if *count == 0 {
            self.core.threads_cv.notify_all();
        }
    }
}

struct SocketCleanupGuard {
    socket: Option<(std::path::PathBuf, u64, u64)>,
    state: Option<(std::path::PathBuf, u64, u64)>,
}

impl Drop for SocketCleanupGuard {
    fn drop(&mut self) {
        // A pathname may have been replaced while we were alive. Only remove
        // the endpoint inode this instance bound, and leave activated sockets
        // entirely under the service manager's ownership.
        if let Some((path, dev, ino)) = &self.socket
            && let Ok(meta) = fs::symlink_metadata(path)
            && meta.dev() == *dev
            && meta.ino() == *ino
        {
            let _ = fs::remove_file(path);
        }
        if let Some((path, dev, ino)) = &self.state
            && let Ok(meta) = fs::symlink_metadata(path)
            && meta.dev() == *dev
            && meta.ino() == *ino
        {
            let _ = fs::remove_file(path);
        }
    }
}

pub(crate) fn prepare_metadata_dir(paths: &DaemonPaths) -> Result<()> {
    let dir = paths
        .lock_path
        .parent()
        .context("missing metadata directory")?;
    let marker = dir.join("owner");
    use std::os::unix::fs::DirBuilderExt;
    match fs::DirBuilder::new().mode(0o700).create(dir) {
        Ok(()) => {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&marker)?;
            file.write_all(b"tsindex daemon runtime v2\n")?;
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            crate::daemon::validate_private_dir(dir)?;
            // A crash can leave the newly created private directory empty
            // before the ownership marker is written. Recover only that empty
            // state, preserving every unrecognized nonempty directory.
            if fs::read_dir(dir)?.next().is_none() {
                let mut file = fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .custom_flags(libc::O_NOFOLLOW)
                    .open(&marker)?;
                file.write_all(b"tsindex daemon runtime v2\n")?;
            }
            let mut file = fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&marker)
                .context("refusing existing daemon metadata directory without ownership marker")?;
            let mut contents = String::new();
            Read::by_ref(&mut file)
                .take(128)
                .read_to_string(&mut contents)?;
            if contents != "tsindex daemon runtime v2\n" {
                bail!("unrecognized daemon metadata directory {}", dir.display());
            }
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

/// Run in the foreground, optionally requiring a manager-owned listener.
pub fn run_orchestrator(paths: DaemonPaths, require_activation: bool) -> Result<()> {
    ensure_private_dir(paths.socket_dir()?)?;
    prepare_metadata_dir(&paths)?;
    let _instance_lease = FileLease::try_acquire(&paths.lock_path)?
        .context("tsindex orchestrator daemon is already running")?;
    let inherited = crate::daemon::activation::inherited_listener(&paths)?;
    if require_activation && inherited.is_none() {
        bail!("service manager did not supply the required listening socket");
    }
    let managed = inherited.is_some();
    #[cfg(target_os = "linux")]
    if unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) } != 0 {
        return Err(io::Error::last_os_error()).context("failed to enable descendant reaping");
    }
    let listener = match inherited {
        Some(listener) => listener,
        None => {
            match fs::symlink_metadata(&paths.socket_path) {
                Ok(meta) => {
                    if !meta.file_type().is_socket() || meta.uid() != crate::daemon::current_uid() {
                        bail!(
                            "refusing to replace non-socket or foreign endpoint {}",
                            paths.socket_path.display()
                        );
                    }
                    match crate::daemon::connect_with_timeout(
                        &paths.socket_path,
                        CONTROL_IO_TIMEOUT,
                    ) {
                        Ok(_) => bail!(
                            "refusing to replace a live listening socket {}",
                            paths.socket_path.display()
                        ),
                        Err(error)
                            if matches!(
                                error.raw_os_error(),
                                Some(libc::ECONNREFUSED | libc::ENOENT)
                            ) => {}
                        // A slow/full listener and permission failures do not
                        // establish staleness and must never trigger unlinking.
                        Err(error) => {
                            return Err(error)
                                .context("cannot establish that the existing socket is stale");
                        }
                    }
                    fs::remove_file(&paths.socket_path)?;
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            let listener = UnixListener::bind(&paths.socket_path)?;
            fs::set_permissions(&paths.socket_path, fs::Permissions::from_mode(0o600))?;
            listener
        }
    };
    let meta = fs::symlink_metadata(&paths.socket_path)?;
    let mut cleanup = SocketCleanupGuard {
        socket: (!managed).then(|| (paths.socket_path.clone(), meta.dev(), meta.ino())),
        state: None,
    };
    write_state_file(&paths)?;
    let state_meta = fs::symlink_metadata(&paths.state_path)?;
    cleanup.state = Some((paths.state_path.clone(), state_meta.dev(), state_meta.ino()));
    let (wake_reader, wake_writer) = UnixStream::pair()?;
    let _signal_guard = SignalGuard::install(wake_writer.try_clone()?)?;
    // launchd cleans its job process group on death. Managed macOS workers
    // must stay in that group so they retain this last-resort containment.
    let worker_groups = !(cfg!(target_os = "macos") && managed);
    let core = Arc::new(DaemonCore::new(paths.clone(), wake_writer, worker_groups)?);
    let sweeper = spawn_owner_sweeper(Arc::clone(&core))?;
    eprintln!(
        "tsindex daemon listening on {} (pid {}, version {})",
        paths.socket_path.display(),
        std::process::id(),
        env!("CARGO_PKG_VERSION")
    );
    let result = accept_loop(&listener, &wake_reader, &core);
    // Every exit path performs cleanup, including poll/accept errors.
    core.request_shutdown();
    core.kill_all("daemon_shutdown");
    core.wait_for_threads(Duration::from_secs(10));
    let _ = sweeper.join();
    eprintln!("tsindex daemon stopped (pid {})", std::process::id());
    result
}

fn write_state_file(paths: &DaemonPaths) -> Result<()> {
    let state = DaemonStateFile {
        pid: std::process::id(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        protocol: PROTOCOL_VERSION,
        started_at: unix_now_secs(),
        socket: paths.socket_path.clone(),
    };
    // The metadata directory is exclusively owned by this daemon namespace.
    // Refuse symlinks and unrelated files even after a previous crash.
    if let Ok(meta) = fs::symlink_metadata(&paths.state_path) {
        if !meta.is_file() || meta.file_type().is_symlink() {
            bail!(
                "refusing unexpected daemon state path {}",
                paths.state_path.display()
            );
        }
        let prior = fs::read(&paths.state_path)?;
        serde_json::from_slice::<DaemonStateFile>(&prior)
            .context("refusing to replace unrecognized daemon state")?;
    }
    let bytes = serde_json::to_vec_pretty(&state)?;
    let mut nonce = [0u8; 8];
    fs::File::open("/dev/urandom")?.read_exact(&mut nonce)?;
    let nonce: String = nonce.iter().map(|byte| format!("{byte:02x}")).collect();
    // PID reuse must not collide with an unpublished temporary from a crash.
    let tmp = paths
        .state_path
        .with_extension(format!("tmp.{}.{nonce}", std::process::id()));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&tmp)?;
    let result = file
        .write_all(&bytes)
        .and_then(|()| fs::rename(&tmp, &paths.state_path));
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result?;
    Ok(())
}

fn accept_loop(
    listener: &UnixListener,
    wake_reader: &UnixStream,
    core: &Arc<DaemonCore>,
) -> Result<()> {
    listener.set_nonblocking(true)?;
    loop {
        if core.is_shutting_down() {
            break;
        }
        let mut fds = [
            libc::pollfd {
                fd: listener.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: wake_reader.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        let ready = unsafe { libc::poll(fds.as_mut_ptr(), 2, -1) };
        if ready < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error).context("orchestrator poll failed");
        }
        if core.is_shutting_down() || fds[1].revents != 0 {
            break;
        }
        if fds[0].revents != 0 {
            loop {
                // A replenished backlog must not postpone shutdown until the
                // listener happens to become empty.
                if core.is_shutting_down() {
                    return Ok(());
                }
                match listener.accept() {
                    Ok((stream, _addr)) => {
                        let Some(pending) = PendingConnection::try_acquire(core) else {
                            // Control admission stays independent of worker
                            // slots, including when all sessions are occupied.
                            drop(stream);
                            continue;
                        };
                        let _ = stream.set_nonblocking(false);
                        let Some(guard) = ActiveThreadGuard::try_enter(Arc::clone(core)) else {
                            drop(stream);
                            continue;
                        };
                        let worker_core = Arc::clone(core);
                        let spawned = thread::Builder::new()
                            .name("tsindex-daemon-conn".into())
                            .spawn(move || {
                                let _guard = guard;
                                if let Err(error) = handle_connection(worker_core, stream, pending)
                                {
                                    eprintln!("tsindex daemon connection error: {error:#}");
                                }
                            });
                        if let Err(error) = spawned {
                            eprintln!("tsindex daemon failed to spawn handler thread: {error}");
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => {
                        eprintln!("tsindex daemon accept error: {error}");
                        // EMFILE leaves the listener readable. Back off instead
                        // of spinning and starving existing session supervisors.
                        thread::sleep(Duration::from_millis(50));
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

fn sweep_interval() -> Duration {
    std::env::var("TSINDEX_SWEEP_INTERVAL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|&ms| ms > 0)
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_SWEEP_INTERVAL)
}

fn spawn_owner_sweeper(core: Arc<DaemonCore>) -> Result<thread::JoinHandle<()>> {
    let interval = sweep_interval();
    thread::Builder::new()
        .name("tsindex-daemon-sweeper".into())
        .spawn(move || {
            let guard = core.sweep_lock.lock().unwrap_or_else(|e| e.into_inner());
            let mut guard = guard;
            while !core.is_shutting_down() {
                let (next, _) = core
                    .sweep_cv
                    .wait_timeout(guard, interval)
                    .unwrap_or_else(|e| e.into_inner());
                guard = next;
                if core.is_shutting_down() {
                    break;
                }
                let candidates: Vec<(String, Arc<SessionHandle>)> = {
                    let active = core
                        .active_sessions
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    if active.is_empty() {
                        continue;
                    }
                    active
                        .iter()
                        .filter(|(_, handle)| handle.owner.is_some())
                        .map(|(id, handle)| (id.clone(), Arc::clone(handle)))
                        .collect()
                };
                for (session_id, handle) in candidates {
                    if let Some(owner) = &handle.owner
                        && owner.has_ended()
                    {
                        let worker_pid = handle
                            .info
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .worker_pid;
                        eprintln!(
                            "tsindex: session {session_id} owner PID {} exited; shutting down worker PID {worker_pid}",
                            owner.pid()
                        );
                        handle.request_stop("owner_exited");
                    }
                }
            }
        })
        .context("failed to start daemon session sweeper")
}

fn handle_connection(
    core: Arc<DaemonCore>,
    stream: UnixStream,
    pending: PendingConnection,
) -> Result<()> {
    let creds =
        verify_peer_credentials(&stream).context("failed to authenticate control client")?;
    stream
        .set_read_timeout(Some(CONTROL_IO_TIMEOUT))
        .context("failed to set control connection read timeout")?;
    stream
        .set_write_timeout(Some(CONTROL_IO_TIMEOUT))
        .context("failed to set control connection write timeout")?;

    let mut writer = stream
        .try_clone()
        .context("failed to clone control connection")?;
    let mut reader = BufReader::new(stream);
    let line = read_control_deadline(&mut reader, CONTROL_IO_TIMEOUT)
        .context("failed to read control handshake")?;
    let request: DaemonRequest = match serde_json::from_str(&line) {
        Ok(req) => req,
        Err(error) => {
            let _ = write_json_line(
                &mut writer,
                &DaemonResponse::Error {
                    error: format!("invalid daemon request: {error}"),
                },
            );
            return Ok(());
        }
    };

    // Finite controls keep their permit through waits and response writes.
    // Long-lived branches transfer it only after claiming a bounded session
    // or watcher slot, keeping every connection handler accounted for.
    match request {
        DaemonRequest::Ping => {
            write_json_line(&mut writer, &DaemonResponse::Pong(core.status()))?;
        }
        DaemonRequest::ListSessions {
            include_recent,
            offset,
        } => {
            let (active, recent) = core.list_sessions(include_recent);
            let total = active.len() + recent.len();
            let active_count = active.len();
            let mut active_page = Vec::new();
            let mut recent_page = Vec::new();
            let mut byte_budget = MAX_CONTROL_LINE_BYTES - 4096;
            let mut consumed = 0;
            for (index, mut info) in active
                .into_iter()
                .chain(recent)
                .enumerate()
                .skip(offset)
                .take(SESSION_PAGE_SIZE)
            {
                summarize_session(&mut info);
                let size = serde_json::to_vec(&info)?.len() + 1;
                if size > byte_budget {
                    break;
                }
                byte_budget -= size;
                consumed += 1;
                if index < active_count {
                    active_page.push(info);
                } else {
                    recent_page.push(info);
                }
            }
            let next_offset = (offset.saturating_add(consumed) < total)
                .then_some(offset.saturating_add(consumed));
            write_json_line(
                &mut writer,
                &DaemonResponse::Sessions {
                    active: active_page,
                    recent: recent_page,
                    next_offset,
                },
            )?;
        }
        DaemonRequest::SessionDetail { session_id } => {
            let (active, recent) = core.list_sessions(true);
            let response = match active
                .into_iter()
                .chain(recent)
                .find(|info| info.session_id == session_id)
            {
                Some(mut session) => {
                    bound_session_fields(&mut session);
                    // Escaped diagnostics can take six serialized bytes per
                    // stored byte. Trim by serialized size as well as storage.
                    while serde_json::to_vec(&session)?.len() > MAX_CONTROL_LINE_BYTES - 4096
                        && !session.recent_stderr.is_empty()
                    {
                        session.recent_stderr.remove(0);
                    }
                    DaemonResponse::Session { session }
                }
                None => DaemonResponse::Error {
                    error: format!("unknown session {session_id:?}"),
                },
            };
            write_json_line(&mut writer, &response)?;
        }
        DaemonRequest::WatchSession { session_id } => {
            let mut pending = Some(pending);
            let mut watcher_lease = None;
            if let Err(error) = core.watch_session(
                &session_id,
                creds.pid,
                &mut writer,
                &mut pending,
                &mut watcher_lease,
            ) {
                let mut error = error.to_string();
                truncate_text(&mut error, 1024);
                let _ = write_json_line(&mut writer, &DaemonResponse::Error { error });
            }
        }
        DaemonRequest::KillSession { session_id } => {
            let response = match core.kill_session(&session_id, "killed_by_user") {
                Ok(killed) => DaemonResponse::Killed { killed },
                Err(error) => DaemonResponse::Error {
                    error: format!("{error:#}"),
                },
            };
            write_json_line(&mut writer, &response)?;
        }
        DaemonRequest::KillAll => {
            let killed = core.kill_all("killed_by_user");
            let response = match core.wait_sessions_stopped(&killed) {
                Ok(()) => DaemonResponse::Killed { killed },
                Err(error) => DaemonResponse::Error {
                    error: error.to_string(),
                },
            };
            write_json_line(&mut writer, &response)?;
        }
        DaemonRequest::Shutdown => {
            write_json_line(&mut writer, &DaemonResponse::Stopping)?;
            core.request_shutdown();
        }
        DaemonRequest::StartSession(req) => {
            if core.is_shutting_down() {
                write_json_line(
                    &mut writer,
                    &DaemonResponse::Error {
                        error: "tsindex daemon is shutting down".to_string(),
                    },
                )?;
                return Ok(());
            }
            handle_start_session(core, creds.pid, req, reader, writer, pending)?;
        }
    }
    Ok(())
}

fn handle_start_session(
    core: Arc<DaemonCore>,
    peer_pid: Option<u32>,
    req: StartSessionRequest,
    reader: BufReader<UnixStream>,
    mut writer: UnixStream,
    pending: PendingConnection,
) -> Result<()> {
    if req.protocol != PROTOCOL_VERSION {
        write_json_line(
            &mut writer,
            &DaemonResponse::Error {
                error: format!(
                    "protocol mismatch: client uses v{}, running daemon uses v{}; restart the daemon with `tsindex daemon restart`",
                    req.protocol, PROTOCOL_VERSION
                ),
            },
        )?;
        return Ok(());
    }

    if req.client_version != env!("CARGO_PKG_VERSION") {
        write_json_line(
            &mut writer,
            &DaemonResponse::Error {
                error: "CLI and daemon versions differ; run `tsindex daemon restart`".into(),
            },
        )?;
        return Ok(());
    }
    if !req.root.exists() {
        write_json_line(
            &mut writer,
            &DaemonResponse::Error {
                error: format!("workspace root does not exist: {}", req.root.display()),
            },
        )?;
        return Ok(());
    }

    let Some(client_pid) = peer_pid else {
        write_json_line(&mut writer, &DaemonResponse::Error {
            error: "daemon sessions require a kernel-verified peer PID; use direct `tsindex serve --mcp` on this platform".into(),
        })?;
        return Ok(());
    };
    let owner = match ProcessOwner::for_peer(client_pid, req.owner_pid, req.owner_explicit) {
        Ok(owner) => owner,
        Err(error) => {
            write_json_line(
                &mut writer,
                &DaemonResponse::Error {
                    error: format!("{error:#}"),
                },
            )?;
            return Ok(());
        }
    };

    if serde_json::to_vec(&req)?.len() > 16 * 1024 {
        write_json_line(
            &mut writer,
            &DaemonResponse::Error {
                error: "session metadata exceeds 16 KiB limit".into(),
            },
        )?;
        return Ok(());
    }
    let seq = core.next_session_seq.fetch_add(1, Ordering::Relaxed);
    let session_id = format!("{}-{seq}", core.instance_id);
    let tracked_owner_pid = owner.as_ref().map(ProcessOwner::pid);
    let handle = Arc::new(SessionHandle {
        info: Mutex::new(SessionInfo {
            session_id: session_id.clone(),
            state: SessionState::Starting,
            root: req.root.clone(),
            db: req.db.clone(),
            languages: req.languages.clone(),
            no_refresh: req.no_refresh,
            worker_pid: 0,
            client_pid,
            owner_pid: tracked_owner_pid,
            harness_name: None,
            harness_version: None,
            started_at: unix_now_secs(),
            ended_at: None,
            last_activity_at: unix_now_secs(),
            requests_received: 0,
            responses_sent: 0,
            tool_calls: 0,
            exit_reason: None,
            recent_stderr: Vec::new(),
        }),
        owner,
        completed: Condvar::new(),
        watcher_registered: AtomicBool::new(false),
        forced_stop: AtomicBool::new(false),
        stop_reason: Mutex::new(None),
        stderr_lines: Mutex::new(VecDeque::new()),
    });
    {
        let mut active = core
            .active_sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        // Reserve before spawning while holding the same lock shutdown uses to
        // enumerate sessions. Starting workers are visible to cancellation.
        if core.is_shutting_down() || active.len() >= core.max_sessions {
            write_json_line(
                &mut writer,
                &DaemonResponse::Error {
                    error: "daemon is shutting down or maximum concurrent sessions reached".into(),
                },
            )?;
            return Ok(());
        }
        active.insert(session_id.clone(), Arc::clone(&handle));
    }
    // The Starting record now bounds the handler through startup and teardown.
    drop(pending);
    let result = run_session(&core, &req, &session_id, &handle, reader, &mut writer);
    let starting =
        handle.info.lock().unwrap_or_else(|e| e.into_inner()).state == SessionState::Starting;
    if core.is_shutting_down() {
        handle.request_stop("daemon_shutdown");
    }
    let reason = handle
        .stop_reason
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .unwrap_or_else(|| match &result {
            Ok(reason) => reason.clone(),
            Err(error) => format!("session_failed: {error:#}"),
        });
    if let Err(error) = result
        && starting
    {
        // Startup errors are still control frames, keeping MCP stdout clean.
        let mut detail = format!("MCP worker startup failed: {error:#}");
        truncate_text(&mut detail, 8192);
        let _ = write_json_line(&mut writer, &DaemonResponse::Error { error: detail });
    }
    core.finish_session(&session_id, reason.clone());
    eprintln!("tsindex: ended session {session_id} (reason: {reason})");
    Ok(())
}

fn read_control_deadline(reader: &mut BufReader<UnixStream>, timeout: Duration) -> Result<String> {
    reader.get_ref().set_nonblocking(true)?;
    let deadline = Instant::now() + timeout;
    let mut raw = Vec::new();
    loop {
        // fill_buf preserves bytes pipelined after the initial control frame.
        match reader.fill_buf() {
            Ok([]) => bail!("connection closed before control frame"),
            Ok(bytes) => {
                let count = bytes
                    .iter()
                    .position(|byte| *byte == b'\n')
                    .map(|n| n + 1)
                    .unwrap_or(bytes.len());
                if raw.len() + count > MAX_CONTROL_LINE_BYTES {
                    bail!("control frame exceeds maximum length");
                }
                let done = bytes[count - 1] == b'\n';
                raw.extend_from_slice(&bytes[..count]);
                reader.consume(count);
                if done {
                    reader.get_ref().set_nonblocking(false)?;
                    return String::from_utf8(raw).context("invalid control frame UTF-8");
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) => {}
            Err(error) => return Err(error.into()),
        }
        if Instant::now() >= deadline {
            bail!("control handshake deadline exceeded");
        }
        let mut fds = [libc::pollfd {
            fd: reader.get_ref().as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        }];
        poll(&mut fds)?;
    }
}

struct PendingConnection(Arc<DaemonCore>);

impl PendingConnection {
    fn try_acquire(core: &Arc<DaemonCore>) -> Option<Self> {
        core.pending_connections
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < MAX_PENDING_CONNECTIONS).then_some(count + 1)
            })
            .ok()
            .map(|_| Self(Arc::clone(core)))
    }
}

impl Drop for PendingConnection {
    fn drop(&mut self) {
        self.0.pending_connections.fetch_sub(1, Ordering::AcqRel);
    }
}

struct WatcherLease(Arc<DaemonCore>);

impl WatcherLease {
    fn try_acquire(core: &Arc<DaemonCore>) -> Option<Self> {
        core.outcome_watchers
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < core.max_sessions).then_some(count + 1)
            })
            .ok()
            .map(|_| Self(Arc::clone(core)))
    }
}

impl Drop for WatcherLease {
    fn drop(&mut self) {
        self.0.outcome_watchers.fetch_sub(1, Ordering::AcqRel);
    }
}

fn run_session(
    core: &DaemonCore,
    req: &StartSessionRequest,
    session_id: &str,
    handle: &Arc<SessionHandle>,
    reader: BufReader<UnixStream>,
    writer: &mut UnixStream,
) -> Result<String> {
    let mut child = spawn_worker(req, &core.worker_executable, core.worker_groups)?;
    let worker_pid = child.id();
    handle
        .info
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .worker_pid = worker_pid;
    core.total_sessions_started.fetch_add(1, Ordering::Relaxed);
    let result = supervise_session(&mut child, core, handle, reader, writer, session_id);
    // Closing every pipe first makes waits independent of inherited pipe ends.
    drop(child.stdin.take());
    drop(child.stdout.take());
    drop(child.stderr.take());
    let status_note = reap_worker(&mut child, worker_pid, core.worker_groups);
    // Global shutdown can precede the accept loop's kill_all snapshot. Record
    // that cause locally before a forced exit status can replace the outcome.
    if core.is_shutting_down() {
        handle.request_stop("daemon_shutdown");
    }
    let explicit = handle
        .stop_reason
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    result.map(|reason| explicit.or(status_note).unwrap_or(reason))
}

fn spawn_worker(
    req: &StartSessionRequest,
    executable: &std::path::Path,
    worker_groups: bool,
) -> Result<Child> {
    let mut cmd = Command::new(executable);
    cmd.arg("--root").arg(&req.root);
    if let Some(db) = &req.db {
        cmd.arg("--db").arg(db);
    }
    for language in &req.languages {
        cmd.arg("--languages").arg(language);
    }
    if req.no_refresh {
        cmd.arg("--no-refresh");
    }
    cmd.args(["daemon", "worker"]);
    if req.root.is_dir() {
        cmd.current_dir(&req.root);
    }
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(target_os = "linux")]
    let parent_pid = std::process::id() as libc::pid_t;
    #[cfg(target_os = "macos")]
    let descriptor_limit = macos_worker_descriptor_limit()?;
    // SAFETY: these are async-signal-safe syscalls in the fork/exec interval.
    unsafe {
        cmd.pre_exec(move || {
            #[cfg(target_os = "macos")]
            mark_worker_fds_cloexec(descriptor_limit)?;
            if worker_groups && libc::setpgid(0, 0) != 0 {
                return Err(io::Error::last_os_error());
            }
            // A fork preserves the launching thread's mask. Reset the worker's
            // dispositions before permitting termination or parent-death delivery.
            let action = default_signal_action();
            for signal in SUPERVISOR_SIGNALS {
                if libc::sigaction(signal, &action, std::ptr::null_mut()) < 0 {
                    return Err(io::Error::last_os_error());
                }
            }
            if libc::sigprocmask(
                libc::SIG_UNBLOCK,
                &supervisor_signal_set(),
                std::ptr::null_mut(),
            ) < 0
            {
                return Err(io::Error::last_os_error());
            }
            #[cfg(target_os = "linux")]
            {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM, 0, 0, 0) != 0 {
                    return Err(io::Error::last_os_error());
                }
                if libc::getppid() != parent_pid {
                    libc::_exit(1);
                }
            }
            Ok(())
        });
    }
    cmd.spawn()
        .context("failed to spawn worker; restart the daemon after upgrading tsindex")
}

#[cfg(target_os = "macos")]
fn macos_worker_descriptor_limit() -> io::Result<libc::c_int> {
    let mut limit: libc::rlimit = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let mut ceiling: libc::c_int = 0;
    let mut size = std::mem::size_of_val(&ceiling);
    // Query before fork, where sysctl may allocate. The kernel ceiling keeps
    // RLIM_INFINITY finite without imposing a new application limit.
    // https://github.com/apple-oss-distributions/xnu/blob/main/bsd/kern/kern_resource.c
    if unsafe {
        libc::sysctlbyname(
            c"kern.maxfilesperproc".as_ptr(),
            (&mut ceiling as *mut libc::c_int).cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    if ceiling < 3 {
        return Err(io::Error::other("invalid kernel descriptor ceiling"));
    }
    Ok(limit.rlim_cur.min(ceiling as libc::rlim_t) as libc::c_int)
}

#[cfg(any(target_os = "macos", test))]
fn mark_worker_fds_cloexec(limit: libc::c_int) -> io::Result<()> {
    // Darwin may set CLOEXEC after creating/accepting sockets. Mark the actual
    // inherited descriptors in the forked child, covering descriptors another
    // daemon thread opened concurrently with worker creation. Only 0..2 are
    // intentionally inherited. The spawn error pipe remains open until exec.
    for fd in 3..limit {
        if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EBADF) {
                return Err(error);
            }
        }
    }
    Ok(())
}

fn nonblocking(fd: libc::c_int) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn poll(fds: &mut [libc::pollfd]) -> io::Result<()> {
    let rc = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, 25) };
    if rc < 0 && io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn read_available(reader: &mut impl Read, output: &mut Vec<u8>, max: usize) -> Result<bool> {
    let mut bytes = [0u8; 8192];
    // One bounded read per turn keeps a noisy stream from starving cancellation.
    match reader.read(&mut bytes) {
        Ok(0) => Ok(true),
        Ok(n) => {
            if output.len() + n > max {
                bail!("stream exceeded maximum length of {max} bytes");
            }
            output.extend_from_slice(&bytes[..n]);
            Ok(false)
        }
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(error.into()),
    }
}

fn flush_pending(writer: &mut impl Write, pending: &mut VecDeque<u8>) -> io::Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let bytes = pending.as_slices().0;
    match writer.write(bytes) {
        Ok(n) => {
            pending.drain(..n);
            Ok(())
        }
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn capture_stderr(child: &mut Child, handle: &SessionHandle, partial: &mut Vec<u8>) -> bool {
    let Some(stderr) = child.stderr.as_mut() else {
        return true;
    };
    let mut bytes = [0u8; 2048];
    match stderr.read(&mut bytes) {
        Ok(0) => {
            if !partial.is_empty() {
                handle.record_stderr(String::from_utf8_lossy(partial).into_owned());
                partial.clear();
            }
            true
        }
        Ok(n) => {
            for byte in &bytes[..n] {
                if *byte == b'\n' || partial.len() >= MAX_STDERR_LINE_BYTES {
                    handle.record_stderr(String::from_utf8_lossy(partial).into_owned());
                    partial.clear();
                }
                if *byte != b'\n' {
                    partial.push(*byte);
                }
            }
            false
        }
        Err(error) => !matches!(
            error.kind(),
            io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
        ),
    }
}

fn supervise_session(
    child: &mut Child,
    core: &DaemonCore,
    handle: &Arc<SessionHandle>,
    mut reader: BufReader<UnixStream>,
    writer: &mut UnixStream,
    session_id: &str,
) -> Result<String> {
    let stdin_fd = child
        .stdin
        .as_ref()
        .context("missing worker stdin")?
        .as_raw_fd();
    let stdout_fd = child
        .stdout
        .as_ref()
        .context("missing worker stdout")?
        .as_raw_fd();
    let stderr_fd = child
        .stderr
        .as_ref()
        .context("missing worker stderr")?
        .as_raw_fd();
    for fd in [stdin_fd, stdout_fd, stderr_fd] {
        nonblocking(fd)?;
    }
    let startup_deadline = Instant::now() + STARTUP_IO_TIMEOUT;
    let mut ready_bytes = Vec::new();
    let mut stderr_partial = Vec::new();
    let mut stderr_eof = false;
    loop {
        if core.is_shutting_down() {
            handle.request_stop("daemon_shutdown");
        }
        if handle.forced_stop.load(Ordering::Acquire) {
            bail!("session cancelled during startup");
        }
        let mut peer = libc::pollfd {
            fd: reader.get_ref().as_raw_fd(),
            events: 0,
            revents: 0,
        };
        unsafe {
            libc::poll(&mut peer, 1, 0);
        }
        if peer.revents & (libc::POLLHUP | libc::POLLERR) != 0 {
            bail!("client disconnected during worker initialization");
        }
        if !stderr_eof {
            stderr_eof = capture_stderr(child, handle, &mut stderr_partial);
        }
        let ended = read_available(
            child.stdout.as_mut().unwrap(),
            &mut ready_bytes,
            MAX_CONTROL_LINE_BYTES,
        )?;
        if let Some(end) = ready_bytes.iter().position(|b| *b == b'\n') {
            let ready: WorkerReady = serde_json::from_slice(&ready_bytes[..end])
                .context("invalid worker readiness acknowledgement")?;
            if ready.protocol != PROTOCOL_VERSION || ready.version != env!("CARGO_PKG_VERSION") {
                bail!("worker version/protocol differs from daemon; run `tsindex daemon restart`");
            }
            ready_bytes.drain(..=end);
            break;
        }
        if ended {
            if !stderr_eof {
                capture_stderr(child, handle, &mut stderr_partial);
            }
            if !stderr_partial.is_empty() {
                handle.record_stderr(String::from_utf8_lossy(&stderr_partial).into_owned());
            }
            let detail = handle.snapshot().recent_stderr.join("\n");
            bail!("worker exited before initialization: {detail}");
        }
        if Instant::now() >= startup_deadline {
            bail!("worker initialization timed out");
        }
        let mut fds = [
            libc::pollfd {
                fd: stdout_fd,
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: if stderr_eof { -1 } else { stderr_fd },
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        poll(&mut fds)?;
    }
    // Publish readiness state before the acknowledgement can reach a client
    // that immediately establishes its outcome watcher on another connection.
    handle.info.lock().unwrap_or_else(|e| e.into_inner()).state = SessionState::Active;
    // Only acknowledge a usable worker, never merely a successful fork.
    write_json_line(
        writer,
        &DaemonResponse::SessionReady {
            session_id: session_id.to_string(),
            worker_pid: child.id(),
            daemon_pid: std::process::id(),
            daemon_version: env!("CARGO_PKG_VERSION").to_string(),
            tracked_owner_pid: handle.owner.as_ref().map(ProcessOwner::pid),
        },
    )?;
    eprintln!(
        "tsindex: started session {session_id} (worker PID {}, owner PID {:?}, root {})",
        child.id(),
        handle.owner.as_ref().map(ProcessOwner::pid),
        handle.snapshot().root.display()
    );
    reader.get_ref().set_nonblocking(true)?;
    let _ = writer.set_write_timeout(None);
    let _ = reader.get_ref().set_read_timeout(None);
    let mut input_line = Vec::new();
    let mut input_scan = 0;
    let mut to_worker = VecDeque::new();
    let mut to_client: VecDeque<u8> = ready_bytes.into();
    let mut input_eof = false;
    let mut output_eof = false;
    let mut drain_deadline = None;
    let mut output_line_bytes = 0usize;
    loop {
        // This deadline is evaluated regardless of progress on any data pipe.
        if core.is_shutting_down() {
            handle.request_stop("daemon_shutdown");
        }
        if handle.forced_stop.load(Ordering::Acquire) {
            return Ok("session_cancelled".into());
        }
        if child_has_exited(child.id())? {
            drain_deadline.get_or_insert(Instant::now() + GRACEFUL_DRAIN_TIMEOUT);
            if output_eof && to_client.is_empty() {
                return Ok(if input_eof {
                    "client_eof".into()
                } else {
                    "worker_exited".into()
                });
            }
        }
        if drain_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Ok("drain_timeout".into());
        }
        if input_eof && to_worker.is_empty() {
            drop(child.stdin.take());
        }
        if input_eof && output_eof && to_client.is_empty() {
            return Ok("client_eof".into());
        }
        // poll reports HUP/ERR even with an empty event mask. Remove
        // inactive/closed endpoints to avoid spinning during bounded drain.
        let socket_events = if !input_eof && to_worker.len() < MAX_MCP_FRAME_BYTES {
            libc::POLLIN
        } else {
            0
        } | if !to_client.is_empty() {
            libc::POLLOUT
        } else {
            0
        };
        let mut fds = [
            libc::pollfd {
                fd: if socket_events == 0 {
                    -1
                } else {
                    reader.get_ref().as_raw_fd()
                },
                events: socket_events,
                revents: 0,
            },
            libc::pollfd {
                fd: if child.stdin.is_some() && !to_worker.is_empty() {
                    stdin_fd
                } else {
                    -1
                },
                events: if to_worker.is_empty() {
                    0
                } else {
                    libc::POLLOUT
                },
                revents: 0,
            },
            libc::pollfd {
                fd: if output_eof || to_client.len() >= MAX_MCP_FRAME_BYTES {
                    -1
                } else {
                    stdout_fd
                },
                events: if to_client.len() < MAX_MCP_FRAME_BYTES {
                    libc::POLLIN
                } else {
                    0
                },
                revents: 0,
            },
            libc::pollfd {
                fd: if stderr_eof { -1 } else { stderr_fd },
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        poll(&mut fds)?;
        if !stderr_eof {
            stderr_eof = capture_stderr(child, handle, &mut stderr_partial);
        }
        if !input_eof && to_worker.len() < MAX_MCP_FRAME_BYTES {
            // Drain buffered complete frames before taking additional input so
            // a burst of valid small messages cannot be mistaken for one large
            // frame while the worker applies backpressure.
            let mut blocked_frame = false;
            while let Some(relative_end) = memchr::memchr(b'\n', &input_line[input_scan..]) {
                let end = input_scan + relative_end;
                if to_worker.len() + end + 1 > MAX_MCP_FRAME_BYTES {
                    blocked_frame = true;
                    break;
                }
                let frame: Vec<u8> = input_line.drain(..=end).collect();
                input_scan = 0;
                observe_client_message(handle, &frame);
                to_worker.extend(frame);
            }
            if !blocked_frame {
                input_scan = input_line.len();
            }
            let eof = if blocked_frame {
                false
            } else {
                read_available(&mut reader, &mut input_line, MAX_MCP_FRAME_BYTES)?
            };
            if eof {
                // A final unterminated frame remains valid JSON-RPC input.
                to_worker.extend(input_line.drain(..));
                input_eof = true;
                drain_deadline = Some(Instant::now() + GRACEFUL_DRAIN_TIMEOUT);
                handle.info.lock().unwrap_or_else(|e| e.into_inner()).state =
                    SessionState::Draining;
            }
        }
        if !to_worker.is_empty()
            && let Some(stdin) = child.stdin.as_mut()
            && flush_pending(stdin, &mut to_worker).is_err()
        {
            return Ok("worker_stdin_closed".into());
        }
        if !output_eof && to_client.len() < MAX_MCP_FRAME_BYTES {
            let mut chunk = Vec::new();
            output_eof = read_available(child.stdout.as_mut().unwrap(), &mut chunk, 8192)?;
            for byte in &chunk {
                output_line_bytes += 1;
                if output_line_bytes > MAX_MCP_FRAME_BYTES {
                    bail!("worker response exceeds MCP frame limit");
                }
                if *byte == b'\n' {
                    output_line_bytes = 0;
                    let mut info = handle.info.lock().unwrap_or_else(|e| e.into_inner());
                    info.responses_sent += 1;
                    info.last_activity_at = unix_now_secs();
                }
            }
            to_client.extend(chunk);
        }
        if flush_pending(writer, &mut to_client).is_err() {
            return Ok("client_disconnected".into());
        }
    }
}

fn truncate_text(text: &mut String, max: usize) {
    if text.len() > max {
        let mut end = max;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
    }
}

fn bound_session_fields(info: &mut SessionInfo) {
    if let Some(value) = &mut info.harness_name {
        truncate_text(value, 256);
    }
    if let Some(value) = &mut info.harness_version {
        truncate_text(value, 256);
    }
    if let Some(value) = &mut info.exit_reason {
        truncate_text(value, 1024);
    }
}

fn summarize_session(info: &mut SessionInfo) {
    info.recent_stderr.clear();
    bound_session_fields(info);
}

/// Inspect bounded frames to collect identity and activity diagnostics.
fn observe_client_message(handle: &SessionHandle, raw: &[u8]) {
    let now = unix_now_secs();
    let parsed = serde_json::from_slice::<Value>(raw).ok();
    let mut info = handle.info.lock().unwrap_or_else(|e| e.into_inner());
    info.requests_received += 1;
    info.last_activity_at = now;
    if let Some(value) = parsed {
        match value.get("method").and_then(Value::as_str) {
            Some("initialize") => {
                if let Some(client) = value.pointer("/params/clientInfo") {
                    if let Some(name) = client.get("name").and_then(Value::as_str) {
                        info.harness_name = Some(name.to_string());
                    }
                    if let Some(version) = client.get("version").and_then(Value::as_str) {
                        info.harness_version = Some(version.to_string());
                    }
                    bound_session_fields(&mut info);
                }
            }
            Some("tools/call") => {
                info.tool_calls += 1;
            }
            _ => {}
        }
    }
}

/// Observe exit without releasing the PID. Retaining the leader until all
/// group signals have been sent prevents a reused PGID selecting other work.
/// https://man7.org/linux/man-pages/man2/waitid.2.html
fn child_has_exited(pid: u32) -> io::Result<bool> {
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    let result = unsafe {
        libc::waitid(
            libc::P_PID,
            pid as libc::id_t,
            &mut info,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { info.si_pid() } != 0)
}

#[cfg(target_os = "linux")]
fn group_has_live_members(group: u32) -> io::Result<bool> {
    for entry in fs::read_dir("/proc")? {
        let entry = entry?;
        if !entry
            .file_name()
            .as_encoded_bytes()
            .iter()
            .all(u8::is_ascii_digit)
        {
            continue;
        }
        let stat = match fs::read(entry.path().join("stat")) {
            Ok(stat) => stat,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        let (pgrp, live) = parse_process_group(&stat)?;
        if pgrp == group && live {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(any(target_os = "linux", test))]
fn parse_process_group(stat: &[u8]) -> io::Result<(u32, bool)> {
    // Match lifecycle::parse_proc_stat's treatment of arbitrary process names
    // and zombie leaders with live threads, including non-UTF-8 comm bytes.
    let end = stat
        .windows(2)
        .rposition(|pair| pair == b") ")
        .ok_or_else(|| io::Error::other("invalid process stat"))?;
    let tail = std::str::from_utf8(&stat[end + 2..])
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let mut fields = tail.split_whitespace();
    let state = fields
        .next()
        .ok_or_else(|| io::Error::other("missing process state"))?;
    let group = fields
        .nth(1)
        .and_then(|field| field.parse::<u32>().ok())
        .ok_or_else(|| io::Error::other("missing process group"))?;
    let threads = fields
        .nth(14)
        .and_then(|field| field.parse::<u64>().ok())
        .ok_or_else(|| io::Error::other("missing process thread count"))?;
    Ok((group, !matches!(state, "Z" | "X" | "x") || threads > 1))
}

/// Reap the direct worker and terminate remaining members of its process group.
fn reap_worker(child: &mut Child, worker_pid: u32, worker_groups: bool) -> Option<String> {
    let deadline = Instant::now() + FORCED_EOF_GRACE;
    while Instant::now() < deadline && !child_has_exited(worker_pid).unwrap_or(true) {
        thread::sleep(POLL_WAIT_STEP);
    }
    let exited = child_has_exited(worker_pid).unwrap_or(false);
    // A zombie leader retains its PID/PGID through the final group signal.
    // Linux adopts orphaned helpers so they can be reaped per owned group.
    if worker_groups || !exited {
        signal_worker(worker_pid, libc::SIGTERM, worker_groups);
        signal_worker(worker_pid, libc::SIGCONT, worker_groups);
        let deadline = Instant::now() + SIGTERM_GRACE;
        while Instant::now() < deadline {
            let leader_exited = child_has_exited(worker_pid).unwrap_or(false);
            if !worker_groups && leader_exited {
                break;
            }
            #[cfg(target_os = "linux")]
            if worker_groups && leader_exited && !group_has_live_members(worker_pid).unwrap_or(true)
            {
                // Keep the zombie leader's PID reserved, but skip TERM grace
                // once every group member has stopped executing. Inspection
                // failures retain the full conservative escalation deadline.
                break;
            }
            thread::sleep(POLL_WAIT_STEP);
        }
        signal_worker(worker_pid, libc::SIGKILL, worker_groups);
    }
    let status = child.wait().ok();
    #[cfg(target_os = "linux")]
    if worker_groups {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let mut raw_status = 0;
            let result = unsafe {
                libc::waitpid(-(worker_pid as libc::pid_t), &mut raw_status, libc::WNOHANG)
            };
            if result < 0 {
                break;
            }
            if result == 0 {
                if Instant::now() >= deadline {
                    break;
                }
                thread::sleep(POLL_WAIT_STEP);
            }
        }
    }
    status
        .filter(|status| !status.success())
        .map(|status| format!("worker_status_{status}"))
}

fn signal_worker(pid: u32, signal: libc::c_int, group: bool) {
    if pid == 0 || pid > i32::MAX as u32 {
        return;
    }
    let target = if group {
        -(pid as libc::pid_t)
    } else {
        pid as libc::pid_t
    };
    unsafe {
        libc::kill(target, signal);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_session_request(root: &std::path::Path) -> StartSessionRequest {
        StartSessionRequest {
            protocol: PROTOCOL_VERSION,
            client_version: env!("CARGO_PKG_VERSION").into(),
            root: root.to_path_buf(),
            db: None,
            languages: Vec::new(),
            no_refresh: true,
            client_pid: std::process::id(),
            owner_pid: None,
            owner_explicit: false,
        }
    }

    fn test_session_handle(root: &std::path::Path, id: &str) -> Arc<SessionHandle> {
        Arc::new(SessionHandle {
            info: Mutex::new(SessionInfo {
                session_id: id.to_string(),
                state: SessionState::Active,
                root: root.to_path_buf(),
                db: None,
                languages: Vec::new(),
                no_refresh: true,
                worker_pid: 0,
                client_pid: std::process::id(),
                owner_pid: None,
                harness_name: None,
                harness_version: None,
                started_at: unix_now_secs(),
                ended_at: None,
                last_activity_at: unix_now_secs(),
                requests_received: 0,
                responses_sent: 0,
                tool_calls: 0,
                exit_reason: None,
                recent_stderr: Vec::new(),
            }),
            owner: None,
            completed: Condvar::new(),
            watcher_registered: AtomicBool::new(false),
            forced_stop: AtomicBool::new(false),
            stop_reason: Mutex::new(None),
            stderr_lines: Mutex::new(VecDeque::new()),
        })
    }

    #[test]
    fn accept_loop_rejects_exhausted_handler_budget_and_recovers() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = DaemonPaths::resolve(Some(&dir.path().join("d.sock")))?;
        let listener = UnixListener::bind(&paths.socket_path)?;
        let (wake_reader, wake_writer) = UnixStream::pair()?;
        let mut core = DaemonCore::new(paths.clone(), wake_writer, true)?;
        core.max_sessions = 1;
        let core = Arc::new(core);
        let ceiling = MAX_PENDING_CONNECTIONS + 2;
        let mut guards: Vec<_> = (0..ceiling)
            .map(|_| ActiveThreadGuard::try_enter(Arc::clone(&core)).unwrap())
            .collect();
        thread::scope(|scope| -> Result<()> {
            struct StopOnDrop(Arc<DaemonCore>);
            impl Drop for StopOnDrop {
                fn drop(&mut self) {
                    self.0.request_shutdown();
                }
            }
            let _stop = StopOnDrop(Arc::clone(&core));
            let accept_core = Arc::clone(&core);
            let accept = scope.spawn(move || accept_loop(&listener, &wake_reader, &accept_core));
            let mut rejected = UnixStream::connect(&paths.socket_path)?;
            rejected.set_read_timeout(Some(Duration::from_secs(1)))?;
            let mut byte = [0u8; 1];
            assert_eq!(
                rejected.read(&mut byte)?,
                0,
                "exhausted handler budget admitted a connection"
            );

            drop(guards.pop());
            let mut recovered = UnixStream::connect(&paths.socket_path)?;
            recovered.set_read_timeout(Some(Duration::from_secs(1)))?;
            write_json_line(&mut recovered, &DaemonRequest::Ping)?;
            let mut line = String::new();
            BufReader::new(recovered).read_line(&mut line)?;
            assert!(matches!(
                serde_json::from_str::<DaemonResponse>(&line)?,
                DaemonResponse::Pong(_)
            ));

            // Refill the outer budget after the recovered handler finishes.
            // Shutdown must wake and join the accept loop even at saturation.
            let deadline = Instant::now() + Duration::from_secs(1);
            let restored = loop {
                if let Some(guard) = ActiveThreadGuard::try_enter(Arc::clone(&core)) {
                    break guard;
                }
                assert!(
                    Instant::now() < deadline,
                    "recovered handler did not release its guard"
                );
                thread::sleep(Duration::from_millis(1));
            };
            guards.push(restored);
            core.request_shutdown();
            accept.join().unwrap()?;
            Ok(())
        })?;
        drop(guards);
        assert_eq!(*core.active_threads.lock().unwrap(), 0);
        assert_eq!(core.pending_connections.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[test]
    fn completed_watchers_keep_a_global_lease_until_outer_handler_finishes() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = DaemonPaths::resolve(Some(&dir.path().join("d.sock")))?;
        let (_wake_reader, wake_writer) = UnixStream::pair()?;
        let mut core = DaemonCore::new(paths, wake_writer, true)?;
        core.max_sessions = 1;
        let core = Arc::new(core);
        core.active_sessions
            .lock()
            .unwrap()
            .insert("first".into(), test_session_handle(dir.path(), "first"));
        thread::scope(|scope| -> Result<()> {
            struct FinishOnDrop(Arc<DaemonCore>);
            impl Drop for FinishOnDrop {
                fn drop(&mut self) {
                    self.0.finish_session("first", "fixture_cleanup".into());
                }
            }
            let _cleanup = FinishOnDrop(Arc::clone(&core));
            let (client, mut server) = UnixStream::pair()?;
            client.set_read_timeout(Some(Duration::from_secs(1)))?;
            let (held_tx, held_rx) = std::sync::mpsc::sync_channel(0);
            let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
            let watch_core = Arc::clone(&core);
            let handler = scope.spawn(move || -> Result<()> {
                let mut pending = Some(PendingConnection::try_acquire(&watch_core).unwrap());
                let mut watcher_lease = None;
                let result = watch_core.watch_session(
                    "first",
                    Some(std::process::id()),
                    &mut server,
                    &mut pending,
                    &mut watcher_lease,
                );
                // Model a descheduled outer handler after terminal delivery,
                // while it still owns any final/error response cleanup.
                held_tx
                    .send(())
                    .map_err(|_| anyhow::anyhow!("fixture coordination closed"))?;
                let _ = release_rx.recv();
                result
            });
            let mut outcome = BufReader::new(client);
            let mut line = String::new();
            outcome.read_line(&mut line)?;
            assert!(matches!(
                serde_json::from_str::<DaemonResponse>(&line)?,
                DaemonResponse::Watching
            ));
            core.finish_session("first", "client_eof".into());
            line.clear();
            outcome.read_line(&mut line)?;
            assert!(matches!(
                serde_json::from_str::<DaemonResponse>(&line)?,
                DaemonResponse::Session { .. }
            ));
            held_rx.recv_timeout(Duration::from_secs(1))?;
            assert!(core.active_sessions.lock().unwrap().is_empty());
            assert_eq!(core.pending_connections.load(Ordering::Acquire), 0);
            assert_eq!(core.outcome_watchers.load(Ordering::Acquire), 1);

            // Reusing the freed worker slot cannot admit another watcher while
            // the completed session's outer connection still owns its lease.
            let next = test_session_handle(dir.path(), "next");
            core.active_sessions
                .lock()
                .unwrap()
                .insert("next".into(), Arc::clone(&next));
            let (_next_client, mut next_server) = UnixStream::pair()?;
            let mut pending = Some(PendingConnection::try_acquire(&core).unwrap());
            let mut watcher_lease = None;
            let error = core
                .watch_session(
                    "next",
                    Some(std::process::id()),
                    &mut next_server,
                    &mut pending,
                    &mut watcher_lease,
                )
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("maximum concurrent outcome watchers")
            );
            assert!(!next.watcher_registered.load(Ordering::Acquire));
            assert!(pending.is_some() && watcher_lease.is_none());
            drop(pending);
            release_tx.send(())?;
            handler.join().unwrap()?;
            assert_eq!(core.outcome_watchers.load(Ordering::Acquire), 0);
            assert!(WatcherLease::try_acquire(&core).is_some());
            Ok(())
        })
    }

    #[test]
    fn duplicate_and_wrong_peer_watchers_return_global_leases() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = DaemonPaths::resolve(Some(&dir.path().join("d.sock")))?;
        let (_wake_reader, wake_writer) = UnixStream::pair()?;
        let mut core = DaemonCore::new(paths, wake_writer, true)?;
        core.max_sessions = 1;
        let core = Arc::new(core);
        let handle = test_session_handle(dir.path(), "claimed");
        // A prior watch whose initial acknowledgement failed leaves the
        // per-session claim set after returning its global connection lease.
        handle.watcher_registered.store(true, Ordering::Release);
        core.active_sessions
            .lock()
            .unwrap()
            .insert("claimed".into(), handle);
        let (_client, mut server) = UnixStream::pair()?;
        let mut pending = Some(PendingConnection::try_acquire(&core).unwrap());
        let mut watcher_lease = None;
        let duplicate = core
            .watch_session(
                "claimed",
                Some(std::process::id()),
                &mut server,
                &mut pending,
                &mut watcher_lease,
            )
            .unwrap_err();
        assert!(duplicate.to_string().contains("already has"));
        assert_eq!(core.outcome_watchers.load(Ordering::Acquire), 0);
        assert!(pending.is_some() && watcher_lease.is_none());
        let wrong_peer = core
            .watch_session(
                "claimed",
                None,
                &mut server,
                &mut pending,
                &mut watcher_lease,
            )
            .unwrap_err();
        assert!(wrong_peer.to_string().contains("original proxy process"));
        assert_eq!(core.outcome_watchers.load(Ordering::Acquire), 0);
        assert!(pending.is_some() && watcher_lease.is_none());
        Ok(())
    }

    #[test]
    fn slow_kill_controls_keep_their_connection_admission_permits() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = DaemonPaths::resolve(Some(&dir.path().join("d.sock")))?;
        let (_wake_reader, wake_writer) = UnixStream::pair()?;
        let mut core = DaemonCore::new(paths, wake_writer, true)?;
        core.max_sessions = 1;
        let core = Arc::new(core);
        let id = "blocked-worker".to_string();
        let handle = test_session_handle(dir.path(), &id);
        core.active_sessions
            .lock()
            .unwrap()
            .insert(id.clone(), Arc::clone(&handle));
        // Reserve all permits before starting handlers. Once the first kill
        // publishes cancellation, an early dispatch-time release is observable
        // deterministically, without waiting for the four-second deadline.
        let permits: Vec<_> = (0..MAX_PENDING_CONNECTIONS)
            .map(|_| PendingConnection::try_acquire(&core).unwrap())
            .collect();
        thread::scope(|scope| -> Result<()> {
            struct FinishOnDrop(Arc<DaemonCore>, String);
            impl Drop for FinishOnDrop {
                fn drop(&mut self) {
                    self.0.finish_session(&self.1, "fixture_cleanup".into());
                }
            }
            let cleanup = FinishOnDrop(Arc::clone(&core), id.clone());
            let mut clients = Vec::new();
            let mut handlers = Vec::new();
            for pending in permits {
                let (mut client, server) = UnixStream::pair()?;
                client.set_read_timeout(Some(Duration::from_secs(1)))?;
                write_json_line(
                    &mut client,
                    &DaemonRequest::KillSession {
                        session_id: id.clone(),
                    },
                )?;
                clients.push(client);
                let connection_core = Arc::clone(&core);
                handlers
                    .push(scope.spawn(move || handle_connection(connection_core, server, pending)));
            }
            let deadline = Instant::now() + Duration::from_secs(1);
            while !handle.forced_stop.load(Ordering::Acquire) {
                assert!(Instant::now() < deadline, "kill control did not dispatch");
                thread::sleep(Duration::from_millis(1));
            }
            assert_eq!(
                core.pending_connections.load(Ordering::Acquire),
                MAX_PENDING_CONNECTIONS
            );
            assert!(PendingConnection::try_acquire(&core).is_none());
            // Free the fake worker without waiting on real child processes.
            // Control requests were accepted despite the only worker slot
            // being occupied, and they all release admission after responding.
            drop(cleanup);
            for client in clients {
                let mut line = String::new();
                BufReader::new(client).read_line(&mut line)?;
                match serde_json::from_str::<DaemonResponse>(&line)? {
                    DaemonResponse::Killed { killed } => assert_eq!(killed, vec![id.clone()]),
                    // A handler scheduled after fixture completion correctly
                    // observes that its target has already left the registry.
                    DaemonResponse::Error { error } => {
                        assert!(error.contains("unknown active session"))
                    }
                    other => bail!("unexpected kill-control response: {other:?}"),
                }
            }
            for handler in handlers {
                handler.join().unwrap()?;
            }
            Ok(())
        })?;
        assert_eq!(core.pending_connections.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[test]
    fn rejected_watcher_retains_admission_through_its_error_response() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = DaemonPaths::resolve(Some(&dir.path().join("d.sock")))?;
        let (_wake_reader, wake_writer) = UnixStream::pair()?;
        let core = Arc::new(DaemonCore::new(paths, wake_writer, true)?);
        let (client, mut server) = UnixStream::pair()?;
        client.set_read_timeout(Some(Duration::from_secs(1)))?;
        let mut pending = Some(PendingConnection::try_acquire(&core).unwrap());
        let mut watcher_lease = None;
        let error = core
            .watch_session(
                "missing-session",
                Some(std::process::id()),
                &mut server,
                &mut pending,
                &mut watcher_lease,
            )
            .unwrap_err();
        // Rejection must leave admission with the caller that still needs to
        // frame and write the finite response, rather than ending its lifetime
        // inside watch_session before that response is sent.
        assert_eq!(core.pending_connections.load(Ordering::Acquire), 1);
        write_json_line(
            &mut server,
            &DaemonResponse::Error {
                error: error.to_string(),
            },
        )?;
        let mut frame = String::new();
        BufReader::new(client).read_line(&mut frame)?;
        let DaemonResponse::Error { error } = serde_json::from_str(&frame)? else {
            bail!("watch rejection was not framed");
        };
        assert!(error.contains("unknown session"));
        assert_eq!(core.pending_connections.load(Ordering::Acquire), 1);
        drop(pending);
        assert_eq!(core.pending_connections.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[test]
    fn missing_kernel_peer_pid_fails_before_worker_admission() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = DaemonPaths::resolve(Some(&dir.path().join("d.sock")))?;
        let (_wake_reader, wake_writer) = UnixStream::pair()?;
        let core = Arc::new(DaemonCore::new(paths, wake_writer, true)?);
        let (client, server) = UnixStream::pair()?;
        client.set_read_timeout(Some(Duration::from_secs(1)))?;
        handle_start_session(
            Arc::clone(&core),
            None,
            test_session_request(dir.path()),
            BufReader::new(server.try_clone()?),
            server,
            PendingConnection::try_acquire(&core).unwrap(),
        )?;
        let mut line = String::new();
        BufReader::new(client).read_line(&mut line)?;
        let response: DaemonResponse = serde_json::from_str(&line)?;
        let DaemonResponse::Error { error } = response else {
            bail!("unverified peer was admitted");
        };
        assert!(error.contains("kernel-verified peer PID") && error.contains("serve --mcp"));
        assert_eq!(core.total_sessions_started.load(Ordering::Acquire), 0);
        assert!(core.active_sessions.lock().unwrap().is_empty());
        Ok(())
    }

    #[test]
    fn shutdown_outcome_precedes_kill_all_even_when_worker_needs_sigkill() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let helper = dir.path().join("worker");
        let ready = serde_json::json!({ "protocol": PROTOCOL_VERSION, "version": env!("CARGO_PKG_VERSION") });
        fs::write(
            &helper,
            format!("#!/bin/sh\ntrap '' TERM\nprintf '%s\\n' '{ready}'\nexec sleep 60\n"),
        )?;
        fs::set_permissions(&helper, fs::Permissions::from_mode(0o700))?;
        let paths = DaemonPaths::resolve(Some(&dir.path().join("d.sock")))?;
        let (_wake_reader, wake_writer) = UnixStream::pair()?;
        let mut core = DaemonCore::new(paths, wake_writer, true)?;
        core.worker_executable = helper;
        let core = Arc::new(core);
        let (client, server) = UnixStream::pair()?;
        client.set_read_timeout(Some(Duration::from_secs(5)))?;
        let proxy_core = Arc::clone(&core);
        let proxy = thread::spawn(move || -> Result<SessionInfo> {
            struct StopOnDrop(Arc<DaemonCore>);
            impl Drop for StopOnDrop {
                fn drop(&mut self) {
                    self.0.shutdown.store(true, Ordering::Release);
                }
            }
            // Any assertion/error also stops the fixture before scoped joins.
            let stop = StopOnDrop(Arc::clone(&proxy_core));
            let mut data = BufReader::new(client);
            let mut line = String::new();
            data.read_line(&mut line)?;
            let DaemonResponse::SessionReady { session_id, .. } = serde_json::from_str(&line)?
            else {
                bail!("fixture did not initialize");
            };
            assert_eq!(proxy_core.pending_connections.load(Ordering::Acquire), 0);
            thread::scope(|scope| -> Result<SessionInfo> {
                let _stop = stop;
                let (watch_client, mut watch_server) = UnixStream::pair()?;
                watch_client.set_read_timeout(Some(Duration::from_secs(5)))?;
                let watch_core = Arc::clone(&proxy_core);
                let watcher = scope.spawn(move || {
                    let mut pending = Some(PendingConnection::try_acquire(&watch_core).unwrap());
                    let mut watcher_lease = None;
                    watch_core.watch_session(
                        &session_id,
                        Some(std::process::id()),
                        &mut watch_server,
                        &mut pending,
                        &mut watcher_lease,
                    )
                });
                let mut outcome = BufReader::new(watch_client);
                line.clear();
                outcome.read_line(&mut line)?;
                assert!(matches!(
                    serde_json::from_str::<DaemonResponse>(&line)?,
                    DaemonResponse::Watching
                ));
                assert_eq!(proxy_core.pending_connections.load(Ordering::Acquire), 0);
                // Simulate the accept thread being descheduled after observing
                // global shutdown, before it can take a kill_all snapshot.
                proxy_core.shutdown.store(true, Ordering::Release);
                line.clear();
                outcome.read_line(&mut line)?;
                watcher.join().unwrap()?;
                let DaemonResponse::Session { session } = serde_json::from_str(&line)? else {
                    bail!("outcome watcher did not receive final state");
                };
                Ok(session)
            })
        });
        let start = Instant::now();
        let result = handle_start_session(
            Arc::clone(&core),
            Some(std::process::id()),
            test_session_request(dir.path()),
            BufReader::new(server.try_clone()?),
            server,
            PendingConnection::try_acquire(&core).unwrap(),
        );
        let outcome = proxy.join().unwrap()?;
        result?;
        assert!(
            start.elapsed() >= SIGTERM_GRACE,
            "fixture did not require forced escalation"
        );
        assert_eq!(outcome.exit_reason.as_deref(), Some("daemon_shutdown"));
        let recent = core.recent_sessions.lock().unwrap();
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].exit_reason.as_deref(), Some("daemon_shutdown"));
        assert!(core.active_sessions.lock().unwrap().is_empty());
        Ok(())
    }

    #[test]
    fn state_publication_survives_an_old_temporary_with_this_process_pid() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let paths = DaemonPaths::resolve(Some(&dir.path().join("d.sock")))?;
        fs::create_dir_all(paths.state_path.parent().unwrap())?;
        let stale = paths
            .state_path
            .with_extension(format!("tmp.{}", std::process::id()));
        fs::write(&stale, "unfinished publication from a reused PID")?;
        write_state_file(&paths)?;
        let published: DaemonStateFile = serde_json::from_slice(&fs::read(&paths.state_path)?)?;
        assert_eq!(published.pid, std::process::id());
        assert_eq!(
            fs::read_to_string(stale)?,
            "unfinished publication from a reused PID"
        );
        Ok(())
    }

    #[test]
    fn worker_exec_closes_a_descriptor_opened_without_cloexec() -> Result<()> {
        use std::os::fd::{FromRawFd, OwnedFd};
        let file = fs::File::open("/dev/null")?;
        let raw = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 128) };
        assert!(raw >= 128);
        let descriptor = unsafe { OwnedFd::from_raw_fd(raw) };
        let mut command = Command::new("sh");
        command.arg("-c").arg(format!("test ! -e /dev/fd/{raw}"));
        unsafe {
            command.pre_exec(move || {
                // Simulate the accept-to-fcntl window in a different daemon thread.
                if libc::fcntl(raw, libc::F_SETFD, 0) < 0 {
                    return Err(io::Error::last_os_error());
                }
                mark_worker_fds_cloexec(raw + 1)
            });
        }
        assert!(command.status()?.success());
        drop(descriptor);
        Ok(())
    }

    #[test]
    fn process_group_inspection_handles_zombies_and_native_process_names() -> Result<()> {
        let stat = |state: &str, threads: u64| {
            let mut fields = vec!["0".to_string(); 20];
            fields[0] = state.to_string();
            fields[2] = "4321".into();
            fields[17] = threads.to_string();
            let mut stat = b"123 (native-\xff ) ) process) ".to_vec();
            stat.extend_from_slice(fields.join(" ").as_bytes());
            stat
        };
        assert_eq!(parse_process_group(&stat("S", 1))?, (4321, true));
        for state in ["Z", "X", "x"] {
            assert_eq!(parse_process_group(&stat(state, 1))?, (4321, false));
            assert_eq!(parse_process_group(&stat(state, 2))?, (4321, true));
        }
        assert!(parse_process_group(b"invalid stat").is_err());
        Ok(())
    }

    #[test]
    fn control_handshake_has_total_deadline_even_while_bytes_arrive() -> Result<()> {
        let (reader, mut writer) = UnixStream::pair()?;
        let sender = thread::spawn(move || {
            for _ in 0..20 {
                if writer.write_all(b"x").is_err() {
                    break;
                }
                thread::sleep(Duration::from_millis(10));
            }
        });
        let start = Instant::now();
        let result = read_control_deadline(&mut BufReader::new(reader), Duration::from_millis(60));
        assert!(result.unwrap_err().to_string().contains("deadline"));
        assert!(start.elapsed() < Duration::from_millis(180));
        sender.join().unwrap();
        Ok(())
    }

    #[test]
    fn reaper_terminates_descendant_after_group_leader_exits() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let pid_file = dir.path().join("descendant.pid");
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("trap '' TERM; sleep 60 & echo $! > \"$1\"")
            .arg("sh")
            .arg(&pid_file)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) != 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        struct GroupFixture {
            child: Child,
            armed: bool,
        }
        impl Drop for GroupFixture {
            fn drop(&mut self) {
                // Failure before the reaper runs still owns a retained leader.
                // Kill the fixture group before waiting, including on panic.
                if self.armed {
                    signal_worker(self.child.id(), libc::SIGKILL, true);
                    let _ = self.child.wait();
                }
            }
        }
        let mut fixture = GroupFixture {
            child: command.spawn()?,
            armed: true,
        };
        let worker_pid = fixture.child.id();
        let deadline = Instant::now() + Duration::from_secs(2);
        while !pid_file.exists() || !child_has_exited(worker_pid)? {
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(10));
        }
        let descendant: libc::pid_t = fs::read_to_string(&pid_file)?.trim().parse()?;
        reap_worker(&mut fixture.child, worker_pid, true);
        // The reaper released the PID, so this guard must never signal again.
        fixture.armed = false;
        // Linux orphan zombies may await PID1 outside the orchestrator's
        // subreaper process. A zombie has stopped executing and holds no FDs.
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            #[cfg(target_os = "linux")]
            let running =
                fs::read_to_string(format!("/proc/{descendant}/stat")).is_ok_and(|stat| {
                    !stat
                        .rsplit_once(')')
                        .unwrap()
                        .1
                        .trim_start()
                        .starts_with('Z')
                });
            #[cfg(not(target_os = "linux"))]
            let running = unsafe { libc::kill(descendant, 0) == 0 };
            if !running {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "descendant survived group cleanup"
            );
            thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }
}
