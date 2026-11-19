//! Long-standing orchestrator daemon that manages per-session MCP worker
//! child processes over a private Unix domain socket.

use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::net::Shutdown;
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde_json::Value;

use crate::daemon::{
    CONTROL_IO_TIMEOUT, DaemonClient, DaemonPaths, DaemonRequest, DaemonResponse, DaemonStateFile,
    DaemonStatus, MAX_CONTROL_LINE_BYTES, PROTOCOL_VERSION, SessionInfo, SessionState,
    StartSessionRequest, ensure_private_dir, read_bounded_line, unix_now_secs,
    verify_peer_credentials, write_json_line,
};
use crate::lifecycle::ProcessOwner;
use crate::maintenance::FileLease;

const DEFAULT_MAX_SESSIONS: usize = 128;
const MAX_RECENT_SESSIONS: usize = 50;
const MAX_SESSION_STDERR_LINES: usize = 64;
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

struct SignalGuard {
    prev_term: libc::sigaction,
    prev_int: libc::sigaction,
    _wake_holder: UnixStream,
}

impl SignalGuard {
    fn install(wake_writer: UnixStream) -> Result<Self> {
        wake_writer.set_nonblocking(true)?;
        SIGNAL_STOP.store(false, Ordering::Release);
        SIGNAL_WAKE_FD.store(wake_writer.as_raw_fd(), Ordering::Release);

        let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
        action.sa_sigaction = handle_termination_signal as *const () as usize;
        action.sa_flags = libc::SA_RESTART;
        unsafe {
            libc::sigemptyset(&mut action.sa_mask);
        }

        let mut prev_term: libc::sigaction = unsafe { std::mem::zeroed() };
        let mut prev_int: libc::sigaction = unsafe { std::mem::zeroed() };
        if unsafe { libc::sigaction(libc::SIGTERM, &action, &mut prev_term) } != 0 {
            SIGNAL_WAKE_FD.store(-1, Ordering::Release);
            return Err(io::Error::last_os_error()).context("failed to install SIGTERM handler");
        }
        if unsafe { libc::sigaction(libc::SIGINT, &action, &mut prev_int) } != 0 {
            unsafe {
                libc::sigaction(libc::SIGTERM, &prev_term, std::ptr::null_mut());
            }
            SIGNAL_WAKE_FD.store(-1, Ordering::Release);
            return Err(io::Error::last_os_error()).context("failed to install SIGINT handler");
        }
        Ok(Self {
            prev_term,
            prev_int,
            _wake_holder: wake_writer,
        })
    }
}

impl Drop for SignalGuard {
    fn drop(&mut self) {
        SIGNAL_WAKE_FD.store(-1, Ordering::Release);
        unsafe {
            libc::sigaction(libc::SIGTERM, &self.prev_term, std::ptr::null_mut());
            libc::sigaction(libc::SIGINT, &self.prev_int, std::ptr::null_mut());
        }
        SIGNAL_STOP.store(false, Ordering::Release);
    }
}

struct SessionHandle {
    info: Mutex<SessionInfo>,
    owner: Option<ProcessOwner>,
    shutdown_stream: Mutex<Option<UnixStream>>,
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
        self.forced_stop.store(true, Ordering::Release);
        {
            let mut slot = self.stop_reason.lock().unwrap_or_else(|e| e.into_inner());
            if slot.is_none() {
                *slot = Some(reason.to_string());
            }
        }
        if let Some(stream) = self
            .shutdown_stream
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }

    fn record_stderr(&self, line: String) {
        let mut buf = self.stderr_lines.lock().unwrap_or_else(|e| e.into_inner());
        if buf.len() >= MAX_SESSION_STDERR_LINES {
            buf.pop_front();
        }
        buf.push_back(line);
    }
}

struct DaemonCore {
    paths: DaemonPaths,
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
    fn new(paths: DaemonPaths, wake_writer: UnixStream) -> Self {
        let max_sessions = std::env::var("TSINDEX_MAX_SESSIONS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|&value| value > 0)
            .unwrap_or(DEFAULT_MAX_SESSIONS);
        Self {
            paths,
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
        }
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

    fn finish_session(&self, session_id: &str, final_reason: String) {
        let removed = self
            .active_sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(session_id);
        if let Some(handle) = removed {
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
    fn enter(core: Arc<DaemonCore>) -> Self {
        let mut count = core
            .active_threads
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        *count += 1;
        drop(count);
        Self { core }
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
    socket_path: std::path::PathBuf,
    state_path: std::path::PathBuf,
}

impl Drop for SocketCleanupGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.socket_path);
        let _ = fs::remove_file(&self.state_path);
    }
}

/// Run the standalone orchestrator daemon in the foreground until stopped via
/// `SIGTERM`, `SIGINT`, or a `shutdown` control message.
///
/// The orchestrator starts completely idle: it opens no index databases and
/// allocates no filesystem watchers until an agent harness starts a session.
pub fn run_orchestrator(paths: DaemonPaths) -> Result<()> {
    let socket_dir = paths.socket_dir()?;
    ensure_private_dir(socket_dir)?;

    let _instance_lease = match FileLease::try_acquire(&paths.lock_path)? {
        Some(lease) => lease,
        None => {
            let detail = DaemonClient::new(paths.clone())
                .ping()
                .map(|status| {
                    format!(
                        " (PID {}, socket {})",
                        status.daemon_pid,
                        status.socket.display()
                    )
                })
                .unwrap_or_default();
            bail!("tsindex orchestrator daemon is already running{detail}");
        }
    };

    // Holding the exclusive instance lease proves no live orchestrator owns
    // this path, so any leftover socket file is stale from a previous crash.
    if paths.socket_path.exists() {
        fs::remove_file(&paths.socket_path).with_context(|| {
            format!(
                "failed to remove stale daemon socket {}",
                paths.socket_path.display()
            )
        })?;
    }

    let listener = UnixListener::bind(&paths.socket_path).with_context(|| {
        format!(
            "failed to bind orchestrator socket {}",
            paths.socket_path.display()
        )
    })?;
    fs::set_permissions(&paths.socket_path, fs::Permissions::from_mode(0o600)).with_context(
        || {
            format!(
                "failed to set permissions on {}",
                paths.socket_path.display()
            )
        },
    )?;

    let _cleanup = SocketCleanupGuard {
        socket_path: paths.socket_path.clone(),
        state_path: paths.state_path.clone(),
    };

    write_state_file(&paths)?;

    let (wake_reader, wake_writer) = UnixStream::pair().context("failed to create wakeup pipe")?;
    let signal_wake = wake_writer.try_clone()?;
    let _signal_guard = SignalGuard::install(signal_wake)?;

    let core = Arc::new(DaemonCore::new(paths.clone(), wake_writer));
    let sweeper = spawn_owner_sweeper(Arc::clone(&core))?;

    eprintln!(
        "tsindex daemon listening on {} (pid {}, version {})",
        paths.socket_path.display(),
        std::process::id(),
        env!("CARGO_PKG_VERSION")
    );

    accept_loop(&listener, &wake_reader, &core)?;

    // Stop accepting new sessions and cleanly tear down every active worker.
    core.request_shutdown();
    let killed = core.kill_all("daemon_shutdown");
    if !killed.is_empty() {
        eprintln!(
            "tsindex daemon shutting down {} active session(s)",
            killed.len()
        );
    }
    core.wait_for_threads(Duration::from_secs(10));
    let _ = sweeper.join();

    eprintln!("tsindex daemon stopped (pid {})", std::process::id());
    Ok(())
}

fn write_state_file(paths: &DaemonPaths) -> Result<()> {
    let state = DaemonStateFile {
        pid: std::process::id(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        protocol: PROTOCOL_VERSION,
        started_at: unix_now_secs(),
        socket: paths.socket_path.clone(),
    };
    let tmp = paths
        .state_path
        .with_extension(format!("json.tmp.{}", std::process::id()));
    let bytes = serde_json::to_vec_pretty(&state)?;
    fs::write(&tmp, bytes)
        .with_context(|| format!("failed to write daemon state {}", tmp.display()))?;
    fs::set_permissions(&tmp, fs::Permissions::from_mode(0o600))?;
    fs::rename(&tmp, &paths.state_path).with_context(|| {
        format!(
            "failed to publish daemon state {}",
            paths.state_path.display()
        )
    })?;
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
                match listener.accept() {
                    Ok((stream, _addr)) => {
                        let _ = stream.set_nonblocking(false);
                        let worker_core = Arc::clone(core);
                        let guard = ActiveThreadGuard::enter(Arc::clone(core));
                        let spawned = thread::Builder::new()
                            .name("tsindex-daemon-conn".into())
                            .spawn(move || {
                                let _guard = guard;
                                if let Err(error) = handle_connection(worker_core, stream) {
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

fn handle_connection(core: Arc<DaemonCore>, stream: UnixStream) -> Result<()> {
    let creds = verify_peer_credentials(&stream)?;
    stream.set_read_timeout(Some(CONTROL_IO_TIMEOUT))?;
    stream.set_write_timeout(Some(CONTROL_IO_TIMEOUT))?;

    let mut writer = stream.try_clone()?;
    let mut reader = BufReader::new(stream);
    let line = read_bounded_line(&mut reader, MAX_CONTROL_LINE_BYTES)?;
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

    match request {
        DaemonRequest::Ping => {
            write_json_line(&mut writer, &DaemonResponse::Pong(core.status()))?;
        }
        DaemonRequest::ListSessions { include_recent } => {
            let (active, recent) = core.list_sessions(include_recent);
            write_json_line(&mut writer, &DaemonResponse::Sessions { active, recent })?;
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
            write_json_line(&mut writer, &DaemonResponse::Killed { killed })?;
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
            handle_start_session(core, creds.pid, req, reader, writer)?;
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

    if !req.root.exists() {
        write_json_line(
            &mut writer,
            &DaemonResponse::Error {
                error: format!("workspace root does not exist: {}", req.root.display()),
            },
        )?;
        return Ok(());
    }

    let client_pid = peer_pid.unwrap_or(req.client_pid);
    let excluded = [client_pid, std::process::id()];
    let owner = match ProcessOwner::capture(req.owner_pid, req.owner_explicit, &excluded) {
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

    // Reserve a session slot before spawning a worker process.
    {
        let active = core
            .active_sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if active.len() >= core.max_sessions {
            write_json_line(
                &mut writer,
                &DaemonResponse::Error {
                    error: format!(
                        "maximum concurrent sessions reached ({}); close an idle session or run `tsindex daemon sessions`",
                        core.max_sessions
                    ),
                },
            )?;
            return Ok(());
        }
    }

    let seq = core.next_session_seq.fetch_add(1, Ordering::Relaxed);
    let session_id = format!("s-{seq}");
    let mut child = match spawn_worker(&req) {
        Ok(child) => child,
        Err(error) => {
            write_json_line(
                &mut writer,
                &DaemonResponse::Error {
                    error: format!("failed to spawn session MCP worker: {error:#}"),
                },
            )?;
            return Ok(());
        }
    };
    let worker_pid = child.id();

    // Long-lived MCP streams may stay idle between turns; clear handshake timeouts.
    let _ = reader.get_ref().set_read_timeout(None);
    let _ = writer.set_write_timeout(None);

    let now = unix_now_secs();
    let tracked_owner_pid = owner.as_ref().map(ProcessOwner::pid);
    let shutdown_clone = writer.try_clone()?;
    let handle = Arc::new(SessionHandle {
        info: Mutex::new(SessionInfo {
            session_id: session_id.clone(),
            state: SessionState::Active,
            root: req.root.clone(),
            db: req.db.clone(),
            languages: req.languages.clone(),
            no_refresh: req.no_refresh,
            worker_pid,
            client_pid,
            owner_pid: tracked_owner_pid,
            harness_name: None,
            harness_version: None,
            started_at: now,
            ended_at: None,
            last_activity_at: now,
            requests_received: 0,
            responses_sent: 0,
            tool_calls: 0,
            exit_reason: None,
            recent_stderr: Vec::new(),
        }),
        owner,
        shutdown_stream: Mutex::new(Some(shutdown_clone)),
        forced_stop: AtomicBool::new(false),
        stop_reason: Mutex::new(None),
        stderr_lines: Mutex::new(VecDeque::new()),
    });

    core.active_sessions
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(session_id.clone(), Arc::clone(&handle));
    core.total_sessions_started.fetch_add(1, Ordering::Relaxed);

    if let Err(error) = write_json_line(
        &mut writer,
        &DaemonResponse::SessionReady {
            session_id: session_id.clone(),
            worker_pid,
            daemon_pid: std::process::id(),
            daemon_version: env!("CARGO_PKG_VERSION").to_string(),
            tracked_owner_pid,
        },
    ) {
        reap_worker(&mut child, worker_pid, true);
        core.finish_session(&session_id, format!("handshake_write_failed: {error}"));
        return Ok(());
    }

    if let Some(owner_pid) = tracked_owner_pid {
        eprintln!(
            "tsindex: started session {session_id} (worker PID {worker_pid}, owner PID {owner_pid}, root {})",
            req.root.display()
        );
    } else {
        eprintln!(
            "tsindex: started session {session_id} (worker PID {worker_pid}, root {})",
            req.root.display()
        );
    }

    let reason = supervise_session(child, worker_pid, Arc::clone(&handle), reader, writer);
    core.finish_session(&session_id, reason.clone());
    eprintln!("tsindex: ended session {session_id} (worker PID {worker_pid}, reason: {reason})");
    Ok(())
}

fn spawn_worker(req: &StartSessionRequest) -> Result<Child> {
    let exe = std::env::current_exe().context("failed to locate tsindex executable")?;
    let mut cmd = Command::new(exe);
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

    // SAFETY: only async-signal-safe syscalls run between fork() and exec().
    unsafe {
        cmd.pre_exec(move || {
            let _ = libc::setpgid(0, 0);
            #[cfg(target_os = "linux")]
            {
                let _ = libc::prctl(
                    libc::PR_SET_PDEATHSIG,
                    libc::SIGTERM as libc::c_ulong,
                    0 as libc::c_ulong,
                    0 as libc::c_ulong,
                    0 as libc::c_ulong,
                );
                if libc::getppid() != parent_pid {
                    let _ = libc::raise(libc::SIGTERM);
                }
            }
            Ok(())
        });
    }

    cmd.spawn()
        .context("failed to spawn tsindex daemon worker process")
}

enum BridgeEvent {
    IngressDone,
    EgressDone { broken_client: bool },
}

fn supervise_session(
    mut child: Child,
    worker_pid: u32,
    handle: Arc<SessionHandle>,
    mut uds_reader: BufReader<UnixStream>,
    mut uds_writer: UnixStream,
) -> String {
    let Some(mut worker_stdin) = child.stdin.take() else {
        reap_worker(&mut child, worker_pid, true);
        return "missing_worker_stdin".to_string();
    };
    let Some(worker_stdout) = child.stdout.take() else {
        drop(worker_stdin);
        reap_worker(&mut child, worker_pid, true);
        return "missing_worker_stdout".to_string();
    };
    let Some(worker_stderr) = child.stderr.take() else {
        drop(worker_stdin);
        drop(worker_stdout);
        reap_worker(&mut child, worker_pid, true);
        return "missing_worker_stderr".to_string();
    };

    let uds_shutdown = uds_writer.try_clone().ok();
    let (event_tx, event_rx) = mpsc::channel::<BridgeEvent>();

    let ingress_tx = event_tx.clone();
    let ingress_handle = Arc::clone(&handle);
    let ingress = thread::Builder::new()
        .name("tsindex-session-in".into())
        .spawn(move || {
            let mut line = Vec::new();
            loop {
                line.clear();
                match uds_reader.read_until(b'\n', &mut line) {
                    Ok(0) => break,
                    Ok(_) => {
                        if ingress_handle.forced_stop.load(Ordering::Acquire) {
                            break;
                        }
                        observe_client_message(&ingress_handle, &line);
                        if worker_stdin.write_all(&line).is_err() || worker_stdin.flush().is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            // Dropping worker_stdin closes the worker's stdin pipe cleanly.
            drop(worker_stdin);
            let _ = ingress_tx.send(BridgeEvent::IngressDone);
        })
        .ok();

    let egress_tx = event_tx;
    let egress_handle = Arc::clone(&handle);
    let egress = thread::Builder::new()
        .name("tsindex-session-out".into())
        .spawn(move || {
            let mut reader = BufReader::new(worker_stdout);
            let mut line = Vec::new();
            let mut broken_client = false;
            loop {
                line.clear();
                match reader.read_until(b'\n', &mut line) {
                    Ok(0) => break,
                    Ok(_) => {
                        {
                            let mut info =
                                egress_handle.info.lock().unwrap_or_else(|e| e.into_inner());
                            info.responses_sent += 1;
                            info.last_activity_at = unix_now_secs();
                        }
                        if uds_writer.write_all(&line).is_err() || uds_writer.flush().is_err() {
                            broken_client = true;
                            egress_handle.request_stop("client_write_failed");
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            let _ = uds_writer.shutdown(Shutdown::Write);
            let _ = egress_tx.send(BridgeEvent::EgressDone { broken_client });
        })
        .ok();

    let stderr_handle = Arc::clone(&handle);
    let session_id_for_log = handle
        .info
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .session_id
        .clone();
    let stderr_pump = thread::Builder::new()
        .name("tsindex-session-err".into())
        .spawn(move || {
            let reader = BufReader::new(worker_stderr);
            for line in reader.lines().map_while(Result::ok) {
                stderr_handle.record_stderr(line.clone());
                eprintln!("[{session_id_for_log} pid={worker_pid}] {line}");
            }
        })
        .ok();

    let forced;
    let mut default_reason = "client_eof".to_string();

    if let Ok(first_event) = event_rx.recv() {
        match first_event {
            BridgeEvent::IngressDone => {
                {
                    let mut info = handle.info.lock().unwrap_or_else(|e| e.into_inner());
                    info.state = SessionState::Draining;
                }
                if handle.forced_stop.load(Ordering::Acquire) {
                    forced = true;
                } else {
                    // Normal harness stdin EOF: let in-flight responses flush.
                    match event_rx.recv_timeout(GRACEFUL_DRAIN_TIMEOUT) {
                        Ok(BridgeEvent::EgressDone { broken_client }) => {
                            forced = broken_client;
                        }
                        _ => {
                            forced = true;
                            default_reason = "drain_timeout".to_string();
                        }
                    }
                }
            }
            BridgeEvent::EgressDone { broken_client } => {
                forced = broken_client;
                default_reason = if broken_client {
                    "client_disconnected".to_string()
                } else {
                    "worker_exited".to_string()
                };
                if let Some(stream) = &uds_shutdown {
                    let _ = stream.shutdown(Shutdown::Both);
                }
            }
        }
    } else {
        forced = true;
        default_reason = "bridge_aborted".to_string();
    }

    if let Some(stream) = &uds_shutdown {
        let _ = stream.shutdown(Shutdown::Both);
    }
    // Clear the stored shutdown handle so no extra fd lingers after teardown.
    *handle
        .shutdown_stream
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = None;

    let status_note = reap_worker(&mut child, worker_pid, forced);

    if let Some(join) = ingress {
        let _ = join.join();
    }
    if let Some(join) = egress {
        let _ = join.join();
    }
    if let Some(join) = stderr_pump {
        let _ = join.join();
    }

    let explicit_reason = handle
        .stop_reason
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    match explicit_reason {
        Some(reason) => reason,
        None => status_note.unwrap_or(default_reason),
    }
}

/// Passively extract harness identity and request counts from raw MCP lines
/// without altering or rejecting malformed frames.
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
                }
            }
            Some("tools/call") => {
                info.tool_calls += 1;
            }
            _ => {}
        }
    }
}

/// Guarantee the spawned worker process (and its process group) exits and is
/// reaped so neither orphaned workers nor zombie entries can accumulate.
fn reap_worker(child: &mut Child, worker_pid: u32, forced: bool) -> Option<String> {
    let grace = if forced {
        FORCED_EOF_GRACE
    } else {
        Duration::from_secs(2)
    };
    if let Some(status) = wait_with_timeout(child, grace) {
        return (!status.success()).then(|| format!("worker_status_{status}"));
    }

    signal_process_group(worker_pid, libc::SIGTERM);
    if let Some(status) = wait_with_timeout(child, SIGTERM_GRACE) {
        return (!status.success()).then(|| format!("worker_terminated_{status}"));
    }

    signal_process_group(worker_pid, libc::SIGKILL);
    let _ = child.kill();
    let _ = child.wait();
    Some("worker_killed".to_string())
}

fn wait_with_timeout(child: &mut Child, timeout: Duration) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            return Some(status);
        }
        if Instant::now() >= deadline {
            return None;
        }
        thread::sleep(POLL_WAIT_STEP);
    }
}

fn signal_process_group(pid: u32, signal: libc::c_int) {
    if pid == 0 || pid > i32::MAX as u32 {
        return;
    }
    let pgid = -(pid as libc::pid_t);
    unsafe {
        if libc::kill(pgid, signal) != 0 {
            let _ = libc::kill(pid as libc::pid_t, signal);
        }
    }
}
