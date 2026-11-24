//! One warm index owner per compatible checkout. Only session workers receive
//! its unnamed Unix socket. Each worker reuses private resolver streams, so
//! harnesses can overlap slow lookups and use identical JSON-RPC IDs.

use std::collections::HashMap;
use std::io::{self, BufReader, Read, Write};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd};
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    MAX_MCP_FRAME_BYTES, PROTOCOL_VERSION, STARTUP_IO_TIMEOUT, fds, read_bounded_line,
    write_bounded_json,
};
use crate::config::{TsIndexConfig, db_path};
use crate::mcp::{InvalidParams, PreparedTool, ToolResolver};
use crate::worktrees::Runtimes;

const DEFAULT_IDLE: Duration = Duration::from_secs(60);
pub(super) const MAX_IDLE: usize = 4;
// Distinguish an unresponsive shared resolver from a failed tool or an
// independently cancelled MCP worker when the supervisor reaps the child.
pub(super) const LOOKUP_TIMEOUT_EXIT: i32 = 75;
/// Signal cleanly that a worker exited normally after timing out at least one
/// owner lookup, so the supervisor retires its generation for new attachments.
pub(crate) const ABANDONED_LANE_EXIT: i32 = 76;

fn generation_digest(config: &TsIndexConfig) -> Result<String> {
    // Both children calculate this themselves. A package replacement between
    // their two execs must fail initialization and allow a fresh connection.
    let inputs = (env!("CARGO_PKG_VERSION"), running_image()?, config);
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&inputs)?)
    ))
}

fn running_image() -> Result<[u8; 16]> {
    #[cfg(target_os = "macos")]
    {
        // The kernel records the mapped Mach-O UUID even when the installation
        // path has been replaced. Custom unsigned builds may omit the UUID.
        let mut info: libc::rusage_info_v0 = unsafe { std::mem::zeroed() };
        let rc = unsafe {
            libc::proc_pid_rusage(libc::getpid(), libc::RUSAGE_INFO_V0, (&raw mut info).cast())
        };
        if rc == 0 && info.ri_uuid != [0; 16] {
            return Ok(info.ri_uuid);
        }
    }
    #[cfg(target_os = "linux")]
    let image = std::fs::metadata("/proc/self/exe")
        .context("managed Linux sessions require /proc/self/exe in the daemon's PID namespace")?;
    #[cfg(not(target_os = "linux"))]
    let image = std::env::current_exe()?.metadata()?;
    let mut identity = [0; 16];
    identity[..8].copy_from_slice(&image.dev().to_be_bytes());
    identity[8..].copy_from_slice(&image.ino().to_be_bytes());
    Ok(identity)
}

// Existing parents can be aliases even when a requested DB does not exist yet.
fn canonical_target(path: &Path) -> io::Result<PathBuf> {
    crate::maintenance::canonical_target(path)
}

#[derive(Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct RuntimeKey {
    #[serde(with = "super::native_path")]
    root: PathBuf,
    #[serde(with = "super::native_path")]
    caller_dir: PathBuf,
    // An explicit DB disables repo pruning even if it names the default file.
    #[serde(with = "super::native_path::optional")]
    db: Option<PathBuf>,
    // Direct and older clients derive their lease from the supplied DB path,
    // even if it is a symlink to the canonical database above.
    #[serde(with = "super::native_path::optional")]
    writer_lease: Option<PathBuf>,
    languages: Vec<String>,
    no_refresh: bool,
    generation: String,
}

impl RuntimeKey {
    pub(super) fn resolve(
        root: &Path,
        db: Option<&Path>,
        languages: &[String],
        no_refresh: bool,
        caller_dir: Option<&Path>,
    ) -> Result<Self> {
        let root = root
            .canonicalize()
            .context("cannot resolve checkout root")?;
        ensure!(root.is_dir(), "workspace root must be a directory");
        let supplied_db = match db {
            Some(path) if path.is_absolute() => Some(path.to_path_buf()),
            Some(path) => {
                let base = caller_dir.unwrap_or(&root).canonicalize()?;
                Some(base.join(path))
            }
            None => None,
        };
        let db = supplied_db.as_deref().map(canonical_target).transpose()?;
        let writer_lease = supplied_db
            .as_deref()
            .map(crate::maintenance::writer_lease_path);
        let checkout = if db.is_none() && !db_path(&root).exists() {
            crate::worktrees::discover(&root)?
                .into_iter()
                .find(|c| c.path == root && c.path != c.main_root)
        } else {
            None
        };
        let settings = if let Some(checkout) = checkout {
            let main = TsIndexConfig::load_or_default(&checkout.main_root)?;
            crate::worktrees::projected_checkout_config(&checkout, &main)?
        } else {
            TsIndexConfig::load_or_default(&root)?
        };
        // Preserve any usable caller working directory so worker/owner
        // matchers (including anchored global Git ignores and linked checkouts
        // discovered after startup) evaluate against the caller's ignore base
        // and incompatible caller bases do not share an owner. Fall back to
        // `root` only when the caller cwd is unavailable and explicit ignore
        // files do not require it.
        let caller_dir = match caller_dir {
            Some(dir) => match dir.canonicalize() {
                Ok(dir) if dir.is_dir() => dir,
                Ok(_) if !settings.has_explicit_ignore_files() => root.clone(),
                Ok(_) => bail!("caller working directory must be a directory"),
                Err(_) if !settings.has_explicit_ignore_files() => root.clone(),
                Err(error) => {
                    return Err(error).context("cannot resolve caller working directory");
                }
            },
            None if settings.has_explicit_ignore_files() => {
                bail!("caller working directory is required by configured ignore files");
            }
            None => root.clone(),
        };
        let mut languages = languages.to_owned();
        languages.sort();
        languages.dedup();
        Ok(Self {
            root,
            caller_dir,
            db,
            writer_lease,
            languages,
            no_refresh,
            generation: generation_digest(&settings)?,
        })
    }
}

/// The session worker resolves settings inside its own cancellable process.
/// Its stdout is still a private control pipe until the second acknowledgement.
#[derive(Serialize, Deserialize)]
pub(super) struct WorkerRuntimeRequest {
    pub protocol: u32,
    pub runtime_key: RuntimeKey,
}

pub(super) fn add_settings(
    cmd: &mut Command,
    root: &Path,
    db: Option<&Path>,
    languages: &[String],
    no_refresh: bool,
) {
    cmd.arg("--root").arg(root);
    if let Some(db) = db {
        cmd.arg("--db").arg(db);
    }
    for language in languages {
        cmd.arg("--languages").arg(language);
    }
    if no_refresh {
        cmd.arg("--no-refresh");
    }
}

pub(super) struct RuntimeLease {
    generation: Arc<Generation>,
    pub pid: u32,
}

impl RuntimeLease {
    pub fn channel(&self) -> BorrowedFd<'_> {
        self.generation.channel.as_fd()
    }

    pub fn mark_ready(&self) {
        self.generation.ready.store(true, Ordering::Release);
    }
}

struct Generation {
    channel: UnixStream,
    started: Instant,
    ready: AtomicBool,
}

impl Generation {
    fn accepts_new_session(&self, now: Instant) -> bool {
        // Let a second client join a slow cold start after the first client's
        // deadline. Repeated overlapping retries must not pin an owner that
        // has never served anyone. A warm owner can take arbitrarily long on
        // its accepted lookups without reaching this startup deadline.
        let cold_expired = !self.ready.load(Ordering::Acquire)
            && now.saturating_duration_since(self.started) >= 2 * STARTUP_IO_TIMEOUT;
        !cold_expired && !intake_closed(&self.channel)
    }
}

struct Entry {
    child: Child,
    generation: Arc<Generation>,
    idle_since: Option<Instant>,
}

impl Entry {
    fn start(key: &RuntimeKey, executable: &Path, max_lanes: usize) -> Result<Self> {
        let (channel, receiver) = UnixStream::pair()?;
        channel.set_write_timeout(Some(STARTUP_IO_TIMEOUT))?;
        let mut cmd = Command::new(executable);
        // A stalled checkout must not block the single pool thread in pre-exec
        // chdir. The new process enters the root after installing its lifeline.
        add_settings(
            &mut cmd,
            &key.root,
            key.db.as_deref(),
            &key.languages,
            key.no_refresh,
        );
        cmd.args(["daemon", "index-runtime", "--runtime-fd"])
            .arg(receiver.as_raw_fd().to_string())
            .arg("--generation")
            .arg(&key.generation)
            .arg("--max-lanes")
            .arg(max_lanes.to_string())
            .arg("--caller-dir")
            .arg(&key.caller_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        if let Some(lease) = &key.writer_lease {
            cmd.arg("--writer-lease").arg(lease);
        }
        fds::prepare_exec(&mut cmd, Some(receiver.as_fd()))?;
        // Linux ties parent-death delivery to the spawning thread. The pool
        // manager lives for the daemon's lifetime, independently of any client.
        super::orchestrator::configure_child(&mut cmd, false, libc::SIGKILL);
        let child = cmd
            .spawn()
            .context("failed to start shared index runtime")?;
        Ok(Self {
            child,
            generation: Arc::new(Generation {
                channel,
                started: Instant::now(),
                ready: AtomicBool::new(false),
            }),
            idle_since: None,
        })
    }

    fn is_stopped(&self) -> bool {
        let mut state: libc::siginfo_t = unsafe { std::mem::zeroed() };
        // Inspect only stop/continue notifications. The Child still owns and
        // reaps the exit status. A stopped owner cannot serve a new connection.
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                self.child.id() as libc::id_t,
                &mut state,
                libc::WSTOPPED | libc::WCONTINUED | libc::WNOHANG,
            )
        };
        result == 0 && unsafe { state.si_pid() } != 0 && state.si_code == libc::CLD_STOPPED
    }

    fn lease(&mut self) -> RuntimeLease {
        self.idle_since = None;
        RuntimeLease {
            generation: self.generation.clone(),
            pid: self.child.id(),
        }
    }

    fn stop(mut self, exiting: &mut Vec<Child>) {
        // EOF releases the runtime on daemon loss. An explicit stop also kills
        // an unresponsive or stopped owner. Its child handle reserves the PID
        // until a later nonblocking wait observes exit.
        drop(self.child.stdin.take());
        if child_pending(&mut self.child) {
            let _ = self.child.kill();
            exiting.push(self.child);
        }
    }
}

fn child_pending(child: &mut Child) -> bool {
    loop {
        match child.try_wait() {
            Ok(status) => return status.is_none(),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            // Never send a numeric signal after losing ownership of the PID.
            Err(_) => return false,
        }
    }
}

enum PoolMessage {
    Acquire(RuntimeKey, PathBuf, mpsc::SyncSender<Result<RuntimeLease>>),
    Retire(Arc<Generation>),
    Stop,
}

pub(super) struct RuntimePool {
    inbox: mpsc::Sender<PoolMessage>,
    resident: Arc<AtomicUsize>,
    thread: Mutex<Option<thread::JoinHandle<()>>>,
}

impl RuntimePool {
    pub fn new(max_sessions: usize) -> Result<Self> {
        let (inbox, requests) = mpsc::channel();
        let idle = std::env::var("TSINDEX_RUNTIME_IDLE_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .map(Duration::from_secs)
            .unwrap_or(DEFAULT_IDLE);
        let resident = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&resident);
        let thread = thread::Builder::new()
            .name("tsindex-runtime-pool".into())
            .spawn(move || run_pool(requests, idle, &count, max_sessions))?;
        Ok(Self {
            inbox,
            resident,
            thread: Mutex::new(Some(thread)),
        })
    }

    pub fn acquire(&self, key: RuntimeKey, executable: &Path) -> Result<RuntimeLease> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.inbox
            .send(PoolMessage::Acquire(key, executable.to_owned(), tx))
            .map_err(|_| anyhow!("index runtime pool stopped"))?;
        rx.recv().context("index runtime pool stopped")?
    }

    pub fn retire(&self, lease: &RuntimeLease) {
        // Compare capability identities. A late failure of an old owner cannot
        // retire a replacement that happens to have the same configuration.
        let _ = self
            .inbox
            .send(PoolMessage::Retire(lease.generation.clone()));
    }

    pub fn resident(&self) -> usize {
        self.resident.load(Ordering::Relaxed)
    }

    pub fn stop(&self) {
        let _ = self.inbox.send(PoolMessage::Stop);
        if let Some(thread) = self.thread.lock().unwrap_or_else(|p| p.into_inner()).take() {
            let _ = thread.join();
        }
    }
}

impl Drop for RuntimePool {
    fn drop(&mut self) {
        self.stop();
    }
}

struct IndexOwners {
    current: HashMap<RuntimeKey, Entry>,
    retiring: Vec<Entry>,
    exiting: Vec<Child>,
    max_owners: usize,
    max_lanes: usize,
}

impl IndexOwners {
    fn new(max_sessions: usize) -> Self {
        Self {
            current: HashMap::new(),
            retiring: Vec::new(),
            exiting: Vec::new(),
            // A replacement may start while its predecessor awaits kernel exit.
            max_owners: max_sessions.saturating_add(MAX_IDLE + 1),
            max_lanes: max_sessions.saturating_mul(crate::mcp::MAX_CONCURRENT_REQUESTS),
        }
    }

    fn resident(&self) -> usize {
        self.current.len() + self.retiring.len() + self.exiting.len()
    }

    fn acquire(&mut self, key: RuntimeKey, image: &Path, now: Instant) -> Result<RuntimeLease> {
        if self.current.get(&key).is_some_and(Entry::is_stopped)
            && let Some(stopped) = self.current.remove(&key)
        {
            // A confirmed stop can strand a writer lease. Request termination
            // before opening the replacement without blocking unrelated owners.
            stopped.stop(&mut self.exiting);
        }
        // New clients get a fresh generation if initialization failed or
        // never completed. Keep prior leases alive to finish their own replies.
        if self
            .current
            .get(&key)
            .is_some_and(|e| !e.generation.accepts_new_session(now))
            && let Some(previous) = self.current.remove(&key)
        {
            self.retiring.push(previous);
        }
        let at_capacity = self.resident() >= self.max_owners;
        match self.current.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut slot) => Ok(slot.get_mut().lease()),
            std::collections::hash_map::Entry::Vacant(slot) => {
                ensure!(
                    !at_capacity,
                    "index owner capacity reached while earlier processes are exiting; retry shortly"
                );
                let entry = Entry::start(slot.key(), image, self.max_lanes)?;
                Ok(slot.insert(entry).lease())
            }
        }
    }

    fn retire(&mut self, generation: &Arc<Generation>) {
        // A late timeout from an old generation cannot retire its replacement.
        // Keep serving any peers that already have a lease on this generation.
        let stale = self
            .current
            .extract_if(|_, e| Arc::ptr_eq(&e.generation, generation));
        self.retiring.extend(stale.map(|(_, entry)| entry));
    }

    fn collect(&mut self, idle: Duration) {
        self.exiting.retain_mut(child_pending);
        for entry in self.retiring.extract_if(.., |e| {
            Arc::strong_count(&e.generation) == 1 || !child_pending(&mut e.child)
        }) {
            entry.stop(&mut self.exiting);
        }
        let now = Instant::now();
        let expired = self.current.extract_if(|_, entry| {
            if !child_pending(&mut entry.child) {
                return true;
            }
            if Arc::strong_count(&entry.generation) > 1 {
                entry.idle_since = None;
                return false;
            }
            now.duration_since(*entry.idle_since.get_or_insert(now)) >= idle
        });
        for (_, entry) in expired {
            entry.stop(&mut self.exiting);
        }
        let mut unused: Vec<_> = self
            .current
            .iter()
            .filter_map(|(key, entry)| entry.idle_since.map(|t| (t, key.clone())))
            .collect();
        if unused.len() > MAX_IDLE {
            unused.sort_unstable_by_key(|(t, _)| *t);
            let excess = unused.len() - MAX_IDLE;
            for (_, key) in unused.into_iter().take(excess) {
                if let Some(entry) = self.current.remove(&key) {
                    entry.stop(&mut self.exiting);
                }
            }
        }
    }

    fn shutdown(mut self) {
        for entry in self.current.into_values().chain(self.retiring) {
            entry.stop(&mut self.exiting);
        }
        // SIGKILL cannot end uninterruptible kernel I/O until the syscall
        // returns. Keep the manager responsive and bound its final wait.
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            self.exiting.retain_mut(child_pending);
            if self.exiting.is_empty() {
                break;
            }
            if Instant::now() >= deadline {
                eprintln!(
                    "{} index owners still await kernel exit",
                    self.exiting.len()
                );
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

fn run_pool(
    requests: mpsc::Receiver<PoolMessage>,
    idle: Duration,
    count: &AtomicUsize,
    max_sessions: usize,
) {
    let mut owners = IndexOwners::new(max_sessions);
    loop {
        let message = if owners.resident() == 0 {
            requests
                .recv()
                .map_err(|_| mpsc::RecvTimeoutError::Disconnected)
        } else {
            requests.recv_timeout(Duration::from_millis(250))
        };
        owners.collect(idle);
        match message {
            Ok(PoolMessage::Acquire(key, image, reply)) => {
                let result = owners.acquire(key, &image, Instant::now());
                count.store(owners.resident(), Ordering::Relaxed);
                let _ = reply.send(result);
            }
            Ok(PoolMessage::Retire(generation)) => owners.retire(&generation),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                count.store(owners.resident(), Ordering::Relaxed)
            }
            Ok(PoolMessage::Stop) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    owners.shutdown();
    count.store(0, Ordering::Relaxed);
}

#[derive(Serialize, Deserialize)]
enum Lookup {
    Prepare {
        repo: Option<String>,
        live_query: bool,
    },
}

#[derive(Serialize, Deserialize)]
enum Reply {
    Ready,
    Prepared(Box<PreparedTool>),
    Error {
        detail: String,
        invalid_params: bool,
    },
}

fn inherited_socket(fd: i32) -> Result<UnixStream> {
    ensure!(fd >= 3, "missing private index runtime descriptor");
    // These hidden entry points receive an owned descriptor from the daemon
    // at exec. Restore CLOEXEC before initialization or starting any threads.
    if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } == -1 {
        return Err(io::Error::last_os_error().into());
    }
    let stream = unsafe { UnixStream::from_raw_fd(fd) };
    stream
        .peer_addr()
        .context("invalid private runtime socket")?;
    Ok(stream)
}

// The worker generates this identity once. Neither MCP payloads nor caller
// request IDs participate in the runtime's per-session admission decision.
#[derive(Serialize, Deserialize)]
struct OpenLane {
    session: [u8; 16],
    protocol: u32,
}

pub(crate) struct SessionResolver {
    channel: UnixStream,
    session: [u8; 16],
    idle: Mutex<Vec<BufReader<UnixStream>>>,
    lookup_timeout: Duration,
    abandoned_lane: AtomicBool,
}

impl SessionResolver {
    pub fn connect(
        fd: i32,
        root: &Path,
        db: Option<&Path>,
        languages: &[String],
        no_refresh: bool,
        caller_dir: Option<&Path>,
    ) -> Result<Self> {
        let bootstrap = inherited_socket(fd)?;
        let key = RuntimeKey::resolve(root, db, languages, no_refresh, caller_dir)?;
        std::env::set_current_dir(&key.caller_dir)?;
        super::write_json_line(
            &mut io::stdout(),
            &WorkerRuntimeRequest {
                protocol: PROTOCOL_VERSION,
                runtime_key: key,
            },
        )?;
        let capability = fds::recv_fd(&bootstrap)?
            .context("supervisor closed before assigning an index owner")?;
        drop(bootstrap);
        let resolver = Self::attach(UnixStream::from(capability), STARTUP_IO_TIMEOUT)?;
        // This shared endpoint carries only descriptor transfers towards the
        // owner. Its EOF applies to the generation, independently of a lane.
        let mut monitor = resolver.channel.try_clone()?;
        thread::Builder::new()
            .name("tsindex-runtime-lifetime".into())
            .spawn(move || {
                let mut byte = [0u8; 1];
                let _ = monitor.read_exact(&mut byte);
                eprintln!("shared index runtime exited");
                std::process::exit(1);
            })?;
        Ok(resolver)
    }

    pub(crate) fn has_abandoned_lane(&self) -> bool {
        self.abandoned_lane.load(Ordering::Acquire)
    }

    fn attach(channel: UnixStream, lookup_timeout: Duration) -> Result<Self> {
        let mut session = [0u8; 16];
        std::fs::File::open("/dev/urandom")?.read_exact(&mut session)?;
        let resolver = Self {
            channel,
            session,
            idle: Mutex::new(Vec::new()),
            lookup_timeout,
            abandoned_lane: AtomicBool::new(false),
        };
        let first = resolver.open_lane()?;
        resolver
            .idle
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(first);
        Ok(resolver)
    }

    fn open_lane(&self) -> Result<BufReader<UnixStream>> {
        let (mut local, remote) = UnixStream::pair()?;
        local.set_read_timeout(Some(STARTUP_IO_TIMEOUT))?;
        local.set_write_timeout(Some(STARTUP_IO_TIMEOUT))?;
        // Queue the whole bounded header before handing over the endpoint. The
        // listener can read it without ever waiting for a worker or its lookup.
        super::write_json_line(
            &mut local,
            &OpenLane {
                session: self.session,
                protocol: PROTOCOL_VERSION,
            },
        )?;
        let receipt = fds::send_fd(&self.channel, remote.as_fd())
            .and_then(|()| await_receipt(&mut local, &self.channel, STARTUP_IO_TIMEOUT));
        if let Err(error) = receipt {
            if matches!(
                error.kind(),
                io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
            ) {
                // Only failure to reach the independent admission loop identifies
                // an unresponsive owner. Ordinary accepted work can take longer.
                eprintln!("shared index runtime did not accept its resolver connection");
                std::process::exit(LOOKUP_TIMEOUT_EXIT);
            }
            return Err(error).context("shared index runtime unavailable");
        }
        // Darwin may still be transferring the file description until receipt.
        // The client keeps its sender-side reference through that acknowledgement.
        drop(remote);
        let mut lane = BufReader::new(local);
        match read_reply(&mut lane)? {
            Reply::Ready => {
                lane.get_ref().set_read_timeout(Some(self.lookup_timeout))?;
                lane.get_ref()
                    .set_write_timeout(Some(self.lookup_timeout))?;
                Ok(lane)
            }
            Reply::Error { detail, .. } => Err(anyhow!(detail)),
            _ => bail!("invalid index runtime readiness acknowledgement"),
        }
    }

    fn request(&self, request: Lookup) -> Result<Reply> {
        let lane = self.idle.lock().unwrap_or_else(|p| p.into_inner()).pop();
        let mut lane = match lane {
            Some(lane) => lane,
            None => self.open_lane()?,
        };
        write_bounded_json(lane.get_mut(), &request, MAX_MCP_FRAME_BYTES)?;
        // A timeout after admission releases just this stream. The server
        // charges its running task to this session until prepare actually ends.
        let reply = match read_reply(&mut lane) {
            Ok(reply) => reply,
            Err(error) => {
                self.abandoned_lane.store(true, Ordering::Release);
                return Err(error).context("index lookup did not complete");
            }
        };
        self.idle
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(lane);
        match reply {
            Reply::Error {
                detail,
                invalid_params: true,
            } => Err(InvalidParams(detail).into()),
            Reply::Error { detail, .. } => Err(anyhow!(detail)),
            reply => Ok(reply),
        }
    }
}

impl ToolResolver for SessionResolver {
    fn prepare(&self, repo: Option<&str>, live_query: bool) -> Result<PreparedTool> {
        match self.request(Lookup::Prepare {
            repo: repo.map(str::to_owned),
            live_query,
        })? {
            Reply::Prepared(prepared) => Ok(*prepared),
            _ => bail!("invalid response from index runtime"),
        }
    }
}

pub(crate) fn verify_generation(config: &TsIndexConfig, expected: &str) -> Result<()> {
    ensure!(
        generation_digest(config)? == expected,
        "checkout configuration or installed binary changed during startup; reconnect"
    );
    Ok(())
}

fn read_reply(lane: &mut BufReader<UnixStream>) -> Result<Reply> {
    let line = read_bounded_line(lane, MAX_MCP_FRAME_BYTES)?;
    Ok(serde_json::from_str(&line)?)
}

const RECEIVED: u8 = 1;

#[cfg(any(target_os = "aix", test))]
fn intake_closed_via_poll(channel: &UnixStream) -> bool {
    let mut fd = libc::pollfd {
        fd: channel.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let ready = loop {
        let rc = unsafe { libc::poll(&mut fd, 1, 0) };
        if rc < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
            continue;
        }
        break rc;
    };
    if ready <= 0 {
        return false;
    }
    if fd.revents & libc::POLLIN == 0 {
        return fd.revents & (libc::POLLHUP | libc::POLLERR | libc::POLLNVAL) != 0;
    }
    let mut byte = 0u8;
    let n = unsafe {
        libc::recv(
            channel.as_raw_fd(),
            (&raw mut byte).cast(),
            1,
            libc::MSG_PEEK,
        )
    };
    n == 0
        || (n < 0
            && matches!(
                io::Error::last_os_error().kind(),
                io::ErrorKind::ConnectionReset | io::ErrorKind::NotConnected
            ))
}

fn intake_closed(channel: &UnixStream) -> bool {
    // The owner never writes on its intake. A non-consuming EOF check works
    // even while its final private replies keep the process alive.
    #[cfg(target_os = "aix")]
    {
        intake_closed_via_poll(channel)
    }
    #[cfg(not(target_os = "aix"))]
    {
        let mut byte = 0u8;
        let n = unsafe {
            libc::recv(
                channel.as_raw_fd(),
                (&raw mut byte).cast(),
                1,
                libc::MSG_PEEK | libc::MSG_DONTWAIT,
            )
        };
        n == 0
            || (n < 0
                && matches!(
                    io::Error::last_os_error().kind(),
                    io::ErrorKind::ConnectionReset | io::ErrorKind::NotConnected
                ))
    }
}

fn await_receipt(local: &mut UnixStream, owner: &UnixStream, timeout: Duration) -> io::Result<()> {
    let deadline = Instant::now() + timeout;
    let mut fds = [
        libc::pollfd {
            fd: local.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        },
        libc::pollfd {
            fd: owner.as_raw_fd(),
            // This channel has no response payload. Explicit read interest
            // makes EOF observable through poll on both Darwin and Linux.
            events: libc::POLLIN,
            revents: 0,
        },
    ];
    loop {
        // The sender still holds `remote`, so local by itself cannot report
        // EOF if the owner dies between sendmsg and acknowledgement.
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::ErrorKind::TimedOut.into());
        }
        let n = unsafe {
            libc::poll(
                fds.as_mut_ptr(),
                2,
                remaining.as_millis().min(i32::MAX as u128) as i32,
            )
        };
        if n < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        if fds[0].revents != 0 {
            let mut ack = [0];
            local.read_exact(&mut ack)?;
            if ack != [RECEIVED] {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid resolver receipt",
                ));
            }
            return Ok(());
        }
        if fds[1].revents != 0 {
            return Err(io::ErrorKind::ConnectionReset.into());
        }
    }
}

pub(crate) fn serve_index(
    fd: i32,
    max_lanes: usize,
    initialize: impl FnOnce() -> Result<Runtimes> + Send,
) -> Result<()> {
    let channel = inherited_socket(fd)?;
    // The parent alone owns stdin's write end. Establish this guard before
    // a cold checkout can block on SQLite or linked-worktree preparation.
    thread::Builder::new()
        .name("tsindex-daemon-lifetime".into())
        .spawn(|| {
            let _ = io::stdin().read_exact(&mut [0u8; 1]);
            std::process::exit(0);
        })?;
    serve_lanes(channel, max_lanes, initialize)
}

fn serve_lanes<T: ToolResolver + Send>(
    channel: UnixStream,
    max_lanes: usize,
    initialize: impl FnOnce() -> Result<T> + Send,
) -> Result<()> {
    // An actual client owns a private stream before initialization can fail.
    // The intake loop keeps acknowledging other attachments during a cold start.
    let Some(first) = next_lane(&channel)? else {
        return Ok(());
    };
    let initialized = OnceLock::new();
    thread::scope(|scope| -> Result<()> {
        let state = &initialized;
        let notify = &channel;
        scope.spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(initialize))
                .unwrap_or_else(|_| Err(anyhow!("index initializer panicked")));
            if result.is_err() {
                // New acquisitions must observe closure before any waiting
                // client receives its detailed error and attempts a repair.
                let _ = notify.shutdown(std::net::Shutdown::Both);
            }
            let _ = state.set(result);
        });
        let mut lanes = Vec::<([u8; 16], thread::ScopedJoinHandle<'_, ()>)>::new();
        let mut next = Some(first);
        while let Some((hello, mut lane)) = next {
            lanes.retain(|(_, handle)| !handle.is_finished());
            let session_count = lanes
                .iter()
                .filter(|(session, _)| *session == hello.session)
                .count();
            if hello.protocol != PROTOCOL_VERSION {
                let _ = send_error(
                    lane.get_mut(),
                    "index runtime protocol mismatch; restart the daemon",
                );
            } else if lanes.len() >= max_lanes
                || session_count >= crate::mcp::MAX_CONCURRENT_REQUESTS
            {
                let _ = send_error(
                    lane.get_mut(),
                    "resolver capacity reached; retry after an outstanding lookup finishes",
                );
            } else {
                let task = thread::Builder::new()
                    .name("tsindex-resolve".into())
                    .spawn_scoped(scope, move || match state.wait() {
                        Ok(runtimes) => {
                            let _ = serve_lane(lane, runtimes);
                        }
                        Err(error) => {
                            let _ = send_error(lane.get_mut(), &format!("{error:#}"));
                        }
                    });
                if let Ok(handle) = task {
                    lanes.push((hello.session, handle));
                }
            }
            next = next_lane(&channel)?;
        }
        if let Err(error) = state.wait() {
            return Err(anyhow!("{error:#}"));
        }
        Ok(())
    })
}

fn next_lane(channel: &UnixStream) -> Result<Option<(OpenLane, BufReader<UnixStream>)>> {
    while let Some(fd) = fds::recv_fd(channel)? {
        let mut lane = BufReader::new(UnixStream::from(fd));
        // A disconnected worker or malformed private header affects only it.
        if let Ok(hello) = receive_lane(&mut lane) {
            return Ok(Some((hello, lane)));
        }
    }
    Ok(None)
}

fn receive_lane(lane: &mut BufReader<UnixStream>) -> Result<OpenLane> {
    lane.get_ref().peer_addr()?;
    lane.get_mut().set_nonblocking(true)?;
    lane.get_mut().write_all(&[RECEIVED])?;
    let line = read_bounded_line(lane, super::MAX_CONTROL_LINE_BYTES)?;
    let hello = serde_json::from_str(&line)?;
    lane.get_mut().set_nonblocking(false)?;
    lane.get_mut().set_write_timeout(Some(STARTUP_IO_TIMEOUT))?;
    Ok(hello)
}

fn send_error(stream: &mut UnixStream, detail: &str) -> Result<()> {
    write_bounded_json(
        stream,
        &Reply::Error {
            detail: detail.into(),
            invalid_params: false,
        },
        MAX_MCP_FRAME_BYTES,
    )
}

fn serve_lane(mut lane: BufReader<UnixStream>, runtimes: &impl ToolResolver) -> Result<()> {
    write_bounded_json(lane.get_mut(), &Reply::Ready, MAX_MCP_FRAME_BYTES)?;
    while let Ok(line) = read_bounded_line(&mut lane, MAX_MCP_FRAME_BYTES) {
        let Lookup::Prepare { repo, live_query } = serde_json::from_str(&line)?;
        let reply = runtimes
            .prepare(repo.as_deref(), live_query)
            .map(|prepared| Reply::Prepared(Box::new(prepared)))
            .unwrap_or_else(|err| Reply::Error {
                invalid_params: err.is::<InvalidParams>(),
                detail: format!("{err:#}"),
            });
        write_bounded_json(lane.get_mut(), &reply, MAX_MCP_FRAME_BYTES)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn owner_fixture(dir: &Path) -> Result<PathBuf> {
        let image = dir.join("owner");
        // The pool supplies a lifetime pipe on stdin. This fixture stays
        // alive until its parent releases that pipe or terminates the child.
        std::fs::write(&image, "#!/bin/sh\nexec cat >/dev/null\n")?;
        std::fs::set_permissions(&image, std::fs::Permissions::from_mode(0o700))?;
        Ok(image)
    }

    #[test]
    fn retries_replace_an_owner_that_never_served_a_client() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let image = owner_fixture(dir.path())?;
        let key = RuntimeKey::resolve(dir.path(), None, &[], true, None)?;
        let mut owners = IndexOwners::new(3);
        let first = owners.acquire(key.clone(), &image, Instant::now())?;
        let retry = first.generation.started + STARTUP_IO_TIMEOUT;
        assert_eq!(owners.acquire(key.clone(), &image, retry)?.pid, first.pid);
        let expired = retry + STARTUP_IO_TIMEOUT;
        let next = owners.acquire(key.clone(), &image, expired)?;
        assert_ne!(next.pid, first.pid);
        // A pending client can finish on its original owner while new clients
        // have a recovery path. Ready owners have no cold-start deadline.
        assert_eq!(unsafe { libc::kill(first.pid as libc::pid_t, 0) }, 0);
        next.mark_ready();
        assert_eq!(
            owners
                .acquire(key, &image, expired + STARTUP_IO_TIMEOUT)?
                .pid,
            next.pid
        );
        owners.shutdown();
        Ok(())
    }

    #[test]
    fn receipt_wait_observes_owner_loss_while_the_sender_retains_the_endpoint() -> Result<()> {
        let (sender, owner) = UnixStream::pair()?;
        let (mut local, remote) = UnixStream::pair()?;
        fds::send_fd(&sender, remote.as_fd())?;
        drop(owner);
        let error = await_receipt(&mut local, &sender, Duration::from_secs(2)).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
        drop(remote);
        Ok(())
    }

    #[test]
    fn failed_initialization_closes_intake_before_answering_all_admitted_clients() -> Result<()> {
        let (sender, owner) = UnixStream::pair()?;
        let (release, gate) = mpsc::channel::<()>();
        let listener = thread::spawn(move || {
            serve_lanes(owner, 8, move || -> Result<Runtimes> {
                gate.recv()?;
                bail!("database initialization failed")
            })
        });
        let mut clients = vec![];
        for id in [1, 2] {
            let (mut local, remote) = UnixStream::pair()?;
            super::super::write_json_line(
                &mut local,
                &OpenLane {
                    session: [id; 16],
                    protocol: PROTOCOL_VERSION,
                },
            )?;
            fds::send_fd(&sender, remote.as_fd())?;
            await_receipt(&mut local, &sender, Duration::from_secs(3))?;
            drop(remote);
            local.set_read_timeout(Some(Duration::from_secs(3)))?;
            clients.push(BufReader::new(local));
        }
        release.send(())?;
        for mut client in clients {
            match read_reply(&mut client)? {
                Reply::Error { detail, .. } => assert_eq!(detail, "database initialization failed"),
                _ => panic!("expected the initialization diagnostic"),
            }
            assert!(
                intake_closed(&sender),
                "pool could still reassign this failed owner"
            );
        }
        assert!(listener.join().unwrap().is_err());
        Ok(())
    }

    #[test]
    fn established_clients_reuse_their_private_streams_under_concurrent_load() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path().to_owned();
        let (sender, owner) = UnixStream::pair()?;
        let listener = thread::spawn(move || {
            serve_lanes(owner, 32, || {
                Ok(Runtimes::new(crate::index::Runtime::new(
                    root.clone(),
                    db_path(&root),
                    TsIndexConfig::default(),
                    vec![],
                )))
            })
        });
        let clients: Vec<_> = (0..8)
            .map(|_| SessionResolver::attach(sender.try_clone()?, Duration::from_secs(3)))
            .collect::<Result<_>>()?;
        // Disable all further descriptor transfers. Each client must complete
        // its own lookups concurrently using the private stream already assigned.
        sender.shutdown(std::net::Shutdown::Write)?;
        thread::scope(|scope| -> Result<()> {
            let calls: Vec<_> = clients
                .into_iter()
                .map(|client| {
                    scope.spawn(move || -> Result<()> {
                        for _ in 0..20 {
                            client.prepare(None, true)?;
                        }
                        Ok(())
                    })
                })
                .collect();
            for call in calls {
                call.join().unwrap()?;
            }
            Ok(())
        })?;
        listener.join().unwrap()?;
        Ok(())
    }

    #[test]
    fn blocked_lookups_remain_charged_to_their_session_and_leave_another_session_live() -> Result<()>
    {
        use std::sync::Condvar;
        type Gate = Arc<(Mutex<bool>, Condvar)>;
        struct Release(Gate);
        impl Drop for Release {
            fn drop(&mut self) {
                *self.0.0.lock().unwrap() = true;
                self.0.1.notify_all();
            }
        }
        struct Controlled {
            started: mpsc::Sender<()>,
            gate: Gate,
            real: Runtimes,
        }
        impl ToolResolver for Controlled {
            fn prepare(&self, repo: Option<&str>, live: bool) -> Result<PreparedTool> {
                if !live {
                    self.started.send(())?;
                    drop(
                        self.gate
                            .1
                            .wait_while(self.gate.0.lock().unwrap(), |blocked| !*blocked)
                            .unwrap(),
                    );
                }
                self.real.prepare(repo, true)
            }
        }
        let dir = tempfile::tempdir()?;
        let root = dir.path().to_path_buf();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let release = Release(gate.clone());
        let (started, events) = mpsc::channel();
        let (sender, owner) = UnixStream::pair()?;
        let listener = thread::spawn(move || {
            serve_lanes(owner, 8, || {
                Ok(Controlled {
                    started,
                    gate,
                    real: Runtimes::new(crate::index::Runtime::new(
                        root.clone(),
                        db_path(&root),
                        TsIndexConfig::default(),
                        vec![],
                    )),
                })
            })
        });
        let busy = Arc::new(SessionResolver::attach(
            sender.try_clone()?,
            Duration::from_millis(750),
        )?);
        let jobs: Vec<_> = (0..crate::mcp::MAX_CONCURRENT_REQUESTS)
            .map(|_| {
                let session = busy.clone();
                thread::spawn(move || session.prepare(None, false))
            })
            .collect();
        for _ in 0..jobs.len() {
            events.recv_timeout(Duration::from_secs(3))?;
        }
        let peer = SessionResolver::attach(sender, Duration::from_secs(2))?;
        for _ in 0..8 {
            peer.prepare(None, true)?;
        }
        for task in jobs {
            let error = task
                .join()
                .unwrap()
                .err()
                .context("expected an isolated lookup timeout")?;
            assert!(
                error
                    .chain()
                    .any(
                        |cause| cause.downcast_ref::<io::Error>().is_some_and(|e| matches!(
                            e.kind(),
                            io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                        ))
                    )
            );
        }
        // Abandoning four streams cannot create a fifth running task for the
        // same session. The independent peer keeps its healthy connection.
        let error = busy
            .prepare(None, true)
            .err()
            .context("expected capacity rejection")?;
        assert!(error.to_string().contains("capacity"), "{error:#}");
        peer.prepare(None, true)?;
        drop(release);
        let deadline = Instant::now() + Duration::from_secs(3);
        while let Err(e) = busy.prepare(None, true) {
            ensure!(Instant::now() < deadline, "session did not recover: {e:#}");
            thread::sleep(Duration::from_millis(10));
        }
        drop(busy);
        drop(peer);
        listener.join().unwrap()?;
        Ok(())
    }

    #[test]
    fn timed_out_generation_is_replaced_without_stopping_healthy_peers() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let image = owner_fixture(dir.path())?;
        let pool = RuntimePool::new(4)?;
        let key = RuntimeKey::resolve(dir.path(), None, &[], true, None)?;
        let first = pool.acquire(key.clone(), &image)?;
        let peer = pool.acquire(key.clone(), &image)?;
        assert_eq!(first.pid, peer.pid);
        pool.retire(&first);
        let replacement = pool.acquire(key.clone(), &image)?;
        assert_ne!(replacement.pid, first.pid);
        // Late notifications from a failed session only refer to its generation.
        pool.retire(&first);
        assert_eq!(pool.acquire(key.clone(), &image)?.pid, replacement.pid);
        drop(first);
        assert_eq!(unsafe { libc::kill(peer.pid as libc::pid_t, 0) }, 0);
        let old_pid = peer.pid;
        drop(peer);
        let deadline = Instant::now() + Duration::from_secs(3);
        while unsafe { libc::kill(old_pid as libc::pid_t, 0) } == 0 {
            ensure!(
                Instant::now() < deadline,
                "unused retired index owner still exists"
            );
            thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }

    #[test]
    fn runtime_key_ignores_unusable_caller_dir_unless_explicit_ignores_exist() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path().join("repo");
        let caller_a = dir.path().join("caller-a");
        let caller_b = dir.path().join("caller-b");
        let caller_file = dir.path().join("not-a-dir");
        std::fs::create_dir_all(&root)?;
        std::fs::create_dir_all(&caller_a)?;
        std::fs::create_dir_all(&caller_b)?;
        std::fs::write(&caller_file, "file")?;
        let missing = dir.path().join("mount-ns-only");

        // Without explicit ignore files, unavailable caller_dirs fall back to
        // root, while usable caller_dirs are preserved and keep distinct keys
        // so anchored global Git ignores and linked checkouts match direct MCP.
        let key_missing = RuntimeKey::resolve(&root, None, &[], true, Some(&missing))?;
        let key_file = RuntimeKey::resolve(&root, None, &[], true, Some(&caller_file))?;
        let key_none = RuntimeKey::resolve(&root, None, &[], true, None)?;
        let key_root = RuntimeKey::resolve(&root, None, &[], true, Some(&root))?;
        let key_a = RuntimeKey::resolve(&root, None, &[], true, Some(&caller_a))?;
        let key_a_again = RuntimeKey::resolve(&root, None, &[], true, Some(&caller_a))?;
        let key_b = RuntimeKey::resolve(&root, None, &[], true, Some(&caller_b))?;
        assert!(key_missing == key_none);
        assert!(key_missing == key_file);
        assert!(key_missing == key_root);
        assert!(key_a == key_a_again);
        assert!(key_a != key_root);
        assert!(key_a != key_b);
        assert_eq!(key_a.caller_dir, caller_a.canonicalize()?);
        assert_eq!(key_root.caller_dir, root.canonicalize()?);

        // With per-repo [[repos]].ignore or absolute ignore files, a usable
        // caller_dir is required and separates runtime keys by caller directory.
        let mut config = TsIndexConfig::default();
        config.repos.push(crate::config::RepoConfig {
            name: "repo".into(),
            path: ".".into(),
            languages: vec![],
            ignore: vec!["/abs/rules.txt".into()],
        });
        config.write(&root)?;
        assert!(RuntimeKey::resolve(&root, None, &[], true, Some(&missing)).is_err());
        assert!(RuntimeKey::resolve(&root, None, &[], true, Some(&caller_file)).is_err());
        assert!(RuntimeKey::resolve(&root, None, &[], true, None).is_err());
        let key_a = RuntimeKey::resolve(&root, None, &[], true, Some(&caller_a))?;
        let key_b = RuntimeKey::resolve(&root, None, &[], true, Some(&caller_b))?;
        assert!(key_a != key_b);
        Ok(())
    }

    #[test]
    fn abandoned_lookup_marks_resolver_for_generation_retirement() -> Result<()> {
        use std::sync::Condvar;
        type Gate = Arc<(Mutex<bool>, Condvar)>;
        struct Release(Gate);
        impl Drop for Release {
            fn drop(&mut self) {
                *self.0.0.lock().unwrap() = true;
                self.0.1.notify_all();
            }
        }
        struct Blocking {
            gate: Gate,
            real: Runtimes,
        }
        impl ToolResolver for Blocking {
            fn prepare(&self, repo: Option<&str>, live: bool) -> Result<PreparedTool> {
                if !live {
                    drop(
                        self.gate
                            .1
                            .wait_while(self.gate.0.lock().unwrap(), |open| !*open)
                            .unwrap(),
                    );
                }
                self.real.prepare(repo, true)
            }
        }
        let dir = tempfile::tempdir()?;
        let root = dir.path().to_path_buf();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let release = Release(gate.clone());
        let (sender, owner) = UnixStream::pair()?;
        let listener = thread::spawn(move || {
            serve_lanes(owner, 4, || {
                Ok(Blocking {
                    gate,
                    real: Runtimes::new(crate::index::Runtime::new(
                        root.clone(),
                        db_path(&root),
                        TsIndexConfig::default(),
                        vec![],
                    )),
                })
            })
        });
        let resolver = SessionResolver::attach(sender, Duration::from_millis(100))?;
        assert!(!resolver.has_abandoned_lane());
        resolver.prepare(None, true)?;
        assert!(!resolver.has_abandoned_lane());
        assert!(resolver.prepare(None, false).is_err());
        assert!(resolver.has_abandoned_lane());
        drop(release);
        drop(resolver);
        listener.join().unwrap()?;
        Ok(())
    }

    #[test]
    fn intake_closed_via_poll_detects_open_readable_and_closed_streams() -> Result<()> {
        let (mut left, mut right) = UnixStream::pair()?;
        assert!(!intake_closed_via_poll(&left));

        right.write_all(&[0x5a])?;
        assert!(!intake_closed_via_poll(&left));
        let mut buf = [0u8; 1];
        left.read_exact(&mut buf)?;
        assert_eq!(buf, [0x5a]);

        drop(right);
        assert!(intake_closed_via_poll(&left));
        Ok(())
    }
}
