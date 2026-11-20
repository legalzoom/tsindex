//! One warm index owner per compatible checkout. Only session workers receive
//! its unnamed Unix socket. Each lookup has a private reply channel, so two
//! harnesses can use the same JSON-RPC IDs or cancel independently.

use std::collections::HashMap;
use std::io::{self, BufReader, Read};
use std::os::fd::{AsFd, AsRawFd, FromRawFd};
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    MAX_MCP_FRAME_BYTES, PROTOCOL_VERSION, STARTUP_IO_TIMEOUT, StartSessionRequest, fds,
    read_bounded_line, write_bounded_json,
};
use crate::config::{TsIndexConfig, db_path};
use crate::mcp::{InvalidParams, PreparedTool, ToolResolver};
use crate::worktrees::Runtimes;

const DEFAULT_IDLE: Duration = Duration::from_secs(60);
pub(super) const MAX_IDLE: usize = 4;
const RESOLVERS: usize = 4;
// Distinguish an unresponsive shared resolver from a failed tool or an
// independently cancelled MCP worker when the supervisor reaps the child.
pub(super) const LOOKUP_TIMEOUT_EXIT: i32 = 75;

fn config_digest(config: &TsIndexConfig) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(config)?)))
}

// Existing parents can be aliases even when a requested DB does not exist yet.
fn canonical_target(path: &Path) -> io::Result<PathBuf> {
    match path.canonicalize() {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .ok_or(error)?;
            Ok(canonical_target(parent)?.join(
                path.file_name()
                    .ok_or_else(|| io::Error::other("missing database filename"))?,
            ))
        }
        Err(error) => Err(error),
    }
}

#[derive(Clone, Hash, PartialEq, Eq)]
struct RuntimeKey {
    root: PathBuf,
    // An explicit DB disables repo pruning even if it names the default file.
    db: Option<PathBuf>,
    languages: Vec<String>,
    no_refresh: bool,
    config: String,
    executable: (u64, u64),
}

impl RuntimeKey {
    fn for_session(req: &StartSessionRequest, executable: &Path) -> Result<Self> {
        let root = req
            .root
            .canonicalize()
            .context("cannot resolve checkout root")?;
        ensure!(root.is_dir(), "workspace root must be a directory");
        let db = req
            .db
            .as_deref()
            .map(|path| canonical_target(&root.join(path)))
            .transpose()?;
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
        let mut languages = req.languages.clone();
        languages.sort();
        languages.dedup();
        let image = executable.metadata()?;
        Ok(Self {
            root,
            db,
            languages,
            no_refresh: req.no_refresh,
            config: config_digest(&settings)?,
            executable: (image.dev(), image.ino()),
        })
    }
}

pub(super) struct RuntimeLease {
    pub channel: Arc<UnixStream>,
    pub pid: u32,
}

struct Entry {
    child: Child,
    channel: Arc<UnixStream>,
    idle_since: Option<Instant>,
}

impl Entry {
    fn start(key: &RuntimeKey, executable: &Path) -> Result<Self> {
        let (channel, receiver) = UnixStream::pair()?;
        channel.set_write_timeout(Some(STARTUP_IO_TIMEOUT))?;
        let mut cmd = Command::new(executable);
        cmd.arg("--root").arg(&key.root).current_dir(&key.root);
        if let Some(db) = &key.db {
            cmd.arg("--db").arg(db);
        }
        for language in &key.languages {
            cmd.arg("--languages").arg(language);
        }
        if key.no_refresh {
            cmd.arg("--no-refresh");
        }
        cmd.args(["daemon", "index-runtime", "--runtime-fd"])
            .arg(receiver.as_raw_fd().to_string())
            .arg("--config-digest")
            .arg(&key.config)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        fds::prepare_exec(&mut cmd, Some(receiver.as_fd()))?;
        // Linux ties parent-death delivery to the spawning thread. The pool
        // manager lives for the daemon's lifetime, independently of any client.
        super::orchestrator::configure_child(&mut cmd, false, libc::SIGKILL);
        let child = cmd
            .spawn()
            .context("failed to start shared index runtime")?;
        Ok(Self {
            child,
            channel: Arc::new(channel),
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
            channel: self.channel.clone(),
            pid: self.child.id(),
        }
    }
}

impl Drop for Entry {
    fn drop(&mut self) {
        // Only the daemon holds the write end. EOF also releases the runtime
        // after an ungraceful daemon exit, even if a session still has a channel.
        drop(self.child.stdin.take());
        // The index owner spawns threads only. Reap its reserved child PID even
        // if initialization is stuck or the process has been stopped.
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

enum PoolMessage {
    Acquire(RuntimeKey, PathBuf, mpsc::SyncSender<Result<RuntimeLease>>),
    Retire(Arc<UnixStream>),
    Stop,
}

pub(super) struct RuntimePool {
    inbox: mpsc::Sender<PoolMessage>,
    resident: Arc<AtomicUsize>,
    thread: Mutex<Option<thread::JoinHandle<()>>>,
}

impl RuntimePool {
    pub fn new() -> Result<Self> {
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
            .spawn(move || run_pool(requests, idle, &count))?;
        Ok(Self {
            inbox,
            resident,
            thread: Mutex::new(Some(thread)),
        })
    }

    pub fn acquire(&self, req: &StartSessionRequest, executable: &Path) -> Result<RuntimeLease> {
        let key = RuntimeKey::for_session(req, executable)?;
        let (tx, rx) = mpsc::sync_channel(1);
        self.inbox
            .send(PoolMessage::Acquire(key, executable.to_owned(), tx))
            .map_err(|_| anyhow!("index runtime pool stopped"))?;
        rx.recv().context("index runtime pool stopped")?
    }

    pub fn retire(&self, lease: &RuntimeLease) {
        // Compare capability identities. A late failure of an old owner cannot
        // retire a replacement that happens to have the same configuration.
        let _ = self.inbox.send(PoolMessage::Retire(lease.channel.clone()));
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

fn run_pool(requests: mpsc::Receiver<PoolMessage>, idle: Duration, count: &AtomicUsize) {
    let mut entries = HashMap::<RuntimeKey, Entry>::new();
    let mut retiring = Vec::<Entry>::new();
    loop {
        // With no children there are no timeouts or exits to supervise.
        let message = if entries.is_empty() && retiring.is_empty() {
            requests
                .recv()
                .map_err(|_| mpsc::RecvTimeoutError::Disconnected)
        } else {
            requests.recv_timeout(Duration::from_millis(250))
        };
        let now = Instant::now();
        retiring.retain_mut(|owner| {
            Arc::strong_count(&owner.channel) > 1 && matches!(owner.child.try_wait(), Ok(None))
        });
        entries.retain(|_, entry| {
            if !matches!(entry.child.try_wait(), Ok(None)) {
                return false;
            }
            if Arc::strong_count(&entry.channel) == 1 {
                now.duration_since(*entry.idle_since.get_or_insert(now)) < idle
            } else {
                entry.idle_since = None;
                true
            }
        });
        let mut idle_times: Vec<_> = entries.values().filter_map(|e| e.idle_since).collect();
        if idle_times.len() > MAX_IDLE {
            idle_times.sort_unstable();
            let mut excess = idle_times.len() - MAX_IDLE;
            let cutoff = idle_times[excess - 1];
            entries.retain(|_, e| {
                if excess > 0 && e.idle_since.is_some_and(|t| t <= cutoff) {
                    excess -= 1;
                    false
                } else {
                    true
                }
            });
        }
        match message {
            Ok(PoolMessage::Acquire(key, image, reply)) => {
                if entries.get(&key).is_some_and(Entry::is_stopped) {
                    // A confirmed stopped owner may hold the writer lease.
                    // Reaping it releases that lease before its replacement opens.
                    entries.remove(&key);
                }
                let result = match entries.entry(key) {
                    std::collections::hash_map::Entry::Occupied(mut slot) => {
                        Ok(slot.get_mut().lease())
                    }
                    std::collections::hash_map::Entry::Vacant(slot) => {
                        Entry::start(slot.key(), &image).map(|entry| slot.insert(entry).lease())
                    }
                };
                count.store(entries.len() + retiring.len(), Ordering::Relaxed);
                let _ = reply.send(result);
            }
            Ok(PoolMessage::Retire(channel)) => {
                // A timed-out lookup is not proof of process death. Existing
                // sessions can finish on their owner. Route new clients to a
                // fresh owner and reap this generation after its last lease.
                if let Some(key) = entries
                    .iter()
                    .find(|(_, entry)| Arc::ptr_eq(&entry.channel, &channel))
                    .map(|(key, _)| key.clone())
                    && let Some(entry) = entries.remove(&key)
                {
                    retiring.push(entry);
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                count.store(entries.len() + retiring.len(), Ordering::Relaxed)
            }
            Ok(PoolMessage::Stop) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    count.store(0, Ordering::Relaxed);
}

#[derive(Serialize, Deserialize)]
enum Lookup {
    Ready {
        protocol: u32,
    },
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

pub(crate) struct SessionResolver {
    channel: UnixStream,
}

impl SessionResolver {
    pub fn connect(fd: i32) -> Result<Self> {
        let resolver = Self {
            channel: inherited_socket(fd)?,
        };
        ensure!(
            matches!(
                resolver.request(Lookup::Ready {
                    protocol: PROTOCOL_VERSION
                })?,
                Reply::Ready
            ),
            "invalid index runtime readiness acknowledgement"
        );
        // Only requests travel on the shared channel, each with its own reply
        // stream. EOF here means the index owner died. End affected MCP sessions
        // so a reconnect can acquire a new owner.
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

    fn request(&self, request: Lookup) -> Result<Reply> {
        let response = self.exchange(request);
        if response.as_ref().is_err_and(|error| {
            error.chain().any(|cause| {
                cause.downcast_ref::<io::Error>().is_some_and(|e| {
                    matches!(
                        e.kind(),
                        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                    )
                })
            })
        }) {
            eprintln!("shared index runtime timed out; reconnect to start a new runtime");
            std::process::exit(LOOKUP_TIMEOUT_EXIT);
        }
        response
    }

    fn exchange(&self, request: Lookup) -> Result<Reply> {
        let (mut local, remote) = UnixStream::pair()?;
        local.set_read_timeout(Some(STARTUP_IO_TIMEOUT))?;
        local.set_write_timeout(Some(STARTUP_IO_TIMEOUT))?;
        fds::send_fd(&self.channel, remote.as_fd()).context("shared index runtime unavailable")?;
        drop(remote);
        write_bounded_json(&mut local, &request, MAX_MCP_FRAME_BYTES)?;
        let line = read_bounded_line(&mut BufReader::new(local), MAX_MCP_FRAME_BYTES)
            .context("shared index runtime did not answer")?;
        match serde_json::from_str(&line)? {
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

pub(crate) fn verify_config(config: &TsIndexConfig, expected: &str) -> Result<()> {
    ensure!(
        config_digest(config)? == expected,
        "checkout configuration changed during startup; reconnect"
    );
    Ok(())
}

pub(crate) fn serve_index(fd: i32, initialize: impl FnOnce() -> Result<Runtimes>) -> Result<()> {
    let mut channel = Mutex::new(inherited_socket(fd)?);
    // The parent alone owns stdin's write end. Establish this guard before
    // a cold checkout can block on SQLite or linked-worktree preparation.
    thread::Builder::new()
        .name("tsindex-daemon-lifetime".into())
        .spawn(|| {
            let _ = io::stdin().read_exact(&mut [0u8; 1]);
            std::process::exit(0);
        })?;
    let runtimes = match initialize() {
        Ok(runtime) => runtime,
        Err(error) => {
            // Return the original initialization error over the private channel
            // before exiting; a later attach can retry a temporary failure.
            if let Some(fd) = fds::recv_fd(channel.get_mut().unwrap_or_else(|p| p.into_inner()))? {
                let mut reply = UnixStream::from(fd);
                reply.set_read_timeout(Some(STARTUP_IO_TIMEOUT))?;
                reply.set_write_timeout(Some(STARTUP_IO_TIMEOUT))?;
                let _ = read_bounded_line(&mut BufReader::new(&reply), MAX_MCP_FRAME_BYTES);
                write_bounded_json(
                    &mut reply,
                    &Reply::Error {
                        detail: format!("{error:#}"),
                        invalid_params: false,
                    },
                    MAX_MCP_FRAME_BYTES,
                )?;
            }
            return Err(error);
        }
    };
    thread::scope(|scope| -> Result<()> {
        let mut handlers = Vec::with_capacity(RESOLVERS);
        for _ in 0..RESOLVERS {
            handlers.push(
                thread::Builder::new()
                    .name("tsindex-resolve".into())
                    .spawn_scoped(scope, || -> Result<()> {
                        loop {
                            let fd = {
                                let channel = channel.lock().unwrap_or_else(|p| p.into_inner());
                                fds::recv_fd(&channel)?
                            };
                            let Some(fd) = fd else { return Ok(()) };
                            // A session may disappear during refresh. Losing that reply
                            // must not cancel useful index work or close other sessions.
                            let _ = answer(UnixStream::from(fd), &runtimes);
                        }
                    })?,
            );
        }
        for handler in handlers {
            handler
                .join()
                .map_err(|_| anyhow!("runtime resolver stopped"))??;
        }
        Ok(())
    })
}

fn answer(mut stream: UnixStream, runtimes: &Runtimes) -> Result<()> {
    stream.set_read_timeout(Some(STARTUP_IO_TIMEOUT))?;
    stream.set_write_timeout(Some(STARTUP_IO_TIMEOUT))?;
    let request = read_bounded_line(&mut BufReader::new(&stream), MAX_MCP_FRAME_BYTES)?;
    let result = match serde_json::from_str(&request)? {
        Lookup::Ready { protocol } if protocol == PROTOCOL_VERSION => Ok(Reply::Ready),
        Lookup::Ready { .. } => Err(anyhow!(
            "index runtime protocol mismatch; restart tsindex daemon"
        )),
        Lookup::Prepare { repo, live_query } => runtimes
            .prepare(repo.as_deref(), live_query)
            .map(|prepared| Reply::Prepared(Box::new(prepared))),
    };
    let reply = result.unwrap_or_else(|err| Reply::Error {
        invalid_params: err.is::<InvalidParams>(),
        detail: format!("{err:#}"),
    });
    write_bounded_json(&mut stream, &reply, MAX_MCP_FRAME_BYTES)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn timed_out_generation_is_replaced_without_stopping_healthy_peers() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let image = dir.path().join("owner");
        // No request processing is needed to exercise the pool's lifecycle.
        // exec keeps the fixture a single, directly owned child.
        std::fs::write(&image, "#!/bin/sh\nexec sleep 30\n")?;
        std::fs::set_permissions(&image, std::fs::Permissions::from_mode(0o700))?;
        let pool = RuntimePool::new()?;
        let request = StartSessionRequest {
            protocol: PROTOCOL_VERSION,
            client_version: env!("CARGO_PKG_VERSION").into(),
            root: dir.path().into(),
            db: None,
            languages: vec![],
            no_refresh: true,
            client_pid: std::process::id(),
            owner_pid: None,
            owner_explicit: false,
        };
        let first = pool.acquire(&request, &image)?;
        let peer = pool.acquire(&request, &image)?;
        assert_eq!(first.pid, peer.pid);
        pool.retire(&first);
        let replacement = pool.acquire(&request, &image)?;
        assert_ne!(replacement.pid, first.pid);
        // Late notifications from a failed session only refer to its generation.
        pool.retire(&first);
        assert_eq!(pool.acquire(&request, &image)?.pid, replacement.pid);
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
}
