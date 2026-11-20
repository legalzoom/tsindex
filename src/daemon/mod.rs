//! Long-standing orchestrator daemon, thin MCP proxy, and OS login service
//! integration for macOS (`launchd`) and Linux (`systemd --user`).

#[cfg(unix)]
pub(crate) mod activation;
#[cfg(unix)]
pub mod orchestrator;
#[cfg(unix)]
pub mod proxy;
#[cfg(unix)]
pub mod service;

use std::env;
use std::ffi::OsString;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};

use crate::mcp::tsindex_home;

pub const PROTOCOL_VERSION: u32 = 3;
pub const STARTUP_IO_TIMEOUT: Duration = Duration::from_secs(120);
pub const MAX_MCP_FRAME_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_CONTROL_LINE_BYTES: usize = 64 * 1024;
pub const CONTROL_IO_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_UNIX_SOCKET_PATH_BYTES: usize = 100;

/// Filesystem locations used by one orchestrator daemon instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonPaths {
    pub socket_path: PathBuf,
    pub lock_path: PathBuf,
    pub state_path: PathBuf,
    pub log_path: PathBuf,
}

impl DaemonPaths {
    /// Resolve daemon paths from CLI override, environment variables, or
    /// platform defaults.
    pub fn resolve(socket_override: Option<&Path>) -> Result<Self> {
        Self::resolve_with(
            socket_override,
            env::var_os("TSINDEX_SOCKET"),
            env::var_os("TSINDEX_HOME"),
            env::var_os("XDG_RUNTIME_DIR"),
            tsindex_home(),
            current_uid(),
        )
    }

    fn resolve_with(
        socket_override: Option<&Path>,
        env_socket: Option<OsString>,
        env_home: Option<OsString>,
        xdg_runtime_dir: Option<OsString>,
        default_home: PathBuf,
        uid: u32,
    ) -> Result<Self> {
        let env_socket = env_socket.filter(|value| !value.is_empty());
        let env_home = env_home.filter(|value| !value.is_empty());
        let socket_path = if let Some(path) = socket_override {
            path.to_path_buf()
        } else if let Some(path) = env_socket {
            PathBuf::from(path)
        } else if let Some(home) = env_home {
            PathBuf::from(home).join("run").join("daemon.sock")
        } else {
            default_socket_path(xdg_runtime_dir, &default_home, uid)
        };

        // Service managers run in a different working directory. Persist one
        // absolute endpoint so manual and installed launches resolve alike.
        let socket_path = if socket_path.is_absolute() {
            socket_path
        } else {
            env::current_dir()
                .context("failed to resolve relative daemon socket path")?
                .join(socket_path)
        };
        let socket_bytes = socket_path.as_os_str().as_encoded_bytes().len();
        if socket_bytes > MAX_UNIX_SOCKET_PATH_BYTES {
            bail!(
                "daemon socket path exceeds Unix socket limit ({socket_bytes} > {MAX_UNIX_SOCKET_PATH_BYTES} bytes): {}",
                socket_path.display()
            );
        }

        let dir = socket_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .ok_or_else(|| {
                anyhow!(
                    "daemon socket path has no parent directory: {}",
                    socket_path.display()
                )
            })?
            .to_path_buf();
        // Separate daemon-owned files from user files and preserve the entire
        // socket filename, including its extension, in the namespace.
        let mut metadata_name = OsString::from(".");
        metadata_name.push(
            socket_path
                .file_name()
                .context("socket path has no filename")?,
        );
        metadata_name.push(".tsindex");
        let metadata_dir = dir.join(metadata_name);
        let lock_path = metadata_dir.join("instance.lock");
        let state_path = metadata_dir.join("state.json");
        let log_path = metadata_dir.join("daemon.log");

        Ok(Self {
            socket_path,
            lock_path,
            state_path,
            log_path,
        })
    }

    pub fn socket_dir(&self) -> Result<&Path> {
        self.socket_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .ok_or_else(|| {
                anyhow!(
                    "daemon socket path has no parent directory: {}",
                    self.socket_path.display()
                )
            })
    }
}

fn default_socket_path(
    xdg_runtime_dir: Option<OsString>,
    default_home: &Path,
    uid: u32,
) -> PathBuf {
    #[cfg(target_os = "linux")]
    if let Some(runtime) = xdg_runtime_dir.filter(|value| !value.is_empty()) {
        let candidate = PathBuf::from(runtime).join("tsindex").join("daemon.sock");
        if candidate.as_os_str().as_encoded_bytes().len() <= MAX_UNIX_SOCKET_PATH_BYTES {
            return candidate;
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = xdg_runtime_dir;

    let candidate = default_home.join("run").join("daemon.sock");
    if candidate.as_os_str().as_encoded_bytes().len() <= MAX_UNIX_SOCKET_PATH_BYTES {
        return candidate;
    }
    PathBuf::from(format!("/tmp/tsindex-{uid}/daemon.sock"))
}

pub(crate) fn current_uid() -> u32 {
    #[cfg(unix)]
    {
        unsafe { libc::geteuid() }
    }
    #[cfg(not(unix))]
    {
        0
    }
}

pub(crate) fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

/// Ensure a directory exists, is not a symlink, is owned by the current user,
/// and restricts permissions to `0700` on Unix.
#[cfg(unix)]
pub(crate) fn ensure_private_dir(dir: &Path) -> Result<()> {
    use std::os::unix::fs::DirBuilderExt;

    if !dir.exists() {
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true).mode(0o700);
        builder
            .create(dir)
            .with_context(|| format!("failed to create directory {}", dir.display()))?;
    }
    validate_private_dir(dir)
}

#[cfg(unix)]
pub(crate) fn validate_private_dir(dir: &Path) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let meta = fs::symlink_metadata(dir)
        .with_context(|| format!("failed to inspect directory {}", dir.display()))?;
    if meta.file_type().is_symlink() || !meta.is_dir() {
        bail!(
            "refusing to use non-directory or symlink path {}",
            dir.display()
        );
    }
    let expected_uid = current_uid();
    if meta.uid() != expected_uid {
        bail!(
            "directory {} is owned by uid {}, expected {}",
            dir.display(),
            meta.uid(),
            expected_uid
        );
    }
    if (meta.permissions().mode() & 0o077) != 0 {
        bail!(
            "directory {} must be private (0700); refusing to change existing permissions",
            dir.display()
        );
    }
    Ok(())
}

/// Peer identity obtained from the kernel over a connected Unix domain socket.
#[cfg(unix)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct PeerCredentials {
    pub uid: u32,
    pub pid: Option<u32>,
}

/// Reject any Unix socket peer whose effective UID differs from this daemon's.
#[cfg(unix)]
pub(crate) fn verify_peer_credentials(
    stream: &std::os::unix::net::UnixStream,
) -> Result<PeerCredentials> {
    let creds = read_peer_credentials(stream)?;
    let expected_uid = current_uid();
    if creds.uid != expected_uid {
        bail!(
            "rejected Unix socket peer with uid {} (expected {})",
            creds.uid,
            expected_uid
        );
    }
    Ok(creds)
}

#[cfg(target_os = "macos")]
fn read_peer_credentials(stream: &std::os::unix::net::UnixStream) -> Result<PeerCredentials> {
    use std::os::fd::AsRawFd;

    let fd = stream.as_raw_fd();
    let mut euid: libc::uid_t = 0;
    let mut egid: libc::gid_t = 0;
    let rc = unsafe { libc::getpeereid(fd, &mut euid, &mut egid) };
    if rc != 0 {
        return Err(io::Error::last_os_error()).context("getpeereid failed");
    }

    let mut peer_pid: libc::pid_t = 0;
    let mut len = std::mem::size_of::<libc::pid_t>() as libc::socklen_t;
    let pid_rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_LOCAL,
            libc::LOCAL_PEERPID,
            (&mut peer_pid as *mut libc::pid_t).cast(),
            &mut len,
        )
    };
    let pid = (pid_rc == 0 && peer_pid > 0).then_some(peer_pid as u32);
    Ok(PeerCredentials { uid: euid, pid })
}

#[cfg(target_os = "linux")]
fn read_peer_credentials(stream: &std::os::unix::net::UnixStream) -> Result<PeerCredentials> {
    use std::os::fd::AsRawFd;

    let fd = stream.as_raw_fd();
    let mut cred = libc::ucred {
        pid: 0,
        uid: 0,
        gid: 0,
    };
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&mut cred as *mut libc::ucred).cast(),
            &mut len,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error()).context("getsockopt(SO_PEERCRED) failed");
    }
    let pid = (cred.pid > 0).then_some(cred.pid as u32);
    Ok(PeerCredentials { uid: cred.uid, pid })
}

#[cfg(all(unix, not(any(target_os = "macos", target_os = "linux"))))]
fn read_peer_credentials(stream: &std::os::unix::net::UnixStream) -> Result<PeerCredentials> {
    use std::os::fd::AsRawFd;

    let fd = stream.as_raw_fd();
    let mut euid: libc::uid_t = 0;
    let mut egid: libc::gid_t = 0;
    let rc = unsafe { libc::getpeereid(fd, &mut euid, &mut egid) };
    if rc != 0 {
        return Err(io::Error::last_os_error()).context("getpeereid failed");
    }
    Ok(PeerCredentials {
        uid: euid,
        pid: None,
    })
}

/// Control messages sent to the orchestrator daemon over its Unix domain socket.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum DaemonRequest {
    Ping,
    ListSessions {
        #[serde(default)]
        include_recent: bool,
        #[serde(default)]
        offset: usize,
    },
    SessionDetail {
        session_id: String,
    },
    WatchSession {
        session_id: String,
    },
    KillSession {
        session_id: String,
    },
    KillAll,
    Shutdown,
    StartSession(StartSessionRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartSessionRequest {
    pub protocol: u32,
    pub client_version: String,
    pub root: PathBuf,
    #[serde(default)]
    pub db: Option<PathBuf>,
    #[serde(default)]
    pub languages: Vec<String>,
    #[serde(default)]
    pub no_refresh: bool,
    pub client_pid: u32,
    #[serde(default)]
    pub owner_pid: Option<u32>,
    #[serde(default)]
    pub owner_explicit: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    Starting,
    Active,
    Draining,
    Exited,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    pub session_id: String,
    pub state: SessionState,
    pub root: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub db: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub languages: Vec<String>,
    pub no_refresh: bool,
    pub worker_pid: u32,
    pub client_pid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub harness_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub harness_version: Option<String>,
    pub started_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<u64>,
    pub last_activity_at: u64,
    pub requests_received: u64,
    pub responses_sent: u64,
    pub tool_calls: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recent_stderr: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonStatus {
    pub daemon_pid: u32,
    pub version: String,
    pub protocol: u32,
    pub started_at: u64,
    pub uptime_secs: u64,
    pub socket: PathBuf,
    pub active_sessions: usize,
    pub total_sessions_started: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DaemonResponse {
    Pong(DaemonStatus),
    Sessions {
        active: Vec<SessionInfo>,
        #[serde(default)]
        recent: Vec<SessionInfo>,
        #[serde(default)]
        next_offset: Option<usize>,
    },
    Session {
        session: SessionInfo,
    },
    Killed {
        killed: Vec<String>,
    },
    Stopping,
    Watching,
    SessionReady {
        session_id: String,
        worker_pid: u32,
        daemon_pid: u32,
        daemon_version: String,
        #[serde(default)]
        tracked_owner_pid: Option<u32>,
    },
    Error {
        error: String,
    },
}

/// Internal startup acknowledgement, consumed before switching to MCP.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerReady {
    pub protocol: u32,
    pub version: String,
}

/// On-disk metadata written atomically by the running orchestrator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonStateFile {
    pub pid: u32,
    pub version: String,
    pub protocol: u32,
    pub started_at: u64,
    pub socket: PathBuf,
}

/// Read one newline-delimited control frame, enforcing `max_bytes` before
/// buffering unbounded input.
pub(crate) fn read_bounded_line(reader: &mut impl BufRead, max_bytes: usize) -> Result<String> {
    let mut raw = Vec::new();
    loop {
        let available = reader.fill_buf().context("failed to read control frame")?;
        if available.is_empty() {
            break;
        }
        if let Some(pos) = available.iter().position(|&b| b == b'\n') {
            let take = pos + 1;
            if raw.len() + take > max_bytes {
                bail!("control frame exceeded maximum length of {max_bytes} bytes");
            }
            raw.extend_from_slice(&available[..take]);
            reader.consume(take);
            break;
        }
        if raw.len() + available.len() > max_bytes {
            bail!("control frame exceeded maximum length of {max_bytes} bytes");
        }
        raw.extend_from_slice(available);
        let len = available.len();
        reader.consume(len);
    }
    if raw.is_empty() {
        bail!("connection closed before receiving control frame");
    }
    String::from_utf8(raw).context("control frame is not valid UTF-8")
}

pub(crate) fn write_json_line(writer: &mut impl Write, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() + 1 > MAX_CONTROL_LINE_BYTES {
        bail!("control frame exceeds maximum length");
    }
    writer.write_all(&bytes)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

/// Format the user- and harness-facing error when the orchestrator daemon is
/// not reachable on its Unix socket.
pub fn daemon_not_running_message(socket_path: &Path) -> String {
    format!(
        "tsindex orchestrator daemon is not running (socket: {}). \
         Start it with `tsindex daemon start` or enable it at login with `tsindex daemon install`.",
        socket_path.display()
    )
}

/// Connect without letting a full activation backlog block the calling thread
/// indefinitely. Linux reports a full AF_UNIX backlog as EAGAIN, requiring a
/// fresh socket for each retry. Other pending connects complete via SO_ERROR.
/// https://man7.org/linux/man-pages/man2/connect.2.html
#[cfg(unix)]
pub(crate) fn connect_with_timeout(
    path: &Path,
    timeout: Duration,
) -> io::Result<std::os::unix::net::UnixStream> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::net::UnixStream;
    use std::time::Instant;

    let bytes = path.as_os_str().as_bytes();
    let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    if bytes.is_empty() || bytes.contains(&0) || bytes.len() >= address.sun_path.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid Unix socket path",
        ));
    }
    address.sun_family = libc::AF_UNIX as libc::sa_family_t;
    for (target, byte) in address.sun_path.iter_mut().zip(bytes) {
        *target = *byte as libc::c_char;
    }
    let length =
        (std::mem::offset_of!(libc::sockaddr_un, sun_path) + bytes.len() + 1) as libc::socklen_t;
    #[cfg(target_os = "macos")]
    {
        address.sun_len = length as u8;
    }
    let deadline = Instant::now() + timeout;
    loop {
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "Unix socket connect deadline exceeded",
            ));
        }
        #[cfg(target_os = "linux")]
        let flags = libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK;
        #[cfg(not(target_os = "linux"))]
        let flags = libc::SOCK_STREAM;
        let raw = unsafe { libc::socket(libc::AF_UNIX, flags, 0) };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // OwnedFd closes every failed attempt, including EAGAIN backlog retries.
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        #[cfg(not(target_os = "linux"))]
        {
            let status = unsafe { libc::fcntl(raw, libc::F_GETFL) };
            if status < 0
                || unsafe { libc::fcntl(raw, libc::F_SETFL, status | libc::O_NONBLOCK) } < 0
                || unsafe { libc::fcntl(raw, libc::F_SETFD, libc::FD_CLOEXEC) } < 0
            {
                return Err(io::Error::last_os_error());
            }
        }
        let result =
            unsafe { libc::connect(raw, (&address as *const libc::sockaddr_un).cast(), length) };
        if result == 0 {
            let stream = UnixStream::from(fd);
            stream.set_nonblocking(false)?;
            return Ok(stream);
        }
        let error = io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::EINPROGRESS | libc::EALREADY) => loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "Unix socket connect deadline exceeded",
                    ));
                }
                let millis = remaining.as_millis().clamp(1, i32::MAX as u128) as i32;
                let mut pending = libc::pollfd {
                    fd: fd.as_raw_fd(),
                    events: libc::POLLOUT,
                    revents: 0,
                };
                let ready = unsafe { libc::poll(&mut pending, 1, millis) };
                if ready < 0 {
                    let error = io::Error::last_os_error();
                    if error.kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(error);
                }
                if ready == 0 {
                    continue;
                }
                let mut code: libc::c_int = 0;
                let mut size = std::mem::size_of_val(&code) as libc::socklen_t;
                if unsafe {
                    libc::getsockopt(
                        raw,
                        libc::SOL_SOCKET,
                        libc::SO_ERROR,
                        (&mut code as *mut libc::c_int).cast(),
                        &mut size,
                    )
                } != 0
                {
                    return Err(io::Error::last_os_error());
                }
                if code != 0 {
                    return Err(io::Error::from_raw_os_error(code));
                }
                let stream = UnixStream::from(fd);
                stream.set_nonblocking(false)?;
                return Ok(stream);
            },
            Some(libc::EAGAIN | libc::EINTR) => {
                drop(fd);
                std::thread::sleep(
                    deadline
                        .saturating_duration_since(Instant::now())
                        .min(Duration::from_millis(10)),
                );
            }
            _ => return Err(error),
        }
    }
}

#[cfg(unix)]
pub struct DaemonClient {
    paths: DaemonPaths,
}

#[cfg(unix)]
impl DaemonClient {
    pub fn new(paths: DaemonPaths) -> Self {
        Self { paths }
    }

    pub fn paths(&self) -> &DaemonPaths {
        &self.paths
    }

    pub fn connect(&self) -> Result<std::os::unix::net::UnixStream> {
        validate_private_dir(self.paths.socket_dir()?)?;
        let stream =
            connect_with_timeout(&self.paths.socket_path, CONTROL_IO_TIMEOUT).map_err(|error| {
                anyhow!(
                    "{} ({error})",
                    daemon_not_running_message(&self.paths.socket_path)
                )
            })?;
        // Authenticate the service before disclosing workspace paths or commands.
        verify_peer_credentials(&stream)?;
        Ok(stream)
    }

    pub fn request(&self, req: &DaemonRequest) -> Result<DaemonResponse> {
        let mut stream = self.connect()?;
        stream.set_read_timeout(Some(CONTROL_IO_TIMEOUT))?;
        stream.set_write_timeout(Some(CONTROL_IO_TIMEOUT))?;
        write_json_line(&mut stream, req)?;
        let mut reader = io::BufReader::new(stream);
        let line = read_bounded_line(&mut reader, MAX_CONTROL_LINE_BYTES)?;
        serde_json::from_str(&line).context("failed to parse daemon response")
    }

    pub fn ping(&self) -> Result<DaemonStatus> {
        match self.request(&DaemonRequest::Ping)? {
            DaemonResponse::Pong(status) => Ok(status),
            DaemonResponse::Error { error } => bail!("{error}"),
            other => bail!("unexpected response to ping: {other:?}"),
        }
    }

    pub fn list_sessions(
        &self,
        include_recent: bool,
    ) -> Result<(Vec<SessionInfo>, Vec<SessionInfo>)> {
        let mut active_all = Vec::new();
        let mut recent_all = Vec::new();
        let mut offset = 0;
        loop {
            match self.request(&DaemonRequest::ListSessions {
                include_recent,
                offset,
            })? {
                DaemonResponse::Sessions {
                    active,
                    recent,
                    next_offset,
                } => {
                    active_all.extend(active);
                    recent_all.extend(recent);
                    match next_offset {
                        Some(next) if next > offset => offset = next,
                        Some(_) => bail!("daemon returned an invalid session cursor"),
                        None => return Ok((active_all, recent_all)),
                    }
                }
                DaemonResponse::Error { error } => bail!("{error}"),
                other => bail!("unexpected response to list_sessions: {other:?}"),
            }
        }
    }

    pub fn session_detail(&self, session_id: &str) -> Result<SessionInfo> {
        match self.request(&DaemonRequest::SessionDetail {
            session_id: session_id.to_string(),
        })? {
            DaemonResponse::Session { session } => Ok(session),
            DaemonResponse::Error { error } => bail!("{error}"),
            other => bail!("unexpected response to session_detail: {other:?}"),
        }
    }

    /// Subscribe before forwarding MCP traffic so the final worker outcome
    /// remains available while the daemon is stopping its accept loop.
    pub(crate) fn watch_session(
        &self,
        session_id: &str,
    ) -> Result<io::BufReader<std::os::unix::net::UnixStream>> {
        let mut stream = self.connect()?;
        stream.set_read_timeout(Some(CONTROL_IO_TIMEOUT))?;
        stream.set_write_timeout(Some(CONTROL_IO_TIMEOUT))?;
        write_json_line(
            &mut stream,
            &DaemonRequest::WatchSession {
                session_id: session_id.to_string(),
            },
        )?;
        let mut reader = io::BufReader::new(stream);
        let line = read_bounded_line(&mut reader, MAX_CONTROL_LINE_BYTES)?;
        match serde_json::from_str::<DaemonResponse>(&line)? {
            DaemonResponse::Watching => {
                reader.get_ref().set_read_timeout(None)?;
                Ok(reader)
            }
            DaemonResponse::Error { error } => bail!("{error}"),
            other => bail!("unexpected response to watch_session: {other:?}"),
        }
    }

    pub fn kill_session(&self, session_id: String) -> Result<Vec<String>> {
        match self.request(&DaemonRequest::KillSession { session_id })? {
            DaemonResponse::Killed { killed } => Ok(killed),
            DaemonResponse::Error { error } => bail!("{error}"),
            other => bail!("unexpected response to kill_session: {other:?}"),
        }
    }

    pub fn kill_all(&self) -> Result<Vec<String>> {
        match self.request(&DaemonRequest::KillAll)? {
            DaemonResponse::Killed { killed } => Ok(killed),
            DaemonResponse::Error { error } => bail!("{error}"),
            other => bail!("unexpected response to kill_all: {other:?}"),
        }
    }

    pub fn shutdown(&self) -> Result<()> {
        match self.request(&DaemonRequest::Shutdown)? {
            DaemonResponse::Stopping => Ok(()),
            DaemonResponse::Error { error } => bail!("{error}"),
            other => bail!("unexpected response to shutdown: {other:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn unix_connect_deadline_bounds_a_full_listener_backlog() -> Result<()> {
        use std::os::fd::AsRawFd;
        use std::os::unix::net::UnixListener;
        use std::time::Instant;
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("listener.sock");
        let listener = UnixListener::bind(&path)?;
        assert_eq!(unsafe { libc::listen(listener.as_raw_fd(), 1) }, 0);
        let mut queued = Vec::new();
        let mut reached_deadline = false;
        for _ in 0..16 {
            let start = Instant::now();
            match connect_with_timeout(&path, Duration::from_millis(100)) {
                Ok(stream) => {
                    // Successful connections retain the standard blocking
                    // stream contract and cannot leak into an exec'd worker.
                    let flags = unsafe { libc::fcntl(stream.as_raw_fd(), libc::F_GETFL) };
                    let fd_flags = unsafe { libc::fcntl(stream.as_raw_fd(), libc::F_GETFD) };
                    assert_eq!(flags & libc::O_NONBLOCK, 0);
                    assert_ne!(fd_flags & libc::FD_CLOEXEC, 0);
                    queued.push(stream);
                }
                Err(error) => {
                    assert_eq!(error.kind(), io::ErrorKind::TimedOut, "{error}");
                    assert!(start.elapsed() >= Duration::from_millis(80));
                    assert!(start.elapsed() < Duration::from_millis(500));
                    reached_deadline = true;
                    break;
                }
            }
        }
        assert!(reached_deadline, "test listener backlog did not fill");
        Ok(())
    }

    #[test]
    fn relative_socket_is_resolved_before_service_installation() -> Result<()> {
        let path = Path::new("runtime/daemon.sock");
        let paths =
            DaemonPaths::resolve_with(Some(path), None, None, None, PathBuf::from("/unused"), 501)?;
        assert_eq!(paths.socket_path, env::current_dir()?.join(path));
        assert!(paths.lock_path.is_absolute());
        assert!(paths.log_path.is_absolute());
        Ok(())
    }

    #[test]
    fn socket_override_takes_precedence_over_env_and_home() -> Result<()> {
        let paths = DaemonPaths::resolve_with(
            Some(Path::new("/tmp/custom/ts.sock")),
            Some(OsString::from("/tmp/env/ts.sock")),
            Some(OsString::from("/tmp/home")),
            Some(OsString::from("/run/user/501")),
            PathBuf::from("/Users/test/.tsindex"),
            501,
        )?;
        assert_eq!(paths.socket_path, PathBuf::from("/tmp/custom/ts.sock"));
        assert_eq!(
            paths.lock_path,
            PathBuf::from("/tmp/custom/.ts.sock.tsindex/instance.lock")
        );
        assert_eq!(
            paths.state_path,
            PathBuf::from("/tmp/custom/.ts.sock.tsindex/state.json")
        );
        assert_eq!(
            paths.log_path,
            PathBuf::from("/tmp/custom/.ts.sock.tsindex/daemon.log")
        );
        Ok(())
    }

    #[test]
    fn tsindex_home_places_socket_and_sidecars_inside_home() -> Result<()> {
        let paths = DaemonPaths::resolve_with(
            None,
            None,
            Some(OsString::from("/tmp/ts-home")),
            Some(OsString::from("/run/user/501")),
            PathBuf::from("/Users/test/.tsindex"),
            501,
        )?;
        assert_eq!(
            paths.socket_path,
            PathBuf::from("/tmp/ts-home/run/daemon.sock")
        );
        assert_eq!(
            paths.lock_path,
            PathBuf::from("/tmp/ts-home/run/.daemon.sock.tsindex/instance.lock")
        );
        assert_eq!(
            paths.log_path,
            PathBuf::from("/tmp/ts-home/run/.daemon.sock.tsindex/daemon.log")
        );
        Ok(())
    }

    #[test]
    fn empty_env_vars_are_treated_as_unset() -> Result<()> {
        let paths = DaemonPaths::resolve_with(
            None,
            Some(OsString::from("")),
            Some(OsString::from("")),
            None,
            PathBuf::from("/Users/test/.tsindex"),
            501,
        )?;
        assert_eq!(
            paths.socket_path,
            PathBuf::from("/Users/test/.tsindex/run/daemon.sock")
        );
        assert_eq!(
            paths.log_path,
            PathBuf::from("/Users/test/.tsindex/run/.daemon.sock.tsindex/daemon.log")
        );
        Ok(())
    }

    #[test]
    fn overly_long_default_home_falls_back_to_short_tmp_socket() -> Result<()> {
        let long_home = PathBuf::from(format!("/Users/{}/.tsindex", "a".repeat(95)));
        let paths = DaemonPaths::resolve_with(None, None, None, None, long_home, 502)?;
        assert_eq!(
            paths.socket_path,
            PathBuf::from("/tmp/tsindex-502/daemon.sock")
        );
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_default_uses_xdg_runtime_dir_when_present() -> Result<()> {
        let paths = DaemonPaths::resolve_with(
            None,
            None,
            None,
            Some(OsString::from("/run/user/1000")),
            PathBuf::from("/home/dev/.tsindex"),
            1000,
        )?;
        assert_eq!(
            paths.socket_path,
            PathBuf::from("/run/user/1000/tsindex/daemon.sock")
        );
        Ok(())
    }

    #[test]
    fn bounded_line_reader_rejects_oversized_frames() {
        let data = format!("{}\n", "x".repeat(128));
        let mut cursor = io::Cursor::new(data.into_bytes());
        let err = read_bounded_line(&mut cursor, 64).unwrap_err();
        assert!(err.to_string().contains("exceeded maximum length"));
    }

    #[test]
    fn bounded_line_reader_preserves_remaining_stream_bytes() -> Result<()> {
        let mut cursor = io::Cursor::new(b"{\"op\":\"ping\"}\n{\"jsonrpc\":\"2.0\"}\n".to_vec());
        let first = read_bounded_line(&mut cursor, 1024)?;
        assert_eq!(first, "{\"op\":\"ping\"}\n");
        let second = read_bounded_line(&mut cursor, 1024)?;
        assert_eq!(second, "{\"jsonrpc\":\"2.0\"}\n");
        Ok(())
    }
}
