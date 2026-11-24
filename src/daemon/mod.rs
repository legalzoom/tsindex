//! Long-standing orchestrator daemon, thin MCP proxy, and OS login service
//! integration for macOS (`launchd`) and Linux (`systemd --user`).

#[cfg(unix)]
pub(crate) mod activation;
#[cfg(unix)]
mod fds;
#[cfg(unix)]
pub mod orchestrator;
#[cfg(unix)]
pub mod proxy;
#[cfg(unix)]
pub(crate) mod runtime;
#[cfg(unix)]
pub mod service;

// Preserve the existing JSON strings for UTF-8 paths while allowing Unix
// paths with arbitrary bytes to make the round trip without lossy conversion.
#[cfg(unix)]
pub(crate) mod native_path {
    use std::ffi::OsString;
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    use std::path::{Path, PathBuf};

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum WirePath {
        Text(String),
        Bytes(Vec<u8>),
    }

    impl From<WirePath> for PathBuf {
        fn from(value: WirePath) -> Self {
            match value {
                WirePath::Text(text) => text.into(),
                WirePath::Bytes(bytes) => OsString::from_vec(bytes).into(),
            }
        }
    }

    pub fn serialize<S: Serializer>(path: &Path, serializer: S) -> Result<S::Ok, S::Error> {
        if let Some(text) = path.to_str() {
            serializer.serialize_str(text)
        } else {
            serializer.serialize_bytes(path.as_os_str().as_bytes())
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<PathBuf, D::Error> {
        WirePath::deserialize(deserializer).map(Into::into)
    }

    pub mod optional {
        use super::*;

        pub fn serialize<S: Serializer>(
            path: &Option<PathBuf>,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            #[derive(Serialize)]
            struct Native<'a>(#[serde(serialize_with = "super::serialize")] &'a Path);
            path.as_deref().map(Native).serialize(serializer)
        }

        pub fn deserialize<'de, D: Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Option<PathBuf>, D::Error> {
            Option::<WirePath>::deserialize(deserializer).map(|path| path.map(Into::into))
        }
    }
}

use std::env;
use std::ffi::OsString;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};

use crate::mcp::tsindex_home;

pub const PROTOCOL_VERSION: u32 = 4;
pub const STARTUP_IO_TIMEOUT: Duration = Duration::from_secs(120);
pub const MAX_MCP_FRAME_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_CONTROL_LINE_BYTES: usize = 64 * 1024;
pub const CONTROL_IO_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const OUTCOME_WATCHER_CAPACITY_ERROR: &str =
    "maximum concurrent outcome watchers reached";
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
        // Endpoint names also go into the daemon's JSON status and state file.
        // Reject unsupported byte paths before spawning a detached process.
        if socket_path.to_str().is_none() {
            bail!(
                "daemon socket path cannot be represented safely in control metadata: expected UTF-8"
            );
        }
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

fn fallback_socket_path(default_home: &Path, uid: u32) -> PathBuf {
    let candidate = default_home.join("run").join("daemon.sock");
    if candidate.as_os_str().as_encoded_bytes().len() <= MAX_UNIX_SOCKET_PATH_BYTES {
        return candidate;
    }
    PathBuf::from(format!("/tmp/tsindex-{uid}/daemon.sock"))
}

#[cfg(any(target_os = "linux", test))]
pub(crate) fn linux_default_socket_path(
    xdg_runtime_dir: Option<&std::ffi::OsStr>,
    default_home: &Path,
    uid: u32,
) -> PathBuf {
    if let Some(runtime) = xdg_runtime_dir.filter(|value| !value.is_empty()) {
        let candidate = PathBuf::from(runtime).join("tsindex").join("daemon.sock");
        if candidate.as_os_str().as_encoded_bytes().len() <= MAX_UNIX_SOCKET_PATH_BYTES {
            return candidate;
        }
        // An oversized custom XDG_RUNTIME_DIR must stay isolated from the
        // ordinary home/tmp fallback so bare commands cannot reach a login
        // service installed at that fallback path.
        use sha2::{Digest, Sha256};
        let mut digest = Sha256::new();
        for component in Path::new(runtime).components() {
            let bytes = component.as_os_str().as_encoded_bytes();
            digest.update((bytes.len() as u64).to_le_bytes());
            digest.update(bytes);
        }
        let hash = digest.finalize();
        let tag = format!(
            "xdg-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            hash[0], hash[1], hash[2], hash[3], hash[4], hash[5], hash[6], hash[7]
        );
        let home_candidate = default_home.join("run").join(&tag).join("daemon.sock");
        if home_candidate.as_os_str().as_encoded_bytes().len() <= MAX_UNIX_SOCKET_PATH_BYTES {
            return home_candidate;
        }
        return PathBuf::from(format!("/tmp/tsindex-{uid}/{tag}/daemon.sock"));
    }
    fallback_socket_path(default_home, uid)
}

pub(crate) fn default_socket_path(
    xdg_runtime_dir: Option<OsString>,
    default_home: &Path,
    uid: u32,
) -> PathBuf {
    #[cfg(target_os = "linux")]
    {
        linux_default_socket_path(xdg_runtime_dir.as_deref(), default_home, uid)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = xdg_runtime_dir;
        fallback_socket_path(default_home, uid)
    }
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
    if !peer_uid_allowed(creds.uid, expected_uid, false) {
        bail!(
            "rejected Unix socket peer with uid {} (expected {})",
            creds.uid,
            expected_uid
        );
    }
    Ok(creds)
}

/// Authenticate an outbound connection before sending workspace metadata.
#[cfg(unix)]
pub(crate) fn verify_server_credentials(
    stream: &std::os::unix::net::UnixStream,
    paths: &DaemonPaths,
) -> Result<PeerCredentials> {
    let expected_uid = current_uid();
    validate_server_socket_path(paths, expected_uid)?;
    let creds = read_peer_credentials(stream).context("failed to inspect daemon server peer")?;
    // XNU caches the listener creator's credentials at listen time. launchd
    // creates the user-owned endpoint as root before passing it to the daemon.
    // Root is already trusted by the OS; allow that creator only on Darwin,
    // with an owned socket leaf in a verified private user directory above.
    // Accepted clients still use verify_peer_credentials with no exception.
    // https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/bsd/kern/uipc_usrreq.c#L1358-L1396
    if !peer_uid_allowed(creds.uid, expected_uid, cfg!(target_os = "macos")) {
        bail!(
            "rejected Unix socket server peer with uid {} (expected {})",
            creds.uid,
            expected_uid
        );
    }
    Ok(creds)
}

#[cfg(unix)]
fn peer_uid_allowed(peer_uid: u32, expected_uid: u32, allow_root_creator: bool) -> bool {
    peer_uid == expected_uid || (allow_root_creator && peer_uid == 0)
}

#[cfg(unix)]
fn validate_server_socket_path(paths: &DaemonPaths, expected_uid: u32) -> Result<()> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};

    validate_private_dir(paths.socket_dir()?)?;
    let metadata = fs::symlink_metadata(&paths.socket_path)
        .with_context(|| format!("failed to inspect daemon endpoint {:?}", paths.socket_path))?;
    if !metadata.file_type().is_socket() || metadata.uid() != expected_uid {
        bail!(
            "daemon endpoint {:?} must be an owned Unix socket without a symlink leaf",
            paths.socket_path
        );
    }
    Ok(())
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

#[cfg(any(
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "ios",
    target_os = "tvos",
    target_os = "watchos",
    target_os = "visionos",
    target_os = "aix",
    target_os = "nto",
    target_os = "cygwin",
))]
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

#[cfg(all(
    unix,
    not(any(
        target_os = "macos",
        target_os = "linux",
        target_os = "freebsd",
        target_os = "dragonfly",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "visionos",
        target_os = "aix",
        target_os = "nto",
        target_os = "cygwin",
    ))
))]
fn read_peer_credentials(_stream: &std::os::unix::net::UnixStream) -> Result<PeerCredentials> {
    bail!("managed daemon peer authentication is unsupported on this platform; use direct mode")
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
    #[cfg_attr(unix, serde(with = "native_path"))]
    pub root: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(unix, serde(with = "native_path::optional"))]
    pub caller_dir: Option<PathBuf>,
    #[serde(default)]
    #[cfg_attr(unix, serde(with = "native_path::optional"))]
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
    #[cfg_attr(unix, serde(with = "native_path"))]
    pub root: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(unix, serde(default, with = "native_path::optional"))]
    pub db: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub languages: Vec<String>,
    pub no_refresh: bool,
    /// Shared index owner. The worker PID below executes this session's requests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_pid: Option<u32>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_sessions: Option<usize>,
    pub total_sessions_started: u64,
    #[serde(default)]
    pub index_runtimes: usize,
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
    write_bounded_json(writer, value, MAX_CONTROL_LINE_BYTES)
}

fn write_bounded_json(writer: &mut impl Write, value: &impl Serialize, limit: usize) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() + 1 > limit {
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
/// fresh socket for each retry. Darwin can report ECONNREFUSED for the same
/// live-backlog condition. Other pending connects complete via SO_ERROR.
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
        // Resolve deferred connect errors before applying the same platform
        // classification used for immediate syscall failures.
        let error = match error.raw_os_error() {
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
                    break io::Error::from_raw_os_error(code);
                }
                let stream = UnixStream::from(fd);
                stream.set_nonblocking(false)?;
                return Ok(stream);
            },
            _ => error,
        };
        #[cfg(target_os = "macos")]
        let error =
            if error.raw_os_error() == Some(libc::ECONNREFUSED) && live_stream_endpoint(path)? {
                // XNU uses refusal for both stale vnodes and full live backlogs.
                // A datagram probe distinguishes them without adding a queued
                // stream connection. Retry the live case under the same deadline.
                io::Error::from_raw_os_error(libc::EAGAIN)
            } else {
                error
            };
        match error.raw_os_error() {
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

#[cfg(all(unix, any(target_os = "macos", test)))]
fn live_stream_endpoint(path: &Path) -> io::Result<bool> {
    use std::os::unix::net::UnixDatagram;

    // XNU checks for an absent vnode socket/PCB before checking socket type,
    // and checks type before attempting the stream backlog. EPROTOTYPE from
    // this nonblocking probe therefore proves that a stream socket is live,
    // whereas ECONNREFUSED identifies the stale vnode case. Unknown failures
    // stay errors so callers cannot misclassify them as permission to unlink.
    // https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/bsd/kern/uipc_usrreq.c#L1290-L1339
    let probe = UnixDatagram::unbound()?;
    probe.set_nonblocking(true)?;
    match probe.connect(path) {
        Err(error) if error.raw_os_error() == Some(libc::EPROTOTYPE) => Ok(true),
        Err(error)
            if matches!(
                error.raw_os_error(),
                Some(libc::ECONNREFUSED | libc::ENOENT)
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(error),
        // A datagram endpoint is live but does not implement this protocol.
        Ok(()) => Err(io::Error::from_raw_os_error(libc::EPROTOTYPE)),
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
        let stream = connect_with_timeout(&self.paths.socket_path, CONTROL_IO_TIMEOUT)
            .with_context(|| {
                format!(
                    "failed to connect to daemon at {}",
                    self.paths.socket_path.display()
                )
            })?;
        // Authenticate the service before disclosing workspace paths or commands.
        verify_server_credentials(&stream, &self.paths)?;
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
        // An earlier watch may still be delivering its final reply even after
        // the worker's admission slot was returned. Preserve the data socket
        // while that bounded control write releases its watcher permit.
        let deadline = std::time::Instant::now() + CONTROL_IO_TIMEOUT + Duration::from_millis(250);
        loop {
            match self.try_watch_session(session_id)? {
                Some(watch) => return Ok(watch),
                None if std::time::Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(25));
                }
                None => bail!("{OUTCOME_WATCHER_CAPACITY_ERROR}"),
            }
        }
    }

    #[cfg(unix)]
    fn try_watch_session(
        &self,
        session_id: &str,
    ) -> Result<Option<io::BufReader<std::os::unix::net::UnixStream>>> {
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
                // On macOS, a daemon that already sent Watching + Session from
                // recent history and closed the peer causes SO_RCVTIMEO clears
                // to return EINVAL even while the terminal record stays readable.
                if let Err(error) = reader.get_ref().set_read_timeout(None)
                    && !matches!(
                        error.kind(),
                        io::ErrorKind::InvalidInput
                            | io::ErrorKind::NotConnected
                            | io::ErrorKind::ConnectionReset
                    )
                {
                    return Err(error.into());
                }
                Ok(Some(reader))
            }
            DaemonResponse::Error { error } if error == OUTCOME_WATCHER_CAPACITY_ERROR => Ok(None),
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
    fn peer_uid_policy_keeps_root_creator_exception_out_of_client_authentication() {
        let user = 501;
        let other = 502;
        for (peer, root_creator_allowed, expected) in [
            (user, false, true),
            (user, true, true),
            (other, false, false),
            (other, true, false),
            (0, false, false),
            (0, true, true),
        ] {
            assert_eq!(
                peer_uid_allowed(peer, user, root_creator_allowed),
                expected,
                "peer={peer}, root_creator_allowed={root_creator_allowed}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn server_authentication_uses_owned_private_socket_and_client_auth_stays_same_uid() -> Result<()>
    {
        use std::os::unix::fs::PermissionsExt;
        use std::os::unix::net::{UnixListener, UnixStream};

        let dir = tempfile::tempdir_in("/tmp")?;
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
        let paths = DaemonPaths::resolve(Some(&dir.path().join("d.sock")))?;
        let listener = UnixListener::bind(&paths.socket_path)?;
        let client = UnixStream::connect(&paths.socket_path)?;
        assert_eq!(
            verify_server_credentials(&client, &paths)?.uid,
            current_uid()
        );
        let (accepted, _) = listener.accept()?;
        assert_eq!(verify_peer_credentials(&accepted)?.uid, current_uid());
        assert!(validate_server_socket_path(&paths, current_uid().wrapping_add(1)).is_err());

        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o755))?;
        assert!(verify_server_credentials(&client, &paths).is_err());
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
        assert!(verify_server_credentials(&client, &paths).is_ok());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn server_authentication_path_rejects_symlink_parents_leaves_and_non_socket_nodes() -> Result<()>
    {
        use std::os::unix::fs::{PermissionsExt, symlink};
        use std::os::unix::net::UnixListener;

        let dir = tempfile::tempdir_in("/tmp")?;
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
        let private = dir.path().join("private");
        fs::create_dir(&private)?;
        fs::set_permissions(&private, fs::Permissions::from_mode(0o700))?;
        let socket = private.join("d.sock");
        let paths = DaemonPaths::resolve(Some(&socket))?;
        assert!(validate_server_socket_path(&paths, current_uid()).is_err());
        let listener = UnixListener::bind(&socket)?;
        assert!(validate_server_socket_path(&paths, current_uid()).is_ok());

        let parent_alias = dir.path().join("private-link");
        symlink(&private, &parent_alias)?;
        let parent_paths = DaemonPaths::resolve(Some(&parent_alias.join("d.sock")))?;
        assert!(validate_server_socket_path(&parent_paths, current_uid()).is_err());
        drop(listener);
        let original = private.join("original.sock");
        fs::rename(&socket, &original)?;
        symlink(&original, &socket)?;
        assert!(validate_server_socket_path(&paths, current_uid()).is_err());
        assert!(fs::symlink_metadata(&socket)?.file_type().is_symlink());
        fs::remove_file(&socket)?;
        fs::write(&socket, "preserve regular file")?;
        assert!(validate_server_socket_path(&paths, current_uid()).is_err());
        assert_eq!(fs::read_to_string(&socket)?, "preserve regular file");
        assert!(original.exists());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn unix_connect_deadline_bounds_a_full_listener_backlog() -> Result<()> {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        use std::os::unix::net::UnixListener;
        use std::time::Instant;
        let dir = tempfile::tempdir()?;
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
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
        let before = fs::symlink_metadata(&path)?;
        let paths = DaemonPaths::resolve(Some(&path))?;
        let refusal = crate::daemon::service::check_socket(&paths).unwrap_err();
        assert_eq!(
            refusal.downcast_ref::<io::Error>().map(io::Error::kind),
            Some(io::ErrorKind::TimedOut)
        );
        let after = fs::symlink_metadata(&path)?;
        assert_eq!(
            (before.dev(), before.ino()),
            (after.dev(), after.ino()),
            "preflight replaced a saturated live endpoint"
        );
        assert!(!queued.is_empty());
        let (accepted, _) = listener.accept()?;
        drop(accepted);
        let recovered = connect_with_timeout(&path, Duration::from_millis(100))?;
        assert_eq!(recovered.peer_addr()?.as_pathname(), Some(path.as_path()));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn stream_liveness_probe_distinguishes_stale_missing_and_wrong_type_endpoints() -> Result<()> {
        use std::os::unix::fs::{FileTypeExt, PermissionsExt};
        use std::os::unix::net::{UnixDatagram, UnixListener};
        use std::time::Instant;
        let dir = tempfile::tempdir()?;
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
        let path = dir.path().join("stream.sock");
        let listener = UnixListener::bind(&path)?;
        assert!(live_stream_endpoint(&path)?);
        drop(listener);
        assert!(fs::symlink_metadata(&path)?.file_type().is_socket());
        // Other unit tests can fork while this listener is open. Its inherited
        // copy remains live until that child reaches exec.
        let deadline = Instant::now() + Duration::from_secs(2);
        while live_stream_endpoint(&path)? {
            anyhow::ensure!(
                Instant::now() < deadline,
                "listener remained live after close"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        let start = Instant::now();
        let error = connect_with_timeout(&path, Duration::from_secs(2)).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused);
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "a stale vnode consumed the live-listener retry deadline"
        );
        crate::daemon::service::check_socket(&DaemonPaths::resolve(Some(&path))?)?;
        assert!(fs::symlink_metadata(&path)?.file_type().is_socket());

        let missing = dir.path().join("missing.sock");
        assert!(!live_stream_endpoint(&missing)?);
        let error = connect_with_timeout(&missing, Duration::from_secs(2)).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        let datagram_path = dir.path().join("datagram.sock");
        let _datagram = UnixDatagram::bind(&datagram_path)?;
        assert_eq!(
            live_stream_endpoint(&datagram_path)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EPROTOTYPE)
        );
        let file = dir.path().join("regular-file");
        fs::write(&file, "preserve regular file")?;
        #[cfg(target_os = "macos")]
        assert_eq!(
            live_stream_endpoint(&file).unwrap_err().raw_os_error(),
            Some(libc::ENOTSOCK)
        );
        assert!(crate::daemon::service::check_socket(&DaemonPaths::resolve(Some(&file))?).is_err());
        assert_eq!(fs::read_to_string(file)?, "preserve regular file");
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
    fn linux_oversized_custom_xdg_runtime_dir_uses_distinct_fallback() {
        let uid = 1000;
        let home = Path::new("/home/dev/.tsindex");
        let ordinary_fallback = PathBuf::from("/home/dev/.tsindex/run/daemon.sock");
        let long_a = OsString::from(format!("/tmp/{}", "a".repeat(95)));
        let long_a_slash = OsString::from(format!("/tmp/{}/", "a".repeat(95)));
        let long_b = OsString::from(format!("/tmp/{}", "b".repeat(95)));

        assert_eq!(
            linux_default_socket_path(None, home, uid),
            ordinary_fallback
        );
        assert_eq!(
            linux_default_socket_path(Some(std::ffi::OsStr::new("/run/user/1000")), home, uid),
            PathBuf::from("/run/user/1000/tsindex/daemon.sock")
        );

        let fallback_a = linux_default_socket_path(Some(long_a.as_os_str()), home, uid);
        let fallback_a_slash = linux_default_socket_path(Some(long_a_slash.as_os_str()), home, uid);
        let fallback_b = linux_default_socket_path(Some(long_b.as_os_str()), home, uid);
        assert_eq!(fallback_a, fallback_a_slash);
        assert_ne!(fallback_a, ordinary_fallback);
        assert_ne!(fallback_b, ordinary_fallback);
        assert_ne!(fallback_a, fallback_b);
        assert!(fallback_a.as_os_str().as_encoded_bytes().len() <= MAX_UNIX_SOCKET_PATH_BYTES);
        assert!(fallback_b.as_os_str().as_encoded_bytes().len() <= MAX_UNIX_SOCKET_PATH_BYTES);
        assert!(fallback_a.starts_with(home.join("run")));

        // Even when $HOME itself exceeds the socket limit, oversized custom
        // XDG_RUNTIME_DIR stays distinct from /tmp/tsindex-<uid>/daemon.sock.
        let long_home = PathBuf::from(format!("/home/{}/.tsindex", "h".repeat(90)));
        let tmp_ordinary = PathBuf::from("/tmp/tsindex-1000/daemon.sock");
        assert_eq!(
            linux_default_socket_path(None, &long_home, uid),
            tmp_ordinary
        );
        let tmp_custom = linux_default_socket_path(Some(long_a.as_os_str()), &long_home, uid);
        assert_ne!(tmp_custom, tmp_ordinary);
        assert!(tmp_custom.starts_with("/tmp/tsindex-1000"));
        assert!(tmp_custom.as_os_str().as_encoded_bytes().len() <= MAX_UNIX_SOCKET_PATH_BYTES);
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

    #[cfg(unix)]
    #[test]
    fn watch_session_preserves_terminal_record_after_peer_closes() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;
        use std::os::unix::net::UnixListener;

        let dir = tempfile::tempdir_in("/tmp")?;
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
        let socket = dir.path().join("daemon.sock");
        let listener = UnixListener::bind(&socket)?;
        fs::set_permissions(&socket, fs::Permissions::from_mode(0o600))?;

        let server = std::thread::spawn(move || -> Result<()> {
            let (mut stream, _) = listener.accept()?;
            let mut reader = io::BufReader::new(stream.try_clone()?);
            let _ = read_bounded_line(&mut reader, MAX_CONTROL_LINE_BYTES)?;
            write_json_line(&mut stream, &DaemonResponse::Watching)?;
            write_json_line(
                &mut stream,
                &DaemonResponse::Session {
                    session: SessionInfo {
                        session_id: "s-1".to_string(),
                        state: SessionState::Exited,
                        root: PathBuf::from("/tmp/repo"),
                        db: None,
                        languages: vec![],
                        no_refresh: false,
                        client_pid: std::process::id(),
                        owner_pid: None,
                        worker_pid: 999,
                        runtime_pid: None,
                        harness_name: None,
                        harness_version: None,
                        started_at: 0,
                        last_activity_at: 0,
                        ended_at: Some(1),
                        requests_received: 0,
                        responses_sent: 0,
                        tool_calls: 0,
                        exit_reason: Some("killed_by_user".to_string()),
                        recent_stderr: vec![],
                    },
                },
            )?;
            // Close immediately so the client observes a closed peer while
            // clearing SO_RCVTIMEO (which returns EINVAL on Darwin).
            drop(reader);
            drop(stream);
            Ok(())
        });

        let paths = DaemonPaths::resolve(Some(&socket))?;
        let mut reader = DaemonClient::new(paths)
            .try_watch_session("s-1")?
            .context("expected watching reader")?;
        server.join().unwrap()?;
        let line = read_bounded_line(&mut reader, MAX_CONTROL_LINE_BYTES)?;
        match serde_json::from_str::<DaemonResponse>(&line)? {
            DaemonResponse::Session { session } => {
                assert_eq!(session.session_id, "s-1");
                assert_eq!(session.state, SessionState::Exited);
                assert_eq!(session.exit_reason.as_deref(), Some("killed_by_user"));
            }
            other => bail!("unexpected terminal outcome: {other:?}"),
        }
        Ok(())
    }
}
