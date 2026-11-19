//! Long-standing orchestrator daemon, thin MCP proxy, and OS login service
//! integration for macOS (`launchd`) and Linux (`systemd --user`).

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

pub const PROTOCOL_VERSION: u32 = 1;
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
        let custom_location =
            socket_override.is_some() || env_socket.is_some() || env_home.is_some();
        let socket_path = if let Some(path) = socket_override {
            path.to_path_buf()
        } else if let Some(path) = env_socket {
            PathBuf::from(path)
        } else if let Some(home) = env_home {
            PathBuf::from(home).join("daemon.sock")
        } else {
            default_socket_path(xdg_runtime_dir, &default_home, uid)
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
        let stem = socket_path
            .file_stem()
            .and_then(|s| s.to_str())
            .filter(|s| !s.is_empty())
            .unwrap_or("daemon");
        let lock_path = dir.join(format!("{stem}.lock"));
        let state_path = dir.join(format!("{stem}.json"));
        let log_path = if custom_location {
            dir.join(format!("{stem}.log"))
        } else {
            default_home.join("daemon.log")
        };

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
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};

    if !dir.exists() {
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true).mode(0o700);
        builder
            .create(dir)
            .with_context(|| format!("failed to create directory {}", dir.display()))?;
    }
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
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("failed to restrict permissions on {}", dir.display()))?;
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
    },
    Killed {
        killed: Vec<String>,
    },
    Stopping,
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
    serde_json::to_writer(&mut *writer, value)?;
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
        std::os::unix::net::UnixStream::connect(&self.paths.socket_path).map_err(|error| {
            anyhow!(
                "{} ({error})",
                daemon_not_running_message(&self.paths.socket_path)
            )
        })
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
        match self.request(&DaemonRequest::ListSessions { include_recent })? {
            DaemonResponse::Sessions { active, recent } => Ok((active, recent)),
            DaemonResponse::Error { error } => bail!("{error}"),
            other => bail!("unexpected response to list_sessions: {other:?}"),
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
        assert_eq!(paths.lock_path, PathBuf::from("/tmp/custom/ts.lock"));
        assert_eq!(paths.state_path, PathBuf::from("/tmp/custom/ts.json"));
        assert_eq!(paths.log_path, PathBuf::from("/tmp/custom/ts.log"));
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
        assert_eq!(paths.socket_path, PathBuf::from("/tmp/ts-home/daemon.sock"));
        assert_eq!(paths.lock_path, PathBuf::from("/tmp/ts-home/daemon.lock"));
        assert_eq!(paths.log_path, PathBuf::from("/tmp/ts-home/daemon.log"));
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
            PathBuf::from("/Users/test/.tsindex/daemon.log")
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
