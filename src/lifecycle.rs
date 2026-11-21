//! Session ownership for long-running servers and watchers.

use std::io::{self, Write};
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, bail};

const CHECK_INTERVAL: Duration = Duration::from_secs(5);
static HANDLER_STARTED: Mutex<bool> = Mutex::new(false);

#[derive(Debug)]
struct SessionLifecycle {
    session_pid: libc::pid_t,
    tsindex_pid: u32,
    started_at: Option<(u64, u64)>,
    track_parent: bool,
}

impl SessionLifecycle {
    fn capture(session_pid: Option<u32>) -> Result<Option<Self>> {
        Self::capture_with(
            session_pid,
            unsafe { libc::getppid() },
            std::process::id(),
            process_start_time,
        )
    }

    fn capture_with(
        session_pid: Option<u32>,
        parent: libc::pid_t,
        tsindex_pid: u32,
        inspect: impl FnOnce(libc::pid_t) -> io::Result<Option<(u64, u64)>>,
    ) -> Result<Option<Self>> {
        // Preserve init/launchd and container launches. Explicit ownership
        // requires inspection; guessing ownership after reparenting is not safe.
        if session_pid.is_none() && parent <= 1 {
            return Ok(None);
        }
        let pid = session_pid.unwrap_or(parent as u32);
        if pid == 0 || pid > i32::MAX as u32 || pid == tsindex_pid {
            bail!(
                "session PID must identify another process (1..={})",
                i32::MAX
            );
        }
        let started_at = if session_pid.is_none() {
            // Cross-user permissions or hidden /proc entries must not break
            // default launches. Fall back to the original getppid-only check.
            inspect(pid as libc::pid_t).ok().flatten()
        } else {
            Some(
                inspect(pid as libc::pid_t)
                    .with_context(|| format!("cannot inspect session PID {pid}"))?
                    .with_context(|| format!("session PID {pid} has already exited"))?,
            )
        };
        Ok(Some(Self {
            session_pid: pid as libc::pid_t,
            tsindex_pid,
            started_at,
            track_parent: session_pid.is_none(),
        }))
    }

    fn has_ended(&self) -> bool {
        self.has_ended_with(unsafe { libc::getppid() }, process_start_time)
    }

    fn has_ended_with(
        &self,
        parent: libc::pid_t,
        inspect: impl FnOnce(libc::pid_t) -> io::Result<Option<(u64, u64)>>,
    ) -> bool {
        if self.track_parent && parent != self.session_pid {
            return true;
        }
        let Some(started_at) = self.started_at else {
            return false;
        };
        // Permission or transient inspection errors are not evidence of death.
        // A missing/zombie process or a changed start time is conclusive.
        matches!(inspect(self.session_pid), Ok(current) if current != Some(started_at))
    }

    fn shutdown(&self) -> ! {
        // Logging to a disconnected client's stderr must not panic before exit.
        let _ = writeln!(
            io::stderr(),
            "tsindex: session PID {} exited; shutting down tsindex PID {}",
            self.session_pid,
            self.tsindex_pid
        );
        // Terminate this process only, including its watcher and request threads.
        // There is no external signal target that could be replaced by PID reuse.
        std::process::exit(0);
    }
}

/// Process-identity handle monitored by the orchestrator's centralized
/// session sweeper. Unlike [`SessionLifecycle`], this does not rely on
/// `getppid()` because the orchestrator is not a child of the harness.
#[derive(Debug, Clone)]
pub(crate) struct ProcessOwner {
    pid: libc::pid_t,
    started_at: (u64, u64),
}

impl ProcessOwner {
    pub(crate) fn for_peer(
        peer_pid: u32,
        requested: Option<u32>,
        explicit: bool,
    ) -> Result<Option<Self>> {
        let excluded = [peer_pid, std::process::id()];
        #[cfg(target_os = "linux")]
        let current_view = procfs_uses_daemon_pid_namespace().unwrap_or(false);
        if explicit {
            #[cfg(target_os = "linux")]
            {
                // Supplied numbers belong to the proxy's PID namespace. The
                // same number can identify a different process in the daemon.
                const GUIDANCE: &str = "managed --session-pid requires a verifiably shared PID namespace; use direct `tsindex serve --mcp --session-pid PID`";
                if !current_view || !peer_shares_pid_namespace(peer_pid).context(GUIDANCE)? {
                    bail!("{GUIDANCE}");
                }
            }
            return Self::capture_explicit_with(requested, &excluded, process_start_time).map(Some);
        }
        #[cfg(target_os = "linux")]
        if !current_view {
            return Ok(None);
        }
        Ok(Self::infer_parent_with(peer_pid, &excluded, process_info))
    }

    fn infer_parent_with(
        peer_pid: u32,
        excluded: &[u32],
        mut inspect: impl FnMut(libc::pid_t) -> io::Result<Option<ProcessInfo>>,
    ) -> Option<Self> {
        // SO_PEERCRED/LOCAL_PEERPID identifies the proxy in the daemon's PID
        // namespace. Resolve its parent there, including for nested harnesses.
        let peer_pid = libc::pid_t::try_from(peer_pid).ok()?;
        let peer = inspect(peer_pid).ok().flatten()?;
        if peer.parent <= 1 || excluded.contains(&peer.parent) {
            return None;
        }
        let pid = peer.parent.try_into().ok()?;
        let parent = inspect(pid).ok().flatten()?;
        // Reparenting or PID reuse between lookups invalidates this inference.
        // On any uncertainty the proxy connection still owns the session.
        if parent.started_at > peer.started_at || inspect(peer_pid).ok().flatten() != Some(peer) {
            return None;
        }
        Some(Self {
            pid,
            started_at: parent.started_at,
        })
    }

    /// A daemon may disappear after answering a control ping. Preserve
    /// inspection errors while treating an already-ended process as absent.
    pub(crate) fn capture_if_running(pid: u32, excluded_pids: &[u32]) -> Result<Option<Self>> {
        Self::capture_if_running_with(pid, excluded_pids, process_start_time)
    }

    fn capture_if_running_with(
        pid: u32,
        excluded_pids: &[u32],
        inspect: impl FnOnce(libc::pid_t) -> io::Result<Option<(u64, u64)>>,
    ) -> Result<Option<Self>> {
        let pid = Self::validate_pid(pid, excluded_pids)?;
        Ok(inspect(pid)
            .with_context(|| format!("cannot inspect process PID {pid}"))?
            .map(|started_at| Self { pid, started_at }))
    }

    fn validate_pid(pid: u32, excluded_pids: &[u32]) -> Result<libc::pid_t> {
        if pid == 0 || pid > i32::MAX as u32 || excluded_pids.contains(&pid) {
            bail!(
                "session PID must identify another process (1..={})",
                i32::MAX
            );
        }
        Ok(pid as libc::pid_t)
    }

    fn capture_explicit_with(
        owner_pid: Option<u32>,
        excluded_pids: &[u32],
        inspect: impl FnOnce(libc::pid_t) -> io::Result<Option<(u64, u64)>>,
    ) -> Result<Self> {
        let pid = owner_pid.context("--session-pid requires an owning process ID")?;
        Self::capture_if_running_with(pid, excluded_pids, inspect)?
            .with_context(|| format!("session PID {pid} has already exited"))
    }

    pub(crate) fn pid(&self) -> u32 {
        self.pid as u32
    }

    pub(crate) fn has_ended(&self) -> bool {
        self.has_ended_with(process_start_time)
    }

    fn has_ended_with(
        &self,
        inspect: impl FnOnce(libc::pid_t) -> io::Result<Option<(u64, u64)>>,
    ) -> bool {
        // Permission or transient inspection errors are not evidence of death.
        // A missing/zombie process or a changed start time is conclusive.
        matches!(inspect(self.pid), Ok(current) if current != Some(self.started_at))
    }
}

/// Start once, before index initialization. An explicit owner takes precedence
/// over a later default start from the MCP transport entry point.
pub(crate) fn spawn_session_handler(session_pid: Option<u32>) -> Result<()> {
    let mut started = HANDLER_STARTED.lock().unwrap();
    if *started {
        return Ok(());
    }
    if let Some(session) = SessionLifecycle::capture(session_pid)? {
        thread::Builder::new()
            .name("tsindex-session".into())
            .spawn(move || {
                let _ = writeln!(
                    io::stderr(),
                    "tsindex: tracking session PID {} for tsindex PID {}; checking every {}s",
                    session.session_pid,
                    session.tsindex_pid,
                    CHECK_INTERVAL.as_secs()
                );
                loop {
                    if session.has_ended() {
                        session.shutdown();
                    }
                    thread::sleep(CHECK_INTERVAL);
                }
            })
            .context("failed to start session lifecycle handler")?;
    }
    *started = true;
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ProcessInfo {
    parent: u32,
    started_at: (u64, u64),
}

fn process_start_time(pid: libc::pid_t) -> io::Result<Option<(u64, u64)>> {
    Ok(process_info(pid)?.map(|info| info.started_at))
}

#[cfg(target_os = "linux")]
fn procfs_uses_daemon_pid_namespace() -> io::Result<bool> {
    // NStgid starts in the namespace that mounted procfs and ends in the
    // process's own namespace. An inherited host mount cannot safely resolve
    // the kernel's peer PID for a daemon in a nested namespace.
    // https://man7.org/linux/man-pages/man5/proc_pid_status.5.html
    let status = std::fs::read("/proc/self/status")?;
    let entry = status
        .split(|&b| b == b'\n')
        .find_map(|line| line.strip_prefix(b"NStgid:"))
        .ok_or_else(|| io::Error::other("procfs does not report PID namespaces"))?;
    let entry = std::str::from_utf8(entry).map_err(io::Error::other)?;
    let mut pids = entry.split_ascii_whitespace();
    Ok(
        pids.next().and_then(|pid| pid.parse::<u32>().ok()) == Some(std::process::id())
            && pids.next().is_none(),
    )
}

#[cfg(target_os = "linux")]
fn peer_shares_pid_namespace(peer_pid: u32) -> io::Result<bool> {
    use std::os::unix::fs::MetadataExt;
    let ours = std::fs::metadata("/proc/self/ns/pid")?;
    let peer = std::fs::metadata(format!("/proc/{peer_pid}/ns/pid"))?;
    Ok(ours.dev() == peer.dev() && ours.ino() == peer.ino())
}

#[cfg(target_os = "macos")]
fn process_info(pid: libc::pid_t) -> io::Result<Option<ProcessInfo>> {
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::uninit();
    let size = std::mem::size_of::<libc::proc_bsdinfo>() as i32;
    let read = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            size,
        )
    };
    if read <= 0 {
        let error = io::Error::last_os_error();
        return if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(None)
        } else {
            Err(error)
        };
    }
    if read != size {
        return Err(io::Error::other("incomplete process information"));
    }
    let info = unsafe { info.assume_init() };
    Ok((info.pbi_status != libc::SZOMB).then_some(ProcessInfo {
        parent: info.pbi_ppid,
        started_at: (info.pbi_start_tvsec, info.pbi_start_tvusec),
    }))
}

#[cfg(target_os = "linux")]
fn process_info(pid: libc::pid_t) -> io::Result<Option<ProcessInfo>> {
    match std::fs::read(format!("/proc/{pid}/stat")) {
        Ok(stat) => parse_proc_stat(&stat),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(any(target_os = "linux", test))]
fn parse_proc_stat(stat: &[u8]) -> io::Result<Option<ProcessInfo>> {
    // comm (field 2) contains arbitrary bytes; Linux can even truncate a UTF-8
    // name mid-character. Decode only the fields after its final ') '.
    let end = stat
        .windows(2)
        .rposition(|pair| pair == b") ")
        .ok_or_else(|| io::Error::other("invalid process stat"))?;
    let tail = std::str::from_utf8(&stat[end + 2..])
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let mut fields = tail.split_whitespace();
    let state = fields.next();
    let parent = fields
        .next()
        .and_then(|value| value.parse::<u32>().ok())
        .ok_or_else(|| io::Error::other("missing process parent"))?;
    // After ppid (4), num_threads is field 20 and starttime is field 22.
    let threads = fields
        .nth(15)
        .and_then(|field| field.parse::<u64>().ok())
        .ok_or_else(|| io::Error::other("missing process thread count"))?;
    let started_at = fields
        .nth(1)
        .and_then(|field| field.parse::<u64>().ok())
        .ok_or_else(|| io::Error::other("missing process start time"))?;
    // A zombie leader still represents a live process when workers remain.
    // The count includes the leader; only an entirely exited group ends ownership.
    let alive = !matches!(state, Some("Z" | "X" | "x")) || threads > 1;
    Ok(alive.then_some(ProcessInfo {
        parent,
        started_at: (started_at, 0),
    }))
}

// Keep the existing Unix support outside the macOS/Linux release targets.
// These platforms can check liveness but do not have a start-time fingerprint.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn process_info(pid: libc::pid_t) -> io::Result<Option<ProcessInfo>> {
    if unsafe { libc::kill(pid, 0) } == 0 {
        return Ok(Some(ProcessInfo {
            parent: 0,
            started_at: (0, 0),
        }));
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(None)
    } else {
        Err(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn captures_both_pids_and_parent_identity() -> Result<()> {
        let session = SessionLifecycle::capture_with(None, 42, 99, |pid| {
            assert_eq!(pid, 42);
            Ok(Some((123, 456)))
        })?
        .unwrap();
        assert_eq!(session.session_pid, 42);
        assert_eq!(session.tsindex_pid, 99);
        assert_eq!(session.started_at, Some((123, 456)));
        assert!(session.track_parent);
        Ok(())
    }

    #[test]
    fn init_launch_does_not_infer_an_owner() -> Result<()> {
        for parent in [0, 1] {
            assert!(
                SessionLifecycle::capture_with(None, parent, 99, |_| {
                    panic!("init must not be inspected without explicit ownership")
                })?
                .is_none()
            );
        }
        Ok(())
    }

    #[test]
    fn explicit_owner_overrides_parent_when_inspectable() -> Result<()> {
        for owner in [1, 7, i32::MAX as u32] {
            let session = SessionLifecycle::capture_with(Some(owner), 42, 99, |pid| {
                assert_eq!(pid, owner as i32);
                Ok(Some((123, 456)))
            })?
            .unwrap();
            assert_eq!(session.session_pid, owner as i32);
            assert!(!session.track_parent);
        }
        Ok(())
    }

    #[test]
    fn invalid_and_self_owned_sessions_are_rejected_before_inspection() {
        for pid in [0, 99, i32::MAX as u32 + 1, u32::MAX] {
            assert!(
                SessionLifecycle::capture_with(Some(pid), 42, 99, |_| {
                    panic!("invalid PID must not reach the process API")
                })
                .is_err()
            );
        }
    }

    #[test]
    fn uninspectable_inferred_parent_uses_parent_only_tracking() -> Result<()> {
        for inspection in [
            Ok(None),
            Err(io::Error::from_raw_os_error(libc::EPERM)),
            Err(io::Error::from_raw_os_error(libc::EACCES)),
            Err(io::Error::from_raw_os_error(libc::EINTR)),
            Err(io::Error::from_raw_os_error(libc::EIO)),
        ] {
            let session = SessionLifecycle::capture_with(None, 42, 99, |_| inspection)?.unwrap();
            assert!(!session.has_ended_with(42, |_| {
                panic!("parent-only tracking must not inspect the owner")
            }));
            for parent in [1, 7] {
                assert!(session.has_ended_with(parent, |_| {
                    panic!("reparenting must end a parent-only session without inspection")
                }));
            }
        }
        Ok(())
    }

    #[test]
    fn explicit_owner_requires_successful_inspection() {
        for owner in [1, 42] {
            let missing =
                SessionLifecycle::capture_with(Some(owner), 42, 99, |_| Ok(None)).unwrap_err();
            assert!(
                missing
                    .to_string()
                    .contains(&format!("session PID {owner} has already exited"))
            );
            for error in [libc::EPERM, libc::EACCES, libc::EINTR, libc::EIO] {
                let denied = SessionLifecycle::capture_with(Some(owner), 42, 99, |_| {
                    Err(io::Error::from_raw_os_error(error))
                })
                .unwrap_err();
                assert!(
                    denied
                        .to_string()
                        .contains(&format!("cannot inspect session PID {owner}"))
                );
                assert_eq!(
                    denied.downcast_ref::<io::Error>().unwrap().raw_os_error(),
                    Some(error)
                );
            }
        }
    }

    fn session(track_parent: bool) -> SessionLifecycle {
        SessionLifecycle {
            session_pid: 42,
            tsindex_pid: 99,
            started_at: Some((123, 456)),
            track_parent,
        }
    }

    #[test]
    fn living_owner_keeps_server_alive() {
        for track_parent in [false, true] {
            assert!(!session(track_parent).has_ended_with(42, |pid| {
                assert_eq!(pid, 42);
                Ok(Some((123, 456)))
            }));
        }
    }

    #[test]
    fn missing_or_reused_owner_ends_session() {
        for current in [None, Some((124, 456)), Some((123, 457))] {
            for track_parent in [false, true] {
                assert!(session(track_parent).has_ended_with(42, |_| Ok(current)));
            }
        }
    }

    #[test]
    fn parent_change_ends_inferred_session_even_when_inspection_would_fail() {
        for parent in [0, 1, 7] {
            assert!(session(true).has_ended_with(parent, |_| {
                panic!("reparenting is conclusive without another process lookup")
            }));
        }
    }

    #[test]
    fn explicit_owner_survives_wrapper_exit() {
        for parent in [0, 1, 7] {
            assert!(!session(false).has_ended_with(parent, |_| Ok(Some((123, 456)))));
        }
    }

    #[test]
    fn inspection_failure_does_not_end_a_live_session() {
        for error in [libc::EPERM, libc::EINTR, libc::EIO] {
            for track_parent in [false, true] {
                assert!(
                    !session(track_parent)
                        .has_ended_with(42, |_| { Err(io::Error::from_raw_os_error(error)) })
                );
            }
        }
    }

    #[test]
    fn process_owner_rejects_excluded_or_missing_explicit_pids() {
        assert!(ProcessOwner::capture_explicit_with(Some(0), &[], |_| Ok(Some((1, 1)))).is_err());
        assert!(
            ProcessOwner::capture_explicit_with(Some(42), &[42], |_| Ok(Some((1, 1)))).is_err()
        );
        assert!(ProcessOwner::capture_explicit_with(Some(99), &[], |_| Ok(None)).is_err());
        assert!(ProcessOwner::capture_explicit_with(None, &[], |_| Ok(Some((1, 1)))).is_err());
    }

    #[test]
    fn process_owner_control_capture_treats_exited_process_as_absent() -> Result<()> {
        assert!(ProcessOwner::capture_if_running_with(99, &[], |_| Ok(None))?.is_none());
        assert!(ProcessOwner::capture_explicit_with(Some(99), &[], |_| Ok(None)).is_err());
        Ok(())
    }

    #[test]
    fn process_owner_control_capture_preserves_inspection_errors_and_pid_validation() {
        let error = ProcessOwner::capture_if_running_with(99, &[], |_| {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "restricted process inspection",
            ))
        })
        .unwrap_err();
        assert_eq!(
            error.downcast_ref::<io::Error>().unwrap().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert!(ProcessOwner::capture_if_running_with(0, &[], |_| Ok(None)).is_err());
        assert!(ProcessOwner::capture_if_running_with(99, &[99], |_| Ok(None)).is_err());
    }

    #[test]
    fn inferred_parent_uses_verified_peer_and_socket_fallback() {
        let peer = ProcessInfo {
            parent: 50,
            started_at: (10, 30),
        };
        let parent = ProcessInfo {
            parent: 1,
            started_at: (10, 20),
        };
        let owner = ProcessOwner::infer_parent_with(71, &[71, 99], |pid| match pid {
            71 => Ok(Some(peer)),
            50 => Ok(Some(parent)),
            _ => panic!("unexpected PID {pid}"),
        })
        .expect("inspectable inferred parent");
        assert_eq!(owner.pid(), 50);
        assert!(!owner.has_ended_with(|_| Ok(Some((10, 20)))));
        assert!(owner.has_ended_with(|_| Ok(Some((10, 21)))));
        assert!(owner.has_ended_with(|_| Ok(None)));
        assert!(!owner.has_ended_with(|_| Err(io::Error::from_raw_os_error(libc::EPERM))));

        for parent_pid in [0, 1, 71, 99] {
            assert!(
                ProcessOwner::infer_parent_with(71, &[71, 99], |pid| {
                    assert_eq!(pid, 71);
                    Ok(Some(ProcessInfo {
                        parent: parent_pid,
                        ..peer
                    }))
                })
                .is_none()
            );
        }
        for parent_info in [
            Ok(None),
            Err(io::Error::from_raw_os_error(libc::EPERM)),
            Ok(Some(ProcessInfo {
                started_at: (11, 0),
                ..parent
            })),
        ] {
            let mut parent_info = Some(parent_info);
            assert!(
                ProcessOwner::infer_parent_with(71, &[], |pid| match pid {
                    71 => Ok(Some(peer)),
                    50 => parent_info.take().unwrap(),
                    _ => unreachable!(),
                })
                .is_none()
            );
        }
        let mut passes = 0;
        assert!(
            ProcessOwner::infer_parent_with(71, &[], |pid| {
                passes += 1;
                Ok(Some(match (pid, passes) {
                    (71, 1) => peer,
                    (50, 2) => parent,
                    (71, 3) => ProcessInfo { parent: 1, ..peer },
                    _ => unreachable!(),
                }))
            })
            .is_none()
        );
    }

    #[test]
    fn native_process_identity_is_stable() -> Result<()> {
        let pid = std::process::id() as libc::pid_t;
        let first = process_start_time(pid)?.expect("test process is alive");
        assert_eq!(process_start_time(pid)?, Some(first));
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert_eq!(
            process_info(pid)?.unwrap().parent,
            unsafe { libc::getppid() } as u32
        );
        Ok(())
    }

    #[test]
    fn linux_stat_handles_process_names_and_zombies() -> Result<()> {
        fn stat(state: &str, threads: impl std::fmt::Display) -> String {
            format!(
                "123 (a name ) with parens) {state} {}{threads} 0 98765 0 0",
                "0 ".repeat(16)
            )
        }
        assert_eq!(
            parse_proc_stat(stat("S", 1).as_bytes())?,
            Some(ProcessInfo {
                parent: 0,
                started_at: (98765, 0)
            })
        );
        for state in ["Z", "X", "x"] {
            assert_eq!(parse_proc_stat(stat(state, 1).as_bytes())?, None);
            assert_eq!(
                parse_proc_stat(stat(state, 2).as_bytes())?,
                Some(ProcessInfo {
                    parent: 0,
                    started_at: (98765, 0)
                })
            );
        }
        let mut non_utf8 = stat("S", 1).into_bytes();
        non_utf8[5] = 0xff;
        assert_eq!(
            parse_proc_stat(&non_utf8)?,
            Some(ProcessInfo {
                parent: 0,
                started_at: (98765, 0)
            })
        );
        assert!(parse_proc_stat(b"malformed").is_err());
        assert!(parse_proc_stat(b"123 (name) S 0").is_err());
        for started_at in ["-1", "18446744073709551616", "unknown"] {
            let invalid = stat("S", 1).replace("98765", started_at);
            assert!(parse_proc_stat(invalid.as_bytes()).is_err());
        }
        for threads in ["-1", "18446744073709551616", "unknown"] {
            let invalid = stat("Z", threads);
            assert!(parse_proc_stat(invalid.as_bytes()).is_err());
        }
        Ok(())
    }
}
