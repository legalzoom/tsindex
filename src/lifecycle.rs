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
    started_at: (u64, u64),
    track_parent: bool,
}

impl SessionLifecycle {
    fn capture(session_pid: Option<u32>) -> Result<Option<Self>> {
        let parent = unsafe { libc::getppid() };
        // Preserve init/launchd and container launches. An explicit PID 1 is
        // still a valid owner; guessing ownership after reparenting is not.
        if session_pid.is_none() && parent <= 1 {
            return Ok(None);
        }
        let pid = session_pid.unwrap_or(parent as u32);
        if pid == 0 || pid > i32::MAX as u32 || pid == std::process::id() {
            bail!(
                "session PID must identify another process (1..={})",
                i32::MAX
            );
        }
        let started_at = process_start_time(pid as libc::pid_t)
            .with_context(|| format!("cannot inspect session PID {pid}"))?
            .with_context(|| format!("session PID {pid} has already exited"))?;
        Ok(Some(Self {
            session_pid: pid as libc::pid_t,
            tsindex_pid: std::process::id(),
            started_at,
            track_parent: session_pid.is_none(),
        }))
    }

    fn has_ended(&self) -> bool {
        if self.track_parent && unsafe { libc::getppid() } != self.session_pid {
            return true;
        }
        // Permission or transient inspection errors are not evidence of death.
        // A missing/zombie process or a changed start time is conclusive.
        matches!(process_start_time(self.session_pid), Ok(current) if current != Some(self.started_at))
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

#[cfg(target_os = "macos")]
fn process_start_time(pid: libc::pid_t) -> io::Result<Option<(u64, u64)>> {
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
    Ok((info.pbi_status != libc::SZOMB).then_some((info.pbi_start_tvsec, info.pbi_start_tvusec)))
}

#[cfg(target_os = "linux")]
fn process_start_time(pid: libc::pid_t) -> io::Result<Option<(u64, u64)>> {
    match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => parse_proc_stat(&stat),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(any(target_os = "linux", test))]
fn parse_proc_stat(stat: &str) -> io::Result<Option<(u64, u64)>> {
    // comm (field 2) can contain spaces and closing parentheses. Fields after
    // its final ')' start with state (3); starttime is field 22.
    let (_, tail) = stat
        .rsplit_once(") ")
        .ok_or_else(|| io::Error::other("invalid process stat"))?;
    let mut fields = tail.split_whitespace();
    let state = fields.next();
    let started_at = fields
        .nth(18)
        .and_then(|field| field.parse::<u64>().ok())
        .ok_or_else(|| io::Error::other("missing process start time"))?;
    Ok((!matches!(state, Some("Z" | "X" | "x"))).then_some((started_at, 0)))
}

// Keep the existing Unix support outside the macOS/Linux release targets.
// These platforms can check liveness but do not have a start-time fingerprint.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn process_start_time(pid: libc::pid_t) -> io::Result<Option<(u64, u64)>> {
    if unsafe { libc::kill(pid, 0) } == 0 {
        return Ok(Some((0, 0)));
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
    fn live_session_and_reused_pid_have_different_lifetimes() -> Result<()> {
        let mut session = SessionLifecycle::capture(Some(unsafe { libc::getppid() } as u32))?
            .expect("test parent should be alive");
        assert!(!session.has_ended());
        session.started_at.0 += 1;
        assert!(
            session.has_ended(),
            "a reused PID must not keep tsindex alive"
        );
        Ok(())
    }

    #[test]
    fn rejects_invalid_and_self_owned_sessions() {
        for pid in [0, u32::MAX, std::process::id()] {
            assert!(SessionLifecycle::capture(Some(pid)).is_err());
        }
    }

    #[test]
    fn linux_stat_handles_process_names_and_zombies() -> Result<()> {
        let stat = |state| {
            format!(
                "123 (a name ) with parens) {state} {} 98765 0 0",
                "0 ".repeat(18)
            )
        };
        assert_eq!(parse_proc_stat(&stat("S"))?, Some((98765, 0)));
        for state in ["Z", "X", "x"] {
            assert_eq!(parse_proc_stat(&stat(state))?, None);
        }
        assert!(parse_proc_stat("malformed").is_err());
        assert!(parse_proc_stat("123 (name) S 0").is_err());
        Ok(())
    }
}
