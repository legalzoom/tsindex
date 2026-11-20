//! Thin stdio-to-Unix-socket MCP proxy invoked by agent harnesses via
//! `tsindex serve --mcp --via-daemon`.

use std::env;
use std::io::{self, BufReader, Read, Write};
use std::net::Shutdown;
use std::os::fd::{AsFd, AsRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};

use crate::daemon::{
    CONTROL_IO_TIMEOUT, DaemonClient, DaemonPaths, DaemonRequest, DaemonResponse,
    MAX_CONTROL_LINE_BYTES, PROTOCOL_VERSION, STARTUP_IO_TIMEOUT, SessionState,
    StartSessionRequest, connect_with_timeout, daemon_not_running_message, read_bounded_line,
    validate_private_dir, verify_peer_credentials, write_json_line,
};
use crate::mcp::CancellableStdin;

const STREAM_STALL_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct McpProxyArgs {
    pub root: PathBuf,
    pub db: Option<PathBuf>,
    pub languages: Vec<String>,
    pub no_refresh: bool,
    pub session_pid: Option<u32>,
    pub socket: Option<PathBuf>,
}

/// Connect to the running orchestrator daemon, request a per-session MCP
/// worker, and proxy JSON-RPC traffic between stdio and the daemon socket.
///
/// Fails fast with an actionable message if the daemon is not running.
pub fn run_mcp_proxy(args: McpProxyArgs) -> Result<()> {
    let paths = DaemonPaths::resolve(args.socket.as_deref())?;
    let root = absolutize(&args.root)?;
    let db = args.db.as_deref().map(absolutize).transpose()?;

    validate_private_dir(paths.socket_dir()?).map_err(|error| {
        anyhow!(
            "{} ({error:#})",
            daemon_not_running_message(&paths.socket_path)
        )
    })?;
    let mut stream =
        connect_with_timeout(&paths.socket_path, CONTROL_IO_TIMEOUT).map_err(|error| {
            anyhow!(
                "{} ({error})",
                daemon_not_running_message(&paths.socket_path)
            )
        })?;
    // Authenticate the server before revealing workspace metadata. Private
    // pathname permissions alone cannot authenticate a shared /tmp endpoint.
    verify_peer_credentials(&stream)?;
    stream.set_read_timeout(Some(STARTUP_IO_TIMEOUT + CONTROL_IO_TIMEOUT))?;
    stream.set_write_timeout(Some(CONTROL_IO_TIMEOUT))?;

    let (owner_pid, owner_explicit) = match args.session_pid {
        Some(pid) => (Some(pid), true),
        None => {
            let ppid = unsafe { libc::getppid() };
            ((ppid > 1).then_some(ppid as u32), false)
        }
    };

    let request = DaemonRequest::StartSession(StartSessionRequest {
        protocol: PROTOCOL_VERSION,
        client_version: env!("CARGO_PKG_VERSION").to_string(),
        root,
        db,
        languages: args.languages,
        no_refresh: args.no_refresh,
        client_pid: std::process::id(),
        owner_pid,
        owner_explicit,
    });
    write_json_line(&mut stream, &request)?;

    let mut uds_reader = BufReader::new(stream);
    let line = read_bounded_line(&mut uds_reader, MAX_CONTROL_LINE_BYTES)
        .context("orchestrator closed connection during session handshake")?;
    let response: DaemonResponse =
        serde_json::from_str(&line).context("invalid handshake response from orchestrator")?;

    let (session_id, worker_pid, daemon_pid, tracked_owner_pid) = match response {
        DaemonResponse::SessionReady {
            session_id,
            worker_pid,
            daemon_pid,
            daemon_version: _,
            tracked_owner_pid,
        } => (session_id, worker_pid, daemon_pid, tracked_owner_pid),
        DaemonResponse::Error { error } => bail!("{error}"),
        other => bail!("unexpected orchestrator handshake response: {other:?}"),
    };

    // Idle sessions may wait indefinitely for input. Writes retain a finite
    // stall deadline so a client that stops reading cannot pin this proxy.
    let _ = uds_reader.get_ref().set_read_timeout(None);
    let _ = uds_reader
        .get_ref()
        .set_write_timeout(Some(STREAM_STALL_TIMEOUT));

    if let Some(tracked) = tracked_owner_pid {
        let _ = writeln!(
            io::stderr(),
            "tsindex: tracking session PID {tracked} for tsindex PID {worker_pid} (session {session_id}, daemon PID {daemon_pid})"
        );
    } else {
        let _ = writeln!(
            io::stderr(),
            "tsindex: attached session {session_id} to daemon PID {daemon_pid} (worker PID {worker_pid})"
        );
    }

    // Register the outcome connection while the daemon still accepts control
    // clients. Its final record survives a whole-daemon stop or restart.
    let mut outcomes = DaemonClient::new(paths).watch_session(&session_id)?;
    let cancel_outcomes = outcomes.get_ref().try_clone()?;
    let cancel_data = uds_reader.get_ref().try_clone()?;
    let stdin = CancellableStdin::new()?;
    let cancellation = Arc::clone(&stdin.cancellation);
    let outcome_failed = Arc::new(AtomicBool::new(false));
    let (sent, received) = mpsc::sync_channel(1);

    thread::scope(|scope| -> Result<()> {
        let watcher_failed = Arc::clone(&outcome_failed);
        scope.spawn(move || {
            let result: Result<_> = (|| {
                let line = read_bounded_line(&mut outcomes, MAX_CONTROL_LINE_BYTES)
                    .context("daemon disconnected before reporting the MCP worker outcome")?;
                match serde_json::from_str::<DaemonResponse>(&line)? {
                    DaemonResponse::Session { session }
                        if session.session_id == session_id
                            && session.state == SessionState::Exited =>
                    {
                        Ok(session)
                    }
                    other => bail!("unexpected MCP worker outcome: {other:?}"),
                }
            })();
            if result.is_err() {
                watcher_failed.store(true, Ordering::Release);
                cancellation.stop();
                let _ = cancel_data.shutdown(Shutdown::Both);
            }
            let _ = sent.send(result);
        });

        if let Err(error) = relay_stdio(uds_reader, stdin, outcome_failed) {
            let _ = cancel_outcomes.shutdown(Shutdown::Both);
            return Err(error);
        }
        let outcome = received
            .recv_timeout(Duration::from_secs(6))
            .map_err(|error| {
                let _ = cancel_outcomes.shutdown(Shutdown::Both);
                anyhow!("MCP worker outcome did not arrive after disconnect: {error}")
            })??;
        let reason = outcome
            .exit_reason
            .as_deref()
            .unwrap_or("missing_exit_reason");
        if !matches!(
            reason,
            "client_eof" | "worker_exited" | "killed_by_user" | "daemon_shutdown" | "owner_exited"
        ) {
            bail!("session MCP worker failed: {reason}");
        }
        Ok(())
    })
}

fn relay_stdio(
    mut uds_reader: BufReader<UnixStream>,
    mut stdin: CancellableStdin,
    outcome_failed: Arc<AtomicBool>,
) -> Result<()> {
    let mut uds_writer = uds_reader
        .get_ref()
        .try_clone()
        .context("failed to clone orchestrator socket")?;
    let cancellation = Arc::clone(&stdin.cancellation);
    let output_error: Mutex<Option<anyhow::Error>> = Mutex::new(None);
    let input_eof = Arc::new(AtomicBool::new(false));

    thread::scope(|scope| {
        let cancel_from_egress = Arc::clone(&cancellation);
        let egress_error = &output_error;
        let egress_input_eof = Arc::clone(&input_eof);
        scope.spawn(move || {
            let mut stdout = match NonblockingStdout::new() {
                Ok(stdout) => stdout,
                Err(error) => {
                    *egress_error.lock().unwrap_or_else(|e| e.into_inner()) = Some(error.into());
                    cancel_from_egress.stop();
                    let _ = uds_reader.get_ref().shutdown(Shutdown::Both);
                    return;
                }
            };
            let mut buf = [0u8; 8192];
            loop {
                match uds_reader.read(&mut buf) {
                    Ok(0) => {
                        cancel_from_egress.stop();
                        let _ = uds_reader.get_ref().shutdown(Shutdown::Both);
                        break;
                    }
                    Ok(n) => {
                        if let Err(error) = stdout.write_all(
                            &buf[..n],
                            uds_reader.get_ref().as_raw_fd(),
                            &egress_input_eof,
                            &outcome_failed,
                        ) {
                            let mut slot = egress_error.lock().unwrap_or_else(|e| e.into_inner());
                            if slot.is_none() {
                                *slot = Some(error.into());
                            }
                            cancel_from_egress.stop();
                            let _ = uds_reader.get_ref().shutdown(Shutdown::Both);
                            break;
                        }
                    }
                    Err(error) => {
                        *egress_error.lock().unwrap_or_else(|e| e.into_inner()) =
                            Some(error.into());
                        cancel_from_egress.stop();
                        let _ = uds_reader.get_ref().shutdown(Shutdown::Both);
                        break;
                    }
                }
            }
        });

        let mut buf = [0u8; 8192];
        loop {
            match stdin.read(&mut buf) {
                Ok(0) => {
                    // Half-close write side so the orchestrator flushes any
                    // remaining responses from the worker before disconnecting.
                    input_eof.store(true, Ordering::Release);
                    let _ = uds_writer.shutdown(Shutdown::Write);
                    break;
                }
                Ok(n) => {
                    if let Err(error) = uds_writer.write_all(&buf[..n]) {
                        *output_error.lock().unwrap_or_else(|e| e.into_inner()) =
                            Some(error.into());
                        let _ = uds_writer.shutdown(Shutdown::Both);
                        break;
                    }
                }
                Err(error) => {
                    let mut slot = output_error.lock().unwrap_or_else(|e| e.into_inner());
                    if slot.is_none() {
                        *slot = Some(error.into());
                    }
                    let _ = uds_writer.shutdown(Shutdown::Both);
                    break;
                }
            }
        }
    });

    if let Some(error) = output_error.into_inner().unwrap_or_else(|e| e.into_inner()) {
        return Err(error);
    }
    Ok(())
}

struct NonblockingStdout {
    fd: OwnedFd,
    previous_flags: libc::c_int,
}

impl NonblockingStdout {
    fn new() -> io::Result<Self> {
        let fd = io::stdout().as_fd().try_clone_to_owned()?;
        let previous_flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
        if previous_flags < 0
            || unsafe {
                libc::fcntl(
                    fd.as_raw_fd(),
                    libc::F_SETFL,
                    previous_flags | libc::O_NONBLOCK,
                )
            } < 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { fd, previous_flags })
    }

    fn write_all(
        &mut self,
        mut bytes: &[u8],
        socket_fd: libc::c_int,
        input_eof: &AtomicBool,
        outcome_failed: &AtomicBool,
    ) -> io::Result<()> {
        let mut deadline = Instant::now() + STREAM_STALL_TIMEOUT;
        while !bytes.is_empty() {
            if outcome_failed.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "MCP worker outcome connection was lost",
                ));
            }
            let written =
                unsafe { libc::write(self.fd.as_raw_fd(), bytes.as_ptr().cast(), bytes.len()) };
            if written > 0 {
                bytes = &bytes[written as usize..];
                deadline = Instant::now() + STREAM_STALL_TIMEOUT;
                continue;
            }
            if written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "MCP stdout closed",
                ));
            }
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            if error.kind() != io::ErrorKind::WouldBlock {
                return Err(error);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "MCP stdout stalled",
                ));
            }
            let mut fds = [
                libc::pollfd {
                    fd: self.fd.as_raw_fd(),
                    events: libc::POLLOUT,
                    revents: 0,
                },
                libc::pollfd {
                    // After normal input EOF, the worker can exit while its
                    // responses remain buffered. Preserve their bounded drain.
                    fd: if input_eof.load(Ordering::Acquire) {
                        -1
                    } else {
                        socket_fd
                    },
                    events: 0,
                    revents: 0,
                },
            ];
            // Socket hangup interrupts a full stdout pipe during daemon teardown.
            let rc =
                unsafe { libc::poll(fds.as_mut_ptr(), 2, remaining.as_millis().min(100) as i32) };
            if rc < 0 && io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
                return Err(io::Error::last_os_error());
            }
            if !input_eof.load(Ordering::Acquire)
                && fds[1].revents & (libc::POLLHUP | libc::POLLERR | libc::POLLNVAL) != 0
            {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "daemon disconnected while MCP stdout was blocked",
                ));
            }
        }
        Ok(())
    }
}

impl Drop for NonblockingStdout {
    fn drop(&mut self) {
        unsafe { libc::fcntl(self.fd.as_raw_fd(), libc::F_SETFL, self.previous_flags) };
    }
}

fn absolutize(path: &Path) -> Result<PathBuf> {
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()
            .context("failed to determine current directory")?
            .join(path)
    };
    Ok(joined.canonicalize().unwrap_or(joined))
}
