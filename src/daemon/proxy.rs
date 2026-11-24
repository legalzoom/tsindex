//! Thin stdio-to-Unix-socket MCP proxy invoked by agent harnesses via
//! `tsindex serve --mcp --via-daemon`.

use std::env;
use std::ffi::{CStr, OsStr};
use std::fs::{File, Metadata, OpenOptions};
use std::io::{self, BufReader, IsTerminal, Read, Write};
use std::net::Shutdown;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};

use crate::daemon::{
    CONTROL_IO_TIMEOUT, DaemonClient, DaemonRequest, DaemonResponse, MAX_CONTROL_LINE_BYTES,
    PROTOCOL_VERSION, STARTUP_IO_TIMEOUT, SessionState, StartSessionRequest, connect_with_timeout,
    daemon_not_running_message, read_bounded_line, validate_private_dir, verify_server_credentials,
    write_json_line,
};
use crate::mcp::CancellableStdin;

const STREAM_STALL_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct McpProxyArgs {
    pub root: Option<PathBuf>,
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
    let paths = crate::daemon::service::client_paths(args.socket.as_deref())?;
    let caller_dir = env::current_dir().context("failed to determine current directory")?;
    // Send absolute spellings without touching the checkout or database in
    // the launcher. Their resolution belongs to the registered worker.
    let root = args
        .root
        .as_ref()
        .map_or_else(|| caller_dir.clone(), |path| caller_dir.join(path));
    let db = args.db.as_ref().map(|path| caller_dir.join(path));

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
    verify_server_credentials(&stream, &paths)?;
    stream.set_read_timeout(Some(STARTUP_IO_TIMEOUT + CONTROL_IO_TIMEOUT))?;
    stream.set_write_timeout(Some(CONTROL_IO_TIMEOUT))?;

    let request = DaemonRequest::StartSession(StartSessionRequest {
        protocol: PROTOCOL_VERSION,
        client_version: env!("CARGO_PKG_VERSION").to_string(),
        root,
        caller_dir: Some(caller_dir),
        db,
        languages: args.languages,
        no_refresh: args.no_refresh,
        client_pid: std::process::id(),
        owner_pid: args.session_pid,
        owner_explicit: args.session_pid.is_some(),
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
    let cancel_blocked_output = AtomicBool::new(false);
    let (sent, received) = mpsc::sync_channel(1);

    thread::scope(|scope| -> Result<()> {
        let cancel_drain = &cancel_blocked_output;
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
            // A normal exit can precede delivery of the worker's buffered replies.
            // An explicit stop, crash, or lost supervisor ends a stalled delivery
            // even when the client has already half-closed stdin.
            if !result.as_ref().is_ok_and(|session| {
                matches!(
                    session.exit_reason.as_deref(),
                    Some("client_eof" | "worker_exited")
                )
            }) {
                cancel_drain.store(true, Ordering::Release);
            }
            if result.is_err() {
                cancellation.stop();
                let _ = cancel_data.shutdown(Shutdown::Both);
            }
            let _ = sent.send(result);
        });

        if let Err(error) = relay_stdio(uds_reader, stdin, &cancel_blocked_output) {
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
    cancel_blocked_output: &AtomicBool,
) -> Result<()> {
    let mut uds_writer = uds_reader
        .get_ref()
        .try_clone()
        .context("failed to clone orchestrator socket")?;
    let cancellation = Arc::clone(&stdin.cancellation);
    let output_error: Mutex<Option<anyhow::Error>> = Mutex::new(None);
    let mut stdout =
        ProxyStdout::new(io::stdout().as_fd()).context("failed to prepare MCP stdout")?;

    thread::scope(|scope| {
        let cancel_from_egress = Arc::clone(&cancellation);
        let egress_error = &output_error;
        scope.spawn(move || {
            let mut buf = [0u8; 8192];
            loop {
                let result = match uds_reader.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => stdout.relay(&buf[..n], cancel_blocked_output),
                    Err(error) => Err(error),
                };
                match result {
                    Ok(true) => continue,
                    Ok(false) => break,
                    Err(error) => {
                        egress_error
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .get_or_insert(error.into());
                        break;
                    }
                }
            }
            cancel_from_egress.stop();
            let _ = uds_reader.get_ref().shutdown(Shutdown::Both);
        });

        let mut buf = [0u8; 8192];
        loop {
            match stdin.read(&mut buf) {
                Ok(0) => {
                    // Half-close write side so the orchestrator flushes any
                    // remaining responses from the worker before disconnecting.
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

/// The harness may keep a duplicate of stdout. File status flags belong to the
/// open file description and changing them through any duplicate changes them
/// for every holder, including after an ungraceful proxy exit.
struct ProxyStdout {
    file: File,
    mode: OutputMode,
}

enum OutputMode {
    Write,
    Socket,
    #[cfg(any(target_os = "macos", test))]
    PollStream,
    PollPipe,
}

#[cfg(any(target_os = "macos", test))]
fn socket_option(fd: BorrowedFd<'_>, option: libc::c_int) -> io::Result<libc::c_int> {
    let mut value: libc::c_int = 0;
    let mut size = std::mem::size_of_val(&value) as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            fd.as_raw_fd(),
            libc::SOL_SOCKET,
            option,
            (&raw mut value).cast(),
            &mut size,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(value)
}

#[cfg(any(target_os = "macos", test))]
fn darwin_socket_mode(fd: BorrowedFd<'_>, flags: libc::c_int) -> io::Result<OutputMode> {
    if flags & libc::O_NONBLOCK != 0 {
        return Ok(OutputMode::Socket);
    }
    if socket_option(fd, libc::SO_TYPE)? == libc::SOCK_STREAM {
        return Ok(OutputMode::PollStream);
    }
    // Readiness promises space for only the low-water mark. Splitting a
    // larger datagram would change its boundaries, so require nonblocking I/O.
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "use a stream socket or a nonblocking message-oriented socket for MCP stdout",
    ))
}

impl ProxyStdout {
    fn new(stdout: BorrowedFd<'_>) -> io::Result<Self> {
        let file = File::from(stdout.try_clone_to_owned()?);
        let metadata = file.metadata()?;
        let kind = metadata.file_type();
        let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        // A private reopen must never grant access the inherited descriptor lacks.
        if flags & libc::O_ACCMODE == libc::O_RDONLY {
            return Err(io::Error::from_raw_os_error(libc::EBADF));
        }
        let mode = if kind.is_socket() {
            #[cfg(target_os = "macos")]
            {
                darwin_socket_mode(file.as_fd(), flags)?
            }
            #[cfg(not(target_os = "macos"))]
            {
                OutputMode::Socket
            }
        } else if kind.is_fifo() {
            // Linux procfs reopens the pipe with an independent file description.
            // Some sandboxes omit procfs or disallow reopening an inherited pipe.
            #[cfg(target_os = "linux")]
            // Packet-mode pipes preserve the original writer's packet boundaries.
            if flags & libc::O_DIRECT == 0
                && let Ok(private) = reopen_output(
                    Path::new(&format!("/proc/self/fd/{}", file.as_raw_fd())),
                    &metadata,
                )
            {
                return Ok(Self {
                    file: private,
                    mode: OutputMode::Write,
                });
            }
            OutputMode::PollPipe
        } else if file.is_terminal() {
            // A terminal can stop accepting output after poll (e.g. flow control).
            // Give the relay its own nonblocking open of the same terminal device.
            let mut name = [0; libc::PATH_MAX as usize];
            let rc = unsafe { libc::ttyname_r(file.as_raw_fd(), name.as_mut_ptr(), name.len()) };
            if rc != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!(
                        "cannot resolve stdout terminal; use a pipe ({})",
                        io::Error::from_raw_os_error(rc)
                    ),
                ));
            }
            let name = unsafe { CStr::from_ptr(name.as_ptr()) };
            let path = Path::new(OsStr::from_bytes(name.to_bytes()));
            // Opening a PTY master device can allocate a different terminal.
            if path
                .file_name()
                .is_some_and(|name| name == "ptmx" || name.as_bytes().starts_with(b"pty"))
            {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "use a terminal slave or a pipe for MCP stdout",
                ));
            }
            let private = reopen_output(path, &metadata).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("cannot reopen stdout terminal; use a pipe ({error})"),
                )
            })?;
            return Ok(Self {
                file: private,
                mode: OutputMode::Write,
            });
        } else if kind.is_char_device()
            && flags & libc::O_NONBLOCK == 0
            && !["/dev/null", "/dev/zero", "/dev/full"].iter().any(|path| {
                std::fs::metadata(path)
                    .is_ok_and(|m| m.file_type().is_char_device() && m.rdev() == metadata.rdev())
            })
        {
            // Arbitrary device drivers can block after poll, and a fresh open
            // can have different driver state. Keep their inherited flags intact.
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "use a pipe or a nonblocking character device for MCP stdout",
            ));
        } else {
            // In particular, regular files must retain the caller's shared offset
            // and append semantics. Kernel filesystem I/O has its usual behavior.
            OutputMode::Write
        };
        Ok(Self { file, mode })
    }

    fn try_write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let bytes = match self.mode {
            OutputMode::Write => return self.file.write(bytes),
            OutputMode::PollPipe => {
                // MCP dedicates this pipe's byte stream to one writer. With no
                // competing writer, Linux and Darwin leave at least PIPE_BUF
                // bytes available after write readiness and a read only frees more.
                let chunk = self.writable_prefix(bytes, libc::PIPE_BUF)?;
                return self.file.write(chunk);
            }
            OutputMode::Socket => bytes,
            #[cfg(any(target_os = "macos", test))]
            OutputMode::PollStream => {
                // XNU can wait for send-buffer space even with MSG_DONTWAIT.
                // POLLOUT promises SO_SNDLOWAT bytes to this stream's sole
                // writer. Read the current threshold without changing the socket.
                let lowat = socket_option(self.file.as_fd(), libc::SO_SNDLOWAT)?;
                if lowat <= 0 {
                    return Err(io::ErrorKind::InvalidData.into());
                }
                self.writable_prefix(bytes, lowat as usize)?
            }
        };
        let n = unsafe {
            libc::send(
                self.file.as_raw_fd(),
                bytes.as_ptr().cast(),
                bytes.len(),
                libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
            )
        };
        if n < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(n as usize)
        }
    }

    fn writable_prefix<'a>(&self, bytes: &'a [u8], limit: usize) -> io::Result<&'a [u8]> {
        if !self.wait_writable(0)? {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        Ok(&bytes[..bytes.len().min(limit)])
    }

    fn wait_writable(&self, timeout_ms: i32) -> io::Result<bool> {
        let mut fd = libc::pollfd {
            fd: self.file.as_raw_fd(),
            events: libc::POLLOUT,
            revents: 0,
        };
        let rc = unsafe { libc::poll(&mut fd, 1, timeout_ms) };
        if rc < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(rc != 0)
        }
    }

    /// Returns false when the supervisor ended a session with blocked output.
    fn relay(&mut self, mut bytes: &[u8], cancel_blocked_output: &AtomicBool) -> io::Result<bool> {
        let mut deadline = Instant::now() + STREAM_STALL_TIMEOUT;
        while !bytes.is_empty() {
            match self.try_write(bytes) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "MCP stdout closed",
                    ));
                }
                Ok(n) => {
                    bytes = &bytes[n..];
                    deadline = Instant::now() + STREAM_STALL_TIMEOUT;
                    continue;
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => (),
                Err(e) => return Err(e),
            }
            if cancel_blocked_output.load(Ordering::Acquire) {
                return Ok(false);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "MCP stdout stalled",
                ));
            }
            if let Err(error) = self.wait_writable(remaining.as_millis().min(100) as i32)
                && error.kind() != io::ErrorKind::Interrupted
            {
                return Err(error);
            }
        }
        Ok(true)
    }
}

fn reopen_output(path: &Path, expected: &Metadata) -> io::Result<File> {
    let file = OpenOptions::new()
        .write(true)
        .custom_flags(libc::O_NONBLOCK | libc::O_NOCTTY)
        .open(path)?;
    let actual = file.metadata()?;
    if (
        actual.dev(),
        actual.ino(),
        actual.rdev(),
        actual.file_type(),
    ) != (
        expected.dev(),
        expected.ino(),
        expected.rdev(),
        expected.file_type(),
    ) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "stdout device changed while reopening it",
        ));
    }
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom};
    use std::os::fd::FromRawFd;

    fn flags(fd: impl AsFd) -> libc::c_int {
        let flags = unsafe { libc::fcntl(fd.as_fd().as_raw_fd(), libc::F_GETFL) };
        assert!(flags >= 0);
        flags
    }

    #[test]
    fn stdout_preserves_shared_file_offset() -> io::Result<()> {
        let mut file = tempfile::tempfile()?;
        file.write_all(b"0123456789")?;
        file.seek(SeekFrom::Start(3))?;
        let before = flags(&file);
        let mut stdout = ProxyStdout::new(file.as_fd())?;
        assert_eq!(flags(&file), before);
        assert!(stdout.relay(b"mcp", &AtomicBool::new(false))?);
        file.write_all(b"!")?;
        file.rewind()?;
        // The harness can change append behavior while the proxy is connected.
        assert_eq!(
            unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETFL, before | libc::O_APPEND) },
            0
        );
        assert!(stdout.relay(b"end", &AtomicBool::new(false))?);
        drop(stdout);
        assert_ne!(flags(&file) & libc::O_APPEND, 0);
        file.rewind()?;
        let mut content = String::new();
        file.read_to_string(&mut content)?;
        assert_eq!(content, "012mcp!789end");
        Ok(())
    }

    #[test]
    fn socket_stdout_preserves_flags_and_stops_when_its_reader_stalls() -> io::Result<()> {
        for exercise_polled_stream in [false, true] {
            let (mut socket, _reader) = UnixStream::pair()?;
            let chunk = [0; 8192];
            socket.set_nonblocking(true)?;
            loop {
                match socket.write(&chunk) {
                    Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                    Ok(_) => (),
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) => return Err(error),
                }
            }
            socket.set_nonblocking(false)?;
            // Keep a regression in the kernel send path from hanging the test
            // suite. The relay itself must see backpressure before this expires.
            let watchdog = Duration::from_secs(4);
            socket.set_write_timeout(Some(watchdog))?;
            let before = flags(&socket);
            let mut stdout = ProxyStdout::new(socket.as_fd())?;
            if exercise_polled_stream {
                stdout.mode = darwin_socket_mode(socket.as_fd(), before)?;
            }
            let start = Instant::now();
            assert!(!stdout.relay(&chunk, &AtomicBool::new(true))?);
            assert!(start.elapsed() < watchdog / 2, "blocked in socket send");
            assert_eq!(flags(&socket), before);
            assert_eq!(socket.write_timeout()?, Some(watchdog));
        }
        Ok(())
    }

    #[test]
    fn stream_stdout_drains_in_order_across_write_readiness_thresholds() -> io::Result<()> {
        let (socket, mut reader) = UnixStream::pair()?;
        reader.set_read_timeout(Some(Duration::from_secs(4)))?;
        socket.set_write_timeout(Some(Duration::from_secs(4)))?;
        let before = flags(&socket);
        let mut stdout = ProxyStdout::new(socket.as_fd())?;
        stdout.mode = darwin_socket_mode(socket.as_fd(), before)?;
        let lowat = socket_option(socket.as_fd(), libc::SO_SNDLOWAT)? as usize;
        let size = 8192.max(2 * lowat + 1);
        let payload: Vec<_> = (0..size).map(|n| (n % 251) as u8).collect();
        thread::scope(|scope| -> io::Result<()> {
            let received = scope.spawn(move || -> io::Result<_> {
                let mut data = vec![0; size];
                reader.read_exact(&mut data)?;
                Ok(data)
            });
            let first = stdout.try_write(&payload)?;
            assert!((1..=lowat).contains(&first));
            assert!(stdout.relay(&payload[first..], &AtomicBool::new(false))?);
            assert_eq!(received.join().unwrap()?, payload);
            Ok(())
        })?;
        assert_eq!(flags(&socket), before);
        assert_eq!(
            socket_option(socket.as_fd(), libc::SO_SNDLOWAT)? as usize,
            lowat
        );
        Ok(())
    }

    #[test]
    fn darwin_message_socket_stdout_sends_a_relay_chunk_in_one_datagram() -> io::Result<()> {
        let (socket, reader) = std::os::unix::net::UnixDatagram::pair()?;
        assert!(matches!(
            darwin_socket_mode(socket.as_fd(), flags(&socket)),
            Err(error) if error.kind() == io::ErrorKind::Unsupported
        ));
        socket.set_nonblocking(true)?;
        reader.set_read_timeout(Some(Duration::from_secs(4)))?;
        let before = flags(&socket);
        let mut stdout = ProxyStdout::new(socket.as_fd())?;
        stdout.mode = darwin_socket_mode(socket.as_fd(), before)?;
        let payload = [23; 511];
        assert!(stdout.relay(&payload, &AtomicBool::new(false))?);
        let mut received = [0; 512];
        let len = reader.recv(&mut received)?;
        assert_eq!(&received[..len], &payload);
        assert_eq!(flags(&socket), before);
        Ok(())
    }

    #[test]
    fn pipe_stdout_bounds_writes_with_an_inherited_blocking_descriptor() -> io::Result<()> {
        let (mut reader, mut writer) = io::pipe()?;
        // Darwin records the first ordinary write in F_GETFL (FWASWRITTEN).
        // Take the baseline after the caller's prefix, then compare every flag.
        writer.write_all(b"m")?;
        let before = flags(&writer);
        let mut stdout = ProxyStdout::new(writer.as_fd())?;
        assert!(stdout.relay(b"cp", &AtomicBool::new(false))?);
        let mut reply = [0; 3];
        reader.read_exact(&mut reply)?;
        assert_eq!(&reply, b"mcp");
        assert_eq!(flags(&writer), before);

        // Exercise the no-procfs path on Linux too. Poll guarantees no more
        // than one atomic write. A larger blocking write could strand teardown.
        let mut stdout = ProxyStdout {
            file: File::from(writer.as_fd().try_clone_to_owned()?),
            mode: OutputMode::PollPipe,
        };
        let chunk = vec![0; libc::PIPE_BUF * 2];
        assert_eq!(stdout.try_write(&chunk)?, libc::PIPE_BUF);
        let cancel = AtomicBool::new(true);
        while stdout.relay(&chunk, &cancel)? {}
        assert_eq!(flags(&writer), before);
        Ok(())
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn terminal_stdout_keeps_the_harness_terminal_blocking() -> io::Result<()> {
        let (mut master, mut slave) = (-1, -1);
        let rc = unsafe {
            libc::openpty(
                &mut master,
                &mut slave,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        // openpty transferred ownership of both descriptors on success.
        let mut master = unsafe { File::from_raw_fd(master) };
        let slave = unsafe { File::from_raw_fd(slave) };
        let before = flags(&slave);
        let mut stdout = ProxyStdout::new(slave.as_fd())?;
        assert_eq!(flags(&slave), before);
        assert!(stdout.relay(b"mcp", &AtomicBool::new(false))?);
        let mut reply = [0; 3];
        master.read_exact(&mut reply)?;
        assert_eq!(&reply, b"mcp");
        assert_eq!(flags(&slave), before);
        Ok(())
    }
}
