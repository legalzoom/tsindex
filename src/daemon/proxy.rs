//! Thin stdio-to-Unix-socket MCP proxy invoked by agent harnesses via
//! `tsindex serve --mcp`.

use std::env;
use std::io::{self, BufReader, Read, Write};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;

use anyhow::{Context, Result, anyhow, bail};

use crate::daemon::{
    CONTROL_IO_TIMEOUT, DaemonPaths, DaemonRequest, DaemonResponse, MAX_CONTROL_LINE_BYTES,
    PROTOCOL_VERSION, StartSessionRequest, daemon_not_running_message, read_bounded_line,
    write_json_line,
};
use crate::mcp::CancellableStdin;

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

    let mut stream = UnixStream::connect(&paths.socket_path).map_err(|error| {
        anyhow!(
            "{} ({error})",
            daemon_not_running_message(&paths.socket_path)
        )
    })?;
    stream.set_read_timeout(Some(CONTROL_IO_TIMEOUT))?;
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

    let (session_id, worker_pid, daemon_pid, daemon_version, tracked_owner_pid) = match response {
        DaemonResponse::SessionReady {
            session_id,
            worker_pid,
            daemon_pid,
            daemon_version,
            tracked_owner_pid,
        } => (
            session_id,
            worker_pid,
            daemon_pid,
            daemon_version,
            tracked_owner_pid,
        ),
        DaemonResponse::Error { error } => bail!("{error}"),
        other => bail!("unexpected orchestrator handshake response: {other:?}"),
    };

    // Clear handshake timeouts before switching to the long-lived MCP stream.
    let _ = uds_reader.get_ref().set_read_timeout(None);
    let _ = uds_reader.get_ref().set_write_timeout(None);

    if daemon_version != env!("CARGO_PKG_VERSION") {
        let _ = writeln!(
            io::stderr(),
            "tsindex: warning: CLI version {} differs from running daemon version {}; run `tsindex daemon restart` to update",
            env!("CARGO_PKG_VERSION"),
            daemon_version
        );
    }

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

    relay_stdio(uds_reader)
}

fn relay_stdio(mut uds_reader: BufReader<UnixStream>) -> Result<()> {
    let mut uds_writer = uds_reader
        .get_ref()
        .try_clone()
        .context("failed to clone orchestrator socket")?;
    let mut stdin = CancellableStdin::new()?;
    let cancellation = Arc::clone(&stdin.cancellation);
    let output_error: Mutex<Option<anyhow::Error>> = Mutex::new(None);

    thread::scope(|scope| {
        let cancel_from_egress = Arc::clone(&cancellation);
        let egress_error = &output_error;
        scope.spawn(move || {
            let mut stdout = io::stdout();
            let mut buf = [0u8; 8192];
            loop {
                match uds_reader.read(&mut buf) {
                    Ok(0) => {
                        cancel_from_egress.stop();
                        break;
                    }
                    Ok(n) => {
                        if let Err(error) =
                            stdout.write_all(&buf[..n]).and_then(|()| stdout.flush())
                        {
                            let mut slot = egress_error.lock().unwrap_or_else(|e| e.into_inner());
                            if slot.is_none() {
                                *slot = Some(error.into());
                            }
                            cancel_from_egress.stop();
                            let _ = uds_reader.get_ref().shutdown(Shutdown::Both);
                            break;
                        }
                    }
                    Err(_) => {
                        cancel_from_egress.stop();
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
                    let _ = uds_writer.shutdown(Shutdown::Write);
                    break;
                }
                Ok(n) => {
                    if uds_writer.write_all(&buf[..n]).is_err() || uds_writer.flush().is_err() {
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
