//! Exercise ownership with real processes while the server transport stays open.

#![cfg(unix)]

use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tempfile::{TempDir, tempdir};

const BIN: &str = env!("CARGO_BIN_EXE_tsindex");

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn fixture() -> Result<TempDir> {
    let dir = tempdir()?;
    fs::write(dir.path().join("example.py"), "def example(): pass\n")?;
    let output = Command::new(BIN)
        .arg("--root")
        .arg(dir.path())
        .arg("init")
        .output()?;
    assert!(output.status.success(), "{output:?}");
    Ok(dir)
}

fn wait_until(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(8);
    while !ready() {
        assert!(Instant::now() < deadline, "lifecycle action timed out");
        thread::sleep(Duration::from_millis(20));
    }
}

fn read_log(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_default()
}

fn explicit_session_exit(args: &[&str], check_alive: bool, reap_owner: bool) -> Result<()> {
    let dir = fixture()?;
    let log = dir.path().join("server.log");
    // The owner is deliberately NOT the server's parent, as with a wrapper.
    let mut owner = Process(Command::new("sleep").arg("60").spawn()?);
    let mut server = Process(
        Command::new(BIN)
            .arg("--root")
            .arg(dir.path())
            .arg("--no-refresh")
            .args(args)
            .arg("--session-pid")
            .arg(owner.0.id().to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(fs::File::create(&log)?)
            .spawn()?,
    );
    let tracking = format!(
        "tracking session PID {} for tsindex PID {}",
        owner.0.id(),
        server.0.id()
    );
    wait_until(|| read_log(&log).contains(&tracking));
    if check_alive {
        thread::sleep(Duration::from_millis(5200));
        assert!(server.0.try_wait()?.is_none(), "live owner lost its server");
    }
    owner.0.kill()?;
    if reap_owner {
        owner.0.wait()?;
    }
    // Keep stdin open: EOF cannot be what terminates the MCP server.
    wait_until(|| server.0.try_wait().unwrap().is_some());
    assert!(server.0.wait()?.success(), "{}", read_log(&log));
    let log = read_log(&log);
    assert_eq!(log.matches("tracking session PID").count(), 1, "{log}");
    assert!(
        log.contains(&format!(
            "session PID {} exited; shutting down tsindex PID {}",
            owner.0.id(),
            server.0.id()
        )),
        "{log}"
    );
    Ok(())
}

#[test]
fn mcp_tracks_explicit_owner_and_stays_alive_until_it_exits() -> Result<()> {
    explicit_session_exit(&["serve", "--mcp"], true, true)
}

#[test]
fn http_exits_when_the_session_becomes_a_zombie() -> Result<()> {
    explicit_session_exit(&["serve", "--http", "--port", "0"], false, false)
}

#[test]
fn watch_tracks_explicit_owner() -> Result<()> {
    explicit_session_exit(&["watch"], false, true)
}

#[test]
fn missing_owner_fails_before_index_initialization() -> Result<()> {
    let dir = tempdir()?;
    let mut owner = Process(Command::new("sleep").arg("60").spawn()?);
    owner.0.kill()?;
    owner.0.wait()?;
    let output = Command::new(BIN)
        .arg("--root")
        .arg(dir.path())
        .args(["serve", "--mcp", "--session-pid"])
        .arg(owner.0.id().to_string())
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("has already exited"));
    assert!(!dir.path().join(".tsindex").exists());
    Ok(())
}

fn wrapper_exit(explicit_owner: bool) -> Result<()> {
    let dir = fixture()?;
    let log = dir.path().join("server.log");
    let mut owner = if explicit_owner {
        Some(Process(Command::new("sleep").arg("60").spawn()?))
    } else {
        None
    };
    // A dedicated process group lets the test clean up both processes on
    // failure. fd 3 keeps the background server's stdin attached to our pipe.
    let mut command = Command::new("sh");
    command
        .args(["-c", "exec 3<&0; \"$@\" <&3 & wait", "session"])
        .arg(BIN)
        .arg("--root")
        .arg(dir.path())
        .args(["--no-refresh", "serve", "--mcp"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(fs::File::create(&log)?)
        .process_group(0);
    if let Some(owner) = &owner {
        command.arg("--session-pid").arg(owner.0.id().to_string());
    }
    let parent = command.spawn()?;
    struct Family(Process);
    impl Drop for Family {
        fn drop(&mut self) {
            unsafe { libc::kill(-(self.0.0.id() as i32), libc::SIGKILL) };
        }
    }
    let mut family = Family(Process(parent));
    // Child::wait closes its stdin handle; retain ours across parent reaping.
    let _stdin = family.0.0.stdin.take().context("session stdin")?;
    let parent_pid = family.0.0.id();
    let session_pid = owner.as_ref().map_or(parent_pid, |owner| owner.0.id());
    wait_until(|| read_log(&log).contains(&format!("tracking session PID {session_pid}")));
    let mut stdout = family.0.0.stdout.take().context("server stdout")?;
    let (sent, received) = mpsc::channel();
    thread::spawn(move || {
        let mut output = Vec::new();
        let _ = sent.send(stdout.read_to_end(&mut output));
    });
    family.0.0.kill()?;
    family.0.0.wait()?;
    if let Some(owner) = &mut owner {
        thread::sleep(Duration::from_millis(5200));
        assert!(
            matches!(received.try_recv(), Err(mpsc::TryRecvError::Empty)),
            "server exited with wrapper despite its live explicit owner: {}",
            read_log(&log)
        );
        owner.0.kill()?;
        owner.0.wait()?;
    }
    // EOF on stdout proves the server exited, independent of its log message.
    received.recv_timeout(Duration::from_secs(8))??;
    let log = read_log(&log);
    assert!(
        log.contains(&format!("session PID {session_pid} exited; shutting down")),
        "{log}"
    );
    Ok(())
}

#[test]
fn mcp_exits_when_its_default_parent_dies_with_stdin_still_open() -> Result<()> {
    wrapper_exit(false)
}

#[test]
fn explicit_owner_keeps_server_alive_after_wrapper_exit() -> Result<()> {
    wrapper_exit(true)
}

#[test]
fn disconnected_stderr_does_not_prevent_shutdown() -> Result<()> {
    let dir = fixture()?;
    let mut owner = Process(Command::new("sleep").arg("60").spawn()?);
    let mut server = Process(
        Command::new(BIN)
            .arg("--root")
            .arg(dir.path())
            .args(["--no-refresh", "serve", "--mcp", "--session-pid"])
            .arg(owner.0.id().to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?,
    );
    let mut stderr = BufReader::new(server.0.stderr.take().context("server stderr")?);
    let (sent, received) = mpsc::channel();
    thread::spawn(move || {
        let mut tracking = String::new();
        let result = stderr.read_line(&mut tracking).map(|_| tracking);
        drop(stderr);
        let _ = sent.send(result);
    });
    let tracking = received.recv_timeout(Duration::from_secs(8))??;
    assert!(tracking.contains("tracking session PID"), "{tracking}");
    owner.0.kill()?;
    owner.0.wait()?;
    wait_until(|| server.0.try_wait().unwrap().is_some());
    assert!(server.0.wait()?.success());
    Ok(())
}
