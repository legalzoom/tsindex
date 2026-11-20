//! End-to-end tests for the orchestrator daemon, thin MCP proxy, per-session
//! worker isolation, and deterministic cleanup.

#![cfg(unix)]

mod common;

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tempfile::tempdir_in;
use tsindex::config::TsIndexConfig;

const BIN: &str = env!("CARGO_BIN_EXE_tsindex");

struct McpSession {
    child: Child,
    responses: mpsc::Receiver<std::io::Result<String>>,
    reader: Option<thread::JoinHandle<()>>,
    next_id: u64,
}

impl McpSession {
    fn connect(socket: &Path, root: &Path, harness_name: &str) -> Result<Self> {
        let mut child = Command::new(BIN)
            .arg("--socket")
            .arg(socket)
            .arg("--root")
            .arg(root)
            .args(["--no-refresh", "serve", "--mcp", "--via-daemon"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("failed to spawn serve --mcp proxy")?;
        let stdout = child.stdout.take().context("missing proxy stdout")?;
        let (tx, responses) = mpsc::channel();
        let reader = thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        let mut session = Self {
            child,
            responses,
            reader: Some(reader),
            next_id: 0,
        };
        let init = session.request(
            "initialize",
            json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": { "name": harness_name, "version": "1.2.3" }
            }),
        )?;
        assert!(init.get("error").is_none(), "initialize failed: {init}");
        let stdin = session.child.stdin.as_mut().context("proxy stdin closed")?;
        writeln!(
            stdin,
            "{}",
            json!({ "jsonrpc": "2.0", "method": "notifications/initialized" })
        )?;
        stdin.flush()?;
        Ok(session)
    }

    fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        self.next_id += 1;
        let payload = json!({
            "jsonrpc": "2.0",
            "id": self.next_id,
            "method": method,
            "params": params,
        });
        let stdin = self.child.stdin.as_mut().context("proxy stdin closed")?;
        writeln!(stdin, "{payload}")?;
        stdin.flush()?;
        let line = self
            .responses
            .recv_timeout(Duration::from_secs(10))
            .context("timed out waiting for MCP response")??;
        serde_json::from_str(&line).context("invalid JSON-RPC response")
    }

    fn call_tool(&mut self, name: &str, arguments: Value) -> Result<Value> {
        let response = self.request(
            "tools/call",
            json!({ "name": name, "arguments": arguments }),
        )?;
        if let Some(error) = response.get("error") {
            bail!("tool {name} returned JSON-RPC error: {error}");
        }
        let text = response
            .pointer("/result/content/0/text")
            .and_then(Value::as_str)
            .context("missing tool content text")?;
        serde_json::from_str(text).context("tool text is not JSON")
    }

    fn close_gracefully(mut self) -> Result<()> {
        drop(self.child.stdin.take());
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.child.try_wait()?.is_none() {
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let _ = self.child.wait();
                bail!("proxy did not exit cleanly after stdin EOF");
            }
            thread::sleep(Duration::from_millis(20));
        }
        let status = self.child.wait()?;
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
        assert!(status.success(), "proxy exited with {status}");
        Ok(())
    }
}

impl Drop for McpSession {
    fn drop(&mut self) {
        drop(self.child.stdin.take());
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

fn pid_exists(pid: u32) -> bool {
    let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if rc == 0 {
        return true;
    }
    let errno = std::io::Error::last_os_error().raw_os_error();
    errno != Some(libc::ESRCH)
}

fn wait_pid_gone(pid: u32, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    while pid_exists(pid) {
        if Instant::now() >= deadline {
            bail!("PID {pid} was still alive after {}s", timeout.as_secs());
        }
        thread::sleep(Duration::from_millis(25));
    }
    Ok(())
}

fn build_fixture(root: &Path, filename: &str, source: &str) -> Result<()> {
    fs::create_dir_all(root)?;
    fs::write(root.join(filename), source)?;
    let mut cfg = TsIndexConfig::default();
    cfg.languages.include = vec!["python".into()];
    cfg.write(root)?;
    let out = Command::new(BIN)
        .arg("--root")
        .arg(root)
        .arg("build")
        .output()?;
    assert!(
        out.status.success(),
        "build failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(())
}

fn daemon_json(socket: &Path, args: &[&str]) -> Result<Value> {
    let output = Command::new(BIN)
        .arg("--socket")
        .arg(socket)
        .arg("--json")
        .args(args)
        .output()?;
    assert!(
        output.status.success(),
        "command {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).context("invalid JSON output")
}

#[test]
fn serve_mcp_fails_fast_when_daemon_is_not_running() -> Result<()> {
    let dir = tempdir_in("/tmp")?;
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
    let missing_socket = dir.path().join("missing.sock");
    let workspace = dir.path().join("repo");
    fs::create_dir_all(&workspace)?;

    let output = Command::new(BIN)
        .arg("--socket")
        .arg(&missing_socket)
        .arg("--root")
        .arg(&workspace)
        .args(["serve", "--mcp", "--via-daemon"])
        .stdin(Stdio::null())
        .output()?;

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("orchestrator daemon is not running"),
        "{stderr}"
    );
    assert!(stderr.contains("tsindex daemon start"), "{stderr}");
    assert!(stderr.contains("tsindex daemon install"), "{stderr}");
    assert!(
        !workspace.join(".tsindex").exists(),
        "proxy must not initialize a workspace when daemon is down"
    );
    Ok(())
}

#[test]
fn daemon_starts_idle_without_sessions_or_watchers() -> Result<()> {
    let daemon = common::TestDaemon::start()?;

    let status = daemon_json(&daemon.socket, &["daemon", "status"])?;
    assert_eq!(status["running"], true);
    assert_eq!(status["daemon"]["daemon_pid"], daemon.pid());
    assert_eq!(status["daemon"]["active_sessions"], 0);
    assert_eq!(status["daemon"]["total_sessions_started"], 0);

    let sessions = daemon_json(&daemon.socket, &["daemon", "sessions"])?;
    assert_eq!(sessions["active"].as_array().unwrap().len(), 0);
    assert_eq!(sessions["recent"].as_array().unwrap().len(), 0);
    Ok(())
}

#[test]
fn orchestrator_dispatches_isolated_workers_for_two_worktree_sessions() -> Result<()> {
    let daemon = common::TestDaemon::start()?;
    let dir = tempdir_in("/tmp")?;
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
    let main = dir.path().join("main");
    let wt = dir.path().join("wt-feature");

    // Set up a main checkout and a sibling linked Git worktree with diverging symbols.
    fs::create_dir_all(main.join(".git/worktrees/wt-feature"))?;
    fs::create_dir_all(&wt)?;
    let wt_meta = main.join(".git/worktrees/wt-feature");
    fs::write(wt.join(".git"), format!("gitdir: {}\n", wt_meta.display()))?;
    fs::write(wt_meta.join("commondir"), "../..\n")?;
    fs::write(
        wt_meta.join("gitdir"),
        format!("{}\n", wt.join(".git").display()),
    )?;

    build_fixture(&main, "app.py", "def from_main():\n    return 1\n")?;
    build_fixture(&wt, "app.py", "def from_worktree():\n    return 2\n")?;

    let mut s1 = McpSession::connect(&daemon.socket, &main, "codex-harness")?;
    let mut s2 = McpSession::connect(&daemon.socket, &wt, "claude-harness")?;

    let main_hit = s1.call_tool("get_symbol", json!({ "names": ["from_main"] }))?;
    assert_eq!(main_hit["matches"].as_array().unwrap().len(), 1);
    let main_miss = s1.call_tool("get_symbol", json!({ "names": ["from_worktree"] }))?;
    assert_eq!(main_miss["matches"].as_array().unwrap().len(), 0);

    let wt_hit = s2.call_tool("get_symbol", json!({ "names": ["from_worktree"] }))?;
    assert_eq!(wt_hit["matches"].as_array().unwrap().len(), 1);
    let wt_miss = s2.call_tool("get_symbol", json!({ "names": ["from_main"] }))?;
    assert_eq!(wt_miss["matches"].as_array().unwrap().len(), 0);

    let sessions = daemon_json(&daemon.socket, &["daemon", "sessions"])?;
    let active = sessions["active"].as_array().unwrap();
    assert_eq!(active.len(), 2, "{sessions}");
    let pid1 = active[0]["worker_pid"].as_u64().unwrap() as u32;
    let pid2 = active[1]["worker_pid"].as_u64().unwrap() as u32;
    assert_ne!(pid1, pid2, "each session must have its own worker process");
    assert_ne!(active[0]["session_id"], active[1]["session_id"]);

    let harnesses: Vec<&str> = active
        .iter()
        .filter_map(|s| s["harness_name"].as_str())
        .collect();
    assert!(harnesses.contains(&"codex-harness"), "{active:?}");
    assert!(harnesses.contains(&"claude-harness"), "{active:?}");
    for entry in active {
        assert_eq!(entry["tool_calls"].as_u64(), Some(2), "{entry}");
    }

    // Closing session 1 must reap only worker 1 while session 2 stays healthy.
    let s1_worker = active
        .iter()
        .find(|s| s["harness_name"] == "codex-harness")
        .unwrap()["worker_pid"]
        .as_u64()
        .unwrap() as u32;
    s1.close_gracefully()?;
    daemon.wait_active_sessions(1, Duration::from_secs(5))?;
    wait_pid_gone(s1_worker, Duration::from_secs(5))?;

    let wt_still_works = s2.call_tool("get_symbol", json!({ "names": ["from_worktree"] }))?;
    assert_eq!(wt_still_works["matches"].as_array().unwrap().len(), 1);

    let all = daemon_json(&daemon.socket, &["daemon", "sessions", "--all"])?;
    assert_eq!(all["active"].as_array().unwrap().len(), 1);
    let recent = all["recent"].as_array().unwrap();
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0]["state"], "exited");
    assert_eq!(recent[0]["exit_reason"], "client_eof");

    s2.close_gracefully()?;
    daemon.wait_active_sessions(0, Duration::from_secs(5))?;
    Ok(())
}

#[test]
fn proxy_sigkill_immediately_tears_down_and_reaps_worker() -> Result<()> {
    let daemon = common::TestDaemon::start()?;
    let dir = tempdir_in("/tmp")?;
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
    let repo = dir.path().join("repo");
    build_fixture(&repo, "lib.py", "def alive():\n    return True\n")?;

    let mut session = McpSession::connect(&daemon.socket, &repo, "crash-test")?;
    let sessions = daemon_json(&daemon.socket, &["daemon", "sessions"])?;
    let worker_pid = sessions["active"][0]["worker_pid"].as_u64().unwrap() as u32;
    assert!(pid_exists(worker_pid));

    // Simulate a hard-killed harness/proxy (SIGKILL).
    session.child.kill()?;
    session.child.wait()?;

    daemon.wait_active_sessions(0, Duration::from_secs(5))?;
    wait_pid_gone(worker_pid, Duration::from_secs(5))?;
    Ok(())
}

#[test]
fn daemon_kill_terminates_single_session_and_kill_all_clears_remaining() -> Result<()> {
    let daemon = common::TestDaemon::start()?;
    let dir = tempdir_in("/tmp")?;
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
    let repo = dir.path().join("repo");
    build_fixture(&repo, "lib.py", "def target():\n    return 42\n")?;

    let mut s1 = McpSession::connect(&daemon.socket, &repo, "first")?;
    let mut s2 = McpSession::connect(&daemon.socket, &repo, "second")?;
    let s3 = McpSession::connect(&daemon.socket, &repo, "third")?;

    let sessions = daemon_json(&daemon.socket, &["daemon", "sessions"])?;
    let active = sessions["active"].as_array().unwrap();
    assert_eq!(active.len(), 3);
    let first = active
        .iter()
        .find(|s| s["harness_name"] == "first")
        .unwrap();
    let first_id = first["session_id"].as_str().unwrap().to_string();
    let first_worker = first["worker_pid"].as_u64().unwrap() as u32;

    let killed = daemon_json(&daemon.socket, &["daemon", "kill", &first_id])?;
    assert_eq!(killed["killed"], json!([first_id]));

    daemon.wait_active_sessions(2, Duration::from_secs(5))?;
    wait_pid_gone(first_worker, Duration::from_secs(5))?;
    // Proxy 1 must also exit cleanly when the orchestrator closes its socket.
    let deadline = Instant::now() + Duration::from_secs(5);
    while s1.child.try_wait()?.is_none() {
        assert!(Instant::now() < deadline, "killed session proxy lingered");
        thread::sleep(Duration::from_millis(20));
    }

    // Surviving sessions remain responsive.
    let hit = s2.call_tool("get_symbol", json!({ "names": ["target"] }))?;
    assert_eq!(hit["matches"].as_array().unwrap().len(), 1);

    let killed_all = daemon_json(&daemon.socket, &["daemon", "kill", "--all"])?;
    assert_eq!(killed_all["killed"].as_array().unwrap().len(), 2);
    daemon.wait_active_sessions(0, Duration::from_secs(5))?;
    drop(s2);
    drop(s3);
    Ok(())
}

#[test]
fn daemon_start_restart_and_stop_manage_detached_lifecycle_and_reap_workers() -> Result<()> {
    let dir = tempdir_in("/tmp")?;
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
    let socket = dir.path().join("detached.sock");
    let repo = dir.path().join("repo");
    build_fixture(&repo, "lib.py", "def booted():\n    return 1\n")?;

    // Ensure the detached daemon is always stopped even if an assertion fails.
    struct DetachedGuard(std::path::PathBuf);
    impl Drop for DetachedGuard {
        fn drop(&mut self) {
            let _ = Command::new(BIN)
                .arg("--socket")
                .arg(&self.0)
                .args(["daemon", "stop"])
                .output();
        }
    }
    let _guard = DetachedGuard(socket.clone());

    let started = daemon_json(&socket, &["daemon", "start"])?;
    let pid1 = started["daemon_pid"].as_u64().unwrap() as u32;
    assert_eq!(started["active_sessions"], 0);

    // `daemon start` is idempotent.
    let started_again = daemon_json(&socket, &["daemon", "start"])?;
    assert_eq!(started_again["daemon_pid"].as_u64().unwrap() as u32, pid1);

    // Attach two live sessions and verify `daemon restart` replaces the daemon
    // and cleanly terminates the previous workers.
    let mut s1 = McpSession::connect(&socket, &repo, "before-restart")?;
    let active_before = daemon_json(&socket, &["daemon", "sessions"])?;
    let worker_before = active_before["active"][0]["worker_pid"].as_u64().unwrap() as u32;

    let restarted = daemon_json(&socket, &["daemon", "restart"])?;
    let pid2 = restarted["daemon_pid"].as_u64().unwrap() as u32;
    assert_ne!(pid1, pid2);
    wait_pid_gone(worker_before, Duration::from_secs(5))?;
    drop(s1.child.wait());

    let mut s2 = McpSession::connect(&socket, &repo, "after-restart")?;
    let hit = s2.call_tool("get_symbol", json!({ "names": ["booted"] }))?;
    assert_eq!(hit["matches"].as_array().unwrap().len(), 1);
    let active_after = daemon_json(&socket, &["daemon", "sessions"])?;
    let worker_after = active_after["active"][0]["worker_pid"].as_u64().unwrap() as u32;

    let stopped = daemon_json(&socket, &["daemon", "stop"])?;
    assert_eq!(stopped["stopped"], true);
    wait_pid_gone(pid2, Duration::from_secs(5))?;
    wait_pid_gone(worker_after, Duration::from_secs(5))?;
    assert!(
        !socket.exists(),
        "socket must be cleaned up on graceful stop"
    );

    let status = daemon_json(&socket, &["daemon", "status"])?;
    assert_eq!(status["running"], false);
    drop(s2);
    Ok(())
}

#[test]
fn daemon_rejects_duplicate_instance_and_recovers_stale_socket_after_hard_kill() -> Result<()> {
    let mut daemon = common::TestDaemon::start()?;
    let socket = daemon.socket.clone();
    let dir = tempdir_in("/tmp")?;
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
    let repo = dir.path().join("repo");
    build_fixture(&repo, "lib.py", "def recovered():\n    return 7\n")?;

    // A second orchestrator on the same socket must refuse to start.
    let dup = Command::new(BIN)
        .arg("--socket")
        .arg(&socket)
        .args(["daemon", "run"])
        .output()?;
    assert!(!dup.status.success());
    let dup_err = String::from_utf8_lossy(&dup.stderr);
    assert!(dup_err.contains("already running"), "{dup_err}");

    // Attach a session, then SIGKILL the orchestrator leaving a stale socket.
    let s1 = McpSession::connect(&socket, &repo, "before-crash")?;
    let active = daemon_json(&socket, &["daemon", "sessions"])?;
    let worker_pid = active["active"][0]["worker_pid"].as_u64().unwrap() as u32;

    daemon.kill_hard()?;
    assert!(socket.exists(), "SIGKILL leaves a stale socket on disk");
    // The orphaned worker must exit promptly when its stdin pipe breaks.
    wait_pid_gone(worker_pid, Duration::from_secs(5))?;
    drop(s1);

    // Starting a new orchestrator at the same socket must safely recover it.
    struct RecoveryGuard(std::path::PathBuf);
    impl Drop for RecoveryGuard {
        fn drop(&mut self) {
            let _ = Command::new(BIN)
                .arg("--socket")
                .arg(&self.0)
                .args(["daemon", "stop"])
                .output();
        }
    }
    let _recovery_guard = RecoveryGuard(socket.clone());

    let started = daemon_json(&socket, &["daemon", "start"])?;
    assert_eq!(started["active_sessions"], 0);

    let mut s2 = McpSession::connect(&socket, &repo, "after-crash")?;
    let hit = s2.call_tool("get_symbol", json!({ "names": ["recovered"] }))?;
    assert_eq!(hit["matches"].as_array().unwrap().len(), 1);
    s2.close_gracefully()?;
    Ok(())
}

fn raw_session(socket: &Path, root: &Path) -> Result<(std::os::unix::net::UnixStream, Value)> {
    use std::os::unix::net::UnixStream;
    let mut stream = UnixStream::connect(socket)?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    writeln!(
        stream,
        "{}",
        json!({
            "op": "start_session", "protocol": tsindex::daemon::PROTOCOL_VERSION,
            "client_version": env!("CARGO_PKG_VERSION"), "root": root,
            "client_pid": std::process::id(), "no_refresh": true,
        })
    )?;
    let mut line = String::new();
    BufReader::new(&mut stream).read_line(&mut line)?;
    let ready: Value = serde_json::from_str(&line)?;
    Ok((stream, ready))
}

#[test]
fn killing_stopped_worker_with_full_stdin_does_not_wait_for_pipe_progress() -> Result<()> {
    let daemon = common::TestDaemon::start()?;
    let dir = tempdir_in("/tmp")?;
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
    build_fixture(dir.path(), "lib.py", "def target():\n    return 1\n")?;
    let (mut stream, ready) = raw_session(&daemon.socket, dir.path())?;
    let pid = ready["worker_pid"].as_u64().context("worker not ready")? as u32;
    let id = ready["session_id"].as_str().unwrap();
    assert_eq!(unsafe { libc::kill(pid as libc::pid_t, libc::SIGSTOP) }, 0);
    // The request exceeds pipe capacity while staying below the frame limit.
    stream.set_write_timeout(Some(Duration::from_secs(3)))?;
    writeln!(
        stream,
        "{}",
        json!({"jsonrpc":"2.0","id":1,"method":"ping","params":{"padding":"x".repeat(1024*1024)}})
    )?;
    thread::sleep(Duration::from_millis(100));
    let start = Instant::now();
    daemon_json(&daemon.socket, &["daemon", "kill", id])?;
    daemon.wait_active_sessions(0, Duration::from_secs(5))?;
    wait_pid_gone(pid, Duration::from_secs(1))?;
    assert!(start.elapsed() < Duration::from_secs(5));
    Ok(())
}

#[test]
fn worker_config_errors_are_returned_before_session_ready() -> Result<()> {
    let daemon = common::TestDaemon::start()?;
    let dir = tempdir_in("/tmp")?;
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
    fs::create_dir(dir.path().join(".tsindex"))?;
    fs::write(dir.path().join(".tsindex/config.toml"), "invalid = [")?;
    let output = Command::new(BIN)
        .arg("--socket")
        .arg(&daemon.socket)
        .arg("--root")
        .arg(dir.path())
        .args(["serve", "--mcp", "--via-daemon"])
        .stdin(Stdio::null())
        .output()?;
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("initialization") && stderr.contains("TOML"),
        "{stderr}"
    );
    daemon.wait_active_sessions(0, Duration::from_secs(5))?;
    Ok(())
}

#[test]
fn concurrent_session_admission_reserves_the_last_slot() -> Result<()> {
    use std::sync::{Arc, Barrier};
    let daemon = common::TestDaemon::start_with_env(&[("TSINDEX_MAX_SESSIONS", "1")])?;
    let dir = tempdir_in("/tmp")?;
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
    build_fixture(dir.path(), "lib.py", "def target():\n    return 1\n")?;
    let barrier = Arc::new(Barrier::new(12));
    let attempts: Vec<_> = (0..12)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            let socket = daemon.socket.clone();
            let root = dir.path().to_path_buf();
            thread::spawn(move || {
                barrier.wait();
                raw_session(&socket, &root)
            })
        })
        .collect();
    let sessions: Vec<_> = attempts
        .into_iter()
        .map(|join| join.join().unwrap())
        .collect::<Result<_>>()?;
    assert_eq!(
        sessions
            .iter()
            .filter(|(_, reply)| reply["kind"] == "session_ready")
            .count(),
        1
    );
    assert_eq!(daemon.ping()?.active_sessions, 1);
    drop(sessions);
    daemon.wait_active_sessions(0, Duration::from_secs(5))?;
    Ok(())
}

#[test]
fn custom_socket_refuses_regular_files_and_keeps_existing_permissions() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempdir_in("/tmp")?;
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
    let socket = dir.path().join("keep.sock");
    let sibling = dir.path().join("keep.json");
    fs::write(&socket, "keep endpoint")?;
    fs::write(&sibling, "keep sibling")?;
    let output = Command::new(BIN)
        .arg("--socket")
        .arg(&socket)
        .args(["daemon", "run"])
        .output()?;
    assert!(!output.status.success());
    assert_eq!(fs::read_to_string(&socket)?, "keep endpoint");
    assert_eq!(fs::read_to_string(&sibling)?, "keep sibling");
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o755))?;
    let output = Command::new(BIN)
        .arg("--socket")
        .arg(&socket)
        .args(["daemon", "run"])
        .output()?;
    assert!(!output.status.success());
    assert_eq!(
        fs::metadata(dir.path())?.permissions().mode() & 0o777,
        0o755
    );
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn atomic_executable_replacement_keeps_worker_startup_available() -> Result<()> {
    use tsindex::daemon::{DaemonClient, DaemonPaths};
    let dir = tempdir_in("/tmp")?;
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
    let exe = dir.path().join("tsindex");
    let socket = dir.path().join("d.sock");
    fs::copy(BIN, &exe)?;
    let mut daemon = Command::new(&exe)
        .arg("--socket")
        .arg(&socket)
        .args(["daemon", "run"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    struct Guard<'a>(&'a mut Child);
    impl Drop for Guard<'_> {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let _guard = Guard(&mut daemon);
    let client = DaemonClient::new(DaemonPaths::resolve(Some(&socket))?);
    let deadline = Instant::now() + Duration::from_secs(5);
    while client.ping().is_err() {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(20));
    }
    fs::copy(BIN, dir.path().join("replacement"))?;
    fs::rename(dir.path().join("replacement"), &exe)?;
    let root = dir.path().join("repo");
    build_fixture(&root, "lib.py", "def target():\n    return 1\n")?;
    let (stream, ready) = raw_session(&socket, &root)?;
    assert_eq!(ready["kind"], "session_ready", "{ready}");
    drop(stream);
    client.shutdown()?;
    Ok(())
}

#[test]
fn session_listing_pages_bounded_summaries_and_keeps_detail_separate() -> Result<()> {
    let daemon = common::TestDaemon::start()?;
    let dir = tempdir_in("/tmp")?;
    build_fixture(dir.path(), "lib.py", "def target():\n    return 1\n")?;
    let mut streams = Vec::new();
    for _ in 0..12 {
        let (mut stream, ready) = raw_session(&daemon.socket, dir.path())?;
        assert_eq!(ready["kind"], "session_ready");
        writeln!(
            stream,
            "{}",
            json!({"jsonrpc":"2.0", "id":1, "method":"initialize", "params":{
                "protocolVersion":"2024-11-05", "capabilities":{},
                "clientInfo":{"name":"x".repeat(16*1024), "version":"v".repeat(4096)}
            }})
        )?;
        let mut line = String::new();
        BufReader::new(&mut stream).read_line(&mut line)?;
        assert!(
            serde_json::from_str::<Value>(&line)?
                .get("result")
                .is_some()
        );
        streams.push(stream);
    }
    let sessions = daemon_json(&daemon.socket, &["daemon", "sessions"])?;
    let active = sessions["active"].as_array().unwrap();
    assert_eq!(active.len(), 12);
    for session in active {
        assert!(session["harness_name"].as_str().unwrap().len() <= 256);
        assert!(session.get("recent_stderr").is_none());
    }
    drop(streams);
    daemon.wait_active_sessions(0, Duration::from_secs(5))?;
    Ok(())
}

#[test]
fn oversized_mcp_frame_ends_only_its_session_and_leaves_daemon_responsive() -> Result<()> {
    let daemon = common::TestDaemon::start()?;
    let dir = tempdir_in("/tmp")?;
    build_fixture(dir.path(), "lib.py", "def target():\n    return 1\n")?;
    let (mut stream, ready) = raw_session(&daemon.socket, dir.path())?;
    assert_eq!(ready["kind"], "session_ready");
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let oversized = vec![b'x'; tsindex::daemon::MAX_MCP_FRAME_BYTES + 1];
    // Early rejection may close the transport before write_all completes.
    let _ = stream.write_all(&oversized);
    daemon.wait_active_sessions(0, Duration::from_secs(5))?;
    assert_eq!(daemon.ping()?.active_sessions, 0);
    Ok(())
}

#[test]
fn refused_existing_state_is_preserved_by_startup_cleanup() -> Result<()> {
    use tsindex::daemon::DaemonPaths;
    let dir = tempdir_in("/tmp")?;
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
    let socket = dir.path().join("d.sock");
    let paths = DaemonPaths::resolve(Some(&socket))?;
    let metadata = paths.lock_path.parent().unwrap();
    fs::create_dir(metadata)?;
    fs::set_permissions(metadata, fs::Permissions::from_mode(0o700))?;
    fs::write(metadata.join("owner"), "tsindex daemon runtime v2\n")?;
    fs::write(&paths.state_path, "preserve this unrelated state")?;
    let output = Command::new(BIN)
        .arg("--socket")
        .arg(&socket)
        .args(["daemon", "run"])
        .output()?;
    assert!(!output.status.success());
    assert_eq!(
        fs::read_to_string(&paths.state_path)?,
        "preserve this unrelated state"
    );
    assert!(!socket.exists());
    Ok(())
}

fn outcome_watch(
    socket: &Path,
    session_id: &str,
) -> Result<BufReader<std::os::unix::net::UnixStream>> {
    let mut stream = std::os::unix::net::UnixStream::connect(socket)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    writeln!(
        stream,
        "{}",
        json!({"op":"watch_session", "session_id":session_id})
    )?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    assert_eq!(serde_json::from_str::<Value>(&line)?["kind"], "watching");
    Ok(reader)
}

#[test]
fn outcome_watcher_receives_final_state_after_daemon_stops_accepting() -> Result<()> {
    let daemon = common::TestDaemon::start()?;
    let dir = tempdir_in("/tmp")?;
    build_fixture(dir.path(), "lib.py", "def target():\n    return 1\n")?;
    let (stream, ready) = raw_session(&daemon.socket, dir.path())?;
    let worker_pid = ready["worker_pid"].as_u64().unwrap() as u32;
    let mut watcher = outcome_watch(&daemon.socket, ready["session_id"].as_str().unwrap())?;
    let start = Instant::now();
    daemon.client()?.shutdown()?;
    let mut line = String::new();
    watcher.read_line(&mut line)?;
    let outcome: Value = serde_json::from_str(&line)?;
    assert_eq!(outcome["kind"], "session");
    assert_eq!(outcome["session"]["state"], "exited");
    assert_eq!(outcome["session"]["exit_reason"], "daemon_shutdown");
    assert!(outcome["session"].get("recent_stderr").is_none());
    wait_pid_gone(worker_pid, Duration::from_secs(1))?;
    #[cfg(target_os = "linux")]
    assert!(
        start.elapsed() < Duration::from_millis(1500),
        "empty process group paid a full TERM grace period"
    );
    #[cfg(not(target_os = "linux"))]
    let _ = start;
    drop(stream);
    Ok(())
}

#[test]
fn active_session_accepts_only_one_outcome_watcher() -> Result<()> {
    let daemon = common::TestDaemon::start()?;
    let dir = tempdir_in("/tmp")?;
    build_fixture(dir.path(), "lib.py", "def target():\n    return 1\n")?;
    let (stream, ready) = raw_session(&daemon.socket, dir.path())?;
    let id = ready["session_id"].as_str().unwrap();
    let mut watcher = outcome_watch(&daemon.socket, id)?;
    let mut duplicate = std::os::unix::net::UnixStream::connect(&daemon.socket)?;
    duplicate.set_read_timeout(Some(Duration::from_secs(5)))?;
    writeln!(
        duplicate,
        "{}",
        json!({"op":"watch_session", "session_id":id})
    )?;
    let mut line = String::new();
    BufReader::new(duplicate).read_line(&mut line)?;
    let denied: Value = serde_json::from_str(&line)?;
    assert_eq!(denied["kind"], "error");
    assert!(denied["error"].as_str().unwrap().contains("already has"));
    daemon.client()?.kill_session(id.to_string())?;
    line.clear();
    watcher.read_line(&mut line)?;
    assert_eq!(
        serde_json::from_str::<Value>(&line)?["session"]["exit_reason"],
        "killed_by_user"
    );
    drop(stream);
    Ok(())
}

#[test]
fn older_protocol_is_rejected_before_worker_spawn() -> Result<()> {
    let daemon = common::TestDaemon::start()?;
    let dir = tempdir_in("/tmp")?;
    let mut stream = std::os::unix::net::UnixStream::connect(&daemon.socket)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    writeln!(
        stream,
        "{}",
        json!({
            "op":"start_session", "protocol":tsindex::daemon::PROTOCOL_VERSION - 1,
            "client_version":env!("CARGO_PKG_VERSION"), "root":dir.path(),
            "client_pid":std::process::id(), "no_refresh":true,
        })
    )?;
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line)?;
    let response: Value = serde_json::from_str(&line)?;
    assert_eq!(response["kind"], "error");
    assert!(
        response["error"]
            .as_str()
            .unwrap()
            .contains("protocol mismatch")
    );
    assert_eq!(daemon.ping()?.total_sessions_started, 0);
    Ok(())
}

#[test]
fn daemon_recovers_empty_metadata_directory_and_ignores_old_state_temp() -> Result<()> {
    use tsindex::daemon::DaemonPaths;
    let dir = tempdir_in("/tmp")?;
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
    let socket = dir.path().join("d.sock");
    let paths = DaemonPaths::resolve(Some(&socket))?;
    let metadata = paths.lock_path.parent().unwrap();
    fs::create_dir(metadata)?;
    fs::set_permissions(metadata, fs::Permissions::from_mode(0o700))?;
    // This is exactly the crash window between mkdir and marker publication.
    let started = daemon_json(&socket, &["daemon", "start"])?;
    assert_eq!(started["active_sessions"], 0);
    daemon_json(&socket, &["daemon", "stop"])?;
    let old_temp = paths
        .state_path
        .with_extension(format!("tmp.{}", std::process::id()));
    fs::write(&old_temp, "old incomplete publication")?;
    let started = daemon_json(&socket, &["daemon", "start"])?;
    assert_eq!(started["active_sessions"], 0);
    daemon_json(&socket, &["daemon", "stop"])?;
    assert_eq!(fs::read_to_string(old_temp)?, "old incomplete publication");
    Ok(())
}

#[test]
fn session_detail_prunes_escaped_stderr_to_the_encoded_frame_budget() -> Result<()> {
    use std::os::unix::net::UnixStream;
    use tsindex::daemon::{DaemonClient, DaemonPaths, DaemonRequest, StartSessionRequest};

    let dir = tempdir_in("/tmp")?;
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
    let exe = dir.path().join("tsindex");
    let socket = dir.path().join("d.sock");
    let root = dir.path().join("repo");
    fs::create_dir(&root)?;
    fs::copy(BIN, &exe)?;
    let child = Command::new(&exe)
        .arg("--socket")
        .arg(&socket)
        .args(["daemon", "run"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    struct DaemonGuard(Child);
    impl Drop for DaemonGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let _guard = DaemonGuard(child);
    let client = DaemonClient::new(DaemonPaths::resolve(Some(&socket))?);
    let deadline = Instant::now() + Duration::from_secs(5);
    while client.ping().is_err() {
        assert!(Instant::now() < deadline, "copied daemon did not start");
        thread::sleep(Duration::from_millis(10));
    }

    // Replace only this fixture's worker path. Controlled shell literals emit
    // arbitrary diagnostics while the unchanged daemon handles the actual
    // capture, retention, SessionDetail pruning, and wire framing.
    let readiness = json!({
        "protocol": tsindex::daemon::PROTOCOL_VERSION,
        "version": env!("CARGO_PKG_VERSION"),
    });
    let mut script = format!("#!/bin/sh\nprintf '%s\\n' '{readiness}'\n");
    let mut original_stderr = Vec::new();
    for index in 0..12 {
        let diagnostic = format!(
            "older-{index:02}:{}{}",
            "\u{1}".repeat(1300),
            "\"\\".repeat(200)
        );
        script.push_str(&format!("printf '%s\\n' '{diagnostic}' >&2\n"));
        original_stderr.push(diagnostic);
    }
    let newest = format!(
        "newest diagnostic \"quoted\" \\escaped\\ {}",
        "\u{2}".repeat(1000)
    );
    script.push_str(&format!("printf '%s\\n' '{newest}' >&2\n"));
    original_stderr.push(newest.clone());
    script.push_str(
        "while IFS= read -r request; do\n\
         printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}'\n\
         done\n",
    );
    let helper = dir.path().join("helper");
    fs::write(&helper, script)?;
    fs::set_permissions(&helper, fs::Permissions::from_mode(0o700))?;
    fs::rename(helper, &exe)?;

    let mut request = StartSessionRequest {
        protocol: tsindex::daemon::PROTOCOL_VERSION,
        client_version: env!("CARGO_PKG_VERSION").to_string(),
        root,
        db: None,
        languages: Vec::new(),
        no_refresh: true,
        client_pid: std::process::id(),
        owner_pid: None,
        owner_explicit: false,
    };
    let metadata_entry = "quote\"backslash\\".repeat(10);
    while serde_json::to_vec(&request)?.len() + serde_json::to_vec(&metadata_entry)?.len() + 1
        < 16 * 1024 - 128
    {
        request.languages.push(metadata_entry.clone());
    }
    assert!(serde_json::to_vec(&request)?.len() > 15 * 1024);
    assert!(serde_json::to_vec(&original_stderr)?.len() > tsindex::daemon::MAX_CONTROL_LINE_BYTES);
    let mut stream = UnixStream::connect(&socket)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    writeln!(
        stream,
        "{}",
        serde_json::to_string(&DaemonRequest::StartSession(request))?
    )?;
    let mut data = BufReader::new(stream);
    let mut line = String::new();
    data.read_line(&mut line)?;
    let ready: Value = serde_json::from_str(&line)?;
    assert_eq!(ready["kind"], "session_ready", "{ready}");
    let session_id = ready["session_id"].as_str().unwrap();
    writeln!(
        data.get_mut(),
        "{}",
        json!({"jsonrpc":"2.0","id":1,"method":"ping"})
    )?;
    line.clear();
    data.read_line(&mut line)?;
    assert_eq!(serde_json::from_str::<Value>(&line)?["result"], json!({}));

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut control = UnixStream::connect(&socket)?;
        control.set_read_timeout(Some(Duration::from_secs(5)))?;
        writeln!(
            control,
            "{}",
            json!({"op":"session_detail", "session_id":session_id})
        )?;
        let mut frame = String::new();
        BufReader::new(control).read_line(&mut frame)?;
        assert!(frame.len() <= tsindex::daemon::MAX_CONTROL_LINE_BYTES);
        let detail: Value = serde_json::from_str(&frame)?;
        let diagnostics = detail["session"]["recent_stderr"].as_array().unwrap();
        if diagnostics.last().and_then(Value::as_str) == Some(newest.as_str()) {
            assert!(diagnostics.len() < original_stderr.len());
            assert!(
                diagnostics
                    .iter()
                    .any(|line| line.as_str().unwrap().contains('\u{1}'))
            );
            assert!(
                diagnostics
                    .iter()
                    .any(|line| line.as_str().unwrap().contains("\"\\"))
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "newest diagnostic did not reach the bounded detail response"
        );
        thread::sleep(Duration::from_millis(10));
    }
    drop(data);
    client.shutdown()?;
    Ok(())
}
