//! End-to-end tests for the orchestrator daemon, thin MCP proxy, per-session
//! worker isolation, and deterministic cleanup.

#![cfg(unix)]

mod common;

use std::fs;
use std::io::{BufRead, BufReader, Write};
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
            .args(["--no-refresh", "serve", "--mcp"])
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
    let missing_socket = dir.path().join("missing.sock");
    let workspace = dir.path().join("repo");
    fs::create_dir_all(&workspace)?;

    let output = Command::new(BIN)
        .arg("--socket")
        .arg(&missing_socket)
        .arg("--root")
        .arg(&workspace)
        .args(["serve", "--mcp"])
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
