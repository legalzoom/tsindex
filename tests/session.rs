#![cfg(any(target_os = "linux", target_os = "macos"))]

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::{PermissionsExt, symlink};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

const BIN: &str = env!("CARGO_BIN_EXE_tsindex");

struct Fixture {
    _temp: TempDir,
    root: PathBuf,
    state: PathBuf,
}

impl Fixture {
    fn new() -> Result<Self> {
        // Keep AF_UNIX paths short on macOS; canonicalize /tmp's symlink.
        let temp = tempfile::Builder::new().prefix("tsi-").tempdir_in("/tmp")?;
        let root = temp.path().canonicalize()?;
        let state = root.join("state");
        fs::write(root.join("a.py"), "def alpha():\n    return 1\n")?;
        let output = Command::new(BIN)
            .arg("--root")
            .arg(&root)
            .args(["--languages", "python", "build"])
            .output()?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(Self {
            _temp: temp,
            root,
            state,
        })
    }

    fn command(&self) -> Command {
        let mut command = Command::new(BIN);
        command
            .env("TSINDEX_SESSION_DIR", &self.state)
            .env("TSINDEX_HOME", self.root.join("telemetry"));
        command
    }

    fn server(&self, scoped: bool, refresh: bool, wrapper: bool) -> Result<Server> {
        let mut command = if wrapper {
            let mut command = Command::new("sh");
            command.args(["-c", "exec 3<&0; \"$@\" <&3 & wait", "parent", BIN]);
            command
                .env("TSINDEX_SESSION_DIR", &self.state)
                .env("TSINDEX_HOME", self.root.join("telemetry"));
            command
        } else {
            self.command()
        };
        command
            .arg("--root")
            .arg(&self.root)
            .args(["--languages", "python"]);
        if !refresh {
            command.arg("--no-refresh");
        }
        command.args(["serve", "--mcp"]);
        if scoped {
            command.arg("--session-lifecycle");
        }
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let (logs_tx, logs) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stderr).lines() {
                if logs_tx.send(line.unwrap()).is_err() {
                    break;
                }
            }
        });
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let line = line.expect("read MCP stdout");
                let value = serde_json::from_str(&line).expect("protocol stdout is JSON only");
                if tx.send(value).is_err() {
                    break;
                }
            }
        });
        let mut server = Server { child, rx, logs };
        assert!(server.request(json!({"id": 0, "method": "initialize"}))["result"].is_object());
        Ok(server)
    }

    fn end(&self, id: &str) -> Result<Output> {
        let mut child = self
            .command()
            .args(["session", "end", "--from-stdin"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        writeln!(
            child.stdin.take().unwrap(),
            "{}",
            json!({"hook_event_name":"SessionEnd", "session_id":id})
        )?;
        let output = child.wait_with_output()?;
        if !output.status.success() {
            eprintln!("{}", String::from_utf8_lossy(&output.stderr));
        }
        Ok(output)
    }

    fn records(&self, id: &str) -> Result<Vec<PathBuf>> {
        let name = format!("s-{:x}.json", Sha256::digest(id.as_bytes()));
        Ok(fs::read_dir(&self.state)?
            .filter_map(|e| e.ok())
            .map(|e| e.path().join(&name))
            .filter(|p| p.exists())
            .collect())
    }
}

struct Server {
    child: Child,
    rx: Receiver<Value>,
    logs: Receiver<String>,
}

impl Server {
    fn send(&mut self, request: Value) {
        writeln!(self.child.stdin.as_mut().unwrap(), "{request}").unwrap();
    }

    fn request(&mut self, mut request: Value) -> Value {
        request["jsonrpc"] = json!("2.0");
        self.send(request);
        self.rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|error| {
                panic!(
                    "MCP response within deadline: {error}; stderr: {:?}",
                    self.logs.try_iter().collect::<Vec<_>>()
                )
            })
    }

    fn register(&mut self, id: &str) {
        let response = self.request(json!({"id":1,"method":"tools/call","params":{"name":"register_session","arguments":{"session_id":id}}}));
        assert!(response.get("error").is_none(), "{response}");
        assert_eq!(
            response["result"]["content"][0]["text"], "{}",
            "non-blocking hook output, no nonce"
        );
    }

    fn usable(&mut self) {
        let response = self.request(json!({"id":2,"method":"tools/call","params":{"name":"get_symbol","arguments":{"names":["alpha"]}}}));
        let payload: Value =
            serde_json::from_str(response["result"]["content"][0]["text"].as_str().unwrap())
                .unwrap();
        assert_eq!(payload["matches"][0]["name"], "alpha");
    }

    fn exited(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(status.success());
                return;
            }
            assert!(
                Instant::now() < deadline,
                "server did not shut down within deadline"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn independent_sessions_same_repo_and_multi_connection_owner() -> Result<()> {
    let f = Fixture::new()?;
    let mut a = f.server(true, false, false)?;
    let mut b = f.server(true, false, false)?;
    let mut c = f.server(true, false, false)?;
    a.register("one");
    c.register("one");
    b.register("two");
    assert_eq!(f.records("one")?.len(), 2);
    assert!(f.end("one")?.status.success());
    a.exited();
    c.exited();
    b.usable();
    assert!(f.end("two")?.status.success());
    b.exited();
    assert!(f.end("one")?.status.success());
    Ok(())
}

#[test]
fn shared_ownership_duplicate_registration_and_release() -> Result<()> {
    let f = Fixture::new()?;
    let mut server = f.server(true, false, false)?;
    server.register("one");
    server.register("one");
    server.register("two");
    assert_eq!(f.records("one")?.len(), 1);
    assert!(f.end("unknown")?.status.success());
    assert!(f.end("one")?.status.success());
    assert!(f.end("one")?.status.success());
    server.usable();
    assert_eq!(f.records("two")?.len(), 1);
    assert!(f.end("two")?.status.success());
    server.exited();
    assert_eq!(fs::read_dir(&f.state)?.count(), 0);
    Ok(())
}

#[test]
fn default_lifecycle_and_unregistered_connection_are_unchanged() -> Result<()> {
    let f = Fixture::new()?;
    for scoped in [false, true] {
        let mut server = f.server(scoped, false, false)?;
        let tools = server.request(json!({"id":1,"method":"tools/list"}));
        assert_eq!(
            tools["result"]["tools"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v["name"] == "register_session"),
            scoped
        );
        assert!(f.end("unknown")?.status.success());
        server.usable();
        if scoped {
            server.register("one");
        }
        server.child.stdin.take(); // EOF still shuts down, even with owners.
        server.exited();
    }
    Ok(())
}

#[test]
fn forged_records_requests_and_paths_never_release_a_live_owner() -> Result<()> {
    let f = Fixture::new()?;
    let mut server = f.server(true, false, false)?;
    server.register("one");
    let path = f.records("one")?.pop().unwrap();
    let original = fs::read(&path)?;
    let mut forged: Value = serde_json::from_slice(&original)?;
    forged["nonce"] = json!("0".repeat(64));
    let mut socket = UnixStream::connect(path.parent().unwrap().join("control.sock"))?;
    writeln!(socket, "{forged}")?;
    drop(socket);
    fs::write(&path, serde_json::to_vec(&forged)?)?;
    let result = f.end("one")?;
    assert!(!result.status.success());
    assert!(!String::from_utf8_lossy(&result.stderr).contains(&"0".repeat(64)));
    server.usable();
    forged = serde_json::from_slice(&original)?;
    forged["instance"] = json!("1".repeat(32));
    fs::write(&path, serde_json::to_vec(&forged)?)?;
    assert!(!f.end("one")?.status.success());
    server.usable();
    // A symlink must not be read, removed, or followed to another record.
    let unrelated = f.root.join("unrelated");
    fs::write(&unrelated, &original)?;
    fs::remove_file(&path)?;
    symlink(&unrelated, &path)?;
    assert!(!f.end("one")?.status.success());
    server.usable();
    assert_eq!(fs::read(&unrelated)?, original);
    fs::remove_file(&path)?;
    fs::write(&path, &original)?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    assert!(f.end("one")?.status.success());
    server.exited();
    Ok(())
}

#[test]
fn replaced_instance_directory_is_not_removed() -> Result<()> {
    let f = Fixture::new()?;
    let mut server = f.server(true, false, false)?;
    server.register("one");
    let path = f.records("one")?.pop().unwrap();
    let dir = path.parent().unwrap();
    let moved = f.root.join("old-instance");
    fs::rename(dir, &moved)?;
    fs::create_dir(dir)?;
    fs::set_permissions(dir, fs::Permissions::from_mode(0o700))?;
    let unrelated = dir.join("keep");
    fs::write(&unrelated, "untouched")?;
    server.child.stdin.take();
    server.exited();
    assert_eq!(fs::read_to_string(unrelated)?, "untouched");
    assert!(moved.join("control.sock").exists());
    Ok(())
}

#[test]
fn stale_record_reconnect_and_delayed_old_request() -> Result<()> {
    let f = Fixture::new()?;
    let mut old = f.server(true, false, false)?;
    old.register("one");
    let path = f.records("one")?.pop().unwrap();
    let record = fs::read(&path)?;
    old.child.kill()?;
    old.child.wait()?; // Simulate a client/server crash.
    assert!(f.end("one")?.status.success()); // Stale socket is harmless.
    assert!(
        path.exists(),
        "cleanup does not delete unauthenticated stale data"
    );
    let mut new = f.server(true, false, false)?;
    new.register("one");
    let new_path = f.records("one")?.into_iter().find(|p| p != &path).unwrap();
    let mut socket = UnixStream::connect(new_path.parent().unwrap().join("control.sock"))?;
    socket.write_all(&record)?;
    socket.write_all(b"\n")?;
    drop(socket);
    new.usable();
    assert!(f.end("one")?.status.success());
    new.exited();
    Ok(())
}

#[test]
fn malformed_protocol_notifications_and_owner_limit() -> Result<()> {
    let f = Fixture::new()?;
    let mut server = f.server(true, false, false)?;
    server.send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}));
    writeln!(server.child.stdin.as_mut().unwrap(), "malformed JSON")?;
    assert_eq!(
        server.rx.recv_timeout(Duration::from_secs(2))?["error"]["code"],
        -32700
    );
    server.child.stdin.as_mut().unwrap().write_all(b"\xff\n")?;
    assert_eq!(
        server.rx.recv_timeout(Duration::from_secs(2))?["error"]["code"],
        -32700
    );
    for args in [
        json!({"session_id":""}),
        json!({"session_id":"x".repeat(257)}),
        json!({"session_id":"x","path":"/untrusted"}),
    ] {
        assert_eq!(server.request(json!({"id":1,"method":"tools/call","params":{"name":"register_session","arguments":args}}))["error"]["code"], -32602);
    }
    for n in 0..128 {
        server.register(&format!("owner-{n}"));
    }
    let response = server.request(json!({"id":1,"method":"tools/call","params":{"name":"register_session","arguments":{"session_id":"overflow"}}}));
    assert!(response.get("error").is_some());
    server.usable();
    server.child.stdin.take();
    server.exited();
    Ok(())
}

#[test]
fn registration_release_race_preserves_other_owner() -> Result<()> {
    let f = Fixture::new()?;
    let mut server = f.server(true, false, false)?;
    server.register("keep");
    server.register("race");
    for _ in 0..10 {
        let mut end = f
            .command()
            .args(["session", "end", "--session-id", "race"])
            .spawn()?;
        server.register("race");
        assert!(end.wait()?.success());
        server.usable();
    }
    assert!(f.end("race")?.status.success());
    server.usable();
    assert!(f.end("keep")?.status.success());
    server.exited();
    Ok(())
}

#[test]
fn concurrent_duplicate_release_is_idempotent() -> Result<()> {
    let f = Fixture::new()?;
    let mut server = f.server(true, false, false)?;
    server.register("one");
    let mut ends: Vec<_> = (0..8)
        .map(|_| {
            f.command()
                .args(["session", "end", "--session-id", "one"])
                .spawn()
        })
        .collect::<std::io::Result<_>>()?;
    for end in &mut ends {
        assert!(end.wait()?.success());
    }
    server.exited();
    Ok(())
}

#[test]
fn stdin_and_many_endpoints_share_one_cleanup_deadline() -> Result<()> {
    let f = Fixture::new()?;
    let mut hung_input = f
        .command()
        .args(["session", "end", "--from-stdin"])
        .stdin(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let start = Instant::now();
    loop {
        if let Some(status) = hung_input.try_wait()? {
            assert!(!status.success());
            break;
        }
        if start.elapsed() >= Duration::from_secs(3) {
            let _ = hung_input.kill();
            let _ = hung_input.wait();
            panic!("cleanup hung waiting for stdin EOF");
        }
        thread::sleep(Duration::from_millis(10));
    }
    fs::create_dir(&f.state)?;
    fs::set_permissions(&f.state, fs::Permissions::from_mode(0o700))?;
    let mut listeners = Vec::new();
    for n in 0..30 {
        let instance = format!("{n:032x}");
        let dir = f.state.join(&instance);
        fs::create_dir(&dir)?;
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;
        let socket = dir.join("control.sock");
        listeners.push(UnixListener::bind(&socket)?);
        fs::set_permissions(&socket, fs::Permissions::from_mode(0o600))?;
        let path = dir.join(format!("s-{:x}.json", Sha256::digest(b"hang")));
        fs::write(
            &path,
            serde_json::to_vec(
                &json!({"version":1,"instance":instance,"session_id":"hang","nonce":"a".repeat(64)}),
            )?,
        )?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    let start = Instant::now();
    assert!(!f.end("hang")?.status.success());
    assert!(
        start.elapsed() < Duration::from_secs(3),
        "deadline must not reset for each endpoint"
    );
    drop(listeners);
    Ok(())
}

#[test]
fn bounded_release_with_initial_refresh_running_and_stdin_open() -> Result<()> {
    let f = Fixture::new()?;
    // One expensive parse exercises the hard deadline, not just an idle watcher.
    fs::write(
        f.root.join("large.py"),
        "def large():\n    return 1\n".repeat(150_000),
    )?;
    let mut server = f.server(true, true, false)?;
    server.register("one");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let line = server
            .logs
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))?;
        if line.starts_with("indexing ") {
            break;
        }
    }
    thread::sleep(Duration::from_millis(100));
    let start = Instant::now();
    assert!(f.end("one")?.status.success());
    server.exited();
    assert!(start.elapsed() < Duration::from_secs(3));
    assert!(
        server.child.stdin.is_some(),
        "no EOF or next request was needed"
    );
    Ok(())
}

#[test]
fn hard_shutdown_deadline_survives_a_blocked_protocol_writer() -> Result<()> {
    let f = Fixture::new()?;
    let mut child = f
        .command()
        .arg("--root")
        .arg(&f.root)
        .args([
            "--languages",
            "python",
            "--no-refresh",
            "serve",
            "--mcp",
            "--session-lifecycle",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    let (resume, paused) = mpsc::channel();
    let reader = thread::spawn(move || {
        let mut output = BufReader::new(stdout);
        for _ in 0..2 {
            let mut line = String::new();
            output.read_line(&mut line).unwrap();
            tx.send(serde_json::from_str(&line).unwrap()).unwrap();
        }
        // Keep stdout open but stop draining it after initialize/registration.
        let _ = paused.recv_timeout(Duration::from_secs(5));
    });
    let (_, logs) = mpsc::channel();
    let mut server = Server { child, rx, logs };
    server.request(json!({"id":0,"method":"initialize"}));
    server.register("one");
    let mut input = server.child.stdin.take().unwrap();
    let writer = thread::spawn(move || {
        for _ in 0..10_000 {
            if writeln!(input, "{}", json!({"jsonrpc":"2.0","id":2,"method":"ping"})).is_err() {
                break;
            }
        }
    });
    thread::sleep(Duration::from_millis(100));
    let start = Instant::now();
    assert!(f.end("one")?.status.success());
    server.exited();
    assert!(
        start.elapsed() >= Duration::from_millis(500),
        "test should exercise the forced deadline, not the idle loop"
    );
    let _ = resume.send(());
    reader.join().unwrap();
    writer.join().unwrap();
    Ok(())
}

#[test]
fn cleanup_deadline_and_event_validation() -> Result<()> {
    let f = Fixture::new()?;
    let mut server = f.server(true, false, false)?;
    server.register("one");
    for event in [
        json!({"hook_event_name":"Stop","session_id":"one"}),
        json!({"hook_event_name":"SessionEnd"}),
        json!({"hook_event_name":"SessionEnd","session_id":42}),
    ] {
        let mut child = f
            .command()
            .args(["session", "end", "--from-stdin"])
            .stdin(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        writeln!(child.stdin.take().unwrap(), "{event}")?;
        assert!(!child.wait()?.success());
        server.usable();
    }
    let path = f.records("one")?.pop().unwrap();
    let socket = path.parent().unwrap().join("control.sock");
    fs::remove_file(&socket)?;
    let _unresponsive = UnixListener::bind(&socket)?;
    fs::set_permissions(&socket, fs::Permissions::from_mode(0o600))?;
    let start = Instant::now();
    assert!(!f.end("one")?.status.success());
    assert!(start.elapsed() < Duration::from_secs(3));
    server.usable();
    server.child.stdin.take();
    server.exited();
    assert!(
        socket.exists(),
        "replacement socket is not deleted by old instance"
    );
    Ok(())
}

#[test]
fn parent_watchdog_still_exits_a_scoped_connection() -> Result<()> {
    let f = Fixture::new()?;
    let mut server = f.server(true, false, true)?;
    server.register("one");
    server.child.kill()?;
    server.child.wait()?;
    // The wrapper died, not the server. Its inherited stdout closes only when
    // the server's own original-parent watchdog terminates it.
    assert!(matches!(
        server.rx.recv_timeout(Duration::from_secs(4)),
        Err(mpsc::RecvTimeoutError::Disconnected)
    ));
    assert!(f.end("one")?.status.success());
    Ok(())
}

#[test]
fn state_permissions_and_symlinks_are_rejected() -> Result<()> {
    let f = Fixture::new()?;
    fs::create_dir(&f.state)?;
    fs::set_permissions(&f.state, fs::Permissions::from_mode(0o755))?;
    let out = f
        .command()
        .args(["session", "end", "--session-id", "one"])
        .output()?;
    assert!(!out.status.success());
    fs::remove_dir(&f.state)?;
    symlink(&f.root, &f.state)?;
    assert!(
        !f.command()
            .args(["session", "end", "--session-id", "one"])
            .output()?
            .status
            .success()
    );
    assert!(
        !f.command()
            .args(["serve", "--session-lifecycle"])
            .output()?
            .status
            .success()
    );
    Ok(())
}

#[test]
fn bundled_hook_contract_is_opt_in_and_nonrecursive() -> Result<()> {
    let hooks: Value = serde_json::from_str(include_str!(
        "../plugins/codex-session-cleanup/hooks/hooks.json"
    ))?;
    for event in ["PreToolUse", "Stop"] {
        let hook = &hooks["hooks"][event][0]["hooks"][0];
        assert_eq!(hook["type"], "mcp_tool");
        assert_eq!(hook["tool"], "register_session");
        assert_eq!(hook["input"], json!({"session_id":"${session_id}"}));
    }
    let end = &hooks["hooks"]["SessionEnd"][0]["hooks"][0];
    assert_eq!(end["type"], "command");
    assert_eq!(end["timeout"], 3);
    assert!(hooks["hooks"].get("SubagentStop").is_none());
    Ok(())
}
