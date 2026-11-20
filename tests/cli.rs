//! End-to-end tests that drive the real `tsindex` binary: CLI round trip,
//! MCP over stdio, and the HTTP server over TCP. Everything else in the suite
//! goes through the library API, so these are the only checks that arg
//! parsing, stdout hygiene, and transport wiring actually work.

#[cfg(unix)]
mod common;

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Command, Stdio};

use anyhow::{Context, Result};
use serde_json::Value;
use tempfile::tempdir;

const BIN: &str = env!("CARGO_BIN_EXE_tsindex");

fn fixture(root: &Path) -> Result<()> {
    fs::create_dir_all(root.join("src"))?;
    fs::write(root.join("pyproject.toml"), "[project]\nname='f'\n")?;
    fs::write(
        root.join("src/a.py"),
        "def alpha():\n    return beta()\n\ndef beta():\n    return 1\n",
    )?;
    Ok(())
}

fn tsindex(root: &Path, args: &[&str]) -> Result<std::process::Output> {
    Command::new(BIN)
        .arg("--root")
        .arg(root)
        .args(args)
        .output()
        .context("spawn tsindex")
}

fn ok_json(root: &Path, args: &[&str]) -> Result<Value> {
    let out = tsindex(root, args)?;
    assert!(
        out.status.success(),
        "{args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(serde_json::from_slice(&out.stdout)?)
}

#[test]
fn watch_rejects_invalid_languages_for_missing_configured_clones() -> Result<()> {
    use std::thread;
    use std::time::{Duration, Instant};

    struct Watch(std::process::Child);
    impl Drop for Watch {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    let dir = tempdir()?;
    let root = dir.path().canonicalize()?;
    fixture(&root.join("healthy"))?;
    fs::create_dir(root.join(".tsindex"))?;
    fs::write(
        root.join(".tsindex/config.toml"),
        "[[repos]]\nname = 'missing'\npath = 'missing'\nlanguages = ['invalid-test-language']\n\n[[repos]]\nname = 'healthy'\npath = 'healthy'\nlanguages = ['python']\n",
    )?;
    let mut watch = Watch(
        Command::new(BIN)
            .arg("--root")
            .arg(&root)
            .arg("watch")
            .env("HOME", &root)
            .env("XDG_CONFIG_HOME", root.join("config"))
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?,
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = watch.0.try_wait()? {
            break Some(status);
        }
        if Instant::now() >= deadline {
            break None;
        }
        thread::sleep(Duration::from_millis(20));
    };
    if status.is_none() {
        watch.0.kill()?;
        watch.0.wait()?;
    }
    let mut stderr = String::new();
    watch.0.stderr.take().unwrap().read_to_string(&mut stderr)?;
    assert!(
        status.is_some(),
        "watch accepted an invalid filter for a missing clone: {stderr}"
    );
    assert!(
        !status.unwrap().success(),
        "invalid configuration succeeded"
    );
    assert!(stderr.contains("invalid language filter for repo missing"));
    assert!(stderr.contains("unsupported language invalid-test-language"));
    assert!(!stderr.contains("repos for changes"));
    assert!(
        !root.join(".tsindex/index.db").exists(),
        "invalid watcher configuration allocated an index"
    );
    Ok(())
}

#[test]
fn cli_init_build_symbol_round_trip() -> Result<()> {
    let dir = tempdir()?;
    fixture(dir.path())?;

    let out = tsindex(dir.path(), &["init"])?;
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(dir.path().join(".tsindex/config.toml").exists());

    let built = ok_json(dir.path(), &["--json", "build"])?;
    assert_eq!(built["indexed"], 1);

    let found = ok_json(dir.path(), &["symbol", "alpha", "beta"])?;
    let names: Vec<&str> = found["matches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["alpha", "beta"]);

    // `query --language` must parse after the subcommand (it collided with
    // the global flag before the rename and panicked debug builds).
    let q = ok_json(
        dir.path(),
        &[
            "query",
            "(function_definition name: (identifier) @fn)",
            "--language",
            "python",
        ],
    )?;
    assert_eq!(q["captures"].as_array().unwrap().len(), 2);
    Ok(())
}

#[cfg(unix)]
#[test]
fn mcp_stdio_emits_only_json_rpc_lines() -> Result<()> {
    let dir = tempdir()?;
    fixture(dir.path())?;
    let out = tsindex(dir.path(), &["build"])?;
    assert!(out.status.success());

    let mut child = Command::new(BIN)
        .arg("--root")
        .arg(dir.path())
        .args(["--no-refresh", "serve", "--mcp"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    {
        let mut stdin = child.stdin.take().unwrap();
        writeln!(
            stdin,
            r#"{{"jsonrpc":"2.0","id":1,"method":"initialize","params":{{"protocolVersion":"2024-11-05","capabilities":{{}},"clientInfo":{{"name":"t","version":"0"}}}}}}"#
        )?;
        writeln!(
            stdin,
            r#"{{"jsonrpc":"2.0","method":"notifications/initialized"}}"#
        )?;
        writeln!(stdin, r#"{{"jsonrpc":"2.0","id":2,"method":"tools/list"}}"#)?;
        writeln!(
            stdin,
            r#"{{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{{"name":"get_symbol","arguments":{{"names":["alpha"]}}}}}}"#
        )?;
        // stdin drops here: the server must exit cleanly on EOF.
    }
    let output = child.wait_with_output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let mut lines: Vec<Value> = BufReader::new(&output.stdout[..])
        .lines()
        .map(|l| serde_json::from_str(&l.unwrap()).expect("every stdout line is JSON-RPC"))
        .collect();
    assert_eq!(
        lines.len(),
        3,
        "one response per request, nothing else on stdout"
    );
    // Concurrent handlers may finish out of order; IDs associate each result
    // with its request while the exact count still detects duplicates/loss.
    lines.sort_by_key(|line| line["id"].as_u64());
    for (i, line) in lines.iter().enumerate() {
        assert_eq!(line["jsonrpc"], "2.0");
        assert_eq!(line["id"], (i as u64) + 1);
        assert!(line.get("error").is_none(), "unexpected error: {line}");
    }
    let tools: Vec<&str> = lines[1]["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(tools.contains(&"get_symbol"));
    assert!(
        tools.contains(&"replace_symbol"),
        "stdio transport is writable"
    );
    let text = lines[2]["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("\"alpha\""));
    Ok(())
}

#[cfg(unix)]
#[test]
fn mcp_exits_when_stdout_closes_even_if_the_parent_keeps_stdin_open() -> Result<()> {
    use std::thread;
    use std::time::{Duration, Instant};

    struct Server(std::process::Child);
    impl Drop for Server {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let daemon = common::TestDaemon::start()?;
    let artifacts = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/transport-fixtures");
    fs::create_dir_all(&artifacts)?;
    let dir = tempfile::tempdir_in(artifacts)?;
    fixture(dir.path())?;
    let mut server = Server(
        Command::new(BIN)
            .arg("--socket")
            .arg(&daemon.socket)
            .arg("--root")
            .arg(dir.path())
            .args(["--no-refresh", "serve", "--mcp", "--via-daemon"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?,
    );
    daemon.wait_active_sessions(1, Duration::from_secs(5))?;
    drop(server.0.stdout.take());
    let input = server.0.stdin.as_mut().unwrap();
    writeln!(input, r#"{{"jsonrpc":"2.0","id":1,"method":"ping"}}"#)?;
    input.flush()?;
    let deadline = Instant::now() + Duration::from_secs(3);
    let status = loop {
        if let Some(status) = server.0.try_wait()? {
            break Some(status);
        }
        if Instant::now() >= deadline {
            break None;
        }
        thread::sleep(Duration::from_millis(20));
    };
    assert!(
        status.is_some(),
        "stdout failure must interrupt the stdin wait"
    );
    assert!(
        !status.unwrap().success(),
        "a broken output pipe is an I/O error"
    );
    daemon.wait_active_sessions(0, Duration::from_secs(5))?;
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn managed_proxy_reports_clean_daemon_stop_and_restart() -> Result<()> {
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    struct Proxy(std::process::Child);
    impl Drop for Proxy {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    struct Endpoint(std::path::PathBuf);
    impl Drop for Endpoint {
        fn drop(&mut self) {
            let _ = Command::new(BIN)
                .arg("--socket")
                .arg(&self.0)
                .args(["daemon", "stop"])
                .output();
        }
    }

    for action in ["stop", "restart"] {
        let daemon = common::TestDaemon::start()?;
        let _endpoint = Endpoint(daemon.socket.clone());
        let dir = tempdir()?;
        fixture(dir.path())?;
        let mut proxy = Proxy(
            Command::new(BIN)
                .arg("--socket")
                .arg(&daemon.socket)
                .arg("--root")
                .arg(dir.path())
                .args(["--no-refresh", "serve", "--mcp", "--via-daemon"])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()?,
        );
        writeln!(
            proxy.0.stdin.as_mut().unwrap(),
            "{}",
            serde_json::json!({"jsonrpc":"2.0","id":1,"method":"ping"})
        )?;
        let stdout = proxy.0.stdout.take().unwrap();
        let (sent, received) = mpsc::channel();
        let reader = thread::spawn(move || {
            let mut line = String::new();
            let _ = sent.send(BufReader::new(stdout).read_line(&mut line).map(|_| line));
        });
        let response = received.recv_timeout(Duration::from_secs(5))??;
        let response: Value = serde_json::from_str(&response)?;
        assert!(response.get("result").is_some(), "{response}");
        let command = Command::new(BIN)
            .arg("--socket")
            .arg(&daemon.socket)
            .args(["daemon", action])
            .output()?;
        assert!(
            command.status.success(),
            "{}",
            String::from_utf8_lossy(&command.stderr)
        );
        // The subscribed outcome arrives during shutdown, with no new control
        // connection and no request timeout after the command completes.
        let deadline = Instant::now() + Duration::from_secs(2);
        let status = loop {
            if let Some(status) = proxy.0.try_wait()? {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "proxy waited after daemon {action}"
            );
            thread::sleep(Duration::from_millis(20));
        };
        assert!(status.success(), "clean daemon {action} failed the proxy");
        reader.join().unwrap();
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn detached_daemon_releases_inherited_launcher_descriptors() -> Result<()> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixStream;
    use std::os::unix::process::CommandExt;
    use std::time::Duration;

    let dir = tempfile::tempdir_in("/tmp")?;
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
    let socket = dir.path().join("d.sock");
    let launcher = || {
        let mut command = Command::new(BIN);
        command
            .arg("--socket")
            .arg(&socket)
            .env("HOME", dir.path())
            .env("XDG_CONFIG_HOME", dir.path());
        command
    };
    let (mut observer, endpoint) = UnixStream::pair()?;
    let fd = endpoint.as_raw_fd();
    let mut start = launcher();
    start.args(["daemon", "start"]);
    // Only this test launcher inherits the descriptor. A long-lived detached
    // daemon retaining it would keep the observer from seeing EOF.
    unsafe {
        start.pre_exec(move || {
            if libc::fcntl(fd, libc::F_SETFD, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let started = start.output()?;
    drop(endpoint);
    // Capture the result before asserting so even a regression stops the daemon.
    observer.set_read_timeout(Some(Duration::from_secs(2)))?;
    let disconnected = observer.read(&mut [0]);
    let stopped = launcher().args(["daemon", "stop"]).output()?;
    assert!(
        started.status.success(),
        "{}",
        String::from_utf8_lossy(&started.stderr)
    );
    assert!(
        stopped.status.success(),
        "{}",
        String::from_utf8_lossy(&stopped.stderr)
    );
    assert_eq!(disconnected?, 0);
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn managed_proxy_drains_buffered_responses_after_normal_input_eof() -> Result<()> {
    use std::collections::HashSet;
    use std::os::fd::AsRawFd;
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    struct Proxy(std::process::Child);
    impl Drop for Proxy {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    let daemon = common::TestDaemon::start()?;
    let dir = tempdir()?;
    fixture(dir.path())?;
    let mut proxy = Proxy(
        Command::new(BIN)
            .arg("--socket")
            .arg(&daemon.socket)
            .arg("--root")
            .arg(dir.path())
            .args(["--no-refresh", "serve", "--mcp", "--via-daemon"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?,
    );
    let fd = proxy.0.stdout.as_ref().unwrap().as_raw_fd();
    assert_eq!(unsafe { libc::fcntl(fd, libc::F_SETPIPE_SZ, 4096) }, 4096);
    daemon.wait_active_sessions(1, Duration::from_secs(5))?;
    {
        let mut stdin = proxy.0.stdin.take().unwrap();
        for id in 0..500 {
            writeln!(
                stdin,
                "{}",
                serde_json::json!({"jsonrpc":"2.0","id":id,"method":"ping"})
            )?;
        }
    }
    // Keep stdout unread until the worker has exited and the daemon has reaped
    // it. Its final socket hangup must preserve the already-buffered responses.
    daemon.wait_active_sessions(0, Duration::from_secs(8))?;
    let stdout = proxy.0.stdout.take().unwrap();
    let (sent, received) = mpsc::channel();
    let reader = thread::spawn(move || {
        let lines: std::io::Result<Vec<String>> = BufReader::new(stdout).lines().collect();
        let _ = sent.send(lines);
    });
    let lines = received.recv_timeout(Duration::from_secs(6))??;
    let ids: HashSet<u64> = lines
        .iter()
        .map(|line| {
            let response: Value = serde_json::from_str(line).unwrap();
            assert!(response.get("result").is_some(), "{response}");
            response["id"].as_u64().unwrap()
        })
        .collect();
    assert_eq!(lines.len(), 500);
    assert_eq!(ids, (0..500).collect());
    let deadline = Instant::now() + Duration::from_secs(6);
    let status = loop {
        if let Some(status) = proxy.0.try_wait()? {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "proxy did not finish its normal drain"
        );
        thread::sleep(Duration::from_millis(20));
    };
    assert!(status.success());
    reader.join().unwrap();
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn managed_proxy_reports_worker_crash_after_readiness() -> Result<()> {
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    struct Proxy(std::process::Child);
    impl Drop for Proxy {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    for close_input in [false, true] {
        let daemon = common::TestDaemon::start()?;
        let dir = tempdir()?;
        fixture(dir.path())?;
        let mut proxy = Proxy(
            Command::new(BIN)
                .arg("--socket")
                .arg(&daemon.socket)
                .arg("--root")
                .arg(dir.path())
                .args(["--no-refresh", "serve", "--mcp", "--via-daemon"])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()?,
        );
        writeln!(
            proxy.0.stdin.as_mut().unwrap(),
            "{}",
            serde_json::json!({"jsonrpc":"2.0","id":1,"method":"ping"})
        )?;
        let stdout = proxy.0.stdout.take().unwrap();
        let (sent, received) = mpsc::channel();
        let reader = thread::spawn(move || {
            let mut line = String::new();
            let _ = sent.send(BufReader::new(stdout).read_line(&mut line).map(|_| line));
        });
        let response = received.recv_timeout(Duration::from_secs(5))??;
        let response: Value = serde_json::from_str(&response)?;
        assert!(response.get("result").is_some(), "{response}");
        let (sessions, _) = daemon.client()?.list_sessions(false)?;
        assert_eq!(sessions.len(), 1);
        if close_input {
            // Stop the worker before EOF so the test can observe the drain phase
            // deterministically, then crash it while the proxy is draining.
            assert_eq!(
                unsafe { libc::kill(sessions[0].worker_pid as i32, libc::SIGSTOP) },
                0
            );
            drop(proxy.0.stdin.take());
            let deadline = Instant::now() + Duration::from_secs(3);
            loop {
                let (active, _) = daemon.client()?.list_sessions(false)?;
                if active
                    .iter()
                    .any(|session| session.state == tsindex::daemon::SessionState::Draining)
                {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "worker did not enter drain phase"
                );
                thread::sleep(Duration::from_millis(20));
            }
        }
        assert_eq!(
            unsafe { libc::kill(sessions[0].worker_pid as i32, libc::SIGKILL) },
            0
        );
        let deadline = Instant::now() + Duration::from_secs(6);
        let status = loop {
            if let Some(status) = proxy.0.try_wait()? {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "proxy did not observe worker crash"
            );
            thread::sleep(Duration::from_millis(20));
        };
        assert!(!status.success(), "worker crash must fail the MCP proxy");
        reader.join().unwrap();
        daemon.wait_active_sessions(0, Duration::from_secs(3))?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn managed_proxy_exits_when_daemon_stops_with_stdout_full() -> Result<()> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::thread;
    use std::time::{Duration, Instant};

    struct Proxy {
        child: std::process::Child,
        writer: Option<thread::JoinHandle<std::process::ChildStdin>>,
    }
    impl Drop for Proxy {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
            // Killing the proxy releases a writer blocked on its stdin. Join
            // even after an assertion fails so no fixture thread survives.
            if let Some(writer) = self.writer.take() {
                let _ = writer.join();
            }
        }
    }

    let daemon = common::TestDaemon::start()?;
    let dir = tempdir()?;
    fixture(dir.path())?;
    for (limit, close_input) in [(None, false), (Some(4096), true)] {
        let mut raw = [0; 2];
        assert_eq!(unsafe { libc::pipe2(raw.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
        // SAFETY: pipe2 succeeded and transfers two distinct descriptors.
        let stdout = unsafe { OwnedFd::from_raw_fd(raw[0]) };
        let observer = unsafe { OwnedFd::from_raw_fd(raw[1]) };
        let inherited_flags = unsafe { libc::fcntl(observer.as_raw_fd(), libc::F_GETFL) };
        assert!(inherited_flags >= 0);
        if let Some(limit) = limit {
            // The kernel rounds the requested capacity up to its page size.
            let actual = unsafe { libc::fcntl(observer.as_raw_fd(), libc::F_SETPIPE_SZ, limit) };
            assert!(actual >= limit, "failed to set stdout pipe capacity");
        }
        let mut proxy = Proxy {
            child: Command::new(BIN)
                .arg("--socket")
                .arg(&daemon.socket)
                .arg("--root")
                .arg(dir.path())
                .args(["--no-refresh", "serve", "--mcp", "--via-daemon"])
                .stdin(Stdio::piped())
                .stdout(Stdio::from(observer.try_clone()?))
                .stderr(Stdio::null())
                .spawn()?,
            writer: None,
        };
        let mut stdin = proxy.child.stdin.take().unwrap();
        proxy.writer = Some(thread::spawn(move || {
            // Tool schemas fill a normal pipe without requiring a large fixture.
            let count = if close_input { 4 } else { 1024 };
            for id in 0..count {
                if writeln!(
                    stdin,
                    "{}",
                    serde_json::json!({"jsonrpc":"2.0","id":id,"method":"tools/list"})
                )
                .is_err()
                {
                    break;
                }
            }
            // The harness keeps stdin open while neglecting output. Retain its
            // handle in the thread result until the proxy has exited.
            stdin
        }));
        daemon.wait_active_sessions(1, Duration::from_secs(5))?;
        let capacity = unsafe { libc::fcntl(stdout.as_raw_fd(), libc::F_GETPIPE_SZ) };
        assert!(capacity > 0);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(
                proxy.child.try_wait()?.is_none(),
                "proxy exited before stdout backpressure"
            );
            let mut available: libc::c_int = 0;
            assert_eq!(
                unsafe { libc::ioctl(stdout.as_raw_fd(), libc::FIONREAD, &mut available) },
                0
            );
            // Page fragmentation can stop writes before the byte capacity is
            // reached. Observe readiness on a write descriptor, matching the
            // proxy's POLLOUT wait, without adding any data to its output.
            let mut writing = libc::pollfd {
                fd: observer.as_raw_fd(),
                events: libc::POLLOUT,
                revents: 0,
            };
            let ready = unsafe { libc::poll(&mut writing, 1, 0) };
            if ready < 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error.into());
            }
            assert_eq!(
                writing.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL),
                0
            );
            if available > 0 && ready == 0 {
                eprintln!("stdout backpressured with {available}/{capacity} bytes queued");
                break;
            }
            assert!(
                Instant::now() < deadline,
                "proxy did not encounter stdout backpressure ({available}/{capacity} bytes): {}",
                daemon.read_log()
            );
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(
            unsafe { libc::fcntl(observer.as_raw_fd(), libc::F_GETFL) },
            inherited_flags,
            "the proxy changed the harness stdout flags"
        );
        let (sessions, _) = daemon.client()?.list_sessions(false)?;
        assert_eq!(sessions.len(), 1);
        let worker_pid = sessions[0].worker_pid;
        assert!(worker_pid > 1);
        if close_input {
            // Hold this worker across EOF so the daemon reports an explicit stop
            // during the drain. Normal EOF may otherwise complete first.
            assert_eq!(unsafe { libc::kill(worker_pid as i32, libc::SIGSTOP) }, 0);
            drop(proxy.writer.take().unwrap().join().unwrap());
            let deadline = Instant::now() + Duration::from_secs(3);
            loop {
                let (sessions, _) = daemon.client()?.list_sessions(false)?;
                if sessions
                    .iter()
                    .any(|s| s.state == tsindex::daemon::SessionState::Draining)
                {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "the half-close never reached the daemon"
                );
                thread::sleep(Duration::from_millis(20));
            }
        }
        daemon.client()?.kill_all()?;
        let deadline = Instant::now() + Duration::from_secs(6);
        while proxy.child.try_wait()?.is_none() {
            assert!(
                Instant::now() < deadline,
                "full stdout prevented proxy teardown"
            );
            thread::sleep(Duration::from_millis(20));
        }
        if let Some(writer) = proxy.writer.take() {
            writer.join().unwrap();
        }
        assert_eq!(
            unsafe { libc::fcntl(observer.as_raw_fd(), libc::F_GETFL) },
            inherited_flags
        );
        daemon.wait_active_sessions(0, Duration::from_secs(3))?;
        assert_eq!(unsafe { libc::kill(worker_pid as i32, 0) }, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH),
            "worker was not reaped"
        );
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn mcp_watcher_skips_dependency_symlinks_and_refreshes_source() -> Result<()> {
    use std::collections::HashSet;
    use std::os::unix::fs::{MetadataExt, symlink};
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    // Kill and reap the real server even when a watcher assertion fails.
    struct Server(std::process::Child);
    impl Drop for Server {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    let dir = tempdir()?;
    let root = dir.path().join("repo");
    fixture(&root)?;
    let dependency = dir.path().join("dependency-store");
    fs::create_dir(&dependency)?;
    fs::create_dir(root.join("node_modules"))?;
    symlink(&dependency, root.join("node_modules/linked"))?;
    // A dependency store linked from the top level sits outside every ignored
    // directory, so only the no-follow registration walk keeps it unwatched.
    symlink(&dependency, root.join("linked"))?;
    fs::create_dir_all(root.join("generated/nested"))?;
    fs::write(root.join(".tsindexignore"), "generated/\n")?;

    let mut server = Server(
        Command::new(BIN)
            .arg("--root")
            .arg(&root)
            .args(["daemon", "worker"])
            // Each step below waits out one debounce window; keep it short.
            .env("TSINDEX_WATCH_DEBOUNCE_MS", "200")
            // Never written, but it must stay open: the server exits on stdin EOF.
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?,
    );
    let stderr = server.0.stderr.take().unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            if line.contains("repos for changes") {
                let _ = ready_tx.send(());
            }
        }
    });

    ready_rx
        .recv_timeout(Duration::from_secs(10))
        .context("MCP watcher did not finish registering directories")?;

    // Inspect the real inotify registrations: filtering dependency events
    // after registration cannot prevent symlink fan-out and memory growth.
    let mut watched_inodes = HashSet::new();
    for info in fs::read_dir(format!("/proc/{}/fdinfo", server.0.id()))? {
        // The startup build is closing its own descriptors concurrently;
        // one that vanished between listing and reading is not inotify's.
        let Ok(contents) = fs::read_to_string(info?.path()) else {
            continue;
        };
        for line in contents.lines() {
            if line.starts_with("inotify ") {
                for field in line.split_whitespace() {
                    if let Some(inode) = field.strip_prefix("ino:") {
                        watched_inodes.insert(u64::from_str_radix(inode, 16)?);
                    }
                }
            }
        }
    }
    assert!(
        watched_inodes.contains(&fs::metadata(root.join("src"))?.ino()),
        "ordinary source directories must still be watched"
    );
    assert!(
        !watched_inodes.contains(&fs::metadata(&dependency)?.ino()),
        "recursive registration must not follow linked dependency trees"
    );
    for ignored in ["node_modules", "generated", "generated/nested"] {
        assert!(
            !watched_inodes.contains(&fs::metadata(root.join(ignored))?.ino()),
            "ignored directory {ignored} must not consume an inotify watch"
        );
    }

    let wait_for_symbol = |name: &str| -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let found = rusqlite::Connection::open_with_flags(
                root.join(".tsindex/index.db"),
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )
            .and_then(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM symbols WHERE name = ?1",
                    [name],
                    |row| row.get::<_, u64>(0),
                )
            });
            if matches!(found, Ok(count) if count > 0) {
                return Ok(());
            }
            anyhow::ensure!(Instant::now() < deadline, "symbol {name} was not refreshed");
            thread::sleep(Duration::from_millis(100));
        }
    };
    wait_for_symbol("alpha")?;
    fs::write(root.join("src/a.py"), "def after_edit():\n    return 2\n")?;
    wait_for_symbol("after_edit")?;
    fs::create_dir_all(root.join("new/nested"))?;
    let added = root.join("new/nested/added.py");
    fs::write(&added, "def added_directory():\n    return 3\n")?;
    wait_for_symbol("added_directory")?;
    fs::write(&added, "def edited_new_directory():\n    return 4\n")?;
    wait_for_symbol("edited_new_directory")?;
    // Un-ignoring a directory must index what is already inside it and
    // register watches there, so later edits inside it are seen as well.
    let unignored = root.join("generated/nested/g.py");
    fs::write(&unignored, "def unignored():\n    return 5\n")?;
    fs::write(root.join(".tsindexignore"), "")?;
    wait_for_symbol("unignored")?;
    fs::write(&unignored, "def edited_unignored():\n    return 6\n")?;
    wait_for_symbol("edited_unignored")?;
    // `server` kills the child when dropped, including on early `?` returns;
    // its stderr then closes and the reader thread ends on its own.
    Ok(())
}

fn http(port: u16, request: &str) -> Result<String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.write_all(request.as_bytes())?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response)
}

#[test]
fn http_serves_health_and_read_only_tool_list() -> Result<()> {
    let dir = tempdir()?;
    fixture(dir.path())?;
    let out = tsindex(dir.path(), &["build"])?;
    assert!(out.status.success());

    let mut child = Command::new(BIN)
        .arg("--root")
        .arg(dir.path())
        .args(["--no-refresh", "serve", "--http", "--port", "0"])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;
    let port = {
        let mut stderr = BufReader::new(child.stderr.take().unwrap());
        let mut line = String::new();
        loop {
            line.clear();
            if stderr.read_line(&mut line)? == 0 {
                panic!("server exited before announcing its port");
            }
            if let Some(rest) = line.trim().rsplit_once(':')
                && line.contains("listening on")
            {
                break rest.1.parse::<u16>()?;
            }
        }
    };

    let health = http(
        port,
        &format!("GET /health HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"),
    )?;
    assert!(health.starts_with("HTTP/1.1 200"), "{health}");

    let list = http(
        port,
        &format!(
            "POST /tools/list HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        ),
    )?;
    assert!(list.starts_with("HTTP/1.1 200"), "{list}");
    let body: Value = serde_json::from_str(list.split("\r\n\r\n").nth(1).unwrap())?;
    let tools: Vec<&str> = body["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(tools.contains(&"get_symbol"));
    assert!(
        !tools.contains(&"replace_symbol"),
        "HTTP transport stays read-only"
    );

    // Wrong Host is refused (DNS-rebinding guard) before any routing.
    let rebind = http(
        port,
        &format!(
            "GET /health HTTP/1.1\r\nHost: attacker.example:{port}\r\nConnection: close\r\n\r\n"
        ),
    )?;
    assert!(rebind.starts_with("HTTP/1.1 403"), "{rebind}");

    child.kill()?;
    child.wait()?;
    Ok(())
}
