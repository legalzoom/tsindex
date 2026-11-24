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
        Self::connect_with(socket, root, harness_name, &["--no-refresh"])
    }

    fn connect_with(
        socket: &Path,
        root: &Path,
        harness_name: &str,
        options: &[&str],
    ) -> Result<Self> {
        Self::connect_configured(socket, root, harness_name, |cmd| {
            cmd.args(options);
        })
    }

    fn connect_configured(
        socket: &Path,
        root: &Path,
        harness_name: &str,
        configure: impl FnOnce(&mut Command),
    ) -> Result<Self> {
        let mut command = Command::new(BIN);
        command.arg("--socket").arg(socket).arg("--root").arg(root);
        configure(&mut command);
        let mut child = command
            .args(["serve", "--mcp", "--via-daemon"])
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
        let response: Value = serde_json::from_str(&line).context("invalid JSON-RPC response")?;
        assert_eq!(response["id"], self.next_id, "reply to a different request");
        Ok(response)
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

    fn wait_for_exit(
        &mut self,
        timeout: Duration,
    ) -> std::io::Result<Option<std::process::ExitStatus>> {
        let deadline = Instant::now() + timeout;
        loop {
            let status = self.child.try_wait()?;
            if status.is_some() || Instant::now() >= deadline {
                return Ok(status);
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn close_gracefully(mut self) -> Result<()> {
        drop(self.child.stdin.take());
        let Some(status) = self.wait_for_exit(Duration::from_secs(5))? else {
            bail!("proxy did not exit cleanly after stdin EOF");
        };
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
    assert_eq!(status["daemon"]["index_runtimes"], 0);

    let sessions = daemon_json(&daemon.socket, &["daemon", "sessions"])?;
    assert_eq!(sessions["active"].as_array().unwrap().len(), 0);
    assert_eq!(sessions["recent"].as_array().unwrap().len(), 0);
    Ok(())
}

fn link_checkout(main: &Path, checkout: &Path, name: &str) -> Result<()> {
    let metadata = main.join(".git/worktrees").join(name);
    fs::create_dir_all(&metadata)?;
    fs::create_dir_all(checkout)?;
    fs::write(
        checkout.join(".git"),
        format!("gitdir: {}\n", metadata.display()),
    )?;
    fs::write(metadata.join("commondir"), "../..\n")?;
    fs::write(
        metadata.join("gitdir"),
        format!("{}\n", checkout.join(".git").display()),
    )?;
    Ok(())
}

#[derive(serde::Deserialize)]
struct ManagedSession {
    session_id: String,
    worker_pid: u32,
    runtime_pid: u32,
}

fn managed_session(daemon: &common::TestDaemon, name: &str) -> Result<ManagedSession> {
    let sessions = daemon_json(&daemon.socket, &["daemon", "sessions"])?;
    let info = sessions["active"]
        .as_array()
        .context("missing sessions")?
        .iter()
        .find(|item| item["harness_name"] == name)
        .with_context(|| format!("missing session for {name}"))?;
    let session: ManagedSession =
        serde_json::from_value(info.clone()).context("missing runtime or session worker PID")?;
    assert_ne!(session.worker_pid, session.runtime_pid);
    assert_ne!(session.runtime_pid, daemon.pid());
    assert!(session.runtime_pid > 1 && pid_exists(session.runtime_pid));
    Ok(session)
}

#[test]
fn compatible_clients_share_indexing_and_keep_independent_mcp_sessions() -> Result<()> {
    let daemon = common::TestDaemon::start_with_env(&[("TSINDEX_RUNTIME_IDLE_SECS", "60")])?;
    let dir = tempdir_in("/tmp")?;
    let root = dir.path();
    build_fixture(root, "lib.py", "def shared_symbol():\n    return 1\n")?;

    // Race the cold attach so admission must elect one runtime for both peers.
    let (left, right) = thread::scope(|scope| {
        let left = scope.spawn(|| McpSession::connect(&daemon.socket, root, "left"));
        let right = scope.spawn(|| McpSession::connect(&daemon.socket, root, "right"));
        (left.join().unwrap(), right.join().unwrap())
    });
    let (mut left, mut right) = (left?, right?);
    let first = managed_session(&daemon, "left")?;
    let second = managed_session(&daemon, "right")?;
    assert_eq!(first.runtime_pid, second.runtime_pid);
    assert_eq!(
        daemon
            .client()?
            .session_detail(&first.session_id)?
            .runtime_pid,
        Some(first.runtime_pid)
    );
    assert_ne!(first.worker_pid, second.worker_pid);
    assert_ne!(first.session_id, second.session_id);
    assert_eq!(daemon.ping()?.index_runtimes, 1);

    // Both initialized clients use request ID 2. A single crossed reply would
    // turn one caller's hit into the other caller's miss.
    let (hit, miss) = thread::scope(|scope| {
        let hit = scope.spawn(|| left.call_tool("get_symbol", json!({"names":["shared_symbol"]})));
        let miss = scope.spawn(|| right.call_tool("get_symbol", json!({"names":["absent"]})));
        (hit.join().unwrap(), miss.join().unwrap())
    });
    assert_eq!(hit?["total"], 1);
    assert_eq!(miss?["total"], 0);
    daemon.client()?.kill_session(first.session_id)?;
    daemon.wait_active_sessions(1, Duration::from_secs(5))?;
    wait_pid_gone(first.worker_pid, Duration::from_secs(2))?;
    assert_eq!(
        right.call_tool("get_symbol", json!({"names":["shared_symbol"]}))?["total"],
        1
    );
    right.close_gracefully()?;
    daemon.wait_active_sessions(0, Duration::from_secs(3))?;
    assert_eq!(
        daemon.ping()?.index_runtimes,
        1,
        "the idle runtime stays resident"
    );

    let resumed = McpSession::connect(&daemon.socket, root, "resumed")?;
    let reattached = managed_session(&daemon, "resumed")?;
    assert_eq!(reattached.runtime_pid, first.runtime_pid);
    assert_ne!(reattached.worker_pid, second.worker_pid);
    resumed.close_gracefully()?;
    daemon.client()?.shutdown()?;
    wait_pid_gone(first.runtime_pid, Duration::from_secs(5))?;
    Ok(())
}

fn block_indexed_requests(
    busy: &mut McpSession,
    root: &Path,
) -> Result<tsindex::maintenance::FileLease> {
    use tsindex::maintenance::{
        FileLease, refresh_lease_path, refresh_marker_path, writer_lease_path,
    };
    let db = tsindex::config::db_path(root);
    let writer = FileLease::acquire(writer_lease_path(&db), Duration::from_secs(3))?;
    if let Err(e) = fs::remove_file(refresh_marker_path(&db))
        && e.kind() != std::io::ErrorKind::NotFound
    {
        return Err(e.into());
    }
    let stdin = busy.child.stdin.as_mut().unwrap();
    for id in 101..105 {
        writeln!(
            stdin,
            "{}",
            json!({"jsonrpc":"2.0", "id":id, "method":"tools/call", "params":{"name":"get_symbol", "arguments":{"name":"live_symbol"}}})
        )?;
    }
    let deadline = Instant::now() + Duration::from_secs(3);
    while FileLease::try_acquire(refresh_lease_path(&db))?.is_some() {
        anyhow::ensure!(
            Instant::now() < deadline,
            "indexed request never entered refresh"
        );
        thread::sleep(Duration::from_millis(5));
    }
    Ok(writer)
}

#[test]
fn a_busy_indexed_client_leaves_new_sessions_and_live_primary_queries_responsive() -> Result<()> {
    let dir = tempdir_in("/tmp")?;
    let root = dir.path();
    build_fixture(root, "lib.py", "def live_symbol():\n    pass\n")?;
    let daemon = common::TestDaemon::start()?;
    let mut busy = McpSession::connect_with(&daemon.socket, root, "busy", &[])?;
    let pid = managed_session(&daemon, "busy")?.runtime_pid;
    let writer = block_indexed_requests(&mut busy, root)?;
    // The indexed work still owns the refresh lock and waits for our writer
    // lease. A new session can attach and run a live primary query on that owner.
    let mut live = McpSession::connect_with(&daemon.socket, root, "live", &[])?;
    assert_eq!(managed_session(&daemon, "live")?.runtime_pid, pid);
    let result = live.call_tool(
        "query",
        json!({"language":"python", "query":"(function_definition name: (identifier) @name)"}),
    )?;
    assert!(result.to_string().contains("live_symbol"), "{result}");
    drop(writer);
    for _ in 101..105 {
        let line = busy.responses.recv_timeout(Duration::from_secs(5))??;
        let response: Value = serde_json::from_str(&line)?;
        assert!(response.get("result").is_some(), "{response}");
    }
    busy.close_gracefully()?;
    live.close_gracefully()?;
    Ok(())
}

#[test]
fn killing_a_session_with_blocked_lookups_leaves_capacity_for_its_replacement() -> Result<()> {
    let dir = tempdir_in("/tmp")?;
    let root = dir.path();
    build_fixture(root, "lib.py", "def live_symbol():\n    pass\n")?;
    let daemon = common::TestDaemon::start_with_env(&[("TSINDEX_MAX_SESSIONS", "1")])?;
    let mut busy = McpSession::connect_with(&daemon.socket, root, "busy", &[])?;
    let original = managed_session(&daemon, "busy")?;
    let _writer = block_indexed_requests(&mut busy, root)?;
    assert!(matches!(
        busy.responses.recv_timeout(Duration::from_millis(150)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    daemon.client()?.kill_session(original.session_id)?;
    let mut next = McpSession::connect_with(&daemon.socket, root, "replacement", &[])?;
    assert_ne!(
        managed_session(&daemon, "replacement")?.runtime_pid,
        original.runtime_pid
    );
    let live = next.call_tool(
        "query",
        json!({"language":"python", "query":"(function_definition name: (identifier) @name)"}),
    )?;
    assert!(live.to_string().contains("live_symbol"), "{live}");
    next.close_gracefully()
}

#[test]
fn managed_indexes_resolve_extra_ignore_files_from_the_callers_directory() -> Result<()> {
    let daemon = common::TestDaemon::start()?;
    let dir = tempdir_in("/tmp")?;
    let root = dir.path().join("repo");
    let other_caller = dir.path().join("other");
    fs::create_dir(&root)?;
    fs::create_dir(&other_caller)?;
    fs::write(root.join("visible.py"), "def visible():\n    pass\n")?;
    fs::write(root.join("ignored.py"), "def excluded():\n    pass\n")?;
    fs::write(dir.path().join("rules.txt"), "ignored.py\n")?;
    fs::write(other_caller.join("rules.txt"), "\n")?;
    let mut config = TsIndexConfig::default();
    config.languages.include = vec!["python".into()];
    config.ignore.extra = vec!["rules.txt".into()];
    config.write(&root)?;
    let mut filtered = McpSession::connect_configured(&daemon.socket, &root, "filtered", |cmd| {
        cmd.current_dir(dir.path());
    })?;
    assert_eq!(
        filtered.call_tool("get_symbol", json!({"name":"visible"}))?["total"],
        1
    );
    assert_eq!(
        filtered.call_tool("get_symbol", json!({"name":"excluded"}))?["total"],
        0
    );
    let mut other = McpSession::connect_configured(&daemon.socket, &root, "other-base", |cmd| {
        cmd.current_dir(&other_caller);
    })?;
    assert_ne!(
        managed_session(&daemon, "filtered")?.runtime_pid,
        managed_session(&daemon, "other-base")?.runtime_pid
    );
    assert_eq!(
        other.call_tool("get_symbol", json!({"name":"excluded"}))?["total"],
        1
    );
    other.close_gracefully()?;
    filtered.close_gracefully()
}

#[cfg(target_os = "linux")]
#[test]
fn managed_session_accepts_a_native_byte_database_path() -> Result<()> {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    let daemon = common::TestDaemon::start()?;
    let dir = tempdir_in("/tmp")?;
    fs::write(dir.path().join("f.py"), "def native_path():\n    pass\n")?;
    let db = dir
        .path()
        .join(OsString::from_vec(b"index-\xff.db".to_vec()));
    let mut session =
        McpSession::connect_configured(&daemon.socket, dir.path(), "native", |cmd| {
            cmd.arg("--db").arg(&db);
        })?;
    assert_eq!(
        daemon.client()?.list_sessions(false)?.0[0].db.as_ref(),
        Some(&db)
    );
    assert_eq!(
        session.call_tool("get_symbol", json!({"name":"native_path"}))?["total"],
        1
    );
    session.close_gracefully()
}

#[test]
fn shared_index_reuses_the_in_memory_linked_checkout_across_clients() -> Result<()> {
    let daemon = common::TestDaemon::start_with_env(&[("TSINDEX_RUNTIME_IDLE_SECS", "60")])?;
    let dir = tempdir_in("/tmp")?;
    let main = dir.path().join("main");
    let linked = dir.path().join("topic");
    link_checkout(&main, &linked, "topic")?;
    build_fixture(&main, "main.py", "def main_symbol():\n    return 1\n")?;
    let mut config = TsIndexConfig::load(&main)?;
    config.server.worktree_refresh_ms = 60_000;
    config.write(&main)?;
    fs::write(
        linked.join("lib.py"),
        "def cached_symbol():\n    return 2\n",
    )?;
    let old_symbol = json!({"repo":linked, "names":["cached_symbol"]});
    let mut first = McpSession::connect(&daemon.socket, &main, "cache-first")?;
    assert_eq!(
        first.call_tool("get_symbol", old_symbol.clone())?["total"],
        1
    );
    let warm = managed_session(&daemon, "cache-first")?;
    first.close_gracefully()?;

    // A fresh process would rebuild without this fixture's durable freshness
    // marker and discover renamed_symbol. An already-warm checkout can serve
    // its cached generation for the configured refresh interval.
    let marker = tsindex::maintenance::refresh_marker_path(&tsindex::config::db_path(&linked));
    fs::remove_file(&marker)?;
    fs::write(
        linked.join("lib.py"),
        "def renamed_symbol():\n    return 3\n",
    )?;
    let mut second = McpSession::connect(&daemon.socket, &main, "cache-second")?;
    assert_eq!(
        managed_session(&daemon, "cache-second")?.runtime_pid,
        warm.runtime_pid
    );
    assert_eq!(second.call_tool("get_symbol", old_symbol)?["total"], 1);
    assert_eq!(
        second.call_tool(
            "get_symbol",
            json!({"repo":linked,"names":["renamed_symbol"]})
        )?["total"],
        0
    );
    assert!(
        !marker.exists(),
        "a cold runtime reconciled the linked checkout"
    );
    second.close_gracefully()?;
    Ok(())
}

#[test]
fn freshly_seeded_checkout_retains_its_projected_runtime_key() -> Result<()> {
    let daemon = common::TestDaemon::start()?;
    let fixture = tempdir_in("/tmp")?;
    let main = fixture.path().join("main");
    let linked = fixture.path().join("feature");
    build_fixture(&main, "one.py", "def primary():\n    pass\n")?;
    link_checkout(&main, &linked, "feature")?;
    fs::write(linked.join("one.py"), "def feature_only():\n    pass\n")?;
    assert!(!tsindex::config::db_path(&linked).exists());
    let mut first = McpSession::connect(&daemon.socket, &linked, "before-seed")?;
    let initial = managed_session(&daemon, "before-seed")?.runtime_pid;
    assert!(tsindex::config::db_path(&linked).is_file());
    let mut second = McpSession::connect(&daemon.socket, &linked, "after-seed")?;
    assert_eq!(managed_session(&daemon, "after-seed")?.runtime_pid, initial);
    for session in [&mut first, &mut second] {
        assert_eq!(
            session.call_tool("get_symbol", json!({"names":["feature_only"]}))?["total"],
            1
        );
    }
    first.close_gracefully()?;
    second.close_gracefully()
}

#[test]
fn incompatible_roots_databases_refresh_languages_and_configs_get_distinct_indexers() -> Result<()>
{
    let daemon = common::TestDaemon::start()?;
    let dir = tempdir_in("/tmp")?;
    let root = dir.path().join("first");
    let other_root = dir.path().join("second");
    build_fixture(&root, "lib.py", "def first_symbol():\n    return 1\n")?;
    build_fixture(&other_root, "lib.py", "def other_symbol():\n    return 2\n")?;
    let mut base = McpSession::connect(&daemon.socket, &root, "base")?;
    let original = managed_session(&daemon, "base")?;
    let external_db = dir.path().join("override.db");
    let db = external_db.to_str().context("UTF-8 fixture path")?;
    let default_db = tsindex::config::db_path(&root);
    let explicit_default = default_db.to_str().context("UTF-8 fixture path")?;
    for (name, path, options) in [
        ("other-root", &other_root, &["--no-refresh"][..]),
        ("other-db", &root, &["--no-refresh", "--db", db][..]),
        // An explicit default database still changes repository pruning.
        (
            "explicit-default",
            &root,
            &["--no-refresh", "--db", explicit_default][..],
        ),
        ("refresh", &root, &[][..]),
        (
            "languages",
            &root,
            &["--no-refresh", "--languages", "rust"][..],
        ),
    ] {
        let session = McpSession::connect_with(&daemon.socket, path, name, options)?;
        assert_ne!(
            managed_session(&daemon, name)?.runtime_pid,
            original.runtime_pid,
            "{name} reused the original runtime"
        );
        session.close_gracefully()?;
    }
    let mut config = TsIndexConfig::load(&root)?;
    config.server.worktree_refresh_ms += 1234;
    config.write(&root)?;
    let changed = McpSession::connect(&daemon.socket, &root, "changed-config")?;
    assert_ne!(
        managed_session(&daemon, "changed-config")?.runtime_pid,
        original.runtime_pid
    );
    assert_eq!(
        base.call_tool("get_symbol", json!({"names":["first_symbol"]}))?["total"],
        1
    );
    changed.close_gracefully()?;
    base.close_gracefully()?;
    let deadline = Instant::now() + Duration::from_secs(3);
    while daemon.ping()?.index_runtimes > 4 {
        anyhow::ensure!(Instant::now() < deadline, "too many idle index owners");
        std::thread::sleep(Duration::from_millis(25));
    }
    Ok(())
}

#[test]
fn managed_lookup_can_resolve_catalog_settings_larger_than_a_control_record() -> Result<()> {
    let daemon = common::TestDaemon::start()?;
    let dir = tempdir_in("/tmp")?;
    build_fixture(dir.path(), "a.py", "def big_catalog():\n    pass\n")?;
    let mut config = TsIndexConfig::load(dir.path())?;
    config.ignore.extra = (0..3000)
        .map(|n| format!("generated/vendor/component-{n}/"))
        .collect();
    config.write(dir.path())?;
    let mut client = McpSession::connect(&daemon.socket, dir.path(), "large-config")?;
    assert_eq!(
        client.call_tool("get_symbol", json!({"names":["big_catalog"]}))?["total"],
        1
    );
    client.close_gracefully()
}

#[test]
fn a_new_session_replaces_a_stopped_index_owner() -> Result<()> {
    let daemon = common::TestDaemon::start()?;
    let dir = tempdir_in("/tmp")?;
    build_fixture(dir.path(), "f.py", "def still_indexed():\n    pass\n")?;
    let _original = McpSession::connect(&daemon.socket, dir.path(), "first")?;
    let original_pid = managed_session(&daemon, "first")?.runtime_pid;
    assert_eq!(
        unsafe { libc::kill(original_pid as libc::pid_t, libc::SIGSTOP) },
        0
    );
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let state = Command::new("ps")
            .args(["-o", "state=", "-p"])
            .arg(original_pid.to_string())
            .output()?;
        if String::from_utf8_lossy(&state.stdout)
            .trim_start()
            .starts_with('T')
        {
            break;
        }
        anyhow::ensure!(Instant::now() < deadline, "index owner did not stop");
        std::thread::sleep(Duration::from_millis(10));
    }
    let mut replacement = McpSession::connect(&daemon.socket, dir.path(), "replacement")?;
    assert_ne!(
        managed_session(&daemon, "replacement")?.runtime_pid,
        original_pid
    );
    wait_pid_gone(original_pid, Duration::from_secs(3))?;
    assert_eq!(
        replacement.call_tool("get_symbol", json!({"names":["still_indexed"]}))?["total"],
        1
    );
    replacement.close_gracefully()
}

#[test]
fn indexer_loss_ends_only_its_attached_sessions_and_new_clients_recover() -> Result<()> {
    let daemon = common::TestDaemon::start()?;
    let dir = tempdir_in("/tmp")?;
    let root = dir.path().join("first");
    let unrelated = dir.path().join("unrelated");
    build_fixture(&root, "a.py", "def recovered_symbol():\n    return 1\n")?;
    build_fixture(
        &unrelated,
        "a.py",
        "def unrelated_symbol():\n    return 2\n",
    )?;
    let mut first = McpSession::connect(&daemon.socket, &root, "first")?;
    let mut second = McpSession::connect(&daemon.socket, &root, "second")?;
    let mut other = McpSession::connect(&daemon.socket, &unrelated, "unrelated")?;
    let runtime = managed_session(&daemon, "first")?.runtime_pid;
    let backend = managed_session(&daemon, "second")?;
    assert_eq!(runtime, backend.runtime_pid);
    assert_ne!(runtime, managed_session(&daemon, "unrelated")?.runtime_pid);
    assert_eq!(
        unsafe { libc::kill(runtime as libc::pid_t, libc::SIGKILL) },
        0
    );
    daemon.wait_active_sessions(1, Duration::from_secs(8))?;
    wait_pid_gone(runtime, Duration::from_secs(3))?;
    for session in [&mut first, &mut second] {
        let deadline = Instant::now() + Duration::from_secs(4);
        let status = loop {
            if let Some(status) = session.child.try_wait()? {
                break status;
            }
            if Instant::now() >= deadline {
                bail!("proxy did not report its runtime's exit");
            }
            thread::sleep(Duration::from_millis(20));
        };
        assert!(
            !status.success(),
            "runtime loss requires a failed proxy exit"
        );
    }
    assert_eq!(
        other.call_tool("get_symbol", json!({"names":["unrelated_symbol"]}))?["total"],
        1
    );
    let mut recovered = McpSession::connect(&daemon.socket, &root, "recovered")?;
    assert_ne!(managed_session(&daemon, "recovered")?.runtime_pid, runtime);
    assert_eq!(
        recovered.call_tool("get_symbol", json!({"names":["recovered_symbol"]}))?["total"],
        1
    );
    recovered.close_gracefully()?;
    other.close_gracefully()?;
    Ok(())
}

#[test]
fn idle_runtime_expires_only_after_the_final_session_releases_it() -> Result<()> {
    let daemon = common::TestDaemon::start_with_env(&[("TSINDEX_RUNTIME_IDLE_SECS", "1")])?;
    let dir = tempdir_in("/tmp")?;
    build_fixture(dir.path(), "a.py", "def stable_symbol():\n    return 1\n")?;
    let first = McpSession::connect(&daemon.socket, dir.path(), "first")?;
    let mut second = McpSession::connect(&daemon.socket, dir.path(), "second")?;
    let runtime = managed_session(&daemon, "first")?.runtime_pid;
    first.close_gracefully()?;
    thread::sleep(Duration::from_secs(2));
    assert_eq!(managed_session(&daemon, "second")?.runtime_pid, runtime);
    assert_eq!(
        second.call_tool("get_symbol", json!({"names":["stable_symbol"]}))?["total"],
        1
    );
    second.close_gracefully()?;
    wait_pid_gone(runtime, Duration::from_secs(8))?;
    let deadline = Instant::now() + Duration::from_secs(2);
    while daemon.ping()?.index_runtimes != 0 {
        assert!(
            Instant::now() < deadline,
            "status still counted the evicted runtime"
        );
        thread::sleep(Duration::from_millis(20));
    }
    let fresh = McpSession::connect(&daemon.socket, dir.path(), "fresh")?;
    assert_ne!(managed_session(&daemon, "fresh")?.runtime_pid, runtime);
    fresh.close_gracefully()?;
    Ok(())
}

#[test]
fn orchestrator_dispatches_isolated_workers_for_two_worktree_sessions() -> Result<()> {
    let daemon = common::TestDaemon::start()?;
    let dir = tempdir_in("/tmp")?;
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
    let main = dir.path().join("main");
    let wt = dir.path().join("wt-feature");

    link_checkout(&main, &wt, "wt-feature")?;

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
fn daemon_normalizes_inherited_job_control_signals_before_spawning_children() -> Result<()> {
    use std::os::unix::process::CommandExt;

    let dir = tempdir_in("/tmp")?;
    build_fixture(dir.path(), "lib.py", "def target():\n    return 1\n")?;
    for termination in [libc::SIGTERM, libc::SIGINT] {
        let daemon = common::TestDaemon::start_configured(&[], |cmd| {
            // Only the fixture's forked child gets this launcher's signal state.
            // The test runner and unrelated test processes retain their masks.
            unsafe {
                cmd.pre_exec(|| {
                    let mut ignored: libc::sigaction = std::mem::zeroed();
                    ignored.sa_sigaction = libc::SIG_IGN;
                    libc::sigemptyset(&mut ignored.sa_mask);
                    let mut blocked: libc::sigset_t = std::mem::zeroed();
                    libc::sigemptyset(&mut blocked);
                    for signal in [libc::SIGTERM, libc::SIGINT, libc::SIGCHLD] {
                        ignored.sa_flags = if signal == libc::SIGCHLD {
                            libc::SA_NOCLDWAIT
                        } else {
                            0
                        };
                        if libc::sigaction(signal, &ignored, std::ptr::null_mut()) < 0
                            || libc::sigaddset(&mut blocked, signal) < 0
                        {
                            return Err(std::io::Error::last_os_error());
                        }
                    }
                    if libc::sigprocmask(libc::SIG_BLOCK, &blocked, std::ptr::null_mut()) < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
        })?;

        let first = McpSession::connect(&daemon.socket, dir.path(), "inherited-signals")?;
        let worker = managed_session(&daemon, "inherited-signals")?;
        first.close_gracefully()?;
        let outcome = daemon.client()?.session_detail(&worker.session_id)?;
        assert_eq!(outcome.exit_reason.as_deref(), Some("client_eof"));
        wait_pid_gone(worker.worker_pid, Duration::from_secs(1))?;

        let (data, ready) = raw_session(&daemon.socket, dir.path())?;
        let id = ready["session_id"].as_str().context("missing session ID")?;
        let pid = ready["worker_pid"].as_u64().context("missing worker PID")? as u32;
        let mut watcher = outcome_watch(&daemon.socket, id)?;
        assert_eq!(
            unsafe { libc::kill(daemon.pid() as libc::pid_t, termination) },
            0
        );
        let mut line = String::new();
        watcher.read_line(&mut line)?;
        let outcome: Value = serde_json::from_str(&line)?;
        assert_eq!(outcome["session"]["exit_reason"], "daemon_shutdown");
        wait_pid_gone(pid, Duration::from_secs(1))?;
        wait_pid_gone(worker.runtime_pid, Duration::from_secs(4))?;
        drop(data);
    }
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
fn losing_proxy_with_backpressured_worker_reaps_its_session() -> Result<()> {
    use std::os::fd::AsRawFd;

    let daemon = common::TestDaemon::start()?;
    let dir = tempdir_in("/tmp")?;
    build_fixture(dir.path(), "lib.py", "def target():\n    return 1\n")?;
    let mut proxy = McpSession::connect(&daemon.socket, dir.path(), "backpressure")?;
    let sessions = daemon.client()?.list_sessions(false)?.0;
    let pid = sessions[0].worker_pid;
    assert_eq!(unsafe { libc::kill(pid as libc::pid_t, libc::SIGSTOP) }, 0);

    // Fill a real proxy's stdin until backpressure reaches the harness. A
    // stopped worker will never make that pipe writable on its own.
    let stdin = proxy.child.stdin.as_mut().context("missing proxy stdin")?;
    let fd = stdin.as_raw_fd();
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    assert!(flags >= 0);
    assert_eq!(
        unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let mut frame = br#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#.to_vec();
    frame.resize(32 * 1024 - 1, b' ');
    frame.push(b'\n');
    let mut offset = 0;
    let mut sent = 0;
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        if Instant::now() >= deadline || sent > 2 * tsindex::daemon::MAX_MCP_FRAME_BYTES {
            bail!("proxy did not apply backpressure to harness writes");
        }
        match stdin.write(&frame[offset..]) {
            Ok(0) => bail!("proxy stopped accepting input"),
            Ok(n) => {
                sent += n;
                offset = (offset + n) % frame.len();
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                let mut writable = libc::pollfd {
                    fd,
                    events: libc::POLLOUT,
                    revents: 0,
                };
                let rc = unsafe { libc::poll(&mut writable, 1, 300) };
                if rc == 0 && sent >= frame.len() {
                    break;
                }
                if rc < 0
                    && std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted
                {
                    return Err(std::io::Error::last_os_error().into());
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    proxy.child.kill()?;
    proxy.child.wait()?;
    daemon.wait_active_sessions(0, Duration::from_secs(4))?;
    wait_pid_gone(pid, Duration::from_secs(1))?;
    Ok(())
}

#[test]
fn proxy_half_close_waits_for_a_slow_accepted_call() -> Result<()> {
    let daemon = common::TestDaemon::start()?;
    let dir = tempdir_in("/tmp")?;
    build_fixture(dir.path(), "lib.py", "def target():\n    return 1\n")?;
    let mut proxy = McpSession::connect(&daemon.socket, dir.path(), "slow-request")?;
    let client = daemon.client()?;
    let session = client.list_sessions(false)?.0.remove(0);
    let pid = session.worker_pid;
    assert_eq!(unsafe { libc::kill(pid as libc::pid_t, libc::SIGSTOP) }, 0);
    let mut stdin = proxy.child.stdin.take().context("missing proxy stdin")?;
    writeln!(
        stdin,
        "{}",
        json!({"jsonrpc":"2.0","id":27,"method":"ping"})
    )?;
    drop(stdin);

    // Keep the client connected beyond the former five-second EOF deadline.
    // Wait until the daemon has actually seen EOF before starting the clock.
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let sessions = client.list_sessions(false)?.0;
        if sessions
            .iter()
            .any(|s| s.worker_pid == pid && s.state == tsindex::daemon::SessionState::Draining)
        {
            break;
        }
        if Instant::now() >= deadline {
            bail!("proxy did not forward stdin EOF");
        }
        thread::sleep(Duration::from_millis(20));
    }
    thread::sleep(Duration::from_secs(6));
    assert!(
        client
            .list_sessions(false)?
            .0
            .iter()
            .any(|s| s.worker_pid == pid)
    );
    assert_eq!(unsafe { libc::kill(pid as libc::pid_t, libc::SIGCONT) }, 0);
    let response = proxy
        .responses
        .recv_timeout(Duration::from_secs(3))
        .with_context(|| {
            use std::io::Read;

            // Closing stdout can precede waitable exit. Give the proxy time to
            // finish reporting the cause before the fixture cleans it up.
            let status = proxy.wait_for_exit(Duration::from_secs(2));
            let mut stderr = String::new();
            if matches!(status, Ok(Some(_)))
                && let Some(pipe) = proxy.child.stderr.take()
            {
                let _ = pipe.take(16 * 1024).read_to_string(&mut stderr);
            }
            let mut log = String::new();
            if let Ok(file) = fs::File::open(&daemon.log) {
                let _ = file.take(16 * 1024).read_to_string(&mut log);
            }
            format!(
                "missing reply after worker resumed: proxy={status:?}, stderr={stderr:?}, session={:?}\n{log}",
                client.session_detail(&session.session_id)
            )
        })??;
    let response: Value = serde_json::from_str(&response)?;
    assert_eq!(response["id"], 27);
    assert!(response.get("result").is_some());
    proxy.close_gracefully()?;
    daemon.wait_active_sessions(0, Duration::from_secs(2))?;
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
    let socket = dir.path().join("detached.sock ");
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
    raw_session_version(socket, root, env!("CARGO_PKG_VERSION"))
}

fn raw_session_version(
    socket: &Path,
    root: &Path,
    version: &str,
) -> Result<(std::os::unix::net::UnixStream, Value)> {
    use std::os::unix::net::UnixStream;
    let mut stream = UnixStream::connect(socket)?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    writeln!(
        stream,
        "{}",
        json!({
            "op": "start_session", "protocol": tsindex::daemon::PROTOCOL_VERSION,
            "client_version": version, "root": root,
            "client_pid": std::process::id(), "no_refresh": true,
        })
    )?;
    let mut line = String::new();
    BufReader::new(&mut stream).read_line(&mut line)?;
    let ready: Value = serde_json::from_str(&line)?;
    Ok((stream, ready))
}

#[test]
fn full_disconnect_retains_its_cause_when_the_worker_needs_a_signal() -> Result<()> {
    let daemon = common::TestDaemon::start()?;
    let dir = tempdir_in("/tmp")?;
    build_fixture(dir.path(), "lib.py", "def target(): return 1\n")?;
    let (stream, ready) = raw_session(&daemon.socket, dir.path())?;
    let id = ready["session_id"].as_str().context("missing session ID")?;
    let pid = ready["worker_pid"]
        .as_u64()
        .and_then(|value| libc::pid_t::try_from(value).ok())
        .filter(|pid| *pid > 1)
        .context("missing worker PID")?;
    assert_eq!(unsafe { libc::kill(pid, libc::SIGSTOP) }, 0);
    drop(stream);

    daemon.wait_active_sessions(0, Duration::from_secs(6))?;
    let detail = daemon.client()?.session_detail(id)?;
    assert_eq!(detail.exit_reason.as_deref(), Some("client_disconnected"));
    wait_pid_gone(pid as u32, Duration::from_secs(1))?;
    Ok(())
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
fn repairing_an_index_database_allows_immediate_reconnect() -> Result<()> {
    let daemon = common::TestDaemon::start()?;
    let dir = tempdir_in("/tmp")?;
    let db = dir.path().join("index.db");
    let db_arg = db.to_str().context("fixture path")?;
    // SQLite rejects an existing directory. Fixing it leaves the same checkout,
    // configuration, and explicit database key for the immediately following open.
    fs::create_dir(&db)?;
    let failed = Command::new(BIN)
        .arg("--socket")
        .arg(&daemon.socket)
        .arg("--root")
        .arg(dir.path())
        .arg("--db")
        .arg(&db)
        .args(["--no-refresh", "serve", "--mcp", "--via-daemon"])
        .stdin(Stdio::null())
        .output()?;
    assert!(!failed.status.success());
    assert!(failed.stdout.is_empty());
    let diagnostic = String::from_utf8_lossy(&failed.stderr);
    assert!(diagnostic.contains("database"), "{diagnostic}");
    fs::remove_dir(&db)?;
    let mut recovered = McpSession::connect_with(
        &daemon.socket,
        dir.path(),
        "recovered-db",
        &["--no-refresh", "--db", db_arg],
    )?;
    assert_eq!(recovered.request("ping", json!({}))?["result"], json!({}));
    assert!(db.is_file());
    recovered.close_gracefully()
}

#[test]
fn inherited_descriptor_limit_preserves_controls_at_full_capacity() -> Result<()> {
    let daemon = common::TestDaemon::start_with_nofile(96)?;
    let client = daemon.client()?;
    let capacity = client.ping()?.max_sessions.context("missing capacity")?;
    assert!(
        (1..10).contains(&capacity),
        "unexpected low-limit capacity {capacity}"
    );
    let dir = tempdir_in("/tmp")?;
    let mut clients = Vec::new();
    for i in 0..capacity {
        let project = dir.path().join(i.to_string());
        fs::create_dir(&project)?;
        build_fixture(&project, "lib.py", "def target(): return 1\n")?;
        let (stream, reply) = raw_session(&daemon.socket, &project)?;
        assert_eq!(reply["kind"], "session_ready", "{reply}");
        let watch = outcome_watch(&daemon.socket, reply["session_id"].as_str().unwrap())?;
        clients.push((stream, watch));
    }
    let (_stream, reply) = raw_session(&daemon.socket, &dir.path().join("0"))?;
    assert_eq!(reply["kind"], "error");
    assert!(
        reply["error"]
            .as_str()
            .unwrap()
            .contains("maximum concurrent sessions")
    );
    let full = client.ping()?;
    assert_eq!(full.active_sessions, capacity);
    assert_eq!(full.max_sessions, Some(capacity));
    assert_eq!(client.kill_all()?.len(), capacity);
    drop(clients);
    daemon.wait_active_sessions(0, Duration::from_secs(3))?;
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
    let mut daemon = spawn_copied_daemon(
        Command::new(&exe)
            .arg("--socket")
            .arg(&socket)
            .args(["daemon", "run"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    )?;
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
    let daemon = common::TestDaemon::start_with_nofile(256)?;
    let dir = tempdir_in("/tmp")?;
    build_fixture(dir.path(), "lib.py", "def target():\n    return 1\n")?;
    let mut last = None;
    for i in 0..12 {
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
        // History and live sessions share a pagination cursor. Build more
        // than one page even when the launcher's descriptor cap is smaller.
        if i == 11 {
            last = Some(stream);
        } else {
            drop(stream);
            daemon.wait_active_sessions(0, Duration::from_secs(3))?;
        }
    }
    let sessions = daemon_json(&daemon.socket, &["daemon", "sessions", "--all"])?;
    let active = sessions["active"].as_array().unwrap();
    let recent = sessions["recent"].as_array().unwrap();
    assert_eq!((active.len(), recent.len()), (1, 11));
    let mut ids = std::collections::HashSet::new();
    for session in active.iter().chain(recent) {
        assert!(ids.insert(session["session_id"].as_str().unwrap()));
        assert!(session["harness_name"].as_str().unwrap().len() <= 256);
        assert!(session.get("recent_stderr").is_none());
    }
    drop(last);
    daemon.wait_active_sessions(0, Duration::from_secs(5))?;
    Ok(())
}

#[test]
fn session_listing_cursor_survives_an_earlier_session_exiting() -> Result<()> {
    use std::os::unix::net::UnixStream;
    let daemon = common::TestDaemon::start()?;
    let root = tempdir_in("/tmp")?;
    build_fixture(root.path(), "lib.py", "def target():\n    pass\n")?;
    let mut sessions = Vec::new();
    for _ in 0..9 {
        let (stream, ready) = raw_session(&daemon.socket, root.path())?;
        assert_eq!(ready["kind"], "session_ready");
        sessions.push((
            stream,
            ready["session_id"]
                .as_str()
                .context("session ID")?
                .to_owned(),
        ));
    }
    let list = |offset| -> Result<Value> {
        let mut stream = UnixStream::connect(&daemon.socket)?;
        stream.set_read_timeout(Some(Duration::from_secs(3)))?;
        writeln!(
            stream,
            "{}",
            json!({"op":"list_sessions", "include_recent":true, "offset":offset})
        )?;
        let mut line = String::new();
        BufReader::new(stream).read_line(&mut line)?;
        Ok(serde_json::from_str(&line)?)
    };
    let first = list(0)?;
    assert_eq!(
        first["active"].as_array().context("active sessions")?.len(),
        8
    );
    let cursor = first["next_offset"].as_u64().context("next page cursor")?;
    assert_eq!(first["active"][0]["session_id"], sessions[0].1);
    daemon.client()?.kill_session(sessions[0].1.clone())?;
    let second = list(cursor)?;
    assert_eq!(
        second["active"]
            .as_array()
            .context("active sessions")?
            .len(),
        1
    );
    assert_eq!(second["active"][0]["session_id"], sessions[8].1);
    assert_eq!(second["recent"], json!([]));
    assert!(second["next_offset"].is_null());
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
    // A raw watcher may signal it has finished sending and keep reading the
    // worker outcome. This is different from losing both proxy connections.
    watcher.get_ref().shutdown(std::net::Shutdown::Write)?;
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
fn compatible_wire_protocol_accepts_a_different_cli_package_version() -> Result<()> {
    let daemon = common::TestDaemon::start()?;
    let dir = tempdir_in("/tmp")?;
    build_fixture(dir.path(), "a.py", "def target():\n    return 1\n")?;
    let (mut stream, ready) = raw_session_version(&daemon.socket, dir.path(), "17.42.1")?;
    assert_eq!(ready["kind"], "session_ready", "{ready}");
    writeln!(
        stream,
        "{}",
        json!({"jsonrpc":"2.0", "id":9, "method":"ping"})
    )?;
    stream.shutdown(std::net::Shutdown::Write)?;
    let mut response = String::new();
    BufReader::new(stream).read_line(&mut response)?;
    let reply: Value = serde_json::from_str(&response)?;
    assert_eq!(reply["id"], 9);
    assert!(reply.get("result").is_some());
    Ok(())
}

#[test]
fn losing_outcome_connection_releases_worker_even_while_data_socket_stays_open() -> Result<()> {
    let daemon = common::TestDaemon::start()?;
    let dir = tempdir_in("/tmp")?;
    build_fixture(dir.path(), "a.py", "def target():\n    return 1\n")?;
    let (data, ready) = raw_session(&daemon.socket, dir.path())?;
    let pid = ready["worker_pid"].as_u64().context("worker PID")? as u32;
    let id = ready["session_id"].as_str().context("session ID")?;
    let watcher = outcome_watch(&daemon.socket, id)?;
    // Hold the data socket open, so it cannot provide the teardown signal.
    // The verified outcome connection is sufficient to notice proxy loss.
    drop(watcher);
    daemon.wait_active_sessions(0, Duration::from_secs(4))?;
    assert_eq!(
        daemon.client()?.session_detail(id)?.exit_reason.as_deref(),
        Some("client_disconnected")
    );
    wait_pid_gone(pid, Duration::from_secs(1))?;
    drop(data);
    Ok(())
}

#[test]
fn adjacent_frames_at_the_limit_arrive_before_eof_on_the_real_transport() -> Result<()> {
    let daemon = common::TestDaemon::start()?;
    let dir = tempdir_in("/tmp")?;
    build_fixture(dir.path(), "a.py", "def target():\n    return 1\n")?;
    let (mut stream, ready) = raw_session(&daemon.socket, dir.path())?;
    assert_eq!(ready["kind"], "session_ready");
    stream.set_write_timeout(Some(Duration::from_secs(6)))?;
    let mut first = br#"{"jsonrpc":"2.0","id":11,"method":"ping"}"#.to_vec();
    first.resize(tsindex::daemon::MAX_MCP_FRAME_BYTES - 1, b' ');
    stream.write_all(&first)?;
    stream.write_all(b"\n{\"jsonrpc\":\"2.0\",\"id\":12,\"method\":\"ping\"}\n")?;
    // Keeping stdin live ensures both responses are driven by the frames
    // themselves, with no extra write or EOF to kick the relay.
    let mut incoming = BufReader::new(stream);
    let mut ids = Vec::new();
    for _ in 0..2 {
        let mut response = String::new();
        incoming.read_line(&mut response)?;
        let reply: Value = serde_json::from_str(&response)?;
        assert!(reply.get("result").is_some());
        ids.push(reply["id"].as_u64().context("ping reply ID")?);
    }
    ids.sort();
    assert_eq!(ids, [11, 12]);
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

struct WrappedDaemon(Child);

impl Drop for WrappedDaemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_copied_daemon(command: &mut Command) -> Result<Child> {
    // A concurrent test's fork can briefly retain the copy's writable fd
    // until it execs. Linux rejects execution of that inode in the meantime.
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match command.spawn() {
            Err(error)
                if error.raw_os_error() == Some(libc::ETXTBSY) && Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(10));
            }
            result => return result.context("failed to start copied test daemon"),
        }
    }
}

/// Run the real orchestrator with a fixture wrapper for its child entry points.
fn start_with_worker_wrapper(
    dir: &Path,
    script: &str,
) -> Result<(
    WrappedDaemon,
    std::path::PathBuf,
    tsindex::daemon::DaemonClient,
)> {
    use tsindex::daemon::{DaemonClient, DaemonPaths};
    fs::set_permissions(dir, fs::Permissions::from_mode(0o700))?;
    let exe = dir.join("tsindex");
    let socket = dir.join("d.sock");
    fs::copy(BIN, &exe)?;
    let child = WrappedDaemon(spawn_copied_daemon(
        Command::new(&exe)
            .env("TSINDEX_TEST_RUNTIME_EXE", BIN)
            .arg("--socket")
            .arg(&socket)
            .args(["daemon", "run"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    )?);
    let client = DaemonClient::new(DaemonPaths::resolve(Some(&socket))?);
    let deadline = Instant::now() + Duration::from_secs(5);
    while client.ping().is_err() {
        assert!(Instant::now() < deadline, "copied daemon did not start");
        thread::sleep(Duration::from_millis(10));
    }
    let helper = dir.join("worker-wrapper");
    fs::write(&helper, script)?;
    fs::set_permissions(&helper, fs::Permissions::from_mode(0o700))?;
    fs::rename(helper, exe)?;
    Ok((child, socket, client))
}

#[cfg(target_os = "linux")]
#[test]
fn an_installation_change_between_child_launches_requires_a_fresh_connection() -> Result<()> {
    let dir = tempdir_in("/tmp")?;
    let root = dir.path().join("repo");
    build_fixture(&root, "lib.py", "def available(): return True\n")?;
    let replacement = dir.path().join("replacement");
    fs::copy(BIN, &replacement)?;
    // The worker has mapped the original inode when the next exec sees the new
    // installation. A failed handoff must never publish the wrong generation.
    let script = r#"#!/bin/sh
for arg do
  if [ "$arg" = index-runtime ] && [ -f "${0%/*}/replacement" ]; then
    exec "${0%/*}/replacement" "$@"
  fi
done
exec "$TSINDEX_TEST_RUNTIME_EXE" "$@"
"#;
    let (_guard, socket, _client) = start_with_worker_wrapper(dir.path(), script)?;
    let (_, failed) = raw_session(&socket, &root)?;
    assert_eq!(failed["kind"], "error", "{failed}");
    assert!(
        failed["error"]
            .as_str()
            .unwrap_or_default()
            .contains("installed binary changed"),
        "{failed}"
    );
    fs::remove_file(replacement)?;
    McpSession::connect(&socket, &root, "fresh-generation")?.close_gracefully()
}

#[test]
fn blocked_workspace_initialization_can_be_cancelled_while_another_checkout_serves() -> Result<()> {
    use std::os::unix::net::UnixStream;
    use tsindex::daemon::SessionState;

    let dir = tempdir_in("/tmp")?;
    let blocked = dir.path().join("await-config");
    let healthy = dir.path().join("healthy");
    fs::create_dir(&blocked)?;
    build_fixture(&healthy, "a.py", "def healthy_symbol():\n    return 1\n")?;
    // Model a worker waiting on an initialization dependency before it can
    // provide a runtime key. The fixture's private input stays open until
    // cancellation, and the other checkout uses the real session binary.
    let script = r#"#!/bin/sh
for arg do
  case "$arg" in
    */await-config) blocked=yes ;;
    worker) worker=yes ;;
  esac
done
if [ "$blocked" = yes ] && [ "$worker" = yes ]; then
  echo 'waiting for workspace configuration' >&2
  IFS= read -r configuration || exit 0
fi
exec "$TSINDEX_TEST_RUNTIME_EXE" "$@"
"#;
    let (_guard, socket, client) = start_with_worker_wrapper(dir.path(), script)?;
    let mut starting = UnixStream::connect(&socket)?;
    starting.set_read_timeout(Some(Duration::from_secs(5)))?;
    writeln!(
        starting,
        "{}",
        json!({
            "op":"start_session", "protocol":tsindex::daemon::PROTOCOL_VERSION,
            "client_version":env!("CARGO_PKG_VERSION"), "root":blocked,
            "client_pid":std::process::id(), "no_refresh":true,
        })
    )?;
    let deadline = Instant::now() + Duration::from_secs(5);
    let held = loop {
        if let Some(session) = client
            .list_sessions(false)?
            .0
            .into_iter()
            .find(|session| session.root == blocked)
        {
            let detail = client.session_detail(&session.session_id)?;
            if detail
                .recent_stderr
                .iter()
                .any(|line| line == "waiting for workspace configuration")
            {
                break detail;
            }
        }
        if Instant::now() >= deadline {
            bail!("fixture did not enter its initialization wait");
        }
        thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(held.state, SessionState::Starting);
    assert!(held.worker_pid > 1 && pid_exists(held.worker_pid));
    assert!(held.runtime_pid.is_none());
    assert_eq!(client.ping()?.index_runtimes, 0);

    let mut sibling = McpSession::connect(&socket, &healthy, "healthy")?;
    assert_eq!(
        sibling.call_tool("get_symbol", json!({"names":["healthy_symbol"]}))?["total"],
        1
    );
    client.kill_session(held.session_id.clone())?;
    let mut response = String::new();
    BufReader::new(starting).read_line(&mut response)?;
    let response: Value = serde_json::from_str(&response)?;
    assert_eq!(response["kind"], "error");
    assert_eq!(
        client
            .session_detail(&held.session_id)?
            .exit_reason
            .as_deref(),
        Some("killed_by_user")
    );
    wait_pid_gone(held.worker_pid, Duration::from_secs(1))?;
    assert_eq!(client.ping()?.index_runtimes, 1);
    assert!(sibling.request("ping", json!({}))?.get("result").is_some());
    sibling.close_gracefully()?;
    client.shutdown()?;
    Ok(())
}

#[test]
fn session_detail_prunes_escaped_stderr_to_the_encoded_frame_budget() -> Result<()> {
    use std::os::unix::net::UnixStream;
    use tsindex::daemon::{DaemonRequest, StartSessionRequest};

    let dir = tempdir_in("/tmp")?;
    let root = dir.path().join("repo");
    fs::create_dir(&root)?;

    // Replace only this fixture's worker path. Controlled shell literals emit
    // arbitrary diagnostics while the unchanged daemon handles the actual
    // capture, retention, SessionDetail pruning, and wire framing.
    let mut script = String::from(
        "#!/bin/sh\nfor arg do\n  if [ \"$arg\" = index-runtime ]; then exec \"$TSINDEX_TEST_RUNTIME_EXE\" \"$@\"; fi\ndone\n",
    );
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
    script.push_str("exec \"$TSINDEX_TEST_RUNTIME_EXE\" \"$@\"\n");
    let (_guard, socket, client) = start_with_worker_wrapper(dir.path(), &script)?;

    let mut request = StartSessionRequest {
        protocol: tsindex::daemon::PROTOCOL_VERSION,
        client_version: env!("CARGO_PKG_VERSION").to_string(),
        root,
        caller_dir: None,
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
