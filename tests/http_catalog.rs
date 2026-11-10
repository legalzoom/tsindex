//! Real HTTP catalog mutations must survive older, blocked refresh requests.
#![cfg(target_os = "linux")]

use std::fs;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use tsindex::config::{RepoConfig, TsIndexConfig, db_path};
use tsindex::maintenance::FileLease;

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn git(root: &Path, args: &[&str]) -> Result<()> {
    let output = Command::new("git").current_dir(root).args(args).output()?;
    ensure!(
        output.status.success(),
        "git: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

fn request(port: u16, path: &str, body: Value) -> Result<(u16, Value)> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;
    let body = serde_json::to_string(&body)?;
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: localhost:{port}\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )?;
    stream.shutdown(Shutdown::Write)?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let (headers, payload) = response
        .split_once("\r\n\r\n")
        .context("missing HTTP body")?;
    let status = headers
        .split_whitespace()
        .nth(1)
        .context("missing HTTP status")?
        .parse()?;
    Ok((status, serde_json::from_str(payload)?))
}

fn wait_for_refresh_attempt(server: &mut Server, refresh: &Path) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        // The parent owns this lease. Seeing the child open the same inode
        // proves it reached reconciliation. Between attempts FileLease closes
        // that descriptor and sleeps; HTTP has no watcher or watchdog sleepers.
        for fd in fs::read_dir(format!("/proc/{}/fd", server.0.id()))? {
            if fs::read_link(fd?.path()).is_ok_and(|path| path == refresh) {
                return Ok(());
            }
        }
        for task in fs::read_dir(format!("/proc/{}/task", server.0.id()))? {
            if fs::read_to_string(task?.path().join("wchan"))
                .is_ok_and(|channel| channel.trim() == "hrtimer_nanosleep")
            {
                return Ok(());
            }
        }
        ensure!(server.0.try_wait()?.is_none(), "HTTP server exited");
        ensure!(
            Instant::now() < deadline,
            "HTTP call never attempted reconciliation"
        );
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn an_older_http_refresh_cannot_undo_an_acknowledged_add() -> Result<()> {
    let artifacts = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/http-catalog-fixtures");
    fs::create_dir_all(&artifacts)?;
    let directory = tempfile::tempdir_in(artifacts)?;
    let root = directory.path().canonicalize()?;
    let catalog = root.join("catalog");
    let source = root.join("source");
    fs::create_dir_all(catalog.join("alpha"))?;
    fs::create_dir(&source)?;
    fs::write(
        catalog.join("alpha/a.py"),
        "def alpha_symbol():\n    pass\n",
    )?;
    fs::write(source.join("b.py"), "def beta_symbol():\n    pass\n")?;
    git(&source, &["init", "-b", "main"])?;
    git(&source, &["add", "."])?;
    git(
        &source,
        &[
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.test",
            "commit",
            "-m",
            "fixture",
        ],
    )?;
    let config = TsIndexConfig {
        repos: vec![RepoConfig {
            name: "alpha".into(),
            path: "alpha".into(),
            languages: vec!["python".into()],
            ignore: vec![],
        }],
        ..Default::default()
    };
    assert_eq!(config.server.worktree_refresh_ms, 2000);
    config.write(&catalog)?;
    let log = root.join("server.log");
    // URL rewriting is scoped to this child. The real add route still validates
    // a GitHub HTTPS URL and runs git clone, without network or global Git edits.
    let child = Command::new(env!("CARGO_BIN_EXE_tsindex"))
        .args([
            "--root",
            catalog.to_str().context("catalog path")?,
            "serve",
            "--http",
            "--port",
            "0",
        ])
        .env("GIT_CONFIG_COUNT", "2")
        .env(
            "GIT_CONFIG_KEY_0",
            format!("url.file://{}/.insteadOf", source.display()),
        )
        .env("GIT_CONFIG_VALUE_0", "https://github.com/fixture/beta.git")
        .env("GIT_CONFIG_KEY_1", "protocol.file.allow")
        .env("GIT_CONFIG_VALUE_1", "always")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(fs::File::create(&log)?)
        .spawn()?;
    let mut server = Server(child);
    let deadline = Instant::now() + Duration::from_secs(10);
    let port = loop {
        let output = fs::read_to_string(&log)?;
        if let Some(address) = output.lines().find_map(|line| {
            line.strip_prefix("tsindex http server listening on http://127.0.0.1:")
        }) {
            break address.parse::<u16>()?;
        }
        ensure!(
            server.0.try_wait()?.is_none(),
            "HTTP server exited: {output}"
        );
        ensure!(
            Instant::now() < deadline,
            "HTTP startup timed out: {output}"
        );
        thread::sleep(Duration::from_millis(10));
    };
    let database = db_path(&catalog);
    let refresh: PathBuf = database.with_extension("refresh.lock");
    let lease = FileLease::acquire(&refresh, Duration::from_secs(1))?;
    // Invalidate startup's marker without changing the default refresh interval.
    fs::remove_file(database.with_extension("refresh-state"))?;
    let older = thread::spawn(move || {
        request(
            port,
            "/tools/call",
            json!({
                "name":"get_symbol", "arguments":{"name":"alpha_symbol"}
            }),
        )
    });
    if let Err(error) = wait_for_refresh_attempt(&mut server, &refresh) {
        drop(lease);
        let completed = older.join().expect("HTTP request worker panicked");
        anyhow::bail!(
            "{error:#}; call result: {completed:?}; server: {}",
            fs::read_to_string(&log)?
        );
    }
    let (added_tx, added_rx) = mpsc::channel();
    let add = thread::spawn(move || {
        let result = request(
            port,
            "/repos/add",
            json!({
                "repo":"beta", "clone_url":"https://github.com/fixture/beta.git"
            }),
        );
        let _ = added_tx.send(result);
    });
    // A correct implementation may fence the add behind the old refresh, or
    // reject/re-resolve the stale build. Both must preserve the acknowledged add.
    let early_add = added_rx.recv_timeout(Duration::from_secs(2)).ok();
    drop(lease);
    let old_result = older.join().expect("HTTP request worker panicked");
    add.join().expect("HTTP add worker panicked");
    let added = match early_add {
        Some(result) => result,
        None => added_rx.recv()?,
    }?;
    ensure!(
        added.0 == 200 && added.1["ok"] == true,
        "add failed: {added:?}"
    );
    let old_result = old_result?;
    ensure!(
        old_result.0 == 200,
        "old indexed call failed: {old_result:?}"
    );
    let queried = request(
        port,
        "/tools/call",
        json!({
            "name":"query", "arguments":{
                "repo":"beta", "language":"python",
                "query":"(function_definition name: (identifier) @name)"
            }
        }),
    )?;
    ensure!(queried.0 == 200, "query failed: {queried:?}");
    assert_eq!(
        queried.1["captures"][0]["text"], "beta_symbol",
        "acknowledged add lost its indexed candidates: {queried:?}"
    );
    Ok(())
}
