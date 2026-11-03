//! Checkout behavior shared by the copied-index and proposed overlay designs.
//! Drive real MCP processes so storage layout is free to change underneath.

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use tempfile::TempDir;
use tsindex::config::TsIndexConfig;

const BIN: &str = env!("CARGO_BIN_EXE_tsindex");

struct CheckoutFixture {
    _directory: TempDir,
    main: PathBuf,
    alpha: PathBuf,
    beta: PathBuf,
}

impl CheckoutFixture {
    fn new(files: &[(&str, &str)]) -> Result<Self> {
        let artifacts = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/contract-fixtures");
        fs::create_dir_all(&artifacts)?;
        let directory = tempfile::tempdir_in(artifacts)?;
        let root = directory.path().canonicalize()?;
        let main = root.join("main");
        let alpha = root.join("alpha");
        let beta = root.join("beta");
        fs::create_dir_all(main.join(".git/worktrees"))?;
        for (path, id) in [(&alpha, "alpha"), (&beta, "beta")] {
            fs::create_dir_all(path)?;
            let metadata = main.join(".git/worktrees").join(id);
            fs::create_dir(&metadata)?;
            fs::write(
                path.join(".git"),
                format!("gitdir: {}\n", metadata.display()),
            )?;
            fs::write(metadata.join("commondir"), "../..\n")?;
            fs::write(
                metadata.join("gitdir"),
                format!("{}\n", path.join(".git").display()),
            )?;
        }
        for checkout in [&main, &alpha, &beta] {
            for (path, source) in files {
                write_source(checkout, path, source)?;
            }
        }
        let mut config = TsIndexConfig::default();
        config.languages.include = vec!["python".into()];
        // Every request reconciles source changes, avoiding sleeps tied to the
        // production refresh interval while retaining the real ingestion path.
        config.server.worktree_refresh_ms = 0;
        config.write(&main)?;
        let result = Command::new(BIN)
            .arg("--root")
            .arg(&main)
            .arg("build")
            .output()?;
        ensure!(
            result.status.success(),
            "fixture build failed: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        Ok(Self {
            _directory: directory,
            main,
            alpha,
            beta,
        })
    }
}

fn write_source(root: &Path, relative: &str, source: &str) -> Result<()> {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().context("source has no parent")?)?;
    fs::write(path, source)?;
    Ok(())
}

struct McpClient {
    child: Child,
    responses: mpsc::Receiver<std::io::Result<String>>,
    reader: Option<thread::JoinHandle<()>>,
    stderr: PathBuf,
    next_id: u64,
}

impl McpClient {
    fn start(root: &Path, label: &str) -> Result<Self> {
        let stderr = root.parent().context("checkout has no parent")?.join(label);
        let mut child = Command::new(BIN)
            .arg("--root")
            .arg(root)
            // The primary is already built. Worktree requests still use their
            // ordinary on-demand refresh, without unrelated watcher traffic.
            .args(["--no-refresh", "serve", "--mcp"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(fs::File::create(&stderr)?)
            .spawn()?;
        let stdout = child.stdout.take().expect("stdout was piped");
        let (sender, responses) = mpsc::channel();
        let reader = thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        let mut client = Self {
            child,
            responses,
            reader: Some(reader),
            stderr,
            next_id: 0,
        };
        client.request(
            "initialize",
            json!({"protocolVersion":"2024-11-05", "capabilities":{},
                "clientInfo":{"name":"worktree-contract", "version":"1"}}),
        )?;
        let input = client.child.stdin.as_mut().context("MCP stdin closed")?;
        writeln!(
            input,
            "{}",
            json!({"jsonrpc":"2.0", "method":"notifications/initialized"})
        )?;
        input.flush()?;
        Ok(client)
    }

    fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        self.next_id += 1;
        let request = json!({"jsonrpc":"2.0", "id":self.next_id,
            "method":method, "params":params});
        let input = self.child.stdin.as_mut().context("MCP stdin closed")?;
        writeln!(input, "{request}")?;
        input.flush()?;
        let line = self
            .responses
            .recv_timeout(Duration::from_secs(40))
            .with_context(|| {
                format!(
                    "MCP response missing: {}",
                    fs::read_to_string(&self.stderr).unwrap_or_default()
                )
            })??;
        let response: Value = serde_json::from_str(&line)?;
        ensure!(
            response["id"] == self.next_id,
            "unexpected response: {response}"
        );
        ensure!(response.get("error").is_none(), "MCP error: {response}");
        Ok(response)
    }

    fn call(&mut self, name: &str, arguments: Value) -> Result<Value> {
        let response = self.request("tools/call", json!({"name":name, "arguments":arguments}))?;
        let text = response["result"]["content"][0]["text"]
            .as_str()
            .context("MCP result has no text content")?;
        Ok(serde_json::from_str(text)?)
    }
}

impl Drop for McpClient {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

#[test]
fn read_tools_and_edits_use_the_selected_checkout() -> Result<()> {
    let fixture = CheckoutFixture::new(&[("a.py", "def identify():\n    return main_marker()\n")])?;
    write_source(
        &fixture.alpha,
        "a.py",
        "def identify():\n    return alpha_marker()\n",
    )?;
    write_source(
        &fixture.beta,
        "a.py",
        "def identify():\n    return beta_marker()\n",
    )?;
    let mut client = McpClient::start(&fixture.main, "routing.log")?;

    for (repo, expected_repo, marker) in [
        (json!(fixture.alpha), "alpha", "alpha_marker"),
        (json!("beta"), "beta", "beta_marker"),
        (Value::Null, "main", "main_marker"),
    ] {
        let symbol = client.call(
            "get_symbol",
            json!({"repo":repo,
            "name":"identify", "include_body":true}),
        )?;
        assert_eq!(symbol["total"], 1);
        assert_eq!(symbol["repo"], expected_repo);
        assert!(
            symbol["matches"][0]["body"]
                .as_str()
                .unwrap()
                .contains(marker)
        );
        let outline = client.call(
            "list_file_outline",
            json!({"repo":repo,
            "file":"a.py", "include_bodies_for":["identify"]}),
        )?;
        assert!(
            outline["symbols"][0]["body"]
                .as_str()
                .unwrap()
                .contains(marker)
        );
        let enclosing = client.call(
            "enclosing_symbol",
            json!({"repo":repo,
            "file":"a.py", "row":2, "include_body":true}),
        )?;
        assert_eq!(enclosing["matches"][0]["name"], "identify");
        assert_eq!(enclosing["repo"], expected_repo);
        assert!(
            enclosing["matches"][0]["body"]
                .as_str()
                .unwrap()
                .contains(marker)
        );
        let references = client.call(
            "find_references",
            json!({"repo":repo,
            "names":[marker], "snippet_lines":1, "group_by_file":false}),
        )?;
        assert_eq!(references["total"], 1);
        assert!(
            references["refs"][0]["snippet"]
                .as_str()
                .unwrap()
                .contains(marker)
        );
        let query = client.call(
            "query",
            json!({"repo":repo, "language":"python",
            "query":"(call function: (identifier) @callee)"}),
        )?;
        assert_eq!(query["captures"].as_array().unwrap().len(), 1);
        assert_eq!(query["captures"][0]["text"], marker);
        assert_eq!(query["captures"][0]["repo"], expected_repo);
    }

    client.call(
        "replace_symbol",
        json!({"repo":"alpha", "file":"a.py",
        "name":"identify", "new_body":"def identify():\n    return edited_alpha()"}),
    )?;
    for (repo, marker) in [
        ("alpha", "edited_alpha"),
        ("beta", "beta_marker"),
        ("main", "main_marker"),
    ] {
        let result = client.call(
            "get_symbol",
            json!({"repo":repo,
            "name":"identify", "include_body":true}),
        )?;
        assert!(
            result["matches"][0]["body"]
                .as_str()
                .unwrap()
                .contains(marker)
        );
    }
    Ok(())
}

#[test]
fn removed_or_excluded_files_never_resurface_from_the_primary_index() -> Result<()> {
    let fixture = CheckoutFixture::new(&[
        ("empty.py", "def emptied():\n    return shared_marker()\n"),
        ("delete.py", "def deleted():\n    return shared_marker()\n"),
        ("rename.py", "def moved():\n    return shared_marker()\n"),
        (
            "ignored.py",
            "def excluded():\n    return shared_marker()\n",
        ),
    ])?;
    let mut client = McpClient::start(&fixture.main, "lifecycle.log")?;
    let names = json!(["emptied", "deleted", "moved", "excluded"]);
    let initial = client.call("get_symbol", json!({"repo":"alpha", "names":names}))?;
    assert_eq!(initial["total"], 4);

    write_source(&fixture.alpha, "empty.py", "# No indexed symbols remain.\n")?;
    fs::remove_file(fixture.alpha.join("delete.py"))?;
    fs::rename(
        fixture.alpha.join("rename.py"),
        fixture.alpha.join("moved.py"),
    )?;
    write_source(&fixture.alpha, ".tsindexignore", "ignored.py\n")?;

    let updated = client.call("get_symbol", json!({"repo":"alpha", "names":names}))?;
    assert_eq!(updated["total"], 1);
    assert_eq!(updated["matches"][0]["name"], "moved");
    assert_eq!(updated["matches"][0]["file"], "moved.py");
    let references = client.call(
        "find_references",
        json!({"repo":"alpha",
        "names":["shared_marker"], "group_by_file":false}),
    )?;
    assert_eq!(references["total"], 1);
    assert_eq!(references["refs"][0]["file"], "moved.py");
    for repo in ["main", "beta"] {
        let unchanged = client.call("get_symbol", json!({"repo":repo, "names":names}))?;
        assert_eq!(unchanged["total"], 4);
    }

    // Reopening exercises the persistent checkout behavior through the same
    // protocol, without asserting whether its storage is a copy or an overlay.
    drop(client);
    let mut client = McpClient::start(&fixture.main, "lifecycle-reopened.log")?;
    let reopened = client.call("get_symbol", json!({"repo":"alpha", "names":names}))?;
    assert_eq!(reopened["total"], 1);
    write_source(
        &fixture.alpha,
        "empty.py",
        "def restored():\n    return shared_marker()\n",
    )?;
    let restored = client.call("get_symbol", json!({"repo":"alpha", "name":"restored"}))?;
    assert_eq!(restored["total"], 1);
    let references = client.call(
        "find_references",
        json!({"repo":"alpha",
        "names":["shared_marker"], "group_by_file":false}),
    )?;
    assert_eq!(references["total"], 2);
    Ok(())
}

#[test]
fn filtered_pages_count_only_the_selected_checkouts_current_files() -> Result<()> {
    let source = "def shared():\n    return shared_marker()\n";
    let fixture = CheckoutFixture::new(&[
        ("src/a.py", source),
        ("src/b.py", source),
        ("src/c.py", source),
        ("src/d.py", source),
    ])?;
    fs::remove_file(fixture.alpha.join("src/a.py"))?;
    write_source(&fixture.alpha, "src/b.py", "# This file is now empty.\n")?;
    write_source(
        &fixture.alpha,
        "src/c.py",
        "def other():\n    return other_marker()\n",
    )?;
    write_source(&fixture.alpha, "src/e.py", source)?;
    let mut client = McpClient::start(&fixture.main, "pagination.log")?;

    for (method, mut arguments, results) in [
        (
            "get_symbol",
            json!({"repo":"alpha", "name":"shared", "file_glob":"src"}),
            "matches",
        ),
        (
            "find_references",
            json!({"repo":"alpha", "names":["shared_marker"],
            "scope":"src", "group_by_file":false}),
            "refs",
        ),
        (
            "query",
            json!({"repo":"alpha", "language":"python", "file_glob":"src",
            "query":"(function_definition name: (identifier) @fn (#eq? @fn \"shared\"))"}),
            "captures",
        ),
    ] {
        arguments["limit"] = json!(1);
        for (offset, expected_file) in [(0, "src/d.py"), (1, "src/e.py")] {
            arguments["offset"] = json!(offset);
            let page = client.call(method, arguments.clone())?;
            assert_eq!(
                page[results].as_array().unwrap().len(),
                1,
                "{method}: {page}"
            );
            assert_eq!(page[results][0]["file"], expected_file, "{method}: {page}");
            assert_eq!(page["truncated"], offset == 0, "{method}: {page}");
            if method != "query" {
                assert_eq!(page["total"], 2, "{method}: {page}");
            }
            if offset == 0 {
                assert_eq!(page["next_offset"], 1, "{method}: {page}");
            } else {
                assert!(page.get("next_offset").is_none(), "{method}: {page}");
            }
        }
    }
    let primary = client.call("get_symbol", json!({"name":"shared", "file_glob":"src"}))?;
    assert_eq!(primary["total"], 4);
    Ok(())
}

#[test]
fn independent_agent_processes_ingest_their_own_worktree_changes() -> Result<()> {
    let fixture = CheckoutFixture::new(&[("a.py", "def identify():\n    return main_marker()\n")])?;
    let alpha_client = McpClient::start(&fixture.main, "agent-alpha.log")?;
    let beta_client = McpClient::start(&fixture.main, "agent-beta.log")?;
    let start = Arc::new(Barrier::new(2));
    thread::scope(|scope| -> Result<()> {
        let mut workers = Vec::new();
        for (mut client, root, repo, markers) in [
            (
                alpha_client,
                &fixture.alpha,
                "alpha",
                ["alpha_first", "alpha_second"],
            ),
            (
                beta_client,
                &fixture.beta,
                "beta",
                ["beta_first", "beta_second"],
            ),
        ] {
            let start = Arc::clone(&start);
            workers.push(scope.spawn(move || -> Result<()> {
                // Synchronize once before fallible work. Later barriers could
                // strand a peer if one agent's request fails an assertion.
                start.wait();
                for marker in markers {
                    write_source(
                        root,
                        "a.py",
                        &format!("def identify():\n    return {marker}()\n"),
                    )?;
                    let result = client.call(
                        "get_symbol",
                        json!({"repo":repo,
                        "name":"identify", "include_body":true}),
                    )?;
                    assert_eq!(result["repo"], repo);
                    assert!(
                        result["matches"][0]["body"]
                            .as_str()
                            .unwrap()
                            .contains(marker)
                    );
                    let references = client.call(
                        "find_references",
                        json!({"repo":repo,
                        "names":[marker], "group_by_file":false}),
                    )?;
                    assert_eq!(references["total"], 1);
                }
                Ok(())
            }));
        }
        for worker in workers {
            worker.join().expect("agent test worker panicked")?;
        }
        Ok(())
    })?;
    let mut client = McpClient::start(&fixture.main, "after-agents.log")?;
    for (repo, marker) in [
        ("main", "main_marker"),
        ("alpha", "alpha_second"),
        ("beta", "beta_second"),
    ] {
        let result = client.call(
            "get_symbol",
            json!({"repo":repo,
            "name":"identify", "include_body":true}),
        )?;
        assert!(
            result["matches"][0]["body"]
                .as_str()
                .unwrap()
                .contains(marker)
        );
    }
    Ok(())
}
