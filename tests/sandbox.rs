#![cfg(target_os = "linux")]

mod common;

use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde_json::{Value, json};

struct SandboxProcess(Option<Child>);

impl Drop for SandboxProcess {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[test]
fn direct_mcp_preserves_harness_filesystem_sandbox() -> Result<()> {
    // Some CI hosts disable unprivileged user namespaces. Probe the actual
    // boundary before starting fixtures so an unavailable runtime is explicit.
    let probe = Command::new("bwrap")
        .args([
            "--unshare-user",
            "--unshare-pid",
            "--unshare-net",
            "--ro-bind",
            "/",
            "/",
            "true",
        ])
        .output();
    match probe {
        Ok(output) if output.status.success() => {}
        other => {
            anyhow::ensure!(
                std::env::var("TSINDEX_REQUIRE_SANDBOX").as_deref() != Ok("1"),
                "required sandbox boundary is unavailable: {other:?}"
            );
            eprintln!("sandbox test skipped because bubblewrap is unavailable: {other:?}");
            return Ok(());
        }
    }

    let daemon = common::TestDaemon::start()?;
    let host_network_namespace = fs::read_link("/proc/self/ns/net")?;
    let dir = tempfile::tempdir()?;
    let repo = dir.path().join("allowed");
    let outside = dir.path().join("outside");
    fs::create_dir_all(repo.join(".tsindex"))?;
    fs::create_dir_all(repo.join("src"))?;
    fs::create_dir_all(repo.join("readonly"))?;
    fs::create_dir_all(&outside)?;
    fs::write(repo.join("src/edit.py"), "def allowed():\n    return 1\n")?;
    let locked_source = "def locked():\n    return 2\n";
    fs::write(repo.join("readonly/locked.py"), locked_source)?;
    let outside_source = "def secret():\n    return 'private-source-sentinel'\n";
    fs::write(outside.join("secret.py"), outside_source)?;
    // The catalog deliberately names a repository that is visible to the
    // outside daemon and hidden from the harness's mount namespace.
    fs::write(
        repo.join(".tsindex/config.toml"),
        format!(
            "[root]\npath = \".\"\n\n[[repos]]\nname = \"allowed\"\npath = \".\"\n\n[[repos]]\nname = \"outside\"\npath = {}\n",
            serde_json::to_string(&outside)?
        ),
    )?;

    let mut sandbox = Command::new("bwrap");
    sandbox.args([
        "--unshare-user",
        "--unshare-pid",
        "--unshare-net",
        "--die-with-parent",
        "--new-session",
    ]);
    for path in ["/usr", "/bin", "/lib", "/lib64"] {
        if std::path::Path::new(path).exists() {
            sandbox.args(["--ro-bind", path, path]);
        }
    }
    // Start with an empty filesystem view, exposing the executable, runtime
    // libraries and this workspace. Host home, tmp and daemon sockets stay out.
    let mut child = SandboxProcess(Some(
        sandbox
            .args(["--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp"])
            .arg("--bind")
            .arg(&repo)
            .arg("/workspace")
            .arg("--ro-bind")
            .arg(repo.join("readonly"))
            .arg("/workspace/readonly")
            .arg("--ro-bind")
            .arg(common::BIN)
            .arg("/tsindex")
            .args([
                "--chdir",
                "/workspace",
                "--setenv",
                "HOME",
                "/workspace/.home",
            ])
            .args(["--setenv", "TSINDEX_HOME", "/workspace/.home"])
            .args(["--setenv", "TSINDEX_SOCKET"])
            .arg(&daemon.socket)
            .args([
                "/bin/sh",
                "-ec",
                "readlink /proc/self/ns/net >&2; exec \"$@\"",
                "sandbox",
                "/tsindex",
                "--root",
                "/workspace",
                "serve",
                "--mcp",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?,
    ));
    let requests = [
        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"sandbox-test","version":"1"}}}),
        json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"get_symbol","arguments":{"repo":"allowed","names":["allowed"],"include_body":true}}}),
        json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"replace_symbol","arguments":{"repo":"allowed","file":"src/edit.py","name":"allowed","new_body":"def allowed():\n    return 3"}}}),
        json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"get_symbol","arguments":{"repo":"outside","names":["secret"],"include_body":true}}}),
        json!({"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"replace_symbol","arguments":{"repo":"outside","file":"secret.py","name":"secret","new_body":"def secret():\n    return 'changed'"}}}),
        json!({"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"replace_symbol","arguments":{"repo":"allowed","file":"readonly/locked.py","name":"locked","new_body":"def locked():\n    return 6"}}}),
    ];
    {
        let mut stdin = child
            .0
            .as_mut()
            .unwrap()
            .stdin
            .take()
            .context("sandbox stdin")?;
        for request in requests {
            writeln!(stdin, "{request}")?;
        }
    }
    let deadline = Instant::now() + Duration::from_secs(20);
    while child.0.as_mut().unwrap().try_wait()?.is_none() {
        anyhow::ensure!(
            Instant::now() < deadline,
            "sandbox MCP session did not exit"
        );
        thread::sleep(Duration::from_millis(25));
    }
    let output = child.0.take().unwrap().wait_with_output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    let sandbox_network_namespace = stderr
        .lines()
        .find(|line| line.starts_with("net:["))
        .context("sandbox launcher did not report its network namespace")?;
    assert_ne!(
        sandbox_network_namespace,
        host_network_namespace.to_string_lossy(),
        "sandbox shared the host network namespace"
    );
    let replies: HashMap<u64, Value> = BufReader::new(&output.stdout[..])
        .lines()
        .map(|line| -> Result<_> {
            let reply: Value = serde_json::from_str(&line?)?;
            Ok((reply["id"].as_u64().context("response ID")?, reply))
        })
        .collect::<Result<_>>()?;
    assert_eq!(replies.len(), 6);
    assert!(replies[&2].get("error").is_none(), "{}", replies[&2]);
    assert!(replies[&2].to_string().contains("allowed"));
    assert!(replies[&3].get("error").is_none(), "{}", replies[&3]);
    for id in [4, 5, 6] {
        assert!(replies[&id].get("error").is_some(), "{}", replies[&id]);
    }
    assert!(
        replies[&6]["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Read-only file system"),
        "{}",
        replies[&6]
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains("private-source-sentinel"));
    assert!(fs::read_to_string(repo.join("src/edit.py"))?.contains("return 3"));
    assert_eq!(
        fs::read_to_string(repo.join("readonly/locked.py"))?,
        locked_source
    );
    assert_eq!(
        fs::read_to_string(outside.join("secret.py"))?,
        outside_source
    );
    daemon.wait_active_sessions(0, Duration::from_secs(1))?;
    assert_eq!(daemon.ping()?.total_sessions_started, 0);
    Ok(())
}
