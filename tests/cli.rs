//! End-to-end tests that drive the real `tsindex` binary: CLI round trip,
//! MCP over stdio, and the HTTP server over TCP. Everything else in the suite
//! goes through the library API, so these are the only checks that arg
//! parsing, stdout hygiene, and transport wiring actually work.

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

    let lines: Vec<Value> = BufReader::new(&output.stdout[..])
        .lines()
        .map(|l| serde_json::from_str(&l.unwrap()).expect("every stdout line is JSON-RPC"))
        .collect();
    assert_eq!(
        lines.len(),
        3,
        "one response per request, nothing else on stdout"
    );
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
