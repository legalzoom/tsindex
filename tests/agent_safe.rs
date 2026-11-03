use std::fs;
use std::path::Path;

use anyhow::Result;
use serde_json::{Value, json};
use tempfile::tempdir;
use tsindex::config::{TsIndexConfig, db_path};
use tsindex::index::{GetSymbolArgs, Runtime};
use tsindex::mcp::handle_request;

fn runtime(root: &Path) -> Result<Runtime> {
    let runtime = Runtime::new(
        root.canonicalize()?,
        db_path(root),
        TsIndexConfig::default(),
        vec![],
    );
    runtime.build(false, None)?;
    Ok(runtime)
}

fn symbol(runtime: &Runtime, extra: Value) -> Result<Value> {
    let mut args = json!({"name": "target"});
    args.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    Ok(serde_json::to_value(
        runtime.get_symbol(serde_json::from_value(args)?)?,
    )?)
}

fn rpc(runtime: &Runtime, tool: &str, args: Value) -> Result<Value> {
    Ok(handle_request(runtime, json!({"jsonrpc":"2.0", "id":1, "method":"tools/call", "params":{"name":tool, "arguments":args}}))?.unwrap())
}

#[test]
fn directory_and_file_filters_work_across_all_filtered_reads() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir(dir.path().join("src"))?;
    fs::write(
        dir.path().join("src/a.py"),
        "def target():\n    pass\ntarget()\n",
    )?;
    fs::write(
        dir.path().join("outside.py"),
        "def target():\n    pass\ntarget()\n",
    )?;
    let runtime = runtime(dir.path())?;
    let patterns = [
        "src".to_string(),
        "src/".into(),
        "./src".into(),
        runtime.root.join("src").display().to_string(),
        "src/**".into(),
        "src/a.py".into(),
    ];
    for pattern in patterns {
        let result = symbol(&runtime, json!({"file_glob":pattern}))?;
        assert_eq!(result["total"], 1, "{pattern}: {result}");
        assert!(result.get("diagnostics").is_none());
        for counts_only in [true, false] {
            let response = rpc(
                &runtime,
                "find_references",
                json!({"names":["target"], "scope":pattern, "counts_only":counts_only}),
            )?;
            let body: Value =
                serde_json::from_str(response["result"]["content"][0]["text"].as_str().unwrap())?;
            assert_eq!(body["total"], 1, "{pattern}: {body}");
            assert!(body.get("diagnostics").is_none());
        }
        let query = runtime.query(serde_json::from_value(
            json!({"language":"python", "query":"(identifier) @id", "file_glob":pattern}),
        )?)?;
        assert!(query.captures.iter().all(|c| c.file == "src/a.py"));
        assert!(!query.captures.is_empty());
    }
    for tool in ["get_symbol", "find_references", "query"] {
        let response = rpc(
            &runtime,
            tool,
            json!({
                "get_symbol": {"name":"target", "file_glob":"typo"},
                "find_references": {"names":["target"], "scope":"typo", "counts_only":true},
                "query": {"language":"python", "query":"(identifier) @id", "file_glob":"typo"}
            })[tool]
                .clone(),
        )?;
        let body: Value =
            serde_json::from_str(response["result"]["content"][0]["text"].as_str().unwrap())?;
        assert_eq!(body["diagnostics"][0]["code"], "filter_matched_no_files");
    }
    let response = rpc(
        &runtime,
        "get_symbol",
        json!({"name":"target", "file_glob":dir.path().parent().unwrap().join("outside")}),
    )?;
    assert_eq!(response["error"]["code"], -32602);
    Ok(())
}

#[test]
fn kind_errors_distinguish_unknown_kinds_from_filtered_names() -> Result<()> {
    let dir = tempdir()?;
    fs::write(
        dir.path().join("a.py"),
        "class Other:\n    pass\ndef target():\n    pass\n",
    )?;
    let runtime = runtime(dir.path())?;
    let response = rpc(
        &runtime,
        "get_symbol",
        json!({"name":"target", "kind":"imaginary"}),
    )?;
    assert_eq!(response["error"]["code"], -32602);
    assert!(
        response["error"]["message"]
            .as_str()
            .unwrap()
            .contains("function")
    );
    let response = symbol(&runtime, json!({"kind":"class"}))?;
    assert_eq!(response["total"], 0);
    assert_eq!(response["diagnostics"][0]["code"], "kind_filtered_out");
    assert_eq!(response["diagnostics"][0]["kinds"]["function"], 1);
    let response = symbol(&runtime, json!({"name":"absent", "kind":"class"}))?;
    assert!(response.get("diagnostics").is_none());
    Ok(())
}

#[test]
fn enclosing_falls_back_to_rows_and_serializes_empty_matches() -> Result<()> {
    let dir = tempdir()?;
    fs::write(
        dir.path().join("a.ts"),
        "// top level\nfunction target() {\n  return 1;\n}\n",
    )?;
    let runtime = runtime(dir.path())?;
    let response = runtime.enclosing_symbol(serde_json::from_value(
        json!({"file":"a.ts", "row":4, "col":99}),
    )?)?;
    assert_eq!(response.matches[0].name, "target");
    assert_eq!(response.diagnostics[0].code, "col_ignored");
    let response =
        runtime.enclosing_symbol(serde_json::from_value(json!({"file":"a.ts", "row":1}))?)?;
    let response = serde_json::to_value(response)?;
    assert_eq!(response["matches"], json!([]));
    assert_eq!(response["diagnostics"][0]["code"], "no_enclosing_symbol");
    let batch = runtime.enclosing_symbol(serde_json::from_value(
        json!({"file":"a.ts", "rows":[1, 3]}),
    )?)?;
    assert_eq!(batch.results[0].diagnostics[0].code, "no_enclosing_symbol");
    assert!(batch.results[1].diagnostics.is_empty());
    Ok(())
}

#[test]
fn unknown_repo_is_invalid_params_on_every_tool() -> Result<()> {
    let dir = tempdir()?;
    fs::write(dir.path().join("a.py"), "def target():\n    pass\n")?;
    let runtime = runtime(dir.path())?;
    for tool in [
        "get_symbol",
        "list_file_outline",
        "enclosing_symbol",
        "find_references",
        "query",
        "replace_symbol",
    ] {
        let response = rpc(&runtime, tool, json!({"repo":"does-not-exist"}))?;
        assert_eq!(response["error"]["code"], -32602, "{response}");
        let message = response["error"]["message"].as_str().unwrap();
        assert!(message.contains(dir.path().file_name().unwrap().to_str().unwrap()));
        assert!(message.contains("indexed=true"));
    }
    Ok(())
}

#[test]
fn query_uses_index_and_skips_parsing_files_without_required_literals() -> Result<()> {
    let dir = tempdir()?;
    fs::write(dir.path().join("a.ts"), "target();")?;
    fs::write(dir.path().join("b.tsx"), "target();")?;
    fs::write(dir.path().join("c.ts"), "other();")?;
    let runtime = runtime(dir.path())?;
    // A ready index must not walk: this new file is deliberately not indexed.
    fs::write(dir.path().join("unindexed.ts"), "target();")?;
    let response = runtime.query(serde_json::from_value(json!({"languages":["typescript","tsx"], "query":"(call_expression function: (identifier) @fn (#eq? @fn \"target\"))"}))?)?;
    assert_eq!(response.captures.len(), 2);
    assert_eq!(response.files_scanned, 3);
    assert_eq!(response.files_parsed, 2);
    assert!(response.diagnostics.is_empty());
    // TSX supports JSX nodes; TypeScript doesn't. One failure must not lose TSX results.
    fs::write(dir.path().join("b.tsx"), "const view = <div />;")?;
    let response = runtime.query(serde_json::from_value(
        json!({"languages":["typescript","tsx"], "query":"(jsx_self_closing_element) @jsx"}),
    )?)?;
    assert_eq!(response.captures.len(), 1);
    assert_eq!(response.diagnostics[0].code, "query_compile_failed");
    Ok(())
}

#[test]
fn query_literal_prefilter_preserves_alternatives_and_optional_captures() -> Result<()> {
    let dir = tempdir()?;
    fs::write(dir.path().join("a.py"), "def target():\n    return 1\n")?;
    let runtime = runtime(dir.path())?;
    for query in [
        "((identifier) @id (#eq? @id \"missing\")) (integer) @number",
        "((function_definition (parameters (identifier)? @arg)) @fn (#eq? @arg \"missing\"))",
    ] {
        let result = runtime.query(serde_json::from_value(
            json!({"language":"python", "query":query}),
        )?);
        assert!(!result?.captures.is_empty(), "{query}");
    }
    // A never-indexed checkout still uses the live fallback.
    let fresh = tempdir()?;
    fs::write(fresh.path().join("a.py"), "target()")?;
    let live = Runtime::new(
        fresh.path().to_path_buf(),
        db_path(fresh.path()),
        TsIndexConfig::default(),
        vec![],
    );
    assert_eq!(
        live.query(serde_json::from_value(
            json!({"language":"python", "query":"(identifier) @id"})
        )?)?
        .captures
        .len(),
        1
    );
    Ok(())
}

#[test]
fn query_literals_decode_escaped_strings_like_tree_sitter() -> Result<()> {
    let dir = tempdir()?;
    fs::write(dir.path().join("a.py"), "value = \"target\"\n")?;
    let runtime = runtime(dir.path())?;
    let query = r#"((string) @s (#eq? @s "\"target\""))"#;
    let response = runtime.query(serde_json::from_value(
        json!({"language":"python", "query":query}),
    )?)?;
    assert_eq!(response.captures.len(), 1);
    assert_eq!(response.files_parsed, 1);
    Ok(())
}

#[test]
fn content_preserving_touches_do_not_reparse_and_schema_invalidation_still_does() -> Result<()> {
    let dir = tempdir()?;
    fs::write(dir.path().join("a.py"), "def target():\n    pass\n")?;
    let runtime = runtime(dir.path())?;
    let source = runtime.root.join("a.py");
    let new_time = fs::metadata(&source)?.modified()? + std::time::Duration::from_secs(10);
    fs::File::options()
        .write(true)
        .open(&source)?
        .set_modified(new_time)?;
    let stats = runtime.build(true, None)?;
    assert_eq!((stats.indexed, stats.touched), (0, 1));
    assert_eq!(runtime.build(true, None)?.skipped, 1);
    fs::File::options()
        .write(true)
        .open(&source)?
        .set_modified(new_time + std::time::Duration::from_secs(1))?;
    assert_eq!(runtime.update_paths(&[source])?.touched, 1);
    let conn = rusqlite::Connection::open(&runtime.db_path)?;
    conn.execute("UPDATE files SET mtime_ns = -1", [])?;
    assert_eq!(runtime.build(true, None)?.indexed, 1);
    let args: GetSymbolArgs = serde_json::from_value(json!({"name":"target"}))?;
    assert_eq!(runtime.get_symbol(args)?.total, 1);
    Ok(())
}
