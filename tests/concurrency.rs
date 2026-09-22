//! Reader/writer interleavings the design relies on (`serve --mcp` runs a
//! writer thread beside the reader) but nothing else exercises.

use std::fs;
use std::path::Path;
use std::thread;

use anyhow::Result;
use rusqlite::Connection;
use tempfile::tempdir;

use tsindex::config::{TsIndexConfig, db_path};
use tsindex::index::{GetSymbolArgs, Runtime};

fn runtime_with_files(root: &Path, files: usize) -> Result<Runtime> {
    fs::create_dir_all(root.join("src"))?;
    fs::write(root.join("pyproject.toml"), "[project]\nname='f'\n")?;
    for i in 0..files {
        fs::write(
            root.join(format!("src/m{i}.py")),
            format!("def fn_{i}(x):\n    return shared(x) + {i}\n"),
        )?;
    }
    let config = TsIndexConfig::default();
    config.write(root)?;
    Ok(Runtime::new(
        root.to_path_buf(),
        db_path(root),
        config,
        vec!["python".to_string()],
    ))
}

fn lookup(runtime: &Runtime, name: &str) -> Result<usize> {
    Ok(runtime
        .get_symbol(GetSymbolArgs {
            name: Some(name.to_string()),
            names: None,
            repo: None,
            kind: None,
            file_glob: None,
            include_body: false,
            context_lines: 0,
            max_body_lines: None,
            limit: None,
            offset: None,
        })?
        .matches
        .len())
}

#[test]
fn reader_during_first_build_sees_not_ready_or_results_never_panics() -> Result<()> {
    let dir = tempdir()?;
    let runtime = runtime_with_files(dir.path(), 300)?;
    let writer = runtime.clone();
    let builder = thread::spawn(move || writer.build(false, None));

    // No `reads > 0` assertion: a fast build may finish before the loop body
    // runs, and the lookup after `join` covers the "read succeeds" leg.
    let mut unexpected = Vec::new();
    while !builder.is_finished() {
        match lookup(&runtime, "fn_7") {
            Ok(_) => {}
            Err(error) => {
                let message = format!("{error:#}");
                let known = message.contains("not ready")
                    || message.contains("failed to open")
                    || message.contains("no such table");
                if !known {
                    unexpected.push(message);
                }
            }
        }
    }
    builder.join().unwrap()?;
    assert!(
        unexpected.is_empty(),
        "unexpected read errors: {unexpected:#?}"
    );
    assert_eq!(lookup(&runtime, "fn_7")?, 1);
    Ok(())
}

#[test]
fn two_incremental_builds_on_one_database_both_succeed() -> Result<()> {
    let dir = tempdir()?;
    let runtime = runtime_with_files(dir.path(), 120)?;
    runtime.build(false, None)?;
    for i in 0..120 {
        fs::write(
            dir.path().join(format!("src/m{i}.py")),
            format!("def fn_{i}(x):\n    return shared(x) * {i}\n"),
        )?;
    }
    let a = runtime.clone();
    let b = runtime.clone();
    let ta = thread::spawn(move || a.build(true, None));
    let tb = thread::spawn(move || b.build(true, None));
    let sa = ta.join().unwrap()?;
    let sb = tb.join().unwrap()?;
    // Both writers may re-index the same file (each diffs against the state
    // it prefetched); the contract is only that neither fails and the index
    // ends up consistent.
    assert!(sa.indexed <= 120 && sb.indexed <= 120);
    assert!(
        sa.indexed + sb.indexed >= 120,
        "every changed file indexed by someone"
    );
    assert_eq!(lookup(&runtime, "fn_5")?, 1);
    Ok(())
}

#[test]
fn newer_schema_version_than_binary_is_rejected_on_read() -> Result<()> {
    let dir = tempdir()?;
    let runtime = runtime_with_files(dir.path(), 2)?;
    runtime.build(false, None)?;
    Connection::open(db_path(dir.path()))?.pragma_update(None, "user_version", 99)?;

    let error = lookup(&runtime, "fn_1")
        .expect_err("a newer on-disk schema must not be read as if compatible");
    let message = format!("{error:#}");
    assert!(
        message.contains("newer") && message.contains("delete the index database"),
        "error should say the index is newer and how to recover: {message}"
    );
    Ok(())
}
