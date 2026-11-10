use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::Result;
use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};
use tsindex::config::{TsIndexConfig, db_path};
use tsindex::index::Runtime;
use tsindex::mcp::handle_request_with_runtimes;
use tsindex::worktrees::{Runtimes, discover};

struct Fixture {
    _dir: TempDir,
    main: PathBuf,
    one: PathBuf,
    two: PathBuf,
}

fn linked(main: &Path, path: &Path, id: &str) -> Result<()> {
    fs::create_dir_all(path)?;
    let metadata = main.join(".git/worktrees").join(id);
    fs::create_dir_all(&metadata)?;
    fs::write(
        path.join(".git"),
        format!("gitdir: {}\n", metadata.display()),
    )?;
    fs::write(metadata.join("commondir"), "../..\n")?;
    fs::write(
        metadata.join("gitdir"),
        format!("{}\n", path.join(".git").display()),
    )?;
    Ok(())
}

impl Fixture {
    fn new() -> Result<Self> {
        let dir = tempdir()?;
        let base = dir.path().canonicalize()?;
        let main = base.join("main");
        fs::create_dir_all(main.join(".git"))?;
        let one = base.join("branch-one");
        let two = main.join("playpen/branch-two");
        linked(&main, &one, "one")?;
        linked(&main, &two, "two")?;
        linked(&main, &base.join("prunable"), "gone")?;
        fs::remove_dir_all(base.join("prunable"))?;
        for root in [&main, &one, &two] {
            fs::write(root.join("shared.py"), "def shared():\n    return 1\n")?;
        }
        fs::write(one.join("only.py"), "def worktree_only():\n    return 2\n")?;
        Ok(Self {
            _dir: dir,
            main,
            one,
            two,
        })
    }

    fn runtime(&self) -> Result<Runtime> {
        let mut config = TsIndexConfig::default();
        config.languages.include = vec!["python".into()];
        config.server.worktree_refresh_ms = 20;
        config.write(&self.main)?;
        let runtime = Runtime::new(self.main.clone(), db_path(&self.main), config, vec![]);
        runtime.build(false, None)?;
        Ok(runtime)
    }
}

fn call(runtimes: &Runtimes, name: &str, args: Value) -> Result<Value> {
    let response = handle_request_with_runtimes(runtimes, json!({"jsonrpc":"2.0", "id":1, "method":"tools/call", "params":{"name":name,"arguments":args}}))?.unwrap();
    assert!(response.get("error").is_none(), "{response}");
    Ok(serde_json::from_str(
        response["result"]["content"][0]["text"].as_str().unwrap(),
    )?)
}

#[test]
fn discovery_finds_main_siblings_nested_worktrees_and_skips_prunable_entries() -> Result<()> {
    let f = Fixture::new()?;
    for root in [&f.main, &f.one, &f.two] {
        let all = discover(root)?;
        assert_eq!(all.len(), 3);
        assert!(all.iter().any(|c| c.path == f.main));
        assert!(all.iter().any(|c| c.path == f.one));
        assert!(all.iter().any(|c| c.path == f.two));
    }
    // Relative gitdir pointers are resolved relative to the .git file.
    fs::write(f.one.join(".git"), "gitdir: ../main/.git/worktrees/one\n")?;
    assert_eq!(discover(&f.one)?.len(), 3);
    Ok(())
}

#[test]
fn first_request_seeds_touched_files_and_routes_edits_to_the_worktree() -> Result<()> {
    let f = Fixture::new()?;
    let runtime = f.runtime()?;
    let conn = rusqlite::Connection::open(&runtime.db_path)?;
    // A preserved extraction proves the first update reused the seeded row.
    conn.execute(
        "UPDATE symbols SET signature = 'seeded extraction' WHERE name = 'shared'",
        [],
    )?;
    let runtimes = Runtimes::new(runtime);
    assert_eq!(runtimes.list()?.len(), 3);
    assert!(!f.one.join(".tsindex").exists());
    assert!(!f.two.join(".tsindex").exists());
    fs::create_dir(f.one.join(".tsindex"))?;
    fs::write(
        f.one.join(".tsindex/.gitignore"),
        "# existing local rules\n",
    )?;
    let result = call(
        &runtimes,
        "get_symbol",
        json!({"repo":"branch-one", "names":["shared","worktree_only"]}),
    )?;
    assert_eq!(result["total"], 2);
    assert_eq!(result["repo"], "branch-one");
    assert_eq!(result["matches"][0]["signature"], "seeded extraction");
    assert!(f.one.join(".tsindex/config.toml").is_file());
    assert!(f.one.join(".tsindex/index.db").is_file());
    assert_eq!(
        fs::read_to_string(f.one.join(".tsindex/.gitignore"))?,
        "# existing local rules\n*\n"
    );
    assert!(!f.two.join(".tsindex").exists());
    let config = TsIndexConfig::load(&f.one)?;
    assert_eq!(config.repos.len(), 1);
    assert_eq!(config.repos[0].name, "branch-one");
    assert_eq!(config.repos[0].path, ".");
    assert_eq!(config.server.worktree_refresh_ms, 20);
    call(
        &runtimes,
        "replace_symbol",
        json!({"repo":f.one, "file":"shared.py", "name":"shared", "new_body":"def shared():\n    return 7"}),
    )?;
    assert!(fs::read_to_string(f.one.join("shared.py"))?.contains("return 7"));
    assert!(fs::read_to_string(f.main.join("shared.py"))?.contains("return 1"));
    // Worktrees are never implicitly merged into the primary's results.
    assert_eq!(
        call(&runtimes, "get_symbol", json!({"name":"worktree_only"}))?["total"],
        0
    );
    let nested = call(
        &runtimes,
        "get_symbol",
        json!({"repo":"branch-two", "name":"shared"}),
    )?;
    assert_eq!(nested["total"], 1);
    assert_eq!(
        call(
            &runtimes,
            "get_symbol",
            json!({"repo":"main", "name":"shared"})
        )?["total"],
        1
    );
    Ok(())
}

#[test]
fn freshness_deletion_and_late_discovery_work_without_watchers() -> Result<()> {
    let f = Fixture::new()?;
    let runtimes = Runtimes::new(f.runtime()?);
    call(
        &runtimes,
        "get_symbol",
        json!({"repo":"branch-one", "name":"shared"}),
    )?;
    fs::write(f.one.join("new.py"), "def newly_added():\n    return 3\n")?;
    std::thread::sleep(std::time::Duration::from_millis(30));
    assert_eq!(
        call(
            &runtimes,
            "get_symbol",
            json!({"repo":"branch-one", "name":"newly_added"})
        )?["total"],
        1
    );
    fs::remove_dir_all(&f.one)?;
    let error = runtimes
        .resolve(Some("branch-one"))
        .unwrap_err()
        .to_string();
    assert!(error.contains("no longer exists"), "{error}");
    let late = f.main.parent().unwrap().join("late");
    linked(&f.main, &late, "late")?;
    fs::write(late.join("late.py"), "def late_symbol():\n    pass\n")?;
    assert_eq!(
        call(
            &runtimes,
            "get_symbol",
            json!({"repo":"late", "name":"late_symbol"})
        )?["total"],
        1
    );
    Ok(())
}

#[test]
fn collisions_are_disambiguated_and_absolute_paths_resolve() -> Result<()> {
    let f = Fixture::new()?;
    let collision = f.main.parent().unwrap().join("elsewhere/main");
    linked(&f.main, &collision, "collision")?;
    fs::write(collision.join("a.py"), "def branch():\n    pass\n")?;
    let names = discover(&f.one)?
        .into_iter()
        .map(|c| c.name)
        .collect::<Vec<_>>();
    assert!(names.contains(&"main@main".into()));
    assert!(names.contains(&"main@collision".into()));
    let runtimes = Runtimes::new(f.runtime()?);
    assert!(runtimes.resolve(Some("main")).is_err());
    assert_eq!(
        call(
            &runtimes,
            "get_symbol",
            json!({"repo":"main@collision", "name":"branch"})
        )?["total"],
        1
    );
    assert_eq!(
        call(
            &runtimes,
            "get_symbol",
            json!({"repo":collision, "name":"branch"})
        )?["total"],
        1
    );
    assert_eq!(
        call(
            &runtimes,
            "get_symbol",
            json!({"repo":"main@main", "name":"shared"})
        )?["total"],
        1
    );
    let response = call(
        &runtimes,
        "get_symbol",
        json!({"repo":"main@main", "name":"shared"}),
    )?;
    assert_eq!(response["repo"], "main@main");
    let unscoped = call(&runtimes, "get_symbol", json!({"name":"shared"}))?;
    assert_eq!(unscoped["repo"], "main@main");
    Ok(())
}

#[test]
fn starting_inside_a_worktree_seeds_and_can_route_back_to_main_and_siblings() -> Result<()> {
    let f = Fixture::new()?;
    let runtime = f.runtime()?;
    let conn = rusqlite::Connection::open(&runtime.db_path)?;
    conn.execute("UPDATE symbols SET signature = 'seeded from main'", [])?;
    let result = Command::new(env!("CARGO_BIN_EXE_tsindex"))
        .args(["--root"])
        .arg(&f.one)
        .args(["--json", "symbol", "shared", "worktree_only"])
        .output()?;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let response: Value = serde_json::from_slice(&result.stdout)?;
    assert_eq!(response["total"], 2);
    assert_eq!(response["matches"][0]["signature"], "seeded from main");
    let runtime = Runtime::new(
        f.one.clone(),
        db_path(&f.one),
        TsIndexConfig::load(&f.one)?,
        vec![],
    );
    let runtimes = Runtimes::new(runtime);
    assert_eq!(
        call(
            &runtimes,
            "get_symbol",
            json!({"repo":"main", "name":"shared"})
        )?["total"],
        1
    );
    assert_eq!(
        call(
            &runtimes,
            "get_symbol",
            json!({"repo":"branch-two", "name":"shared"})
        )?["total"],
        1
    );
    Ok(())
}

#[test]
fn worktrees_cli_reports_status_without_creating_unused_indexes() -> Result<()> {
    let f = Fixture::new()?;
    f.runtime()?;
    let result = Command::new(env!("CARGO_BIN_EXE_tsindex"))
        .arg("--root")
        .arg(&f.one)
        .args(["worktrees", "--json"])
        .output()?;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let response: Value = serde_json::from_slice(&result.stdout)?;
    assert_eq!(response.as_array().unwrap().len(), 3);
    assert!(!f.one.join(".tsindex").exists());
    let main = response
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "main")
        .unwrap();
    assert_eq!(main["indexed"], true);
    assert!(main["updated_at"].is_number());
    Ok(())
}
