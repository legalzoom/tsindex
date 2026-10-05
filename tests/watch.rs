#![cfg(target_os = "linux")]

use std::fs;
use std::io::{BufRead, BufReader};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OpenFlags};
use tempfile::tempdir;

const BIN: &str = env!("CARGO_BIN_EXE_tsindex");

struct Server {
    child: Child,
    db: PathBuf,
    reader: Option<thread::JoinHandle<()>>,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

impl Server {
    fn start(root: &Path) -> Result<Self> {
        let mut child = Command::new(BIN)
            .arg("--root")
            .arg(root)
            .args(["serve", "--mcp"])
            .env("TSINDEX_WATCH_DEBOUNCE_MS", "50")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;
        let stderr = child.stderr.take().unwrap();
        let (tx, rx) = mpsc::channel();
        let reader = thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if line.contains("repos for changes") {
                    let _ = tx.send(());
                }
            }
        });
        // Construct the guard before waiting so a failed startup is also reaped.
        let server = Self {
            child,
            db: root.join(".tsindex/index.db"),
            reader: Some(reader),
        };
        rx.recv_timeout(Duration::from_secs(10))
            .context("watcher did not start")?;
        Ok(server)
    }

    fn has_symbol(&self, name: &str) -> bool {
        Connection::open_with_flags(&self.db, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .and_then(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM symbols WHERE name = ?1",
                    [name],
                    |row| row.get::<_, u64>(0),
                )
            })
            .is_ok_and(|count| count > 0)
    }

    fn watches(&self, path: &Path) -> Result<bool> {
        let inode = fs::metadata(path)?.ino();
        for entry in fs::read_dir(format!("/proc/{}/fdinfo", self.child.id()))? {
            // Files opened by the indexer can disappear while fdinfo is listed.
            let Ok(contents) = fs::read_to_string(entry?.path()) else {
                continue;
            };
            if contents
                .lines()
                .filter(|line| line.starts_with("inotify "))
                .any(|line| {
                    line.split_whitespace().any(|field| {
                        field
                            .strip_prefix("ino:")
                            .and_then(|value| u64::from_str_radix(value, 16).ok())
                            == Some(inode)
                    })
                })
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn wait(&self, description: &str, mut predicate: impl FnMut() -> bool) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !predicate() {
            ensure!(Instant::now() < deadline, "{description}");
            thread::sleep(Duration::from_millis(20));
        }
        Ok(())
    }
}

fn fixture(root: &Path, symbol: &str) -> Result<()> {
    fs::create_dir_all(root.join("src"))?;
    fs::create_dir_all(root.join(".git"))?;
    fs::write(
        root.join("src/a.py"),
        format!("def {symbol}():\n    return 1\n"),
    )?;
    Ok(())
}

#[test]
fn configured_ignore_file_edits_refresh_watches_and_remove_excluded_watches() -> Result<()> {
    let dir = tempdir()?;
    let root = dir.path().canonicalize()?.join("repo");
    fixture(&root, "initial")?;
    fs::create_dir_all(root.join("later/deep"))?;
    let source = root.join("later/deep/b.py");
    fs::write(&source, "def newly_included():\n    return 2\n")?;
    // The explicit rule file may live outside the workspace. Watch its parent
    // so atomic replacement, removal and recreation all invalidate the rules.
    let rules = dir.path().canonicalize()?.join("rules.txt");
    fs::write(&rules, "later/\n")?;
    fs::create_dir(root.join(".tsindex"))?;
    fs::write(
        root.join(".tsindex/config.toml"),
        format!("[ignore]\nextra = [{}]\n", serde_json::to_string(&rules)?),
    )?;
    let server = Server::start(&root)?;
    server.wait("initial build", || server.has_symbol("initial"))?;
    assert!(!server.watches(source.parent().unwrap())?);
    let replacement = rules.with_extension("new");
    fs::write(&replacement, "")?;
    fs::rename(&replacement, &rules)?;
    server.wait("custom rules did not un-ignore existing source", || {
        server.has_symbol("newly_included")
    })?;
    assert!(server.watches(source.parent().unwrap())?);
    fs::write(&source, "def followup_edit():\n    return 3\n")?;
    server.wait("unignored subtree stayed unwatched", || {
        server.has_symbol("followup_edit")
    })?;
    fs::write(&rules, "later/\n")?;
    server.wait("newly ignored subtree kept its watches", || {
        !server.watches(source.parent().unwrap()).unwrap()
    })?;
    server.wait("newly ignored symbols were retained", || {
        !server.has_symbol("followup_edit")
    })?;
    fs::remove_file(&rules)?;
    server.wait("removed custom rules did not restore source", || {
        server.has_symbol("followup_edit")
    })?;
    assert!(server.watches(source.parent().unwrap())?);
    Ok(())
}

#[test]
fn arriving_nested_repo_uses_its_own_rules_and_survives_recreation() -> Result<()> {
    let dir = tempdir()?;
    let catalog = dir.path().canonicalize()?.join("catalog");
    let outer = dir.path().canonicalize()?.join("outer");
    let inner = outer.join("inner");
    fixture(&outer, "outer_symbol")?;
    fs::write(outer.join(".gitignore"), "inner/\n")?;
    fs::create_dir_all(catalog.join(".tsindex"))?;
    fs::write(
        catalog.join(".tsindex/config.toml"),
        format!(
            "[[repos]]\nname = 'outer'\npath = {}\nlanguages = ['python']\n\n[[repos]]\nname = 'inner'\npath = {}\nlanguages = ['python']\n",
            serde_json::to_string(&outer)?,
            serde_json::to_string(&inner)?,
        ),
    )?;
    let server = Server::start(&catalog)?;
    server.wait("outer build", || server.has_symbol("outer_symbol"))?;
    fixture(&inner, "inner_symbol")?;
    server.wait("arriving repo was not indexed", || {
        server.has_symbol("inner_symbol")
    })?;
    assert!(server.watches(&inner.join("src"))?);
    fs::write(inner.join("src/a.py"), "def inner_edit():\n    return 2\n")?;
    server.wait("arriving repo remained unwatched", || {
        server.has_symbol("inner_edit")
    })?;
    // Delete and recreate the same directory within a debounce window: a
    // path-only registry must not mistake its removed kernel watch for a live one.
    fs::remove_dir_all(inner.join("src"))?;
    fs::create_dir(inner.join("src"))?;
    fs::write(inner.join("src/a.py"), "def recreated():\n    return 3\n")?;
    server.wait("recreated directory was not indexed", || {
        server.has_symbol("recreated")
    })?;
    fs::write(
        inner.join("src/a.py"),
        "def recreated_edit():\n    return 4\n",
    )?;
    server.wait("recreated directory remained unwatched", || {
        server.has_symbol("recreated_edit")
    })?;
    Ok(())
}

#[test]
fn missing_repo_below_an_ignored_parent_is_discovered_and_healthy_repo_keeps_updating() -> Result<()>
{
    let dir = tempdir()?;
    let catalog = dir.path().canonicalize()?.join("catalog");
    let outer = dir.path().canonicalize()?.join("outer");
    let inner = outer.join("ignored/not-yet/inner");
    let healthy = dir.path().canonicalize()?.join("healthy");
    fixture(&outer, "outer_initial")?;
    fixture(&healthy, "healthy_initial")?;
    fs::create_dir(outer.join("ignored"))?;
    fs::write(outer.join(".gitignore"), "ignored/\n")?;
    fs::create_dir_all(catalog.join(".tsindex"))?;
    let mut config = String::new();
    for (name, path) in [("outer", &outer), ("inner", &inner), ("healthy", &healthy)] {
        config.push_str(&format!(
            "[[repos]]\nname = '{name}'\npath = {}\nlanguages = ['python']\n\n",
            serde_json::to_string(path)?
        ));
    }
    fs::write(catalog.join(".tsindex/config.toml"), config)?;
    let server = Server::start(&catalog)?;
    server.wait("healthy initial build", || {
        server.has_symbol("healthy_initial")
    })?;
    fixture(&inner, "late_clone")?;
    server.wait("ignored ancestry blocked clone discovery", || {
        server.has_symbol("late_clone")
    })?;
    fs::write(inner.join("src/a.py"), "def late_edit():\n    return 2\n")?;
    server.wait("late clone edits were not watched", || {
        server.has_symbol("late_edit")
    })?;
    fs::remove_dir_all(&outer)?;
    fs::create_dir_all(healthy.join("new/deep"))?;
    let source = healthy.join("new/deep/b.py");
    fs::write(&source, "def healthy_after_removal():\n    return 3\n")?;
    server.wait("vanished clones blocked healthy updates", || {
        server.has_symbol("healthy_after_removal")
    })?;
    fs::write(&source, "def healthy_followup():\n    return 4\n")?;
    server.wait("vanished clones blocked healthy watch registration", || {
        server.has_symbol("healthy_followup")
    })?;
    fixture(&inner, "second_clone")?;
    server.wait("re-cloned root was not rediscovered", || {
        server.has_symbol("second_clone")
    })?;
    fs::write(
        inner.join("src/a.py"),
        "def second_clone_edit():\n    return 5\n",
    )?;
    server.wait("re-cloned root remained unwatched", || {
        server.has_symbol("second_clone_edit")
    })?;
    Ok(())
}
