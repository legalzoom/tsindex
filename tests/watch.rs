#![cfg(target_os = "linux")]

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::symlink;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
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
    updates: mpsc::Receiver<()>,
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
        Self::start_with_env(root, &[])
    }

    fn start_with_env(root: &Path, environment: &[(&str, &Path)]) -> Result<Self> {
        let (server, rx) = Self::spawn_with_env(root, environment)?;
        rx.recv_timeout(Duration::from_secs(10))
            .context("watcher did not start")?;
        Ok(server)
    }

    fn spawn(root: &Path) -> Result<(Self, mpsc::Receiver<()>)> {
        Self::spawn_with_env(root, &[])
    }

    fn spawn_with_env(
        root: &Path,
        environment: &[(&str, &Path)],
    ) -> Result<(Self, mpsc::Receiver<()>)> {
        let mut child = Command::new(BIN)
            .arg("--root")
            .arg(root)
            .args(["serve", "--mcp"])
            .env("TSINDEX_WATCH_DEBOUNCE_MS", "50")
            .envs(environment.iter().copied())
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;
        let stderr = child.stderr.take().unwrap();
        let (tx, rx) = mpsc::channel();
        let (update_tx, updates) = mpsc::channel();
        let reader = thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if line.contains("repos for changes") {
                    let _ = tx.send(());
                }
                if line.contains("update complete") {
                    let _ = update_tx.send(());
                }
            }
        });
        // Construct the guard before waiting so a failed startup is also reaped.
        let server = Self {
            child,
            db: root.join(".tsindex/index.db"),
            reader: Some(reader),
            updates,
        };
        Ok((server, rx))
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

#[test]
fn optional_input_probe_denial_preserves_readable_workspace_updates() -> Result<()> {
    // Root bypasses DAC permissions, so this regression needs a normal Linux
    // user, as used by CI and the actual MCP server.
    if unsafe { libc::geteuid() } == 0 {
        return Ok(());
    }
    let dir = tempdir()?;
    let base = dir.path().canonicalize()?;
    let parent = base.join("execute-only");
    let restricted = parent.join("repo");
    let healthy = base.join("healthy");
    let catalog = base.join("catalog");
    fixture(&restricted, "restricted_initial")?;
    fixture(&healthy, "healthy_initial")?;
    fs::create_dir_all(catalog.join(".tsindex"))?;
    fs::write(
        catalog.join(".tsindex/config.toml"),
        format!(
            "[[repos]]\nname = 'restricted'\npath = {}\nlanguages = ['python']\n\n[[repos]]\nname = 'healthy'\npath = {}\nlanguages = ['python']\n",
            serde_json::to_string(&restricted)?,
            serde_json::to_string(&healthy)?,
        ),
    )?;
    struct RestorePermissions(PathBuf, fs::Permissions);
    impl Drop for RestorePermissions {
        fn drop(&mut self) {
            let _ = fs::set_permissions(&self.0, self.1.clone());
        }
    }
    let _restore = RestorePermissions(parent.clone(), fs::metadata(&parent)?.permissions());
    // 0711 is readable by its owner; 0111 exercises execute-only traversal
    // without privileged ownership changes or touching anyone else's files.
    fs::set_permissions(&parent, fs::Permissions::from_mode(0o111))?;
    assert!(fs::read_dir(&parent).is_err());
    assert!(fs::read_dir(&restricted).is_ok());
    let server = Server::start(&catalog)?;
    for symbol in ["restricted_initial", "healthy_initial"] {
        server.wait("optional probe blocked initial build", || {
            server.has_symbol(symbol)
        })?;
    }
    for (root, symbol) in [
        (&restricted, "restricted_followup"),
        (&healthy, "healthy_followup"),
    ] {
        assert!(server.watches(&root.join("src"))?);
        fs::write(
            root.join("src/a.py"),
            format!("def {symbol}():\n    return 2\n"),
        )?;
        server.wait("optional probe blocked source refresh", || {
            server.has_symbol(symbol)
        })?;
    }
    Ok(())
}

#[test]
fn ignore_rule_directory_symlink_repoint_restores_source_watches() -> Result<()> {
    for tail in ["rules", "../current/rules"] {
        let dir = tempdir()?;
        let base = dir.path().canonicalize()?;
        let root = base.join("repo");
        fixture(&root, "route_initial")?;
        let source = root.join("later/deep/a.py");
        fs::create_dir_all(source.parent().unwrap())?;
        fs::write(&source, "def route_later():\n    return 1\n")?;
        let external = base.join("external");
        let old = external.join("old");
        let new = external.join("new");
        fs::create_dir_all(&old)?;
        fs::create_dir(&new)?;
        fs::write(old.join("rules"), "later/\n")?;
        fs::write(new.join("rules"), "")?;
        let route = external.join("current");
        symlink("old", &route)?;
        symlink(route.join(tail), root.join(".ignore"))?;
        let server = Server::start(&root)?;
        server.wait("initial rule route build", || {
            server.has_symbol("route_initial")
        })?;
        assert!(!server.has_symbol("route_later"));
        assert!(!server.watches(source.parent().unwrap())?);
        let replacement = external.join("replacement");
        symlink("new", &replacement)?;
        fs::rename(&replacement, &route)?;
        server.wait("directory-link replacement did not restore source", || {
            server.has_symbol("route_later")
        })?;
        server.wait("directory-link replacement left source unwatched", || {
            server.watches(source.parent().unwrap()).unwrap_or(false)
        })?;
        fs::write(&source, "def route_followup():\n    return 2\n")?;
        server.wait("repointed rule route lost later source edits", || {
            server.has_symbol("route_followup")
        })?;
        assert!(server.watches(&external)?);
        assert!(server.watches(&new)?);
        server.wait("old rule route watch was retained", || {
            !server.watches(&old).unwrap_or(true)
        })?;
        fs::write(new.join("rules"), "later/\n")?;
        server.wait("repointed route lost final rule-file edits", || {
            !server.has_symbol("route_followup")
        })?;
        fs::write(new.join("rules"), "")?;
        server.wait("repointed route did not restore source again", || {
            server.has_symbol("route_followup")
        })?;
    }
    Ok(())
}

#[test]
fn deleted_ignore_owner_retires_external_probe_without_directory_refresh() -> Result<()> {
    let dir = tempdir()?;
    let base = dir.path().canonicalize()?;
    let root = base.join("repo");
    fixture(&root, "delete_initial")?;
    let nested = root.join("nested");
    fs::create_dir(&nested)?;
    fs::write(
        nested.join("a.py"),
        "def deleted_rule_owner():\n    return 1\n",
    )?;
    let external = base.join("external");
    fs::create_dir(&external)?;
    let rules = external.join("rules");
    fs::write(&rules, "")?;
    symlink(&rules, nested.join(".ignore"))?;
    let server = Server::start(&root)?;
    server.wait("nested ignore owner initial build", || {
        server.has_symbol("deleted_rule_owner")
    })?;
    assert!(server.watches(&external)?);
    // Wait for an ordinary update to reach the event loop: deleting during
    // the initial build's final retention can accidentally hide the leak.
    fs::write(
        root.join("src/a.py"),
        "def delete_settled():\n    return 2\n",
    )?;
    server.updates.recv_timeout(Duration::from_secs(5))?;
    server.wait("startup did not settle", || {
        server.has_symbol("delete_settled")
    })?;
    fs::remove_dir_all(&nested)?;
    server.wait("deleted ignore owner rows remained indexed", || {
        !server.has_symbol("deleted_rule_owner")
    })?;
    server.wait(
        "deleted ignore owner left external probe registered",
        || !server.watches(&external).unwrap_or(true),
    )?;
    fs::write(
        root.join("src/a.py"),
        "def delete_followup():\n    return 3\n",
    )?;
    server.wait("healthy source edit stopped after owner deletion", || {
        server.has_symbol("delete_followup")
    })?;
    assert!(!server.watches(&external)?);
    Ok(())
}

#[test]
fn nested_clone_arriving_during_startup_registration_receives_source_watches() -> Result<()> {
    let dir = tempdir()?;
    let catalog = dir.path().canonicalize()?.join("catalog");
    let outer = dir.path().canonicalize()?.join("outer");
    let inner = outer.join("ignored/existing/inner");
    fixture(&outer, "outer_startup")?;
    fs::create_dir_all(outer.join("ignored/existing"))?;
    // The registration walk reads root ignore rules before yielding its root.
    // Hold that read on a FIFO so clone arrival is synchronized with startup
    // rather than relying on runner speed or a large artificial directory tree.
    let rules = outer.join(".gitignore");
    ensure!(
        Command::new("mkfifo").arg(&rules).status()?.success(),
        "cannot create startup barrier"
    );
    fs::create_dir_all(catalog.join(".tsindex"))?;
    fs::write(
        catalog.join(".tsindex/config.toml"),
        format!(
            "[[repos]]\nname = 'outer'\npath = {}\nlanguages = ['python']\n\n[[repos]]\nname = 'inner'\npath = {}\nlanguages = ['python']\n",
            serde_json::to_string(&outer)?,
            serde_json::to_string(&inner)?,
        ),
    )?;
    let (server, ready) = Server::spawn(&catalog)?;
    let mut writer = None;
    server.wait(
        "registration did not open its ignore rules",
        || match fs::OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&rules)
        {
            Ok(file) => {
                writer = Some(file);
                true
            }
            Err(_) => false,
        },
    )?;
    assert!(
        !server.watches(&outer.join("src"))?,
        "barrier must precede source registration"
    );
    fixture(&inner, "during_startup")?;
    writer.as_mut().unwrap().write_all(b"ignored/\n")?;
    // Subsequent build/update walks need a regular rule file. The open reader
    // still sees the FIFO's rules and EOF when its writer is dropped.
    let replacement = rules.with_extension("ready");
    fs::write(&replacement, "ignored/\n")?;
    fs::rename(&replacement, &rules)?;
    drop(writer);
    ready
        .recv_timeout(Duration::from_secs(10))
        .context("startup registration did not finish")?;
    server.wait("startup did not index arriving clone", || {
        server.has_symbol("during_startup")
    })?;
    server.wait(
        "startup indexed clone without installing source watches",
        || server.watches(&inner.join("src")).unwrap_or(false),
    )?;
    fs::write(
        inner.join("src/a.py"),
        "def after_startup_edit():\n    return 2\n",
    )?;
    server.wait("startup clone's follow-up edit stayed stale", || {
        server.has_symbol("after_startup_edit")
    })?;
    Ok(())
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
fn ancestor_ignore_inputs_refresh_every_affected_workspace() -> Result<()> {
    for filename in [".ignore", ".gitignore", ".tsindexignore"] {
        let dir = tempdir()?;
        let base = dir.path().canonicalize()?;
        let catalog = base.join("catalog");
        let parent = base.join("sources");
        let roots = [parent.join("nested/first"), parent.join("nested/second")];
        let rules = parent.join(filename);
        let mut config = String::new();
        for (i, root) in roots.iter().enumerate() {
            fixture(root, &format!("ancestor_initial_{i}"))?;
            // Git rules stop at repository boundaries; place the Git marker
            // at the shared ancestor so its .gitignore governs both roots.
            if filename == ".gitignore" {
                fs::remove_dir_all(root.join(".git"))?;
                fs::create_dir_all(parent.join(".git"))?;
            }
            fs::create_dir_all(root.join("later/deep"))?;
            fs::write(
                root.join("later/deep/a.py"),
                format!("def ancestor_later_{i}():\n    return 2\n"),
            )?;
            config.push_str(&format!(
                "[[repos]]\nname = 'repo{i}'\npath = {}\nlanguages = ['python']\n\n",
                serde_json::to_string(root)?,
            ));
        }
        fs::write(&rules, "later/\n")?;
        fs::create_dir_all(catalog.join(".tsindex"))?;
        fs::write(catalog.join(".tsindex/config.toml"), config)?;
        let server = Server::start(&catalog)?;
        server.wait("ancestor rule initial build", || {
            server.has_symbol("ancestor_initial_1")
        })?;
        for (i, root) in roots.iter().enumerate() {
            assert!(!server.has_symbol(&format!("ancestor_later_{i}")));
            assert!(!server.watches(&root.join("later/deep"))?);
        }

        let replacement = rules.with_extension("new");
        fs::write(&replacement, "")?;
        fs::rename(&replacement, &rules)?;
        for (i, root) in roots.iter().enumerate() {
            server.wait("ancestor rule replacement did not restore source", || {
                server.has_symbol(&format!("ancestor_later_{i}"))
            })?;
            server.wait("ancestor rule left restored source unwatched", || {
                server.watches(&root.join("later/deep")).unwrap_or(false)
            })?;
            fs::write(
                root.join("later/deep/a.py"),
                format!("def ancestor_followup_{i}():\n    return 3\n"),
            )?;
            server.wait("ancestor rule left follow-up edits invisible", || {
                server.has_symbol(&format!("ancestor_followup_{i}"))
            })?;
        }
        fs::write(&rules, "later/\n")?;
        for (i, root) in roots.iter().enumerate() {
            server.wait("ancestor rule edit did not purge excluded source", || {
                !server.has_symbol(&format!("ancestor_followup_{i}"))
            })?;
            server.wait("ancestor rule retained excluded source watches", || {
                !server.watches(&root.join("later/deep")).unwrap_or(true)
            })?;
        }
        fs::remove_file(&rules)?;
        for i in 0..roots.len() {
            server.wait("ancestor rule removal did not restore source", || {
                server.has_symbol(&format!("ancestor_followup_{i}"))
            })?;
        }
    }
    Ok(())
}

#[test]
fn symlinked_directory_ignore_inputs_observe_external_targets() -> Result<()> {
    for filename in [".ignore", ".gitignore", ".tsindexignore"] {
        let dir = tempdir()?;
        let base = dir.path().canonicalize()?;
        let root = base.join("repo");
        // Use a rule below the root as well: discovering only root-level
        // symlinks would still leave ignored descendants without probes.
        let governed = root.join("nested");
        fixture(&root, "symlink_initial")?;
        fs::create_dir_all(governed.join("later/deep"))?;
        let source = governed.join("later/deep/a.py");
        fs::write(&source, "def symlink_later():\n    return 2\n")?;
        let external = base.join("external-rules");
        fs::create_dir(&external)?;
        let target = external.join("rules");
        let rules = governed.join(filename);
        fs::write(&target, "later/\n")?;
        symlink(&target, &rules)?;
        let server = Server::start(&root)?;
        server.wait("symlink rule initial build", || {
            server.has_symbol("symlink_initial")
        })?;
        assert!(!server.has_symbol("symlink_later"));
        assert!(!server.watches(source.parent().unwrap())?);

        fs::write(&target, "")?;
        server.wait("symlink target edit did not restore source", || {
            server.has_symbol("symlink_later")
        })?;
        server.wait("symlink target left restored source unwatched", || {
            server.watches(source.parent().unwrap()).unwrap_or(false)
        })?;
        fs::write(&source, "def symlink_followup():\n    return 3\n")?;
        server.wait("symlink target left follow-up edits invisible", || {
            server.has_symbol("symlink_followup")
        })?;
        let replacement = target.with_extension("new");
        fs::write(&replacement, "later/\n")?;
        fs::rename(&replacement, &target)?;
        server.wait("symlink target replacement did not purge source", || {
            !server.has_symbol("symlink_followup")
        })?;
        server.wait("symlink target retained excluded source watches", || {
            !server.watches(source.parent().unwrap()).unwrap_or(true)
        })?;
        fs::remove_file(&target)?;
        server.wait("symlink target removal did not restore source", || {
            server.has_symbol("symlink_followup")
        })?;
        // Removing the target makes the rule symlink dangling. Its target
        // parent must remain probed so recreating the rules can exclude again.
        fs::write(&target, "later/\n")?;
        server.wait(
            "dangling symlink target recreation did not purge source",
            || !server.has_symbol("symlink_followup"),
        )?;
        fs::write(&target, "")?;
        server.wait(
            "recreated symlink target edit did not restore source",
            || server.has_symbol("symlink_followup"),
        )?;

        let redirected = base.join("redirected-rules");
        fs::create_dir(&redirected)?;
        let new_target = redirected.join("rules");
        fs::write(&new_target, "")?;
        let replacement_link = rules.with_extension("new");
        symlink(&new_target, &replacement_link)?;
        fs::rename(&replacement_link, &rules)?;
        server.wait("replacement symlink target was not probed", || {
            server.watches(&redirected).unwrap_or(false)
        })?;
        fs::write(&new_target, "later/\n")?;
        server.wait(
            "replacement symlink target edit did not purge source",
            || !server.has_symbol("symlink_followup"),
        )?;
    }
    Ok(())
}

#[test]
fn symlinked_ignore_targets_replaced_during_discovery_reconcile_source_watches() -> Result<()> {
    for (startup, exclude_now) in [(true, false), (false, false), (true, true)] {
        let dir = tempdir()?;
        let base = dir.path().canonicalize()?;
        let root = base.join("repo");
        let prepared = base.join("prepared");
        let governed = root.join("nested");
        let external = base.join("external-rules");
        let rules = external.join("rules");
        fixture(&root, "discovery_initial")?;
        fs::create_dir_all(prepared.join("later/deep"))?;
        fs::write(
            prepared.join("later/deep/a.py"),
            "def discovery_later():\n    return 2\n",
        )?;
        fs::create_dir(&external)?;
        ensure!(
            Command::new("mkfifo").arg(&rules).status()?.success(),
            "cannot create rule-discovery barrier"
        );
        symlink(&rules, prepared.join(".ignore"))?;
        if startup {
            fs::rename(&prepared, &governed)?;
        }
        let (server, ready) = Server::spawn(&root)?;
        if !startup {
            ready
                .recv_timeout(Duration::from_secs(10))
                .context("initial watcher registration did not finish")?;
            server.wait("initial watcher build", || {
                server.has_symbol("discovery_initial")
            })?;
            fs::rename(&prepared, &governed)?;
        }
        let mut writer = None;
        server.wait("registration did not read external ignore rules", || {
            match fs::OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(&rules)
            {
                Ok(file) => {
                    writer = Some(file);
                    true
                }
                Err(_) => false,
            }
        })?;
        assert!(!server.watches(&external)?);
        // Eligibility is already being read, but the external input's probe
        // does not exist yet. Replace its pathname before unblocking the old
        // read so discovery and indexing would otherwise see different rules.
        writer
            .as_mut()
            .unwrap()
            .write_all(if exclude_now { b"\n" } else { b"later/\n" })?;
        let replacement = external.join("replacement");
        fs::write(&replacement, if exclude_now { "later/\n" } else { "" })?;
        fs::rename(&replacement, &rules)?;
        drop(writer);
        if startup {
            ready
                .recv_timeout(Duration::from_secs(10))
                .context("startup rule discovery did not finish")?;
        }
        let source = governed.join("later/deep/a.py");
        if exclude_now {
            server.wait("initial build did not finish", || {
                server.has_symbol("discovery_initial")
            })?;
            assert!(!server.has_symbol("discovery_later"));
            server.wait("discovery retained newly excluded source watches", || {
                !server.watches(source.parent().unwrap()).unwrap_or(true)
            })?;
            fs::write(&rules, "")?;
        }
        server.wait("new eligibility did not restore source", || {
            server.has_symbol("discovery_later")
        })?;
        server.wait(
            "discovery indexed source without installing watches",
            || server.watches(source.parent().unwrap()).unwrap_or(false),
        )?;
        fs::write(&source, "def discovery_followup():\n    return 3\n")?;
        server.wait("discovery left later source edits invisible", || {
            server.has_symbol("discovery_followup")
        })?;
    }
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

#[test]
fn replacing_an_ignored_ancestor_reinstalls_watches_on_new_inodes() -> Result<()> {
    let dir = tempdir()?;
    let root = dir.path().canonicalize()?;
    let catalog = root.join("catalog");
    let outer = root.join("outer");
    let ignored = outer.join("ignored");
    let inner = ignored.join("existing/inner");
    fixture(&outer, "outer_kept")?;
    fixture(&inner, "original_clone")?;
    let unconfigured = ignored.join("unconfigured/deep");
    fs::create_dir_all(&unconfigured)?;
    fs::write(
        unconfigured.join("a.py"),
        "def ignored_source():\n    pass\n",
    )?;
    fs::write(outer.join(".gitignore"), "ignored/\n")?;
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
    server.wait("original clone was not indexed", || {
        server.has_symbol("original_clone")
    })?;
    // Ancestor rule probes now watch ignored itself nonrecursively; they
    // still must not allocate watches in its unconfigured ignored subtrees.
    assert!(!server.watches(&unconfigured)?);
    assert!(!server.has_symbol("ignored_source"));
    assert!(server.watches(&inner.join("src"))?);

    // The old directories stay alive elsewhere, so pathname membership cannot
    // prove that existing watches refer to the replacement clone's inodes.
    fs::rename(&ignored, root.join("archived"))?;
    fixture(&inner, "replacement_clone")?;
    server.wait("replacement source inode remained unwatched", || {
        server.watches(&inner.join("src")).unwrap_or(false)
    })?;
    fs::write(
        inner.join("src/a.py"),
        "def replacement_edit():\n    return 3\n",
    )?;
    server.wait("replacement clone edit stayed stale", || {
        server.has_symbol("replacement_edit")
    })?;
    assert!(server.has_symbol("outer_kept"));
    Ok(())
}

#[test]
fn repository_exclude_edits_install_watches_for_newly_eligible_sources() -> Result<()> {
    let dir = tempdir()?;
    let root = dir.path().canonicalize()?.join("repo");
    fixture(&root, "exclude_initial")?;
    fs::create_dir_all(root.join(".git/info"))?;
    fs::create_dir_all(root.join("later/deep"))?;
    let source = root.join("later/deep/a.py");
    fs::write(&source, "def exclude_later():\n    return 2\n")?;
    let rules = root.join(".git/info/exclude");
    fs::write(&rules, "later/\n")?;
    let server = Server::start(&root)?;
    server.wait("initial build", || server.has_symbol("exclude_initial"))?;
    assert!(!server.watches(source.parent().unwrap())?);

    let replacement = rules.with_extension("new");
    fs::write(&replacement, "")?;
    fs::rename(&replacement, &rules)?;
    server.wait("Git exclude edit did not restore source", || {
        server.has_symbol("exclude_later")
    })?;
    assert!(server.watches(source.parent().unwrap())?);
    fs::write(&source, "def exclude_followup():\n    return 3\n")?;
    server.wait("Git exclude edit did not install source watches", || {
        server.has_symbol("exclude_followup")
    })?;
    fs::write(&rules, "later/\n")?;
    server.wait("Git exclude edit did not purge source", || {
        !server.has_symbol("exclude_followup")
    })?;
    server.wait("Git exclude edit retained obsolete watches", || {
        !server.watches(source.parent().unwrap()).unwrap()
    })?;
    fs::remove_file(&rules)?;
    server.wait("removed Git exclude did not restore source", || {
        server.has_symbol("exclude_followup")
    })?;
    Ok(())
}

#[test]
fn inherited_ignore_edits_purge_nested_symbols_before_their_watches_disappear() -> Result<()> {
    let dir = tempdir()?;
    let catalog = dir.path().canonicalize()?.join("catalog");
    let outer = dir.path().canonicalize()?.join("outer");
    let inner = outer.join("inner");
    fixture(&outer, "inherited_outer")?;
    fixture(&inner, "inherited_inner")?;
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
    server.wait("nested initial build", || {
        server.has_symbol("inherited_inner")
    })?;
    let rules = outer.join(".tsindexignore");
    fs::write(&rules, "inner/src/\n")?;
    server.wait("inherited ignore left stale nested symbols", || {
        !server.has_symbol("inherited_inner")
    })?;
    server.wait("inherited ignore retained nested source watches", || {
        !server.watches(&inner.join("src")).unwrap()
    })?;
    assert!(server.has_symbol("inherited_outer"));
    fs::remove_file(&rules)?;
    server.wait(
        "removed inherited ignore did not restore nested symbols",
        || server.has_symbol("inherited_inner"),
    )?;
    fs::write(
        inner.join("src/a.py"),
        "def inherited_followup():\n    return 4\n",
    )?;
    server.wait("restored nested source stayed unwatched", || {
        server.has_symbol("inherited_followup")
    })?;
    Ok(())
}

#[test]
fn linked_worktrees_observe_shared_repository_excludes() -> Result<()> {
    let dir = tempdir()?;
    let root = dir.path().canonicalize()?;
    let catalog = root.join("catalog");
    let common = root.join("shared.git");
    fs::create_dir_all(common.join("info"))?;
    let rules = common.join("info/exclude");
    fs::write(&rules, "later/\n")?;
    let clones = [root.join("one"), root.join("two")];
    let mut config = String::new();
    for (i, clone) in clones.iter().enumerate() {
        fixture(clone, &format!("shared_initial_{i}"))?;
        fs::remove_dir(clone.join(".git"))?;
        let gitdir = common.join(format!("worktrees/{i}"));
        fs::create_dir_all(&gitdir)?;
        fs::write(gitdir.join("commondir"), "../..\n")?;
        fs::write(
            clone.join(".git"),
            format!("gitdir: {}\n", gitdir.display()),
        )?;
        fs::create_dir_all(clone.join("later/deep"))?;
        fs::write(
            clone.join("later/deep/a.py"),
            format!("def shared_later_{i}():\n    return 2\n"),
        )?;
        config.push_str(&format!(
            "[[repos]]\nname = 'clone{i}'\npath = {}\nlanguages = ['python']\n\n",
            serde_json::to_string(clone)?,
        ));
    }
    fs::create_dir_all(catalog.join(".tsindex"))?;
    fs::write(catalog.join(".tsindex/config.toml"), config)?;
    let server = Server::start(&catalog)?;
    server.wait("worktree initial build", || {
        server.has_symbol("shared_initial_1")
    })?;
    for clone in &clones {
        assert!(!server.watches(&clone.join("later/deep"))?);
    }

    fs::remove_file(&rules)?;
    for (i, clone) in clones.iter().enumerate() {
        server.wait(
            "shared exclude removal did not restore every worktree",
            || server.has_symbol(&format!("shared_later_{i}")),
        )?;
        assert!(server.watches(&clone.join("later/deep"))?);
        fs::write(
            clone.join("later/deep/a.py"),
            format!("def shared_followup_{i}():\n    return 3\n"),
        )?;
        server.wait("shared exclude left a worktree unwatched", || {
            server.has_symbol(&format!("shared_followup_{i}"))
        })?;
    }
    fs::write(&rules, "later/\n")?;
    for (i, clone) in clones.iter().enumerate() {
        server.wait(
            "shared exclude recreation did not purge every worktree",
            || !server.has_symbol(&format!("shared_followup_{i}")),
        )?;
        server.wait("shared exclude retained source watches", || {
            !server.watches(&clone.join("later/deep")).unwrap()
        })?;
    }
    Ok(())
}

#[test]
fn global_exclude_edits_and_config_redirects_refresh_source_watches() -> Result<()> {
    for configured in [false, true] {
        let dir = tempdir()?;
        let base = dir.path().canonicalize()?;
        let root = base.join("repo");
        let home = base.join("home");
        let xdg = home.join(".config");
        fs::create_dir_all(xdg.join("git"))?;
        let rules = if configured {
            base.join("custom-rules")
        } else {
            xdg.join("git/ignore")
        };
        if configured {
            fs::write(
                home.join(".gitconfig"),
                format!("[core]\nexcludesFile = {}\n", rules.display()),
            )?;
        }
        fixture(&root, "global_initial")?;
        fs::create_dir_all(root.join("later/deep"))?;
        let source = root.join("later/deep/a.py");
        fs::write(&source, "def global_later():\n    return 2\n")?;
        fs::write(&rules, "later/\n")?;
        // Isolate global Git settings in the child, without changing the test
        // process environment or the collaborator's configuration.
        let server = Server::start_with_env(&root, &[("HOME", &home), ("XDG_CONFIG_HOME", &xdg)])?;
        server.wait("global initial build", || {
            server.has_symbol("global_initial")
        })?;
        assert!(!server.watches(source.parent().unwrap())?);

        let replacement = rules.with_extension("new");
        fs::write(&replacement, "")?;
        fs::rename(&replacement, &rules)?;
        server.wait("global exclude replacement did not restore source", || {
            server.has_symbol("global_later")
        })?;
        assert!(server.watches(source.parent().unwrap())?);
        fs::write(&source, "def global_followup():\n    return 3\n")?;
        server.wait("global excludes left source unwatched", || {
            server.has_symbol("global_followup")
        })?;
        fs::write(&rules, "later/\n")?;
        server.wait("global exclude edit did not purge source", || {
            !server.has_symbol("global_followup")
        })?;
        fs::remove_file(&rules)?;
        server.wait("global exclude removal did not restore source", || {
            server.has_symbol("global_followup")
        })?;

        let redirected = base.join("new-settings/excludes");
        fs::create_dir(redirected.parent().unwrap())?;
        fs::write(&redirected, "")?;
        fs::write(
            home.join(".gitconfig"),
            format!("[core]\nexcludesFile = {}\n", redirected.display()),
        )?;
        server.wait("new global exclude parent was not probed", || {
            server
                .watches(redirected.parent().unwrap())
                .unwrap_or(false)
        })?;
        fs::write(&redirected, "later/\n")?;
        server.wait("redirected global exclude did not refresh source", || {
            !server.has_symbol("global_followup")
        })?;
        fs::remove_dir_all(redirected.parent().unwrap())?;
        server.wait(
            "removed global exclude parent did not restore source",
            || server.has_symbol("global_followup"),
        )?;
    }
    Ok(())
}

#[test]
fn unconfigured_nested_git_repository_excludes_are_observed() -> Result<()> {
    let dir = tempdir()?;
    let root = dir.path().canonicalize()?.join("outer");
    let inner = root.join("embedded");
    fixture(&root, "embedded_outer")?;
    fixture(&inner, "embedded_initial")?;
    fs::create_dir_all(inner.join(".git/info"))?;
    fs::create_dir_all(inner.join("later/deep"))?;
    let source = inner.join("later/deep/a.py");
    fs::write(&source, "def embedded_later():\n    return 2\n")?;
    let rules = inner.join(".git/info/exclude");
    fs::write(&rules, "later/\n")?;
    let server = Server::start(&root)?;
    server.wait("embedded initial build", || {
        server.has_symbol("embedded_initial")
    })?;
    assert!(!server.watches(source.parent().unwrap())?);
    fs::remove_file(&rules)?;
    server.wait("embedded Git excludes were not observed", || {
        server.has_symbol("embedded_later")
    })?;
    assert!(server.watches(source.parent().unwrap())?);
    fs::write(&source, "def embedded_followup():\n    return 3\n")?;
    server.wait("embedded Git source stayed unwatched", || {
        server.has_symbol("embedded_followup")
    })?;
    Ok(())
}

#[test]
fn git_metadata_arriving_in_an_existing_source_directory_acquires_exclude_probes() -> Result<()> {
    let dir = tempdir()?;
    let root = dir.path().canonicalize()?.join("outer");
    let inner = root.join("embedded");
    fixture(&root, "metadata_outer")?;
    fs::create_dir_all(inner.join("later/deep"))?;
    let source = inner.join("later/deep/a.py");
    fs::write(&source, "def metadata_initial():\n    return 1\n")?;
    let server = Server::start(&root)?;
    server.wait("embedded source initial build", || {
        server.has_symbol("metadata_initial")
    })?;
    assert!(server.watches(source.parent().unwrap())?);

    fs::create_dir_all(inner.join(".git/info"))?;
    let rules = inner.join(".git/info/exclude");
    fs::write(&rules, "later/\n")?;
    server.wait("new Git metadata did not activate exclude rules", || {
        !server.has_symbol("metadata_initial")
    })?;
    server.wait("new Git exclude parent was not probed", || {
        server.watches(rules.parent().unwrap()).unwrap_or(false)
    })?;
    server.wait(
        "new Git excludes did not remove obsolete source watches",
        || !server.watches(source.parent().unwrap()).unwrap_or(true),
    )?;
    fs::remove_file(&rules)?;
    server.wait("new Git input removal did not restore source", || {
        server.has_symbol("metadata_initial")
    })?;
    fs::write(&source, "def metadata_followup():\n    return 2\n")?;
    server.wait("new Git input left source unwatched", || {
        server.has_symbol("metadata_followup")
    })?;
    Ok(())
}
