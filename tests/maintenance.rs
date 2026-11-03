//! Maintenance contracts at the process and lease interfaces.
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use tempfile::TempDir;
use tsindex::config::{RepoConfig, TsIndexConfig, db_path};
use tsindex::index::Runtime;
use tsindex::maintenance::{self, FileLease};
use tsindex::mcp::handle_request_with_runtimes;
use tsindex::worktrees::Runtimes;

const BIN: &str = env!("CARGO_BIN_EXE_tsindex");

fn fixture() -> Result<(TempDir, PathBuf)> {
    let artifacts = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/maintenance-fixtures");
    fs::create_dir_all(&artifacts)?;
    let directory = tempfile::tempdir_in(artifacts)?;
    let root = directory.path().canonicalize()?;
    fs::write(root.join("a.py"), "def before_edit():\n    return 1\n")?;
    let mut config = TsIndexConfig::default();
    config.languages.include = vec!["python".into()];
    config.server.worktree_refresh_ms = 0;
    config.write(&root)?;
    Runtime::new(root.clone(), db_path(&root), config, vec![]).build(false, None)?;
    Ok((directory, root))
}

fn linked_checkout(main: &Path, name: &str, symbol: &str) -> Result<PathBuf> {
    let checkout = main.join(".worktrees").join(name);
    let metadata = main.join(".git/worktrees").join(name);
    fs::create_dir_all(&checkout)?;
    fs::create_dir_all(&metadata)?;
    fs::write(
        checkout.join(".git"),
        format!("gitdir: {}\n", metadata.display()),
    )?;
    fs::write(metadata.join("commondir"), "../..\n")?;
    fs::write(
        metadata.join("gitdir"),
        format!("{}\n", checkout.join(".git").display()),
    )?;
    fs::write(
        checkout.join("a.py"),
        format!("def {symbol}():\n    return 2\n"),
    )?;
    Ok(checkout)
}

fn symbol(runtimes: &Runtimes, repo: &str, name: &str) -> Result<Value> {
    let response = handle_request_with_runtimes(
        runtimes,
        json!({"jsonrpc":"2.0", "id":1, "method":"tools/call",
            "params":{"name":"get_symbol", "arguments":{"repo":repo, "name":name}}}),
    )?
    .context("missing response")?;
    ensure!(response.get("error").is_none(), "{response}");
    Ok(serde_json::from_str(
        response["result"]["content"][0]["text"]
            .as_str()
            .context("missing tool result")?,
    )?)
}

fn wait_for_file(path: &Path) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !path.is_file() {
        ensure!(
            Instant::now() < deadline,
            "{} was never created",
            path.display()
        );
        thread::sleep(Duration::from_millis(10));
    }
    Ok(())
}

fn waiting_requests(
    runtimes: &Arc<Runtimes>,
    checkout: &Path,
    repo: &str,
    name: &str,
) -> Result<Vec<Result<Value>>> {
    let writer = FileLease::acquire(
        maintenance::writer_lease_path(&db_path(checkout)),
        Duration::from_secs(1),
    )?;
    let (entered, started) = mpsc::channel();
    let (finished, responses) = mpsc::channel();
    let mut callers = Vec::new();
    let waiting = (|| -> Result<()> {
        for caller in 0..3 {
            let runtimes = Arc::clone(runtimes);
            let repo = repo.to_owned();
            let name = name.to_owned();
            let entered = entered.clone();
            let finished = finished.clone();
            callers.push(thread::spawn(move || {
                let _ = entered.send(());
                let response = symbol(&runtimes, &repo, &name);
                let _ = finished.send(());
                response
            }));
            if caller == 0 {
                wait_for_file(&checkout.join(".tsindex/config.toml"))?;
            }
        }
        for _ in 0..callers.len() {
            started.recv_timeout(Duration::from_secs(5))?;
        }
        ensure!(
            matches!(
                responses.recv_timeout(Duration::from_millis(150)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ),
            "a caller completed before the blocked writer was released"
        );
        Ok(())
    })();
    drop(writer);
    let results = callers
        .into_iter()
        .map(|caller| caller.join().expect("worktree caller panicked"))
        .collect();
    waiting?;
    Ok(results)
}

struct Client {
    child: Child,
    responses: mpsc::Receiver<std::io::Result<String>>,
    reader: Option<thread::JoinHandle<()>>,
    next_id: u64,
    log: PathBuf,
}

impl Client {
    #[cfg(target_os = "linux")]
    fn without_inotify(root: &Path, no_refresh: bool) -> Result<Self> {
        use std::os::unix::process::CommandExt;
        static NEXT_LOG: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let log = root.join(format!(
            "server-{}.log",
            NEXT_LOG.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let mut command = Command::new(BIN);
        command.arg("--root").arg(root);
        if no_refresh {
            command.arg("--no-refresh");
        }
        command
            .args(["serve", "--mcp"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(fs::File::create(&log)?);
        // Restrict only this child. Exhausting the host's inotify budget would
        // interfere with unrelated agents and make the regression nondeterministic.
        unsafe {
            command.pre_exec(|| {
                let mut filter = [
                    libc::sock_filter {
                        code: 0x20,
                        jt: 0,
                        jf: 0,
                        k: 0,
                    },
                    libc::sock_filter {
                        code: 0x15,
                        jt: 1,
                        jf: 0,
                        k: libc::SYS_inotify_init as u32,
                    },
                    libc::sock_filter {
                        code: 0x15,
                        jt: 0,
                        jf: 1,
                        k: libc::SYS_inotify_init1 as u32,
                    },
                    libc::sock_filter {
                        code: 0x06,
                        jt: 0,
                        jf: 0,
                        k: 0x0005_0000 | libc::EMFILE as u32,
                    },
                    libc::sock_filter {
                        code: 0x06,
                        jt: 0,
                        jf: 0,
                        k: 0x7fff_0000,
                    },
                ];
                let program = libc::sock_fprog {
                    len: filter.len() as u16,
                    filter: filter.as_mut_ptr(),
                };
                if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0
                    || libc::prctl(libc::PR_SET_SECCOMP, 2, &program) != 0
                {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command.spawn()?;
        let stdout = child.stdout.take().expect("piped stdout");
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
            next_id: 0,
            log,
        };
        client.request(
            "initialize",
            json!({"protocolVersion":"2024-11-05",
            "capabilities":{}, "clientInfo":{"name":"maintenance-test", "version":"1"}}),
        )?;
        let input = client.child.stdin.as_mut().context("stdin closed")?;
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
        let input = self.child.stdin.as_mut().context("stdin closed")?;
        writeln!(input, "{request}")?;
        input.flush()?;
        let response = self.responses.recv_timeout(Duration::from_secs(10))??;
        Ok(serde_json::from_str(&response)?)
    }

    fn symbol(&mut self, name: &str) -> Result<Value> {
        self.symbol_in_repo(None, name)
    }

    fn symbol_in_repo(&mut self, repo: Option<&str>, name: &str) -> Result<Value> {
        let mut arguments = json!({"name":name});
        if let Some(repo) = repo {
            arguments["repo"] = json!(repo);
        }
        let response = self.request(
            "tools/call",
            json!({"name":"get_symbol", "arguments":arguments}),
        )?;
        ensure!(response.get("error").is_none(), "{response}");
        Ok(serde_json::from_str(
            response["result"]["content"][0]["text"]
                .as_str()
                .context("missing tool result")?,
        )?)
    }

    fn wait_for_watch_failure(&self) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if fs::read_to_string(&self.log)?.contains("Too many open files") {
                return Ok(());
            }
            ensure!(
                Instant::now() < deadline,
                "watcher failure was not injected"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

#[test]
fn lease_excludes_another_owner_and_releases_when_dropped() -> Result<()> {
    let artifacts = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/maintenance-fixtures");
    fs::create_dir_all(&artifacts)?;
    let directory = tempfile::tempdir_in(artifacts)?;
    let path = directory.path().join("owner.lock");
    let first = FileLease::acquire(&path, Duration::from_secs(1))?;
    assert!(FileLease::try_acquire(&path)?.is_none());
    drop(first);
    assert!(FileLease::try_acquire(&path)?.is_some());
    Ok(())
}

#[test]
fn a_blocked_worktree_writer_does_not_block_another_worktree() -> Result<()> {
    let (_directory, root) = fixture()?;
    let one = linked_checkout(&root, "branch-one", "one_edit")?;
    linked_checkout(&root, "branch-two", "two_edit")?;
    let runtimes = Arc::new(Runtimes::new(Runtime::new(
        root.clone(),
        db_path(&root),
        TsIndexConfig::load(&root)?,
        vec![],
    )));
    let writer = FileLease::acquire(
        maintenance::writer_lease_path(&db_path(&one)),
        Duration::from_secs(1),
    )?;
    let first_runtimes = Arc::clone(&runtimes);
    let first = thread::spawn(move || symbol(&first_runtimes, "branch-one", "one_edit"));
    let mut second = None;
    let progress = (|| -> Result<()> {
        // First-use configuration is published before ingestion takes its
        // writer lease. Its presence proves A has entered its blocked job.
        wait_for_file(&one.join(".tsindex/config.toml"))?;
        let second_runtimes = Arc::clone(&runtimes);
        let (finished, response) = mpsc::channel();
        second = Some(thread::spawn(move || {
            let _ = finished.send(symbol(&second_runtimes, "branch-two", "two_edit"));
        }));
        ensure!(
            response.recv_timeout(Duration::from_secs(5))??["total"] == 1,
            "the independent worktree did not return its edited symbol"
        );
        Ok(())
    })();
    // Always release A and reap both callers, including assertion failures
    // represented as errors from the deadline-bound request above.
    drop(writer);
    let first_response = first.join().expect("first worktree caller panicked");
    if let Some(second) = second {
        second.join().expect("second worktree caller panicked");
    }
    progress?;
    assert_eq!(first_response?["total"], 1);
    Ok(())
}

#[test]
fn all_waiters_receive_the_completed_worktree_refresh() -> Result<()> {
    let (_directory, root) = fixture()?;
    let checkout = linked_checkout(&root, "branch-one", "shared_edit")?;
    let runtimes = Arc::new(Runtimes::new(Runtime::new(
        root.clone(),
        db_path(&root),
        TsIndexConfig::load(&root)?,
        vec![],
    )));
    for response in waiting_requests(&runtimes, &checkout, "branch-one", "shared_edit")? {
        assert_eq!(response?["total"], 1);
    }
    Ok(())
}

#[test]
fn a_zero_interval_linked_checkout_reconciles_each_request() -> Result<()> {
    let (_directory, root) = fixture()?;
    let checkout = linked_checkout(&root, "branch-one", "first_edit")?;
    let runtimes = Runtimes::new(Runtime::new(
        root.clone(),
        db_path(&root),
        TsIndexConfig::load(&root)?,
        vec![],
    ));
    assert_eq!(symbol(&runtimes, "branch-one", "first_edit")?["total"], 1);
    fs::write(checkout.join("a.py"), "def next_edit():\n    return 7\n")?;
    assert_eq!(symbol(&runtimes, "branch-one", "next_edit")?["total"], 1);
    assert_eq!(symbol(&runtimes, "branch-one", "first_edit")?["total"], 0);
    Ok(())
}

#[test]
fn all_waiters_receive_a_refresh_failure_and_the_next_request_can_retry() -> Result<()> {
    let (_directory, root) = fixture()?;
    let checkout = linked_checkout(&root, "branch-one", "recovered_edit")?;
    let mut config = TsIndexConfig::load(&root)?;
    config.languages.include = vec!["invalid-maintenance-language".into()];
    let runtimes = Arc::new(Runtimes::new(Runtime::new(
        root.clone(),
        db_path(&root),
        config,
        vec![],
    )));
    for response in waiting_requests(&runtimes, &checkout, "branch-one", "recovered_edit")? {
        let failure = response.expect_err("every waiter must observe the failed refresh");
        assert!(format!("{failure:#}").contains("invalid-maintenance-language"));
    }
    let mut config = TsIndexConfig::load(&checkout)?;
    config.languages.include = vec!["python".into()];
    config.write(&checkout)?;
    assert_eq!(
        symbol(&runtimes, "branch-one", "recovered_edit")?["total"],
        1
    );
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn watcher_allocation_failure_keeps_indexed_reads_fresh() -> Result<()> {
    let (_directory, root) = fixture()?;
    let mut client = Client::without_inotify(&root, false)?;
    client.wait_for_watch_failure()?;
    fs::write(root.join("a.py"), "def after_edit():\n    return 2\n")?;
    assert_eq!(client.symbol("after_edit")?["total"], 1);
    assert_eq!(client.symbol("before_edit")?["total"], 0);
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn explicit_no_refresh_retains_the_existing_opt_out() -> Result<()> {
    let (_directory, root) = fixture()?;
    let mut client = Client::without_inotify(&root, true)?;
    fs::write(root.join("a.py"), "def after_edit():\n    return 2\n")?;
    assert_eq!(client.symbol("before_edit")?["total"], 1);
    assert_eq!(client.symbol("after_edit")?["total"], 0);
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn concurrent_agents_share_one_successful_reconciliation() -> Result<()> {
    let (_directory, root) = fixture()?;
    let mut config = TsIndexConfig::load(&root)?;
    config.server.worktree_refresh_ms = 60_000;
    config.write(&root)?;
    // The fixture's old configuration has a different marker. Both servers
    // must establish freshness, while inotify failure prevents hidden builds.
    let first = Client::without_inotify(&root, false)?;
    first.wait_for_watch_failure()?;
    let second = Client::without_inotify(&root, false)?;
    let logs = [first.log.clone(), second.log.clone()];
    fs::write(
        root.join("a.py"),
        "def shared_agent_edit():\n    return 9\n",
    )?;
    let writer = FileLease::acquire(
        maintenance::writer_lease_path(&db_path(&root)),
        Duration::from_secs(1),
    )?;
    let (entered, started) = mpsc::channel();
    let (finished, responses) = mpsc::channel();
    let callers = [first, second].map(|mut client| {
        let entered = entered.clone();
        let finished = finished.clone();
        thread::spawn(move || {
            let _ = entered.send(());
            let result = client.symbol("shared_agent_edit");
            let _ = finished.send(());
            result
        })
    });
    let waiting = (|| -> Result<()> {
        for _ in 0..callers.len() {
            started.recv_timeout(Duration::from_secs(5))?;
        }
        ensure!(
            matches!(
                responses.recv_timeout(Duration::from_millis(150)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ),
            "a server returned before freshness could be established"
        );
        Ok(())
    })();
    drop(writer);
    let results = callers.map(|caller| caller.join().expect("agent caller panicked"));
    waiting?;
    for response in results {
        assert_eq!(response?["total"], 1);
    }
    let builds = logs
        .into_iter()
        .map(|path| Ok(fs::read_to_string(path)?.matches("build complete:").count()))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .sum::<usize>();
    assert_eq!(
        builds, 1,
        "followers must reuse the owner's successful full scan"
    );
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn two_servers_share_one_linked_checkout_reconciliation() -> Result<()> {
    let (_directory, root) = fixture()?;
    let mut config = TsIndexConfig::load(&root)?;
    config.server.worktree_refresh_ms = 60_000;
    config.write(&root)?;
    let checkout = linked_checkout(&root, "branch-one", "shared_linked_edit")?;
    // Disable primary watching altogether: this contract concerns two linked
    // checkout workers and must not allocate any of the host's watcher budget.
    let first = Client::without_inotify(&root, true)?;
    let second = Client::without_inotify(&root, true)?;
    let logs = [first.log.clone(), second.log.clone()];
    let writer = FileLease::acquire(
        maintenance::writer_lease_path(&db_path(&checkout)),
        Duration::from_secs(1),
    )?;
    let (entered, started) = mpsc::channel();
    let (finished, responses) = mpsc::channel();
    let callers = [first, second].map(|mut client| {
        let entered = entered.clone();
        let finished = finished.clone();
        thread::spawn(move || {
            let _ = entered.send(());
            let result = client.symbol_in_repo(Some("branch-one"), "shared_linked_edit");
            let _ = finished.send(());
            result
        })
    });
    let waiting = (|| -> Result<()> {
        for _ in 0..callers.len() {
            started.recv_timeout(Duration::from_secs(5))?;
        }
        wait_for_file(&checkout.join(".tsindex/config.toml"))?;
        ensure!(
            matches!(
                responses.recv_timeout(Duration::from_millis(150)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ),
            "a server completed while the linked checkout writer was blocked"
        );
        Ok(())
    })();
    drop(writer);
    // Join both callers before inspecting either result so failures also
    // release every test server and its stdout-reader thread.
    let results = callers.map(thread::JoinHandle::join);
    waiting?;
    for response in results {
        assert_eq!(
            response.expect("linked checkout caller panicked")?["total"],
            1
        );
    }
    let builds = logs
        .into_iter()
        .map(|path| Ok(fs::read_to_string(path)?.matches("build complete:").count()))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .sum::<usize>();
    assert_eq!(
        builds, 1,
        "linked checkout followers must reuse the completed full scan"
    );
    Ok(())
}

#[test]
fn successful_reconciliation_is_shared_only_with_matching_configuration() -> Result<()> {
    let (_directory, root) = fixture()?;
    let mut config = TsIndexConfig::load(&root)?;
    config.server.worktree_refresh_ms = 60_000;
    let runtime = Runtime::new(root.clone(), db_path(&root), config.clone(), vec![]);
    runtime.build(true, None)?;
    maintenance::register_primary(&runtime, false);
    // A valid shared freshness marker avoids entering the writer lease at all.
    let writer = FileLease::acquire(
        maintenance::writer_lease_path(&runtime.db_path),
        Duration::from_secs(1),
    )?;
    maintenance::refresh_if_managed(&runtime)?;
    drop(writer);

    config.languages.include = vec!["invalid-maintenance-language".into()];
    let incompatible = Runtime::new(root, runtime.db_path.clone(), config, vec![]);
    let failure = maintenance::refresh_if_managed(&incompatible)
        .expect_err("another configuration's marker must not hide an invalid language");
    assert!(format!("{failure:#}").contains("freshness unavailable"));
    Ok(())
}

#[test]
fn reusing_a_shared_reconciliation_preserves_its_completion_time() -> Result<()> {
    let (_directory, root) = fixture()?;
    let mut config = TsIndexConfig::load(&root)?;
    config.server.worktree_refresh_ms = 60_000;
    let runtime = Runtime::new(root.clone(), db_path(&root), config, vec![]);
    maintenance::reconcile_if_due(&runtime)?;
    let completed_by = Instant::now();
    thread::sleep(Duration::from_millis(75));
    let reused = maintenance::reconcile_if_due(&runtime)?;
    assert!(
        reused <= completed_by,
        "reusing a full scan must retain its original completion time"
    );
    Ok(())
}

#[test]
fn targeted_notifications_do_not_hide_changes_missed_by_the_watcher() -> Result<()> {
    let (_directory, root) = fixture()?;
    let mut config = TsIndexConfig::load(&root)?;
    config.server.worktree_refresh_ms = 60_000;
    let runtime = Runtime::new(root.clone(), db_path(&root), config, vec![]);
    runtime.build(true, None)?;
    maintenance::register_primary(&runtime, false);
    fs::write(root.join("a.py"), "def delivered_edit():\n    return 3\n")?;
    fs::write(root.join("missed.py"), "def missed_edit():\n    return 4\n")?;
    runtime.update_paths(&[root.join("a.py")])?;
    let runtimes = Runtimes::new(runtime);
    assert_eq!(
        symbol(
            &runtimes,
            root.to_str().context("UTF-8 root")?,
            "missed_edit"
        )?["total"],
        1
    );
    Ok(())
}

#[test]
fn a_single_repo_refresh_does_not_hide_changes_in_another_repo() -> Result<()> {
    let (_directory, root) = fixture()?;
    let other = root.join("other");
    fs::create_dir_all(&other)?;
    fs::write(other.join("b.py"), "def other_before():\n    return 3\n")?;
    let mut config = TsIndexConfig::load(&root)?;
    config.server.worktree_refresh_ms = 60_000;
    config.repos = vec![
        RepoConfig {
            name: "main".into(),
            path: ".".into(),
            ..Default::default()
        },
        RepoConfig {
            name: "other".into(),
            path: "other".into(),
            ..Default::default()
        },
    ];
    let runtime = Runtime::new(root.clone(), db_path(&root), config, vec![]);
    runtime.build(true, None)?;
    maintenance::register_primary(&runtime, false);
    fs::write(root.join("a.py"), "def delivered_edit():\n    return 4\n")?;
    fs::write(other.join("b.py"), "def other_after():\n    return 5\n")?;
    runtime.build(true, Some("main"))?;
    let runtimes = Runtimes::new(runtime);
    assert_eq!(symbol(&runtimes, "other", "other_after")?["total"], 1);
    assert_eq!(symbol(&runtimes, "other", "other_before")?["total"], 0);
    Ok(())
}

#[test]
fn a_failed_watcher_driver_can_recover_without_restarting_the_server() -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    let (_directory, root) = fixture()?;
    let runtime = Runtime::new(
        root.clone(),
        db_path(&root),
        TsIndexConfig::load(&root)?,
        vec![],
    );
    let attempts = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&attempts);
    let (started, recovered) = mpsc::channel();
    let (release, finish) = mpsc::channel();
    let finish = Mutex::new(finish);
    maintenance::register_primary_with_watcher(&runtime, move |_| {
        if counter.fetch_add(1, Ordering::SeqCst) == 0 {
            anyhow::bail!("simulated exhausted notification budget");
        }
        started.send(())?;
        finish
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(5))?;
        Ok(())
    });
    recovered.recv_timeout(Duration::from_secs(5))?;
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert!(FileLease::try_acquire(maintenance::watcher_lease_path(&runtime)?)?.is_none());
    release.send(())?;
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if FileLease::try_acquire(maintenance::watcher_lease_path(&runtime)?)?.is_some() {
            break;
        }
        ensure!(
            Instant::now() < deadline,
            "watcher driver retained its lease after exit"
        );
        thread::sleep(Duration::from_millis(10));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn another_agent_takes_notification_ownership_after_owner_exit() -> Result<()> {
    let (_directory, root) = fixture()?;
    let runtime = Runtime::new(
        root.clone(),
        db_path(&root),
        TsIndexConfig::load(&root)?,
        vec![],
    );
    let first = Client::without_inotify(&root, false)?;
    first.wait_for_watch_failure()?;
    assert!(FileLease::try_acquire(maintenance::watcher_lease_path(&runtime)?)?.is_none());
    let mut second = Client::without_inotify(&root, false)?;
    assert_eq!(second.symbol("before_edit")?["total"], 1);
    assert!(!fs::read_to_string(&second.log)?.contains("Too many open files"));
    drop(first);
    thread::sleep(Duration::from_millis(2100));
    assert_eq!(second.symbol("before_edit")?["total"], 1);
    second.wait_for_watch_failure()?;
    assert!(FileLease::try_acquire(maintenance::watcher_lease_path(&runtime)?)?.is_none());
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn refresh_failure_is_reported_instead_of_returning_old_results() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    if unsafe { libc::geteuid() } == 0 {
        return Ok(());
    }
    let (_directory, root) = fixture()?;
    let mut config = TsIndexConfig::load(&root)?;
    config.server.worktree_refresh_ms = 60_000;
    config.write(&root)?;
    let mut client = Client::without_inotify(&root, false)?;
    client.wait_for_watch_failure()?;
    let path = root.join("a.py");
    struct Restore(PathBuf, fs::Permissions);
    impl Drop for Restore {
        fn drop(&mut self) {
            let _ = fs::set_permissions(&self.0, self.1.clone());
        }
    }
    let _restore = Restore(path.clone(), fs::metadata(&path)?.permissions());
    fs::write(&path, "def inaccessible_edit():\n    return 29\n")?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o0))?;
    // The second read must fail again: a partial build cannot stamp a marker
    // that makes later callers accept old rows for the configured full minute.
    for _ in 0..2 {
        let response = client.request(
            "tools/call",
            json!({"name":"get_symbol", "arguments":{"name":"before_edit"}}),
        )?;
        assert!(
            response["error"]["message"]
                .as_str()
                .context("expected a freshness error")?
                .contains("freshness unavailable")
        );
    }
    Ok(())
}
