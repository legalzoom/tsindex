//! Git worktree discovery and lazy, independently refreshed checkout indexes.
//! Git metadata is read directly; discovery never shells out to Git.
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
    mpsc,
};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{
    Connection, OpenFlags,
    backup::{Backup, StepResult},
    params,
};
use serde::Serialize;

use crate::config::{RepoConfig, TsIndexConfig, db_path};
use crate::index::Runtime;
use crate::mcp::InvalidParams;

#[derive(Debug, Clone)]
pub struct Checkout {
    pub name: String,
    pub path: PathBuf,
    pub id: String,
    pub common_dir: PathBuf,
    pub main_root: PathBuf,
}

fn resolve_path(base: &Path, value: &str) -> PathBuf {
    base.join(value.trim())
}

pub(crate) fn common_dir(root: &Path) -> Option<PathBuf> {
    let marker = root.join(".git");
    if marker.is_dir() {
        return marker.canonicalize().ok();
    }
    let raw = fs::read_to_string(marker).ok()?;
    let gitdir = resolve_path(root, raw.lines().next()?.strip_prefix("gitdir:")?);
    let common = fs::read_to_string(gitdir.join("commondir")).ok()?;
    resolve_path(&gitdir, common.lines().next()?)
        .canonicalize()
        .ok()
}

/// Includes the main checkout, live linked worktrees, and no prunable entries.
pub fn discover(root: &Path) -> Result<Vec<Checkout>> {
    let Some(common) = common_dir(root) else {
        return Ok(Vec::new());
    };
    let main = common
        .parent()
        .ok_or_else(|| anyhow!("Git common directory has no parent"))?
        .to_path_buf();
    let mut checkouts = vec![Checkout {
        name: basename(&main),
        path: main.clone(),
        id: "main".into(),
        common_dir: common.clone(),
        main_root: main.clone(),
    }];
    let entries = match fs::read_dir(common.join("worktrees")) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(checkouts),
        Err(e) => return Err(e.into()),
    };
    for entry in entries {
        let entry = entry?;
        let Ok(raw) = fs::read_to_string(entry.path().join("gitdir")) else {
            continue;
        };
        let marker = resolve_path(&entry.path(), raw.trim());
        if !marker.is_file() {
            continue;
        }
        let Some(parent) = marker.parent() else {
            continue;
        };
        let Ok(path) = parent.canonicalize() else {
            continue;
        };
        if common_dir(&path).as_ref() != Some(&common) {
            continue;
        }
        checkouts.push(Checkout {
            name: basename(&path),
            path,
            id: entry.file_name().to_string_lossy().into_owned(),
            common_dir: common.clone(),
            main_root: main.clone(),
        });
    }
    checkouts.sort_by(|a, b| a.path.cmp(&b.path));
    disambiguate(&mut checkouts);
    Ok(checkouts)
}

fn basename(path: &Path) -> String {
    path.file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned()
}

fn disambiguate(checkouts: &mut [Checkout]) {
    let mut counts = HashMap::new();
    for checkout in checkouts.iter() {
        *counts.entry(checkout.name.clone()).or_insert(0) += 1;
    }
    for checkout in checkouts.iter_mut() {
        if counts[&checkout.name] > 1 {
            checkout.name = format!("{}@{}", checkout.name, checkout.id);
        }
    }
    // A literal basename can itself contain '@', or a linked id can be 'main'.
    let mut used = HashSet::new();
    for checkout in checkouts {
        let base = checkout.name.clone();
        let mut suffix = 2;
        while !used.insert(checkout.name.clone()) {
            checkout.name = format!("{base}@{suffix}");
            suffix += 1;
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RepoStatus {
    pub name: String,
    pub path: PathBuf,
    pub indexed: bool,
    pub updated_at: Option<i64>,
}

fn index_status(db: &Path) -> (bool, Option<i64>) {
    let Ok(conn) = Connection::open_with_flags(db, OpenFlags::SQLITE_OPEN_READ_ONLY) else {
        return (false, None);
    };
    conn.query_row(
        "SELECT ready, updated_at FROM index_state WHERE id = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .unwrap_or((false, None))
}

#[derive(Default)]
struct WorktreeEntry {
    name: String,
    runtime: Option<Runtime>,
    pending: Option<mpsc::Receiver<Result<Runtime>>>,
    refreshed: Option<Instant>,
}

pub struct Runtimes {
    pub primary: Runtime,
    worktrees: Mutex<HashMap<PathBuf, WorktreeEntry>>,
}

impl std::ops::Deref for Runtimes {
    type Target = Runtime;
    fn deref(&self) -> &Runtime {
        &self.primary
    }
}

impl std::ops::DerefMut for Runtimes {
    fn deref_mut(&mut self) -> &mut Runtime {
        &mut self.primary
    }
}

impl Runtimes {
    pub fn new(primary: Runtime) -> Self {
        Self {
            primary,
            worktrees: Mutex::new(HashMap::new()),
        }
    }

    fn checkouts(&self) -> Result<Vec<Checkout>> {
        let mut seen = HashSet::new();
        let mut all = Vec::new();
        for workspace in self.primary.workspaces()? {
            let discovered = discover(&workspace.root)?;
            for mut checkout in discovered {
                if !seen.insert(checkout.path.clone()) {
                    continue;
                }
                // Preserve configured catalog names, including the primary's.
                if checkout.path == workspace.root && checkout.name == basename(&checkout.path) {
                    checkout.name = workspace.name.clone();
                }
                all.push(checkout);
            }
            if seen.insert(workspace.root.clone()) {
                all.push(Checkout {
                    name: workspace.name,
                    path: workspace.root.clone(),
                    id: "main".into(),
                    common_dir: PathBuf::new(),
                    main_root: workspace.root,
                });
            }
        }
        all.sort_by(|a, b| a.path.cmp(&b.path));
        disambiguate(&mut all);
        Ok(all)
    }

    pub fn list(&self) -> Result<Vec<RepoStatus>> {
        let primary = self.primary.workspaces()?;
        Ok(self
            .checkouts()?
            .into_iter()
            .map(|checkout| {
                let db = if primary.iter().any(|w| w.root == checkout.path) {
                    self.primary.db_path.clone()
                } else {
                    db_path(&checkout.path)
                };
                let (indexed, updated_at) = index_status(&db);
                RepoStatus {
                    name: checkout.name,
                    path: checkout.path,
                    indexed,
                    updated_at,
                }
            })
            .collect())
    }

    pub(crate) fn repo_aliases(&self, runtime: &Runtime) -> Result<HashMap<String, String>> {
        let checkouts = self.checkouts()?;
        Ok(runtime
            .workspaces()?
            .into_iter()
            .filter_map(|workspace| {
                checkouts
                    .iter()
                    .find(|checkout| {
                        checkout.path == workspace.root && checkout.name != workspace.name
                    })
                    .map(|checkout| (workspace.name, checkout.name.clone()))
            })
            .collect())
    }

    /// Resolve names or absolute roots, refreshing a used worktree at most once
    /// per interval. Slow builds continue in the background after the 30s wait.
    pub fn resolve(&self, repo: Option<&str>) -> Result<(Runtime, Option<String>)> {
        let Some(repo) = repo else {
            return Ok((self.primary.clone(), None));
        };
        let absolute = Path::new(repo).is_absolute().then(|| {
            Path::new(repo)
                .canonicalize()
                .unwrap_or_else(|_| PathBuf::from(repo))
        });
        let mut cache = self.worktrees.lock().unwrap_or_else(|p| p.into_inner());
        let missing = cache
            .iter()
            .find(|(path, entry)| {
                (entry.name == repo || absolute.as_ref() == Some(*path)) && !path.is_dir()
            })
            .map(|(path, entry)| (path.clone(), entry.name.clone()));
        if let Some((path, name)) = missing {
            cache.remove(&path);
            return Err(InvalidParams(format!(
                "worktree {name} no longer exists at {}",
                path.display()
            ))
            .into());
        }
        cache.retain(|path, _| path.is_dir());
        // Metadata discovery is cheap and also notices worktrees created after
        // startup, renamed entries, and removals without adding any watchers.
        let checkout = self
            .checkouts()?
            .into_iter()
            .find(|c| c.name == repo || absolute.as_ref() == Some(&c.path));
        let Some(checkout) = checkout else {
            let known = self
                .list()?
                .into_iter()
                .map(|r| format!("{} (indexed={})", r.name, r.indexed))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(
                InvalidParams(format!("unknown repo {repo:?}; known repos: {known}")).into(),
            );
        };
        if let Some(workspace) = self
            .primary
            .workspaces()?
            .into_iter()
            .find(|w| w.root == checkout.path)
        {
            return Ok((self.primary.clone(), Some(workspace.name)));
        }
        let entry = cache.entry(checkout.path.clone()).or_default();
        entry.name = checkout.name.clone();
        let refresh = Duration::from_millis(self.primary.config.server.worktree_refresh_ms);
        if entry.pending.is_none() && entry.refreshed.is_none_or(|time| time.elapsed() >= refresh) {
            let (tx, rx) = mpsc::channel();
            let mut config = self.primary.config.clone();
            if let Some(workspace) = self
                .primary
                .workspaces()?
                .into_iter()
                .find(|w| common_dir(&w.root).as_ref() == Some(&checkout.common_dir))
            {
                config.languages.include = workspace.languages;
                config.ignore.extra = workspace.ignore;
                config.repos.clear();
            }
            let languages = self.primary.languages.clone();
            let jobs = self.primary.jobs;
            let checkout = checkout.clone();
            let previous = entry.runtime.take();
            entry.pending = Some(rx);
            std::thread::spawn(move || {
                let result = (|| {
                    let runtime = match previous {
                        Some(runtime)
                            if runtime
                                .config
                                .repos
                                .first()
                                .is_some_and(|r| r.name == checkout.name) =>
                        {
                            runtime
                        }
                        _ => open_checkout(&checkout, &config, languages)?.with_jobs(jobs),
                    };
                    let stats = runtime.build(true, None)?;
                    if stats.failed > 0 {
                        bail!(
                            "worktree index update failed for {} files; retry shortly",
                            stats.failed
                        );
                    }
                    Ok(runtime)
                })();
                let _ = tx.send(result);
            });
        }
        if let Some(pending) = &entry.pending {
            match pending.recv_timeout(Duration::from_secs(30)) {
                Ok(result) => {
                    entry.pending = None;
                    entry.runtime = Some(result?);
                    entry.refreshed = Some(Instant::now());
                }
                Err(mpsc::RecvTimeoutError::Timeout) => bail!(
                    "index is not ready for worktree {}; update exceeded 30 s; retry shortly",
                    checkout.name
                ),
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    entry.pending = None;
                    bail!("worktree index worker stopped; retry shortly");
                }
            }
        }
        let runtime = entry.runtime.as_ref().unwrap().clone();
        let repo = runtime
            .workspaces()?
            .into_iter()
            .find(|workspace| workspace.root == checkout.path)
            .ok_or_else(|| anyhow!("worktree config does not index {}", checkout.path.display()))?
            .name;
        Ok((runtime, Some(repo)))
    }
}

/// Prepare configuration and seed without building. Also used before starting
/// a server in a fresh linked checkout, so its normal startup build is cheap.
pub fn open_checkout(
    checkout: &Checkout,
    primary_config: &TsIndexConfig,
    languages: Vec<String>,
) -> Result<Runtime> {
    let root = &checkout.path;
    if !root.is_dir() {
        return Err(InvalidParams(format!(
            "worktree {} no longer exists at {}",
            checkout.name,
            root.display()
        ))
        .into());
    }
    let db = db_path(root);
    let config_path = crate::config::config_path(root);
    if db.exists() && config_path.exists() {
        return Ok(Runtime::new(
            root.clone(),
            db,
            TsIndexConfig::load(root)?,
            languages,
        ));
    }
    let mut config = if config_path.exists() {
        TsIndexConfig::load(root)?
    } else {
        primary_config.clone()
    };
    let template = config
        .repos
        .iter()
        .find(|r| {
            let config_root = if config_path.exists() {
                root
            } else {
                &checkout.main_root
            };
            config_root
                .join(&r.path)
                .canonicalize()
                .ok()
                .is_some_and(|path| path == *root || path == checkout.main_root)
        })
        .cloned();
    config.root.path = ".".into();
    config.repos = vec![RepoConfig {
        name: checkout.name.clone(),
        path: ".".into(),
        languages: template
            .as_ref()
            .map(|r| r.languages.clone())
            .unwrap_or_default(),
        ignore: template
            .as_ref()
            .map(|r| r.ignore.clone())
            .unwrap_or_default(),
    }];
    config.write(root)?;
    let ignore = root.join(".tsindex/.gitignore");
    let mut rules = if ignore.exists() {
        fs::read_to_string(&ignore)?
    } else {
        String::new()
    };
    if rules.lines().last() != Some("*") {
        if !rules.is_empty() && !rules.ends_with('\n') {
            rules.push('\n');
        }
        rules.push_str("*\n");
        fs::write(ignore, rules)?;
    }
    if !db.exists() {
        seed(checkout, &db)?;
    }
    Ok(Runtime::new(root.clone(), db, config, languages))
}

fn seed(checkout: &Checkout, destination: &Path) -> Result<()> {
    let source = db_path(&checkout.main_root);
    if source == destination || !source.is_file() {
        return Ok(());
    }
    // Both endpoints must still belong to the discovered Git repository.
    if common_dir(&checkout.main_root).as_ref() != Some(&checkout.common_dir)
        || common_dir(&checkout.path).as_ref() != Some(&checkout.common_dir)
    {
        return Ok(());
    }
    let source = Connection::open_with_flags(source, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    static SEED_ID: AtomicU64 = AtomicU64::new(0);
    let temporary = destination.with_extension(format!(
        "seed-{}-{}",
        std::process::id(),
        SEED_ID.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| -> Result<()> {
        let mut dest = Connection::open(&temporary)?;
        {
            let backup = Backup::new(&source, &mut dest)?;
            let started = Instant::now();
            loop {
                match backup.step(256)? {
                    StepResult::Done => break,
                    StepResult::Busy | StepResult::Locked => {
                        std::thread::sleep(Duration::from_millis(10))
                    }
                    _ => {}
                }
                if started.elapsed() > Duration::from_secs(30) {
                    bail!("worktree backup exceeded 30 s; retry shortly");
                }
            }
        }
        // Keep only the main checkout's rows if its database is a catalog.
        let repos = dest
            .prepare("SELECT id, root_path FROM repos")?
            .query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let Some((id, _)) = repos.iter().find(|(_, root)| {
            Path::new(root).canonicalize().ok().as_ref() == Some(&checkout.main_root)
        }) else {
            return Ok(());
        };
        dest.execute_batch("PRAGMA foreign_keys = ON;")?;
        dest.execute("DELETE FROM repos WHERE id != ?1", [id])?;
        dest.execute(
            "UPDATE repos SET name = ?1, root_path = ?2 WHERE id = ?3",
            params![checkout.name, checkout.path.to_string_lossy(), id],
        )?;
        // Checkout timestamps cannot establish content identity across roots.
        // Force the first refresh through hashing while retaining schema-stale
        // sentinels, which must still force parsing.
        dest.execute("UPDATE files SET mtime_ns = 0 WHERE mtime_ns >= 0", [])?;
        if dest.prepare("SELECT ready FROM index_state").is_ok() {
            dest.execute("UPDATE index_state SET ready = 0 WHERE id = 1", [])?;
        }
        dest.close().map_err(|(_, error)| error)?;
        // Publish only a complete copy, without overwriting a concurrent build.
        match fs::hard_link(&temporary, destination) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
        Ok(())
    })();
    let _ = fs::remove_file(temporary);
    result.context("failed to seed worktree index")
}
