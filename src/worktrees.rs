//! Git worktree discovery and lazy, independently refreshed checkout indexes.
//! Git metadata is read directly; discovery never shells out to Git.
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow, bail};
use rusqlite::{Connection, OpenFlags};
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
    runtime: Option<Runtime>,
    pending: bool,
    started: u64,
    completed: u64,
    failure: Option<Arc<anyhow::Error>>,
    refreshed: Option<Instant>,
}

#[derive(Default)]
struct CheckoutState {
    entry: Mutex<WorktreeEntry>,
    ready: Condvar,
}

#[derive(Default)]
struct CachedCheckout {
    name: String,
    state: Arc<CheckoutState>,
}

pub struct Runtimes {
    pub primary: Runtime,
    worktrees: Mutex<HashMap<PathBuf, CachedCheckout>>,
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
            crate::maintenance::refresh_if_managed(&self.primary)?;
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
        drop(cache);
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
            crate::maintenance::refresh_if_managed(&self.primary)?;
            return Ok((self.primary.clone(), Some(workspace.name)));
        }
        let state = {
            let mut cache = self.worktrees.lock().unwrap_or_else(|p| p.into_inner());
            let cached = cache.entry(checkout.path.clone()).or_default();
            cached.name = checkout.name.clone();
            Arc::clone(&cached.state)
        };
        // Only this checkout's condition is retained while work is pending.
        // Waiting releases its entry lock; the global registry is already free.
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut entry = state.entry.lock().unwrap_or_else(|p| p.into_inner());
        let refresh = Duration::from_millis(self.primary.config.server.worktree_refresh_ms);
        if !entry.pending && entry.refreshed.is_none_or(|time| time.elapsed() >= refresh) {
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
            // A server's automatic job setting must not create one whole-machine
            // parser pool for every simultaneously active worktree.
            let jobs = if self.primary.jobs == 0 {
                1
            } else {
                self.primary.jobs
            };
            let checkout = checkout.clone();
            let previous = entry.runtime.clone();
            let worker_state = Arc::clone(&state);
            let generation = entry.started.checked_add(1).ok_or_else(|| {
                anyhow!("worktree refresh generation exhausted; restart the server")
            })?;
            entry.started = generation;
            entry.pending = true;
            let spawned = std::thread::Builder::new()
                .name("tsindex-worktree".into())
                .spawn(move || {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                        || -> Result<(Runtime, Instant)> {
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
                            let refreshed = crate::maintenance::reconcile_if_due(&runtime)?;
                            Ok((runtime, refreshed))
                        },
                    ))
                    .unwrap_or_else(|_| {
                        Err(anyhow!("worktree index worker panicked; retry shortly"))
                    });
                    let mut entry = worker_state.entry.lock().unwrap_or_else(|p| p.into_inner());
                    entry.pending = false;
                    entry.completed = generation;
                    match result {
                        Ok((runtime, refreshed)) => {
                            entry.runtime = Some(runtime);
                            entry.refreshed = Some(refreshed);
                            entry.failure = None;
                        }
                        Err(error) => {
                            entry.failure = Some(Arc::new(error));
                            entry.refreshed = None;
                        }
                    }
                    worker_state.ready.notify_all();
                });
            // Thread allocation may fail under the same resource pressure as
            // watcher creation. Clear pending so every waiter can fail or retry
            // instead of waiting forever for a worker that never started.
            if let Err(error) = spawned {
                entry.pending = false;
                entry.completed = generation;
                entry.failure = Some(Arc::new(
                    anyhow!(error).context("failed to start worktree index worker; retry shortly"),
                ));
                entry.refreshed = None;
                state.ready.notify_all();
            }
        }
        // A zero-interval request may start the next job before notified
        // waiters regain this lock. They joined the completed generation and
        // must not be dragged through every later refresh under sustained load.
        let joined = entry.started;
        while entry.completed < joined {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                bail!(
                    "index is not ready for worktree {}; update exceeded 30 s; retry shortly",
                    checkout.name
                );
            }
            let (next, _) = state
                .ready
                .wait_timeout(entry, remaining)
                .unwrap_or_else(|p| p.into_inner());
            entry = next;
        }
        if let Some(error) = &entry.failure {
            if error.downcast_ref::<InvalidParams>().is_some() {
                return Err(InvalidParams(error.to_string()).into());
            }
            bail!("{error:#}");
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
    // Two servers may discover the same new checkout together. Serialize its
    // configuration and seeding so neither reads another's truncated config.
    // The distinct lease is acquired before prepare's ingestion lease; normal
    // builds never acquire it, so the ordering cannot recursively lock a file.
    let _preparing = crate::maintenance::FileLease::acquire(
        db.with_extension("prepare.lock"),
        Duration::from_secs(30),
    )?;
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
    let runtime = Runtime::new(root.clone(), db, config, languages);
    crate::overlay::prepare(checkout, &runtime)?;
    Ok(runtime)
}
