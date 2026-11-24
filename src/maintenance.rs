//! Cross-process writer ownership and primary-index freshness.
//!
//! Notification resources are an optimization: failed or absent watchers must
//! leave a bounded on-demand path that can establish freshness before reads.
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::index::Runtime;

/// An advisory lease whose OS handle releases ownership on process exit.
/// The pathname is never removed: unlinking a leased inode would allow two
/// different owners to lock different files at the same logical location.
pub struct FileLease {
    #[cfg(unix)]
    _file: File,
    #[cfg(not(unix))]
    _connection: rusqlite::Connection,
}

impl FileLease {
    pub fn try_acquire(path: impl AsRef<Path>) -> Result<Option<Self>> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            use std::os::unix::fs::OpenOptionsExt;
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .mode(0o600)
                .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
                .open(path)
                .with_context(|| format!("failed to open lease {}", path.display()))?;
            // flock owns the open file description, so a crash or kill releases
            // this lease without PID files, stale timeouts, or owner guessing.
            // https://man7.org/linux/man-pages/man2/flock.2.html
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
                return Ok(Some(Self { _file: file }));
            }
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::WouldBlock {
                return Ok(None);
            }
            Err(error).with_context(|| format!("failed to lock {}", path.display()))
        }
        #[cfg(not(unix))]
        {
            // SQLite's process-safe file locking supplies the same lease on
            // platforms without flock. No index data lives in this lock DB.
            let connection = rusqlite::Connection::open(path)?;
            connection.busy_timeout(Duration::ZERO)?;
            match connection.execute_batch("BEGIN EXCLUSIVE") {
                Ok(()) => Ok(Some(Self {
                    _connection: connection,
                })),
                Err(rusqlite::Error::SqliteFailure(code, _))
                    if matches!(
                        code.code,
                        rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
                    ) =>
                {
                    Ok(None)
                }
                Err(error) => Err(error.into()),
            }
        }
    }

    pub fn acquire(path: impl AsRef<Path>, timeout: Duration) -> Result<Self> {
        let path = path.as_ref();
        let started = Instant::now();
        loop {
            if let Some(lease) = Self::try_acquire(path)? {
                return Ok(lease);
            }
            if started.elapsed() >= timeout {
                bail!("index writer is busy at {}; retry shortly", path.display());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

pub fn writer_lease_path(db_path: &Path) -> PathBuf {
    // Keep the original writer-lock name so older overlay binaries still
    // serialize writes with this version. Same-stem custom databases may
    // contend on this lock, but never share freshness or watcher ownership.
    db_path.with_extension("ingest.lock")
}

fn sidecar_path(db_path: &Path, suffix: &str) -> PathBuf {
    let mut path = db_path.as_os_str().to_os_string();
    path.push(".");
    path.push(suffix);
    PathBuf::from(path)
}

pub fn watcher_lease_path(runtime: &Runtime) -> Result<PathBuf> {
    Ok(sidecar_path(
        &key(&runtime.db_path),
        &format!("watch-{}.lock", configuration(runtime)?),
    ))
}

pub fn refresh_lease_path(db_path: &Path) -> PathBuf {
    sidecar_path(&key(db_path), "refresh.lock")
}

struct Primary {
    watcher: Option<Arc<WatchFactory>>,
    watcher_running: AtomicBool,
    retry_after: Mutex<Option<Instant>>,
    refresh: Mutex<()>,
}

type WatchFactory = dyn Fn(&Runtime) -> Result<()> + Send + Sync;

#[derive(Serialize, Deserialize)]
struct RefreshMarker {
    configuration: String,
    completed_ms: u128,
    generation: i64,
}

pub(crate) struct Reconciliation {
    completed_at: Instant,
    generation: Option<i64>,
}

impl Reconciliation {
    pub(crate) fn is_current(&self, runtime: &Runtime, interval: Duration) -> bool {
        self.completed_at.elapsed() < interval
            && self
                .generation
                .is_some_and(|expected| generation(runtime).ok() == Some(expected))
    }
}

fn configuration(runtime: &Runtime) -> Result<String> {
    let database = key(&runtime.db_path);
    let mut languages = runtime.languages.clone();
    languages.sort();
    languages.dedup();
    let input = serde_json::to_vec(&(
        // Generations are local to one database. A copied or misplaced marker
        // must not certify another index with an equal generation number.
        // Native path bytes also preserve valid non-UTF-8 database filenames.
        database.as_os_str().as_encoded_bytes(),
        &runtime.root,
        &runtime.config,
        &languages,
        runtime.prune_repos,
        env!("CARGO_PKG_VERSION"),
        crate::index::SCHEMA_VERSION,
    ))?;
    let mut digest = Sha256::new();
    digest.update(input);
    // Global Git exclude inputs come from each process's environment (HOME,
    // XDG_CONFIG_HOME, core.excludesFile). Keep watcher leases and freshness
    // markers scoped to the effective rule sources this process evaluates.
    let global_excludes = crate::ignore_inputs::global_inputs();
    digest.update(b"\0global-git-excludes:");
    digest.update((global_excludes.len() as u64).to_le_bytes());
    for path in global_excludes {
        let resolved = key(&path);
        let bytes = resolved.as_os_str().as_encoded_bytes();
        digest.update((bytes.len() as u64).to_le_bytes());
        digest.update(bytes);
    }
    // Every configured ignore file (`[ignore].extra` or `[[repos]].ignore`),
    // relative or absolute, is matched against the process working directory.
    if runtime.config.has_explicit_ignore_files() {
        let cwd = std::env::current_dir()?;
        let bytes = cwd.as_os_str().as_encoded_bytes();
        digest.update(b"\0explicit-ignore-cwd:");
        digest.update((bytes.len() as u64).to_le_bytes());
        digest.update(bytes);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn generation(runtime: &Runtime) -> Result<i64> {
    let connection = rusqlite::Connection::open_with_flags(
        &runtime.db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    Ok(connection.query_row(
        "SELECT generation FROM index_state WHERE id = 1",
        [],
        |row| row.get(0),
    )?)
}

pub fn refresh_marker_path(db_path: &Path) -> PathBuf {
    // Direct Runtime users need not have generated .tsindex ignore rules.
    // Keep maintenance artifacts outside the source extension allowlist.
    sidecar_path(&key(db_path), "refresh-state")
}

fn marker_reconciliation(runtime: &Runtime) -> Option<Reconciliation> {
    let interval = u128::from(runtime.config.server.worktree_refresh_ms);
    if interval == 0 {
        return None;
    }
    let marker: RefreshMarker =
        serde_json::from_slice(&fs::read(refresh_marker_path(&runtime.db_path)).ok()?).ok()?;
    if marker.configuration != configuration(runtime).ok()? {
        return None;
    }
    let checked_at = Instant::now();
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    // A clock moving backwards or another configuration publishing this DB
    // invalidates reuse rather than manufacturing a successful refresh.
    let age = now.as_millis().checked_sub(marker.completed_ms)?;
    if age >= interval || generation(runtime).ok()? != marker.generation {
        return None;
    }
    // Preserve the owner's completion age in a follower's monotonic cache.
    // Adding one millisecond accounts conservatively for marker clock rounding.
    let completed_at = checked_at.checked_sub(
        Duration::from_millis(u64::try_from(age).ok()?).saturating_add(Duration::from_millis(1)),
    )?;
    Some(Reconciliation {
        completed_at,
        generation: Some(marker.generation),
    })
}

/// Called after a successful build while the database's writer lease is held.
/// Publishing this marker inside that lease ties it to the last writer's
/// effective configuration, so other processes can coalesce their scans.
pub fn record_refresh(runtime: &Runtime) -> Result<()> {
    let marker = RefreshMarker {
        configuration: configuration(runtime)?,
        completed_ms: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
        generation: generation(runtime)?,
    };
    static NEXT_MARKER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let path = refresh_marker_path(&runtime.db_path);
    let temporary = sidecar_path(
        &path,
        &format!(
            "refresh-{}-{}.tmp",
            std::process::id(),
            NEXT_MARKER.fetch_add(1, Ordering::Relaxed)
        ),
    );
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        use std::io::Write;
        file.write_all(&serde_json::to_vec(&marker)?)?;
        file.sync_all()?;
        fs::rename(&temporary, &path)?;
        Ok(())
    })();
    let _ = fs::remove_file(temporary);
    result
}

fn primaries() -> &'static Mutex<HashMap<PathBuf, Arc<Primary>>> {
    static PRIMARIES: OnceLock<Mutex<HashMap<PathBuf, Arc<Primary>>>> = OnceLock::new();
    PRIMARIES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Resolve existing targets, symlinked parents, and dangling symlink leaves to
/// one canonical path so direct and managed runtimes coalesce sidecars.
pub(crate) fn canonical_target(path: &Path) -> std::io::Result<PathBuf> {
    resolve_canonical_target(path, 0)
}

fn resolve_canonical_target(path: &Path, depth: u8) -> std::io::Result<PathBuf> {
    match path.canonicalize() {
        Ok(resolved) => Ok(resolved),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if depth < 40
                && let Ok(meta) = fs::symlink_metadata(path)
                && meta.file_type().is_symlink()
            {
                let target = fs::read_link(path)?;
                let next = if target.is_absolute() {
                    target
                } else {
                    path.parent().unwrap_or(Path::new("")).join(target)
                };
                return resolve_canonical_target(&next, depth + 1);
            }
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .ok_or(error)?;
            Ok(resolve_canonical_target(parent, depth)?.join(
                path.file_name()
                    .ok_or_else(|| std::io::Error::other("missing path filename"))?,
            ))
        }
        Err(error) => Err(error),
    }
}

fn key(path: &Path) -> PathBuf {
    canonical_target(path).unwrap_or_else(|_| path.to_path_buf())
}

fn primary(path: &Path) -> Option<Arc<Primary>> {
    primaries()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(&key(path))
        .cloned()
}

/// Register only automatic server maintenance. Explicit `--no-refresh`
/// servers are deliberately absent from this registry and retain their opt-out.
pub fn register_primary(runtime: &Runtime, watch: bool) {
    let watcher = watch.then(|| Arc::new(|runtime: &Runtime| runtime.watch()) as Arc<WatchFactory>);
    register(runtime, watcher);
}

/// The watcher driver is an explicit seam for deterministic startup/recovery
/// tests; production uses the normal OS watcher through `register_primary`.
pub fn register_primary_with_watcher(
    runtime: &Runtime,
    watcher: impl Fn(&Runtime) -> Result<()> + Send + Sync + 'static,
) {
    register(runtime, Some(Arc::new(watcher)));
}

fn register(runtime: &Runtime, watcher: Option<Arc<WatchFactory>>) {
    let state = Arc::new(Primary {
        watcher,
        watcher_running: AtomicBool::new(false),
        retry_after: Mutex::new(None),
        refresh: Mutex::new(()),
    });
    primaries()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .insert(key(&runtime.db_path), Arc::clone(&state));
    ensure_watcher(runtime, &state);
}

fn ensure_watcher(runtime: &Runtime, state: &Arc<Primary>) {
    let Some(watcher) = state.watcher.as_ref().cloned() else {
        return;
    };
    if state
        .retry_after
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .is_some_and(|time| Instant::now() < time)
        || state.watcher_running.swap(true, Ordering::AcqRel)
    {
        return;
    }
    let worker_state = Arc::clone(state);
    let runtime = runtime.clone();
    let spawned = std::thread::Builder::new().name("tsindex-watch".into()).spawn(move || {
        let state = worker_state;
        struct Finish(Arc<Primary>);
        impl Drop for Finish {
            fn drop(&mut self) {
                *self.0.retry_after.lock().unwrap_or_else(|e| e.into_inner()) =
                    Some(Instant::now() + Duration::from_secs(2));
                self.0.watcher_running.store(false, Ordering::Release);
            }
        }
        let _finish = Finish(Arc::clone(&state));
        let path = match watcher_lease_path(&runtime) {
            Ok(path) => path,
            Err(error) => {
                eprintln!("warning: watcher configuration unavailable: {error:#}");
                return;
            }
        };
        let lease = match FileLease::try_acquire(path) {
            Ok(Some(lease)) => lease,
            // Another server owns notification delivery for this database.
            // Reads still establish freshness through their own coordinator.
            Ok(None) => return,
            Err(error) => {
                eprintln!(
                    "warning: watcher ownership unavailable; using on-demand refresh: {error:#}"
                );
                return;
            }
        };
        let _lease = lease;
        const ATTEMPTS: u32 = 5;
        for attempt in 1..=ATTEMPTS {
            match watcher(&runtime) {
                Ok(()) => return,
                Err(error) => {
                    eprintln!(
                        "warning: background index watch failed (attempt {attempt}/{ATTEMPTS}); using on-demand refresh: {error:#}"
                    );
                    if attempt < ATTEMPTS {
                        std::thread::sleep(Duration::from_secs(2 * u64::from(attempt)));
                    }
                }
            }
        }
    });
    // Resource exhaustion can prevent the supervisor thread itself starting.
    // Leave on-demand maintenance available and allow a later request to retry.
    if let Err(error) = spawned {
        *state.retry_after.lock().unwrap_or_else(|e| e.into_inner()) =
            Some(Instant::now() + Duration::from_secs(2));
        state.watcher_running.store(false, Ordering::Release);
        eprintln!("warning: watcher worker unavailable; using on-demand refresh: {error}");
    }
}

/// Ensure managed primary reads have a successful refresh path even when
/// notification allocation or ownership is unavailable. Its lock is scoped
/// to one database; no registry or other checkout lock is held during I/O.
/// A live watcher also uses this bounded reconciliation: receiving some
/// events does not establish coverage for every configured source directory.
pub fn refresh_if_managed(runtime: &Runtime) -> Result<()> {
    let Some(state) = primary(&runtime.db_path) else {
        return Ok(());
    };
    ensure_watcher(runtime, &state);
    let _local = state.refresh.lock().unwrap_or_else(|e| e.into_inner());
    reconcile_if_due(runtime).map(|_| ())
}

/// Reconcile one checkout only when no compatible completed full scan is
/// fresh. The durable marker and cross-process lease let independent servers
/// reuse an owner's result. A zero interval deliberately requires a new scan;
/// explicit CLI builds call Runtime::build directly and remain unconditional.
/// The returned completion time retains the shared marker's age, so caching
/// its reuse cannot give an almost-expired full scan another entire interval.
pub fn reconcile_if_due(runtime: &Runtime) -> Result<Instant> {
    Ok(reconcile(runtime)?.completed_at)
}

pub(crate) fn reconcile(runtime: &Runtime) -> Result<Reconciliation> {
    if let Some(completed) = marker_reconciliation(runtime) {
        return Ok(completed);
    }
    // This lease coalesces callers before Runtime::build takes the writer
    // lease. It is a different inode, so parsing does not recursively lock it.
    let _refresh = FileLease::acquire(
        refresh_lease_path(&runtime.db_path),
        Duration::from_secs(30),
    )
    .context("index freshness unavailable: refresh coordinator is busy")?;
    if let Some(completed) = marker_reconciliation(runtime) {
        return Ok(completed);
    }
    let started = Instant::now();
    let stats = runtime
        .build(true, None)
        .context("index freshness unavailable: on-demand refresh failed; retry shortly")?;
    if stats.failed > 0 {
        bail!(
            "index freshness unavailable: {} files failed to refresh; retry shortly",
            stats.failed
        );
    }
    // A zero interval never has a reusable marker. A concurrent publication
    // may also invalidate it before this read. Never cache a generation the
    // configuration-matched marker didn't certify as ours.
    Ok(marker_reconciliation(runtime).unwrap_or(Reconciliation {
        completed_at: started,
        generation: None,
    }))
}
