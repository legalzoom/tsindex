//! Persistent checkout overlays over immutable repository index snapshots.
//!
//! Published overlays name their private extraction tables explicitly. Only
//! connection-local views expose the logical checkout, so older binaries cannot
//! mistake the private delta for a complete index.

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use rusqlite::backup::{Backup, StepResult};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use sha2::{Digest, Sha256};

use crate::config::db_path;
use crate::index::{BuildStats, Runtime, SCHEMA_VERSION};
use crate::worktrees::{Checkout, common_dir};

// Parent indexed reads reject this version. Their raw-query fallback also
// requires a physical `files` table, which published overlays never expose.
pub const OVERLAY_FORMAT_VERSION: i64 = 1_000_005;

pub(crate) fn disk_version(conn: &Connection) -> Result<i64> {
    Ok(if table_exists(conn, "overlay_meta")? {
        OVERLAY_FORMAT_VERSION
    } else {
        SCHEMA_VERSION
    })
}

pub(crate) fn validate_format(conn: &Connection) -> Result<()> {
    let version: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
    if version != OVERLAY_FORMAT_VERSION {
        bail!("unsupported checkout overlay format {version}; rebuild its derived index");
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct Metadata {
    base: PathBuf,
    repo_id: i64,
    staging: bool,
    identity: Option<String>,
}

/// Accounting at the storage interface. Private files count extraction records,
/// not the small per-path observations used to avoid repeatedly hashing sources.
#[derive(Debug)]
pub struct StorageStats {
    pub base_path: Option<PathBuf>,
    pub base_files: usize,
    pub private_files: usize,
    pub base_bytes: u64,
    pub private_bytes: u64,
    pub generation: i64,
}

pub fn storage_stats(path: &Path) -> Result<StorageStats> {
    let conn = open_readonly(path)?;
    let metadata = metadata(&conn)?;
    let base_files = if let Some(meta) = &metadata {
        let base = open_readonly(&meta.base)?;
        validate_base_identity(&base, meta, false)?;
        base.query_row(
            "SELECT COUNT(*) FROM files WHERE repo_id = ?1",
            [meta.repo_id],
            |r| r.get(0),
        )?
    } else {
        0
    };
    Ok(StorageStats {
        base_files,
        private_files: conn.query_row(
            if metadata.as_ref().is_some_and(|m| !m.staging) {
                "SELECT COUNT(*) FROM main.overlay_files"
            } else {
                "SELECT COUNT(*) FROM main.files"
            },
            [],
            |r| r.get(0),
        )?,
        base_bytes: metadata
            .as_ref()
            .map(|m| fs::metadata(&m.base))
            .transpose()?
            .map_or(0, |m| m.len()),
        private_bytes: fs::metadata(path)?.len(),
        generation: conn.query_row(
            "SELECT generation FROM main.index_state WHERE id = 1",
            [],
            |r| r.get(0),
        )?,
        base_path: metadata.map(|m| m.base),
    })
}

/// Open one coherent logical read view. The returned connection owns the read
/// transaction; dropping it releases the old WAL snapshot after publication.
/// Base generations are immutable and intentionally retained until an explicit
/// cross-process reclamation protocol is implemented.
pub fn read_connection(path: &Path) -> Result<Connection> {
    let conn = open_readonly(path)?;
    conn.execute_batch("BEGIN DEFERRED")?;
    attach_views(&conn)?;
    Ok(conn)
}

fn open_readonly(path: &Path) -> Result<Connection> {
    Ok(Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI,
    )?)
}

pub(crate) fn table_exists(conn: &Connection, name: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM main.sqlite_master WHERE type='table' AND name=?1)",
        [name],
        |r| r.get(0),
    )?)
}

fn metadata(conn: &Connection) -> Result<Option<Metadata>> {
    if !table_exists(conn, "overlay_meta")? {
        return Ok(None);
    }
    let has_identity = conn
        .prepare("PRAGMA main.table_info(overlay_meta)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .iter()
        .any(|column| column == "base_identity");
    // Older experimental overlays have no immutable identity pin. Keep them
    // recognizable as overlays so refresh can cold-recover them, but never
    // trust their pathname as evidence that the attached base is unchanged.
    let sql = if has_identity {
        "SELECT base_path, base_repo_id, staging, base_identity FROM main.overlay_meta WHERE id = 1"
    } else {
        "SELECT base_path, base_repo_id, staging, NULL FROM main.overlay_meta WHERE id = 1"
    };
    Ok(conn
        .query_row(sql, [], |r| {
            Ok(Metadata {
                base: PathBuf::from(r.get::<_, String>(0)?),
                repo_id: r.get(1)?,
                staging: r.get(2)?,
                identity: r.get(3)?,
            })
        })
        .optional()?)
}

pub(crate) fn is_overlay(path: &Path) -> Result<bool> {
    if !path.is_file() {
        return Ok(false);
    }
    Ok(metadata(&open_readonly(path)?)?.is_some())
}

pub(crate) fn is_staging(path: &Path) -> Result<bool> {
    if !path.is_file() {
        return Ok(false);
    }
    is_staging_connection(&open_readonly(path)?)
}

pub(crate) fn is_staging_connection(conn: &Connection) -> Result<bool> {
    Ok(metadata(conn)?.is_some_and(|m| m.staging))
}

/// Prepare a fresh linked checkout. Existing standalone databases are preserved,
/// including their compatibility with the parent version of tsindex.
pub(crate) fn prepare(checkout: &Checkout, runtime: &Runtime) -> Result<()> {
    fs::create_dir_all(runtime.db_path.parent().context("index has no parent")?)?;
    // First-use preparation and every build share this lease. A competing
    // opener can observe an unready database, but cannot build it while its
    // creator is still adding the overlay metadata and private table names.
    let _lease = crate::maintenance::FileLease::acquire(
        crate::maintenance::writer_lease_path(&runtime.db_path),
        Duration::from_secs(30),
    )?;
    if runtime.db_path.exists() {
        return Ok(());
    }
    if !single_checkout(runtime)? {
        return initialize_standalone(runtime, "overlay storage requires one checkout repository");
    }
    let source_path = db_path(&checkout.main_root);
    if source_path == runtime.db_path {
        return initialize_standalone(runtime, "this checkout is the primary index");
    }
    if !source_path.is_file() {
        return initialize_standalone(runtime, "the primary index is unavailable");
    }
    // A repository can move between discovery and first use. Never seed it
    // from an unrelated directory that happens to have the old pathname.
    if common_dir(&checkout.main_root).as_ref() != Some(&checkout.common_dir)
        || common_dir(&checkout.path).as_ref() != Some(&checkout.common_dir)
    {
        return initialize_standalone(
            runtime,
            "the Git repository identity changed during discovery",
        );
    }
    let directory = checkout.main_root.join(".tsindex/bases");
    fs::create_dir_all(&directory)?;
    let base = directory.join(format!("extraction-v{SCHEMA_VERSION}.db"));
    if !base.is_file() {
        let source = open_readonly(&source_path)?;
        let version: i64 = source.pragma_query_value(None, "user_version", |r| r.get(0))?;
        let ready = source
            .query_row("SELECT ready FROM index_state WHERE id = 1", [], |r| {
                r.get::<_, bool>(0)
            })
            .unwrap_or(false);
        if version != SCHEMA_VERSION {
            return initialize_standalone(runtime, "the primary extraction schema is incompatible");
        }
        if !ready {
            return initialize_standalone(runtime, "the primary index is not ready");
        }
        let scratch = Scratch::new(&base)?;
        let mut destination = Connection::open(&scratch.path)?;
        copy_database(&source, &mut destination, 256, Duration::from_secs(30))?;
        destination.execute_batch("PRAGMA journal_mode = DELETE;")?;
        destination.close().map_err(|(_, e)| e)?;
        // A pathname can be deleted and reused while older overlays still pin
        // its previous generation. Stamp the completed snapshot with its own
        // content identity before publication, then pin this value in every
        // overlay rather than trusting the reusable extraction-version name.
        let identity = snapshot_identity(&scratch.path)?;
        let destination = Connection::open(&scratch.path)?;
        destination.execute_batch(
            "CREATE TABLE overlay_base_identity (
               id INTEGER PRIMARY KEY CHECK(id = 1), identity TEXT NOT NULL
             );",
        )?;
        destination.execute(
            "INSERT INTO overlay_base_identity VALUES(1, ?1)",
            [identity],
        )?;
        destination.close().map_err(|(_, e)| e)?;
        // The hard link publishes our immutable completed copy, never the
        // mutable source database. Concurrent creators may safely lose here.
        match fs::hard_link(&scratch.path, &base) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
    let base_conn = open_readonly(&base)?;
    let version: i64 = base_conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
    if version != SCHEMA_VERSION {
        return initialize_standalone(runtime, "the shared base extraction schema is incompatible");
    }
    let Some(identity) = recorded_base_identity(&base_conn, false)? else {
        // Never upgrade a shared snapshot in place: an older base may still
        // have readers in other processes. Fresh checkouts can safely build
        // standalone until a compatible immutable generation is available.
        return initialize_standalone(runtime, "the shared base has no immutable identity");
    };
    let root = checkout.main_root.canonicalize()?;
    let repo_id = base_conn
        .prepare("SELECT id, root_path FROM repos")?
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .find_map(|(id, path)| {
            (Path::new(&path).canonicalize().ok().as_ref() == Some(&root)).then_some(id)
        });
    let Some(repo_id) = repo_id else {
        return initialize_standalone(runtime, "the shared base does not contain this repository");
    };
    runtime.initialize()?;
    let mut conn = Connection::open(&runtime.db_path)?;
    let transaction = conn.transaction()?;
    transaction.execute_batch(
        "CREATE TABLE overlay_meta (
           id INTEGER PRIMARY KEY CHECK(id = 1), base_path TEXT NOT NULL,
           base_repo_id INTEGER NOT NULL, staging INTEGER NOT NULL DEFAULT 0,
           base_identity TEXT NOT NULL
         );
         CREATE TABLE overlay_state (
           path TEXT PRIMARY KEY, mtime_ns INTEGER NOT NULL,
           byte_size INTEGER NOT NULL, hidden INTEGER NOT NULL DEFAULT 0
         );",
    )?;
    transaction.execute(
        "INSERT INTO overlay_meta(id, base_path, base_repo_id, base_identity) VALUES(1, ?1, ?2, ?3)",
        params![base.to_string_lossy(), repo_id, identity],
    )?;
    transaction.pragma_update(None, "user_version", OVERLAY_FORMAT_VERSION)?;
    private_tables(&transaction, false)?;
    transaction.commit()?;
    attach_views(&conn)?;
    rebuild_file_directory(&conn)?;
    Ok(())
}

fn initialize_standalone(runtime: &Runtime, reason: &str) -> Result<()> {
    // Only fresh preparation uses this path. Existing cache opens return
    // before it, so a storage fallback is visible once without request spam.
    eprintln!(
        "note: using standalone index for {}: {reason}",
        runtime.root.display()
    );
    runtime.initialize()
}

fn file_uri(path: &Path) -> Result<String> {
    let path = path.to_str().context("index path is not UTF-8")?;
    let mut uri = String::from("file:");
    for byte in path.bytes() {
        if byte.is_ascii_alphanumeric() || b"/-._~:".contains(&byte) {
            uri.push(byte as char);
        } else {
            use std::fmt::Write;
            write!(uri, "%{byte:02X}")?;
        }
    }
    uri.push_str("?mode=ro&immutable=1");
    Ok(uri)
}

fn snapshot_identity(path: &Path) -> Result<String> {
    let mut source = fs::File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0; 65_536];
    loop {
        let count = source.read(&mut buffer)?;
        if count == 0 {
            return Ok(format!("{:x}", digest.finalize()));
        }
        digest.update(&buffer[..count]);
    }
}

fn recorded_base_identity(conn: &Connection, attached: bool) -> Result<Option<String>> {
    let (exists, identity) = if attached {
        (
            "SELECT EXISTS(SELECT 1 FROM overlay_base.sqlite_master WHERE type='table' AND name='overlay_base_identity')",
            "SELECT identity FROM overlay_base.overlay_base_identity WHERE id=1",
        )
    } else {
        (
            "SELECT EXISTS(SELECT 1 FROM main.sqlite_master WHERE type='table' AND name='overlay_base_identity')",
            "SELECT identity FROM main.overlay_base_identity WHERE id=1",
        )
    };
    if !conn.query_row(exists, [], |row| row.get::<_, bool>(0))? {
        return Ok(None);
    }
    Ok(conn.query_row(identity, [], |row| row.get(0)).optional()?)
}

fn validate_base_identity(conn: &Connection, meta: &Metadata, attached: bool) -> Result<()> {
    if meta.identity.is_none() || recorded_base_identity(conn, attached)? != meta.identity {
        bail!("overlay base identity changed or is unavailable; rebuild the checkout index");
    }
    Ok(())
}

pub(crate) fn attach_views(conn: &Connection) -> Result<bool> {
    let Some(meta) = metadata(conn)? else {
        return Ok(false);
    };
    validate_format(conn)?;
    let repositories: i64 = conn.query_row("SELECT COUNT(*) FROM main.repos", [], |r| r.get(0))?;
    if repositories != 1 {
        bail!("checkout overlays require exactly one repository; rebuild as a standalone catalog");
    }
    // SQLite parameters bind the filename. The only formatted SQL value below
    // is the checked integer repository id, never a caller-provided identifier.
    conn.execute(
        "ATTACH DATABASE ?1 AS overlay_base",
        [file_uri(&meta.base)?],
    )?;
    let version: i64 = conn.pragma_query_value(
        Some(rusqlite::DatabaseName::Attached("overlay_base")),
        "user_version",
        |r| r.get(0),
    )?;
    if version != SCHEMA_VERSION {
        bail!("overlay base has incompatible extraction schema; refresh the checkout");
    }
    validate_base_identity(conn, &meta, true)?;
    // Stages use the ordinary writer schema; published databases deliberately
    // hide these tables from binaries that do not understand merged views.
    let (files, symbols, refs) = if meta.staging {
        ("files", "symbols", "refs")
    } else {
        ("overlay_files", "overlay_symbols", "overlay_refs")
    };
    // Facts join their immutable file directly so a selective name lookup can
    // use its name index before checking visibility. Joining the merged files
    // view here would build the full file catalog again inside each fact arm.
    let visible_base_file = format!(
        "b.repo_id = {} AND COALESCE(o.hidden, 0) = 0
         AND NOT EXISTS(SELECT 1 FROM main.{files} p WHERE p.repo_id = r.id AND p.path = b.path)",
        meta.repo_id
    );
    // This is a derived index, not a second source of extraction truth. An
    // older binary can still publish facts without maintaining the directory;
    // its generation change makes readers use the original merged view until
    // the next writer rebuilds the directory inside a private stage.
    let effective_files = if file_directory_is_current(conn)? {
        "SELECT id, repo_id, path, language, sha, mtime_ns, byte_size, partial
         FROM main.overlay_file_directory"
            .to_owned()
    } else {
        format!(
            "SELECT * FROM main.{files}
             UNION ALL
             SELECT -b.id, r.id, b.path, b.language, b.sha,
                    COALESCE(o.mtime_ns, 0), COALESCE(o.byte_size, b.byte_size), b.partial
             FROM overlay_base.files b CROSS JOIN main.repos r
             LEFT JOIN main.overlay_state o ON o.path = b.path
             WHERE {visible_base_file}"
        )
    };
    let sql = format!(
        "CREATE TEMP VIEW files AS {effective_files};
         CREATE TEMP VIEW symbols AS
           SELECT * FROM main.{symbols}
           UNION ALL
           SELECT -s.id, -s.file_id, s.kind, s.name, s.qualified,
                  s.start_row, s.start_col, s.end_row, s.end_col, s.signature, s.docstring
           FROM overlay_base.symbols s JOIN overlay_base.files b ON b.id = s.file_id
           CROSS JOIN main.repos r
           LEFT JOIN main.overlay_state o ON o.path = b.path
           WHERE {visible_base_file};
         CREATE TEMP VIEW refs AS
           SELECT * FROM main.{refs}
           UNION ALL
           SELECT -s.id, -s.file_id, s.name, s.start_row, s.start_col,
                  s.end_row, s.end_col, s.context
           FROM overlay_base.refs s JOIN overlay_base.files b ON b.id = s.file_id
           CROSS JOIN main.repos r
           LEFT JOIN main.overlay_state o ON o.path = b.path
           WHERE {visible_base_file};"
    );
    conn.execute_batch(&sql)?;
    Ok(true)
}

fn file_directory_is_current(conn: &Connection) -> Result<bool> {
    if !table_exists(conn, "overlay_file_directory")?
        || !table_exists(conn, "overlay_directory_state")?
    {
        return Ok(false);
    }
    Ok(conn
        .query_row(
            "SELECT d.version = 1 AND d.generation = i.generation
             FROM main.overlay_directory_state d JOIN main.index_state i ON i.id=d.id
             WHERE d.id=1",
            [],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(false))
}

fn stamp_file_directory(conn: &Connection) -> Result<()> {
    conn.execute(
        "INSERT INTO main.overlay_directory_state(id, version, generation)
         SELECT 1, 1, generation FROM main.index_state WHERE id=1
         ON CONFLICT(id) DO UPDATE SET version=excluded.version, generation=excluded.generation",
        [],
    )?;
    Ok(())
}

/// The caller has already attached the authoritative merged view. Rebuild only
/// for old/mismatched derived state, retaining compatibility with existing
/// overlays and writers rather than requiring a new extraction format.
fn rebuild_file_directory(conn: &Connection) -> Result<bool> {
    if file_directory_is_current(conn)? {
        return Ok(false);
    }
    let transaction = conn.unchecked_transaction()?;
    transaction.execute_batch(
        "DROP TABLE IF EXISTS main.overlay_file_directory;
         DROP TABLE IF EXISTS main.overlay_directory_state;
         CREATE TABLE overlay_file_directory (
           id INTEGER PRIMARY KEY, repo_id INTEGER NOT NULL,
           path TEXT NOT NULL UNIQUE, language TEXT NOT NULL, sha TEXT NOT NULL,
           mtime_ns INTEGER NOT NULL, byte_size INTEGER NOT NULL, partial INTEGER NOT NULL,
           FOREIGN KEY(repo_id) REFERENCES repos(id) ON DELETE CASCADE
         );
         CREATE TABLE overlay_directory_state (
           id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL,
           generation INTEGER NOT NULL
         );
         INSERT INTO main.overlay_file_directory
         SELECT id, repo_id, path, language, sha, mtime_ns, byte_size, partial
         FROM temp.files ORDER BY id;",
    )?;
    stamp_file_directory(&transaction)?;
    transaction.commit()?;
    Ok(true)
}

pub(crate) fn touch_file_directory(
    conn: &Connection,
    path: &str,
    mtime_ns: i64,
    byte_size: i64,
) -> Result<()> {
    if table_exists(conn, "overlay_file_directory")? {
        conn.execute(
            "UPDATE main.overlay_file_directory SET mtime_ns=?2, byte_size=?3 WHERE path=?1",
            params![path, mtime_ns, byte_size],
        )?;
    }
    Ok(())
}

pub(crate) fn sync_private_file_directory(conn: &Connection, path: &str) -> Result<()> {
    if table_exists(conn, "overlay_file_directory")? {
        conn.execute(
            "INSERT INTO main.overlay_file_directory
             SELECT id, repo_id, path, language, sha, mtime_ns, byte_size, partial
             FROM main.files WHERE path=?1
             ON CONFLICT(path) DO UPDATE SET id=excluded.id, repo_id=excluded.repo_id,
             language=excluded.language, sha=excluded.sha, mtime_ns=excluded.mtime_ns,
             byte_size=excluded.byte_size, partial=excluded.partial",
            [path],
        )?;
    }
    Ok(())
}

fn private_tables(conn: &Connection, staging: bool) -> Result<()> {
    conn.execute_batch(if staging {
        "ALTER TABLE overlay_files RENAME TO files;
         ALTER TABLE overlay_symbols RENAME TO symbols;
         ALTER TABLE overlay_refs RENAME TO refs;"
    } else {
        "ALTER TABLE files RENAME TO overlay_files;
         ALTER TABLE symbols RENAME TO overlay_symbols;
         ALTER TABLE refs RENAME TO overlay_refs;"
    })?;
    Ok(())
}

pub(crate) fn observe_base_file(
    conn: &Connection,
    path: &str,
    mtime_ns: i64,
    byte_size: i64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO main.overlay_state(path, mtime_ns, byte_size, hidden)
         VALUES(?1, ?2, ?3, 0) ON CONFLICT(path) DO UPDATE SET
         mtime_ns=excluded.mtime_ns, byte_size=excluded.byte_size, hidden=0",
        params![path, mtime_ns, byte_size],
    )?;
    touch_file_directory(conn, path, mtime_ns, byte_size)?;
    Ok(())
}

pub(crate) fn reuse_base(
    conn: &Connection,
    path: &str,
    language: &str,
    sha: &str,
    mtime_ns: i64,
    byte_size: i64,
) -> Result<bool> {
    let Some(meta) = metadata(conn)? else {
        return Ok(false);
    };
    let same: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM overlay_base.files
         WHERE repo_id=?1 AND path=?2 AND language=?3 AND sha=?4)",
        params![meta.repo_id, path, language, sha],
        |r| r.get(0),
    )?;
    if same {
        restore_base(conn, path, mtime_ns, byte_size)?;
    }
    Ok(same)
}

pub(crate) fn restore_base(
    conn: &Connection,
    path: &str,
    mtime_ns: i64,
    byte_size: i64,
) -> Result<()> {
    conn.execute("DELETE FROM main.files WHERE path=?1", [path])?;
    observe_base_file(conn, path, mtime_ns, byte_size)?;
    if table_exists(conn, "overlay_file_directory")? {
        let meta = metadata(conn)?.context("file directory has no overlay base")?;
        conn.execute(
            "INSERT INTO main.overlay_file_directory
             SELECT -b.id, r.id, b.path, b.language, b.sha, ?3, ?4, b.partial
             FROM overlay_base.files b CROSS JOIN main.repos r WHERE b.repo_id=?1 AND b.path=?2
             ON CONFLICT(path) DO UPDATE SET id=excluded.id, repo_id=excluded.repo_id,
             language=excluded.language, sha=excluded.sha, mtime_ns=excluded.mtime_ns,
             byte_size=excluded.byte_size, partial=excluded.partial",
            params![meta.repo_id, path, mtime_ns, byte_size],
        )?;
    }
    Ok(())
}

pub(crate) fn base_file_states<'a>(
    conn: &Connection,
    paths: impl Iterator<Item = &'a str>,
) -> Result<HashMap<String, (String, String)>> {
    let Some(meta) = metadata(conn)? else {
        return Ok(HashMap::new());
    };
    let mut statement =
        conn.prepare("SELECT language, sha FROM overlay_base.files WHERE repo_id=?1 AND path=?2")?;
    let mut states = HashMap::new();
    for path in paths {
        if let Some(state) = statement
            .query_row(params![meta.repo_id, path], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()?
        {
            states.insert(path.to_owned(), state);
        }
    }
    Ok(states)
}

pub(crate) fn hide_path(conn: &Connection, path: &str) -> Result<()> {
    if metadata(conn)?.is_some() {
        conn.execute(
            "INSERT INTO main.overlay_state(path, mtime_ns, byte_size, hidden)
             VALUES(?1, 0, 0, 1) ON CONFLICT(path) DO UPDATE SET hidden=1",
            [path],
        )?;
        if table_exists(conn, "overlay_file_directory")? {
            conn.execute(
                "DELETE FROM main.overlay_file_directory WHERE path=?1",
                [path],
            )?;
        }
    }
    Ok(())
}

/// Recover base state for a restored file that is currently hidden from reads.
/// Its timestamp is poisoned so the worker verifies local content before reuse.
pub(crate) fn supplement_file_states(
    conn: &Connection,
    repo_id: i64,
    states: &mut HashMap<String, (String, String, i64, i64)>,
    paths: &[&str],
) -> Result<()> {
    let Some(meta) = metadata(conn)? else {
        return Ok(());
    };
    let _ = repo_id;
    let mut statement = conn.prepare(
        "SELECT language, sha, byte_size FROM overlay_base.files WHERE repo_id=?1 AND path=?2",
    )?;
    for path in paths {
        if states.contains_key(*path) {
            continue;
        }
        if let Some((lang, sha, size)) = statement
            .query_row(params![meta.repo_id, path], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .optional()?
        {
            // Zero is a valid filesystem mtime (UNIX_EPOCH). Keep an
            // unobserved restored path distinct so a hash match cannot report
            // Unchanged while its base rows are still hidden. The base-state
            // fallback reuses extraction; -1 remains the schema-stale marker.
            states.insert((*path).to_owned(), (lang, sha, -2, size));
        }
    }
    Ok(())
}

struct Scratch {
    path: PathBuf,
}

impl Scratch {
    fn new(destination: &Path) -> Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = destination.with_extension(format!(
            "overlay-stage-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        Ok(Self { path })
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
        for suffix in ["-wal", "-shm"] {
            let mut sidecar = self.path.as_os_str().to_os_string();
            sidecar.push(suffix);
            let _ = fs::remove_file(PathBuf::from(sidecar));
        }
    }
}

fn copy_database(
    source: &Connection,
    destination: &mut Connection,
    pages: i32,
    timeout: Duration,
) -> Result<()> {
    // The destination write transaction lasts from the first backup step until
    // SQLITE_DONE; dropping an incomplete backup rolls it back. This publishes
    // the entire thin generation atomically while existing WAL readers retain
    // their old snapshots: https://sqlite.org/c3ref/backup_finish.html
    let backup = Backup::new(source, destination)?;
    let started = Instant::now();
    loop {
        match backup.step(pages)? {
            StepResult::Done => return Ok(()),
            StepResult::Busy | StepResult::Locked => std::thread::sleep(Duration::from_millis(10)),
            _ => {}
        }
        if started.elapsed() >= timeout {
            bail!("overlay snapshot publication exceeded its timeout; retry shortly");
        }
    }
}

fn fingerprint(conn: &Connection) -> Result<Vec<u8>> {
    let mut digest = Sha256::new();
    for sql in [
        "SELECT ready FROM main.index_state WHERE id = 1",
        "SELECT id, name, root_path FROM main.repos ORDER BY id",
        "SELECT path, language, sha, mtime_ns, byte_size, partial FROM main.files ORDER BY path",
        "SELECT path, mtime_ns, byte_size, hidden FROM main.overlay_state ORDER BY path",
    ] {
        let mut statement = conn.prepare(sql)?;
        let count = statement.column_count();
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            for column in 0..count {
                digest.update(format!("{:?}\0", row.get_ref(column)?).as_bytes());
            }
        }
    }
    Ok(digest.finalize().to_vec())
}

fn validate_sources(conn: &Connection) -> Result<()> {
    let mut statement = conn.prepare(
        "SELECT f.path, f.mtime_ns, f.byte_size, f.sha, r.root_path
         FROM files f JOIN repos r ON r.id=f.repo_id",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let path: String = row.get(0)?;
        let modified: i64 = row.get(1)?;
        let size: i64 = row.get(2)?;
        let sha: String = row.get(3)?;
        let root: String = row.get(4)?;
        let absolute = Path::new(&root).join(&path);
        let observed = fs::metadata(&absolute)
            .with_context(|| format!("source changed while indexing {path}; retry shortly"))?;
        // Match ingestion's representation for pre-epoch or unavailable
        // timestamps. Zero cannot prove identity, so the SHA check below still
        // validates the source bytes before this generation is published.
        let timestamp = observed
            .modified()
            .ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|duration| duration.as_nanos() as i64)
            .unwrap_or_default();
        if timestamp != modified || observed.len() as i64 != size {
            bail!("source changed while indexing {path}; retry shortly");
        }
        // Checkout timestamps are not identities: agents can replace content
        // while preserving both mtime and size. Validate the bytes belonging to
        // every published extraction before making this generation visible.
        let bytes = fs::read(&absolute)?;
        let actual = format!("{:x}", Sha256::digest(&bytes));
        if actual != sha {
            bail!("source changed while indexing {path}; retry shortly");
        }
    }
    Ok(())
}

/// The caller owns the checkout's cross-process writer lease for this entire
/// read-stage-parse-validate-publish cycle. Other checkout leases are unrelated.
pub(crate) fn refresh(
    runtime: &Runtime,
    incremental: bool,
    only_repo: Option<&str>,
) -> Result<BuildStats> {
    // Validate scoped selectors before widening publication to the complete
    // configured checkout. Unknown selectors must retain their normal error
    // instead of being silently discarded by a cold-recovery fallback.
    for language in &runtime.languages {
        if crate::lang::lookup_language(language).is_none() {
            bail!("unsupported language {language}");
        }
    }
    if let Some(name) = only_repo
        && !runtime
            .workspaces()?
            .iter()
            .any(|workspace| workspace.name == name)
    {
        bail!("repo {name} is not configured");
    }
    // Base masks use checkout-relative paths. A multi-repository catalog or an
    // outside root needs the ordinary materialized schema to keep identities
    // disjoint, so replace this derivation through the same atomic cold path.
    if !single_checkout(runtime)? || !published_repo_matches(runtime)? {
        eprintln!(
            "note: rebuilding {} as a standalone index: overlay repository configuration changed",
            runtime.root.display()
        );
        return recover(runtime);
    }
    let scratch = Scratch::new(&runtime.db_path)?;
    let mut staged = Connection::open(&scratch.path)?;
    let source = open_readonly(&runtime.db_path)?;
    copy_database(&source, &mut staged, 256, Duration::from_secs(30))?;
    let transaction = staged.transaction()?;
    transaction.execute("UPDATE overlay_meta SET staging=1 WHERE id=1", [])?;
    private_tables(&transaction, true)?;
    transaction.commit()?;
    let before = fingerprint(&staged)?;
    // A deleted, replaced or incompatible immutable base invalidates this
    // derivation. Rebuild into a separate standalone stage; the old published
    // bytes survive any failure and are replaced only after full validation.
    if let Err(error) = attach_views(&staged) {
        eprintln!("warning: rebuilding checkout after overlay base became unavailable: {error:#}");
        drop(staged);
        drop(source);
        return recover(runtime);
    }
    let directory_rebuilt = rebuild_file_directory(&staged)?;
    staged.close().map_err(|(_, e)| e)?;
    drop(source);

    let mut worker = runtime.clone();
    worker.db_path = scratch.path.clone();
    worker.private_stage = true;
    // Every visible base row needs a verified local identity before a complete
    // logical generation can be published. CLI language filters scope an
    // operation, whereas the configured language/ignore policy defines storage.
    worker.languages.clear();
    let stats = worker.build(incremental, only_repo)?;
    if stats.failed > 0 {
        // Leave the published logical generation intact on partial failure.
        // Callers still receive the failure count and retain their retry policy.
        return Ok(stats);
    }
    let staged = Connection::open(&scratch.path)?;
    // Facts, observations and the derived directory were maintained together
    // in the writer's private transactions. Stamp only the complete build;
    // failed stages never make a stale directory look current to readers.
    stamp_file_directory(&staged)?;
    attach_views(&staged)?;
    validate_sources(&staged)?;
    let changed = directory_rebuilt || fingerprint(&staged)? != before;
    if !changed {
        return Ok(stats);
    }
    staged.execute_batch("DROP VIEW temp.refs; DROP VIEW temp.symbols; DROP VIEW temp.files;")?;
    let transaction = staged.unchecked_transaction()?;
    transaction.execute("UPDATE main.overlay_meta SET staging=0 WHERE id=1", [])?;
    private_tables(&transaction, false)?;
    transaction.commit()?;
    let mut destination = Connection::open(&runtime.db_path)?;
    copy_database(&staged, &mut destination, 256, Duration::from_secs(30))?;
    Ok(stats)
}

fn recover(runtime: &Runtime) -> Result<BuildStats> {
    let scratch = Scratch::new(&runtime.db_path)?;
    let mut worker = runtime.clone();
    worker.db_path = scratch.path.clone();
    worker.private_stage = true;
    worker.languages.clear();
    // A partial repository build cannot replace a derivation whose base is
    // gone: every currently configured source must belong to the replacement.
    let stats = worker.build(false, None)?;
    if stats.failed > 0 {
        return Ok(stats);
    }
    let staged = read_connection(&scratch.path)?;
    validate_sources(&staged)?;
    let mut destination = Connection::open(&runtime.db_path)?;
    copy_database(&staged, &mut destination, 256, Duration::from_secs(30))?;
    Ok(stats)
}

fn single_checkout(runtime: &Runtime) -> Result<bool> {
    let workspaces = runtime.workspaces()?;
    let root = runtime.root.canonicalize()?;
    Ok(runtime.config.repos.len() <= 1
        && workspaces.len() == 1
        && workspaces[0].root.canonicalize()? == root)
}

fn published_repo_matches(runtime: &Runtime) -> Result<bool> {
    let conn = open_readonly(&runtime.db_path)?;
    let repositories = conn
        .prepare("SELECT name, root_path FROM main.repos")?
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let workspaces = runtime.workspaces()?;
    Ok(repositories.len() == 1
        && repositories[0].0 == workspaces[0].name
        && Path::new(&repositories[0].1).canonicalize()? == runtime.root.canonicalize()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn directory() -> Result<tempfile::TempDir> {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/overlay-publication");
        fs::create_dir_all(&root)?;
        Ok(tempfile::tempdir_in(root)?)
    }

    fn database(path: &Path, value: &str) -> Result<Connection> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE value(data TEXT)")?;
        conn.execute("INSERT INTO value VALUES(?1)", [value])?;
        Ok(conn)
    }

    #[test]
    fn publication_preserves_an_existing_reader_snapshot() -> Result<()> {
        let directory = directory()?;
        let path = directory.path().join("live.db");
        let mut live = database(&path, "old complete generation")?;
        let stage = database(
            &directory.path().join("stage.db"),
            "new complete generation",
        )?;
        let reader = open_readonly(&path)?;
        reader.execute_batch("BEGIN DEFERRED")?;
        assert_eq!(
            reader.query_row("SELECT data FROM value", [], |r| r.get::<_, String>(0))?,
            "old complete generation"
        );
        copy_database(&stage, &mut live, 256, Duration::from_secs(1))?;
        assert_eq!(
            reader.query_row("SELECT data FROM value", [], |r| r.get::<_, String>(0))?,
            "old complete generation"
        );
        reader.execute_batch("COMMIT")?;
        assert_eq!(
            reader.query_row("SELECT data FROM value", [], |r| r.get::<_, String>(0))?,
            "new complete generation"
        );
        Ok(())
    }

    #[test]
    fn interrupted_publication_rolls_back_the_entire_destination() -> Result<()> {
        let directory = directory()?;
        let mut live = database(&directory.path().join("live.db"), "old complete generation")?;
        let stage = database(
            &directory.path().join("stage.db"),
            "new complete generation",
        )?;
        stage.execute_batch(
            "CREATE TABLE padding(data BLOB); INSERT INTO padding VALUES(zeroblob(2000000))",
        )?;
        let error = copy_database(&stage, &mut live, 1, Duration::ZERO).unwrap_err();
        assert!(error.to_string().contains("timeout"), "{error:#}");
        assert_eq!(
            live.query_row("SELECT data FROM value", [], |r| r.get::<_, String>(0))?,
            "old complete generation"
        );
        assert!(!table_exists(&live, "padding")?);
        Ok(())
    }

    #[test]
    fn source_validation_rejects_changed_bytes_even_with_preserved_metadata() -> Result<()> {
        let directory = directory()?;
        let path = directory.path().join("source.py");
        let original = b"def original():\n    pass\n";
        fs::write(&path, original)?;
        let modified = fs::metadata(&path)?.modified()?;
        let timestamp = modified.duration_since(std::time::UNIX_EPOCH)?.as_nanos() as i64;
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            "CREATE TABLE repos(id INTEGER PRIMARY KEY, root_path TEXT);
             CREATE TABLE files(path TEXT, repo_id INTEGER, mtime_ns INTEGER,
               byte_size INTEGER, sha TEXT);",
        )?;
        conn.execute(
            "INSERT INTO repos VALUES(1, ?1)",
            [directory.path().to_string_lossy()],
        )?;
        conn.execute(
            "INSERT INTO files VALUES('source.py', 1, ?1, ?2, ?3)",
            params![
                timestamp,
                original.len(),
                format!("{:x}", Sha256::digest(original))
            ],
        )?;
        validate_sources(&conn)?;
        fs::write(&path, b"def replaced():\n    pass\n")?;
        fs::File::options()
            .write(true)
            .open(&path)?
            .set_times(fs::FileTimes::new().set_modified(modified))?;
        let error = validate_sources(&conn).unwrap_err();
        assert!(error.to_string().contains("source changed"), "{error:#}");
        Ok(())
    }
}
