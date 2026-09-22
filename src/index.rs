use std::collections::{HashMap, HashSet};
use std::fs;
#[cfg(test)]
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use globset::{Glob, GlobMatcher};
use ignore::WalkBuilder;
use notify_debouncer_full::DebounceEventResult;
use rayon::prelude::*;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use streaming_iterator::StreamingIterator;
use tree_sitter::{Node, Parser, Query, QueryCapture, QueryCursor};

use crate::config::{RepoConfig, ServerConfig, TsIndexConfig};
use crate::detect::detect_languages;
use crate::lang::{LanguageSpec, detect_language_from_file, lookup_language};
use crate::model::{
    EnclosingHit, EnclosingSymbolResponse, FileOutlineResponse, FileRefCount,
    FindReferencesResponse, GetSymbolResponse, IndexedSymbol, NameRefCount, OutlineSymbol,
    QueryCapture as RawQueryCapture, QueryResponse, RangePoint, RefCountsResponse, ReferenceGroup,
    ReferenceMatch, ReferenceRow, ReplaceSymbolResponse, RepoInfo, RepoLanguageReport, SourceRange,
    SymbolMatch,
};

#[derive(Debug, Clone)]
pub struct Runtime {
    pub root: PathBuf,
    pub db_path: PathBuf,
    pub config: TsIndexConfig,
    pub languages: Vec<String>,
    pub jobs: usize,
    /// Whether `build` may delete `repos` rows (and, by cascade, their files)
    /// for repos absent from the loaded config. Off when the CLI `--db` flag
    /// points at a database owned by a different catalog, so a one-off command
    /// against a shared index cannot wipe the other catalog's repos.
    pub prune_repos: bool,
    /// Set when the on-disk index predates the `files.partial` column and the
    /// additive read-path migration could not run (e.g. a read-only
    /// filesystem). Reads then project `0 AS partial` instead of `f.partial`
    /// so a v3 index stays readable; `partial` simply reports `false`.
    legacy_partial: std::cell::Cell<bool>,
}

#[derive(Debug, Clone)]
pub struct Workspace {
    pub name: String,
    pub root: PathBuf,
    pub languages: Vec<String>,
    pub ignore: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetSymbolArgs {
    /// A single symbol name. Optional when `names` is provided.
    #[serde(default)]
    pub name: Option<String>,
    /// Several symbol names to fetch in one call. Combined with `name`,
    /// deduped. Use this to batch lookups and save round-trips.
    #[serde(default)]
    pub names: Option<Vec<String>>,
    #[serde(default)]
    pub repo: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub file_glob: Option<String>,
    #[serde(default)]
    pub include_body: bool,
    #[serde(default)]
    pub context_lines: usize,
    /// Keep this many source lines per returned body. When eliding, one
    /// additional marker line reports the true count. Omit to use the server's
    /// `default_body_line_limit`; explicit values are clamped to
    /// `max_body_line_limit`.
    #[serde(default)]
    pub max_body_lines: Option<usize>,
    /// Cap on the number of matches returned per name before truncation.
    /// Defaults to the server config's `default_result_limit`, clamped to
    /// `max_result_limit`.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Stable pagination offset — skip this many matches (per name, in
    /// `name, repo, file, row, col` order) before emitting. `next_offset`
    /// in the response carries the value to resume with.
    #[serde(default)]
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutlineArgs {
    pub file: String,
    #[serde(default)]
    pub repo: Option<String>,
    #[serde(default = "default_outline_depth")]
    pub depth: usize,
    #[serde(default = "default_true")]
    pub include_signatures: bool,
    #[serde(default = "default_true")]
    pub include_docstrings: bool,
    /// Include `import`/`use` rows in the outline. Off by default: they are
    /// rarely useful for orientation and dominate the payload (their `name`
    /// and `signature` are identical full lines). See issue #38.
    #[serde(default)]
    pub include_imports: bool,
    /// Names whose full body to inline into the outline. Lets one call both
    /// orient (structure) and fetch selected bodies — saving a round-trip vs
    /// an outline followed by separate get_symbol calls.
    #[serde(default)]
    pub include_bodies_for: Option<Vec<String>>,
    /// Keep this many source lines per inlined body. When eliding, one
    /// additional marker line reports the true count. Omit to use the server's
    /// `default_body_line_limit`; explicit values are clamped to
    /// `max_body_line_limit`.
    #[serde(default)]
    pub max_body_lines: Option<usize>,
    /// Cap on the number of top-level outline symbols returned before
    /// truncation. Defaults to the server config's `default_result_limit`,
    /// clamped to `max_result_limit`.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Stable pagination offset over the top-level symbol list.
    #[serde(default)]
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FindReferencesArgs {
    /// A single identifier, as a convenience for in-process Rust callers.
    /// Combined with `names` and deduped.
    ///
    /// NOT part of the MCP input contract: `schemas/find_references.json`
    /// requires `names`, and `tools/call` rejects `name` outright rather than
    /// carrying two spellings of one argument into tool-calling clients. Reach
    /// for `names` unless you are calling [`Runtime::find_references`] directly.
    #[serde(default)]
    pub name: Option<String>,
    /// One or more identifiers to find references for in ONE call — the
    /// canonical input, and the only one MCP accepts. Each reference is tagged
    /// with its `name` so you can tell which symbol it belongs to.
    #[serde(default)]
    pub names: Option<Vec<String>>,
    #[serde(default)]
    pub repo: Option<String>,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub exclude_tests: bool,
    /// Include identifier occurrences that are declaration names. Off by
    /// default because callers usually want uses/call sites, not the defining
    /// declaration echoed back as a "reference".
    #[serde(default)]
    pub include_declarations: bool,
    #[serde(default = "default_true")]
    pub group_by_file: bool,
    #[serde(default = "default_snippet_lines")]
    pub snippet_lines: usize,
    /// Cap on the number of references returned before truncation. Defaults
    /// to the server config's `default_result_limit`, clamped to
    /// `max_result_limit`.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Stable pagination offset over the reference list (in `name, repo,
    /// file, row, col` order).
    #[serde(default)]
    pub offset: Option<usize>,
    /// Aggregate-only mode: return exact per-(repo, file) reference counts and
    /// the overall total WITHOUT any per-reference rows, snippets, or
    /// pagination. Use it for "how many / which files" questions — `total`,
    /// `total_files`, and `names` are always exact (computed over the full
    /// filtered set in one pass) and the response is a fraction of the
    /// row-level payload. The `files` array is capped by the byte budget
    /// (top-N by count, `files_truncated` set) so a many-file repo can't
    /// overflow. When set, `limit`, `offset`, `group_by_file`, and
    /// `snippet_lines` are ignored.
    #[serde(default)]
    pub counts_only: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryArgs {
    pub language: String,
    #[serde(default)]
    pub repo: Option<String>,
    pub query: String,
    #[serde(default)]
    pub file_glob: Option<String>,
    #[serde(default)]
    pub capture: Option<String>,
    /// Cap on the number of captures returned before truncation. Defaults
    /// to the server config's `default_result_limit`, clamped to
    /// `max_result_limit`.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Stable pagination offset over the capture stream.
    #[serde(default)]
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnclosingSymbolArgs {
    /// Repository-relative file path (matches `files.path`).
    pub file: String,
    /// 1-based row to look up. Optional when `rows` is provided.
    #[serde(default)]
    pub row: Option<usize>,
    /// Several 1-based rows in the same file to look up in ONE call — e.g.
    /// every line of a stack trace or diff hunk. Results come back per row.
    /// Row-only (no column); use `row`+`col` for a point-precise single lookup.
    #[serde(default)]
    pub rows: Option<Vec<usize>>,
    /// Optional 0-based column. When omitted the lookup is row-only;
    /// when provided it narrows symbols to those whose range contains
    /// the exact `(row, col)` point. Applies to the single-`row` path only.
    #[serde(default)]
    pub col: Option<usize>,
    #[serde(default)]
    pub repo: Option<String>,
    #[serde(default)]
    pub include_body: bool,
    #[serde(default)]
    pub context_lines: usize,
    /// How many enclosing levels to return. `1` (default) is the
    /// innermost symbol; `2` adds its parent; etc. The response is
    /// ordered outermost → innermost so callers can read it as a
    /// "module → class → method" trail.
    #[serde(default = "default_enclosing_depth")]
    pub depth: usize,
    /// Keep this many source lines per returned body. When eliding, one
    /// additional marker line reports the true count. Omit to use the server's
    /// `default_body_line_limit`; explicit values are clamped to
    /// `max_body_line_limit`.
    #[serde(default)]
    pub max_body_lines: Option<usize>,
    /// Cap on how many `rows` are resolved in one call (batch path only).
    /// Defaults to the server `default_result_limit`, clamped to
    /// `max_result_limit`. The effective page size may be smaller than `limit`
    /// because of the server's serialized-byte budget (`max_response_chars`),
    /// which shrinks a page that would otherwise exceed the client's
    /// tool-result cap. Always follow `next_offset` to resume rather than
    /// assuming you received `limit` results.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Stable pagination offset over the requested `rows` list (batch path
    /// only) — skip this many rows before resolving. Use `next_offset` from
    /// the response to resume.
    #[serde(default)]
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplaceSymbolArgs {
    pub file: String,
    pub name: String,
    pub new_body: String,
    #[serde(default)]
    pub repo: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    /// Dotted container path as reported by get_symbol's `qualified` field
    /// (e.g. `Outer.method`). Disambiguates same-name, same-kind symbols in
    /// different containers, which `kind` cannot.
    #[serde(default)]
    pub qualified: Option<String>,
    /// 1-based start row of the intended symbol, as reported in `range[0]`.
    /// Disambiguates duplicates that `qualified` cannot (e.g. two
    /// `#ifdef`-guarded definitions of the same C function in one file).
    #[serde(default)]
    pub row: Option<usize>,
}

fn default_outline_depth() -> usize {
    2
}

fn default_true() -> bool {
    true
}

fn default_snippet_lines() -> usize {
    1
}

fn default_enclosing_depth() -> usize {
    1
}

/// Resolve a caller-supplied `limit` against the server config: use the
/// configured default when `None`, then clamp to the configured maximum so a
/// single request can never bypass the global ceiling. A floor of 1 keeps the
/// result meaningful. Used by every read endpoint so the clamp lives in one
/// place rather than being re-derived at each call site.
fn resolve_limit(config: &ServerConfig, requested: Option<usize>) -> usize {
    let base = requested.unwrap_or(config.default_result_limit);
    base.clamp(1, config.max_result_limit.max(1))
}

fn resolve_body_limit(config: &ServerConfig, requested: Option<usize>) -> Option<usize> {
    Some(
        requested
            .unwrap_or(config.default_body_line_limit)
            .clamp(1, config.max_body_line_limit.max(1)),
    )
}

// Bumping SCHEMA_VERSION declares "data captured by older binaries is
// stale and should be rebuilt." It does not necessarily change SQL DDL —
// when an extractor change (e.g. expanding ranges to include decorators)
// alters what gets stored, the version bumps so existing indexes get a
// "run `tsindex build` to refresh" warning rather than silent drift.
const SCHEMA_VERSION: i64 = 5;

impl Runtime {
    pub fn new(
        root: PathBuf,
        db_path: PathBuf,
        config: TsIndexConfig,
        languages: Vec<String>,
    ) -> Self {
        Self {
            root,
            db_path,
            config,
            languages,
            jobs: 1,
            prune_repos: true,
            legacy_partial: std::cell::Cell::new(false),
        }
    }

    pub fn with_jobs(mut self, jobs: usize) -> Self {
        self.jobs = jobs;
        self
    }

    pub fn with_repo_pruning(mut self, prune: bool) -> Self {
        self.prune_repos = prune;
        self
    }

    pub fn initialize(&self) -> Result<()> {
        let parent = self
            .db_path
            .parent()
            .ok_or_else(|| anyhow!("invalid database path {}", self.db_path.display()))?;
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
        let conn = Connection::open(&self.db_path)
            .with_context(|| format!("failed to open {}", self.db_path.display()))?;
        create_schema(&conn, &self.workspaces()?)?;
        Ok(())
    }

    pub fn build(&self, incremental: bool, only_repo: Option<&str>) -> Result<BuildStats> {
        self.initialize()?;
        let mut conn = Connection::open(&self.db_path)
            .with_context(|| format!("failed to open {}", self.db_path.display()))?;
        let all_workspaces = self.workspaces()?;
        create_schema(&conn, &all_workspaces)?;
        if self.prune_repos && only_repo.is_none() {
            prune_missing_repos(&conn, &self.configured_repo_names())?;
        }
        mark_index_refreshing(&conn)?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;

        let result = self.build_with_conn(&mut conn, all_workspaces, incremental, only_repo);
        if result.is_err() {
            // Do not leave `refreshing = 1` behind on a failed build; `ready`
            // is left as-is (a fresh DB stays not-ready, an existing index
            // keeps serving its last complete state).
            let _ = conn.execute("UPDATE index_state SET refreshing = 0 WHERE id = 1", []);
        }
        result
    }

    fn build_with_conn(
        &self,
        conn: &mut Connection,
        all_workspaces: Vec<Workspace>,
        incremental: bool,
        only_repo: Option<&str>,
    ) -> Result<BuildStats> {
        let workspaces: Vec<_> = match only_repo {
            Some(name) => {
                let filtered: Vec<_> = all_workspaces
                    .into_iter()
                    .filter(|w| w.name == name)
                    .collect();
                if filtered.is_empty() {
                    return Err(anyhow!("repo {} is not configured", name));
                }
                filtered
            }
            None => all_workspaces,
        };

        let mut active_paths: HashMap<i64, HashSet<String>> = HashMap::new();
        let mut purge_languages: HashMap<i64, Option<HashSet<String>>> = HashMap::new();
        let mut repo_roots: HashMap<i64, PathBuf> = HashMap::new();
        let has_cli_language_filter = !self.languages.is_empty();

        eprintln!("indexing {} repos", workspaces.len());

        let thread_count = effective_thread_count(self.jobs);
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(thread_count)
            .build()
            .context("failed to create thread pool")?;

        let mut processed_so_far: usize = 0;
        let mut stats = BuildStats::default();
        // Stream files through a fixed-size buffer instead of collecting every
        // job for the whole workspace set up front. The buffer can span repo
        // boundaries; each `FileJob` carries its own `repo_id`, and the batch
        // helpers group by repo where it matters.
        let mut batch: Vec<FileJob> = Vec::with_capacity(BUILD_CHUNK_SIZE);
        for workspace in &workspaces {
            let repo_id = repo_id_by_name(conn, &workspace.name)?
                .ok_or_else(|| anyhow!("repo {} is not registered", workspace.name))?;
            let allowed = self.allowed_languages(workspace)?;
            active_paths.entry(repo_id).or_default();
            purge_languages.insert(
                repo_id,
                has_cli_language_filter.then(|| allowed.keys().cloned().collect()),
            );
            repo_roots.insert(repo_id, workspace.root.clone());

            // Consuming the walker's Vec by value frees each PathBuf as it is
            // turned into a job rather than holding the whole repo's paths.
            for path in self.walk_source_files(workspace)? {
                let Some(language_id) = detect_language_from_file(&workspace.root, &path)? else {
                    continue;
                };
                if !allowed.is_empty() && !allowed.contains_key(language_id) {
                    continue;
                }
                let Some(spec) = allowed.get(language_id).cloned() else {
                    continue;
                };
                let relative = relative_path(&workspace.root, &path);
                batch.push(FileJob {
                    repo_id,
                    workspace_name: workspace.name.clone(),
                    path,
                    relative,
                    spec,
                });
                if batch.len() >= BUILD_CHUNK_SIZE {
                    process_batch(
                        conn,
                        &pool,
                        &batch,
                        incremental,
                        &mut stats,
                        &mut active_paths,
                    )?;
                    processed_so_far += batch.len();
                    eprintln!(
                        "  [{}] indexed={} skipped={} failed={}",
                        processed_so_far, stats.indexed, stats.skipped, stats.failed
                    );
                    batch.clear();
                }
            }
        }
        // Flush the trailing partial batch.
        if !batch.is_empty() {
            process_batch(
                conn,
                &pool,
                &batch,
                incremental,
                &mut stats,
                &mut active_paths,
            )?;
            processed_so_far += batch.len();
            eprintln!(
                "  [{}] indexed={} skipped={} failed={}",
                processed_so_far, stats.indexed, stats.skipped, stats.failed
            );
        }

        purge_stale_files(conn, &active_paths, &purge_languages, &repo_roots)?;
        mark_index_ready(conn)?;
        if only_repo.is_none() && !has_cli_language_filter {
            // Every file has now been (re-)extracted by this binary, so the
            // on-disk data matches SCHEMA_VERSION. Stamping here rather than
            // in `create_schema` keeps the stale-schema warning alive until a
            // rebuild has actually happened. A `--repo` or `--languages`
            // scoped build leaves other rows untouched, so it must not stamp.
            set_user_version(conn, SCHEMA_VERSION)?;
        }
        eprintln!(
            "build complete: {} indexed, {} skipped, {} failed",
            stats.indexed, stats.skipped, stats.failed
        );
        Ok(stats)
    }

    /// Incrementally update the index for an explicit set of changed paths.
    ///
    /// This is the targeted counterpart to [`build`]: instead of walking every
    /// file in every repo to discover what changed, it processes only the paths
    /// reported by the filesystem watcher. For each changed path it either
    /// re-indexes the file (created/modified) or deletes its rows (removed).
    ///
    /// Ignore rules are still honored exactly as a full build would: rather than
    /// reimplementing `.gitignore`/`.tsindexignore` matching, it runs the same
    /// `ignore::WalkBuilder` rooted at the workspace, but prunes the traversal
    /// with `filter_entry` so it only descends into the directories on the path
    /// to a changed file. On a large repo this visits a handful of directories
    /// instead of tens of thousands of files, while producing byte-for-byte the
    /// same include/exclude decisions as `walk_source_files`.
    ///
    /// Paths outside every configured workspace are ignored. Returns the same
    /// `BuildStats` shape as `build` (indexed / skipped / failed counts; deleted
    /// files are not counted in any of those fields).
    pub fn update_paths(&self, changed: &[PathBuf]) -> Result<BuildStats> {
        self.initialize()?;
        self.update_paths_cached(changed, &mut None)
    }

    /// Internal counterpart to [`update_paths`] that accepts an optional
    /// per-workspace language-allowlist memo. The watch loop owns this cache so
    /// that, when no explicit language filter is configured, it computes the
    /// `detect_languages` full-tree walk for a workspace at most once across the
    /// lifetime of the watch session instead of on every debounced event batch.
    fn update_paths_cached(
        &self,
        changed: &[PathBuf],
        language_cache: &mut Option<HashMap<String, HashMap<String, LanguageSpec>>>,
    ) -> Result<BuildStats> {
        // Schema bootstrap happens once in `watch()`/`update_paths`, not per
        // batch: `create_schema` upserts repos and stamps pragmas, which would
        // take a write lock on every debounced event even when nothing changed.
        let mut conn = Connection::open(&self.db_path)
            .with_context(|| format!("failed to open {}", self.db_path.display()))?;
        let workspaces = self.workspaces()?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        // `delete_file_rows_or_prefix` relies on `ON DELETE CASCADE`; the
        // pragma is per-connection, so set it here rather than trusting the
        // `bundled` SQLite default.
        conn.pragma_update(None, "foreign_keys", "ON")?;

        // Group the changed paths by the workspace that contains them. A path
        // can belong to at most one workspace (the deepest-rooted match wins, so
        // nested catalogs behave sensibly).
        let mut by_workspace: HashMap<usize, Vec<PathBuf>> = HashMap::new();
        for path in changed {
            let mut absolute = if path.is_absolute() {
                path.clone()
            } else {
                self.root.join(path)
            };
            // An edited ignore file changes which of its siblings/descendants
            // are indexable; re-evaluate the whole directory it governs.
            if absolute
                .file_name()
                .is_some_and(|name| name == ".gitignore" || name == ".tsindexignore")
                && let Some(parent) = absolute.parent()
            {
                absolute = parent.to_path_buf();
            }
            if let Some(idx) = best_workspace_for_path(&workspaces, &absolute) {
                by_workspace.entry(idx).or_default().push(absolute);
            }
        }

        let mut stats = BuildStats::default();
        let mut touched = 0usize;
        let mut deleted = 0usize;

        for (idx, paths) in by_workspace {
            let workspace = &workspaces[idx];
            let repo_id = match repo_id_by_name(&conn, &workspace.name)? {
                Some(id) => id,
                None => {
                    // A nested repo whose clone was missing when the watcher
                    // started was skipped by `create_schema`; register it now
                    // that files under it arrive instead of failing the batch.
                    sync_repos(&conn, std::slice::from_ref(workspace))?;
                    repo_id_by_name(&conn, &workspace.name)?
                        .ok_or_else(|| anyhow!("repo {} is not registered", workspace.name))?
                }
            };
            // Reuse a previously computed allowlist for this workspace when a
            // cache is supplied (watch loop); otherwise compute it directly. The
            // no-explicit-language path runs a full-tree `detect_languages` walk,
            // so caching it avoids re-walking the repo on every event batch.
            let allowed = match language_cache {
                Some(cache) => {
                    if let Some(cached) = cache.get(&workspace.name) {
                        cached.clone()
                    } else {
                        let computed = self.allowed_languages(workspace)?;
                        cache.insert(workspace.name.clone(), computed.clone());
                        computed
                    }
                }
                None => self.allowed_languages(workspace)?,
            };

            // Resolve the changed paths against the same ignore-aware walk a full
            // build would use, restricted to the changed files. Anything the walk
            // does not yield (ignored, or no longer present on disk) is treated as
            // a deletion candidate.
            let existing_jobs = self.resolve_changed_jobs(workspace, repo_id, &allowed, &paths)?;
            let existing = prefetch_file_state_for_chunk(&conn, &existing_jobs)?;
            let resolved: HashSet<String> =
                existing_jobs.iter().map(|j| j.relative.clone()).collect();

            // Do all file I/O and tree-sitter parsing *before* opening the write
            // transaction, mirroring `build()`. Holding an IMMEDIATE transaction
            // across parsing would keep the DB write lock for the duration of the
            // I/O, increasing contention and making `--jobs` ineffective.
            let thread_count = effective_thread_count(self.jobs);
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(thread_count)
                .build()
                .context("failed to create thread pool")?;
            // Only files the watcher named are force-hashed (a `touch` must be
            // caught). Files reached by expanding a directory event — the
            // whole repo after a root `.gitignore` edit — take the mtime/size
            // fast path so an ignore edit is not a full-repo re-hash.
            let named: HashSet<&Path> = paths.iter().map(PathBuf::as_path).collect();
            let results: Vec<ProcessedFile> = pool.install(|| {
                existing_jobs
                    .par_iter()
                    .map(|job| {
                        process_file_job(job, true, named.contains(job.path.as_path()), &existing)
                    })
                    .collect::<Result<Vec<_>>>()
            })?;

            // The paths reported as changed but not yielded by the ignore-aware
            // walk were deleted, or are now ignored / unreadable. Their rows
            // should not remain in the index. Treat a path as both an exact file
            // and a possible directory prefix so delete/rename events for
            // directories purge their children too.
            //
            // A still-present directory must NOT be a deletion candidate: it is
            // never itself an indexed row, and `resolved` only ever holds file
            // paths, so an unfiltered directory event would purge every indexed
            // file under its prefix even when all children are `Unchanged`. Only
            // treat a path as deleted when it no longer exists as a directory on
            // disk — a vanished file/dir (`is_dir()` false) is a real deletion,
            // and a still-present file that is no longer yielded by the walk
            // (e.g. newly ignored) is also correctly purged.
            //
            // Finally, only keep candidates that actually have rows in the index
            // (an exact path, or a directory prefix of some indexed path). A
            // per-candidate `path_has_indexed_rows` DB probe checks this, so an
            // ignored-but-never-indexed path (e.g. `target/`/`node_modules/`
            // churn) does not push `to_delete` non-empty and trigger a 0-row
            // write transaction — keeping such batches a true no-op.
            let mut to_delete: Vec<String> = Vec::new();
            for path in &paths {
                let relative = relative_path(&workspace.root, path);
                if path.is_dir() {
                    // A still-present directory was expanded by the walk above;
                    // any indexed file below it that the walk did not yield is
                    // now ignored or gone and must be purged.
                    for (indexed, language) in indexed_paths_under(&conn, repo_id, &relative)? {
                        // Mirror `purge_languages` in `build`: under a CLI
                        // `--languages` filter the walk never yields other
                        // languages, so their rows are out of scope, not stale.
                        if !self.languages.is_empty() && !allowed.contains_key(&language) {
                            continue;
                        }
                        if !resolved.contains(&indexed) {
                            to_delete.push(indexed);
                        }
                    }
                    continue;
                }
                if resolved.contains(&relative) {
                    continue;
                }
                if path_has_indexed_rows(&conn, repo_id, &relative)? {
                    to_delete.push(relative);
                }
            }

            // Partition parse results before touching the DB so we can decide
            // whether any write is actually needed.
            let mut to_upsert = Vec::new();
            for result in results {
                match result {
                    ProcessedFile::Indexed { .. } => to_upsert.push(result),
                    ProcessedFile::Unchanged { .. } => stats.skipped += 1,
                    ProcessedFile::ParseFailed {
                        workspace_name,
                        relative,
                        error,
                        ..
                    } => {
                        eprintln!("warning: failed to parse {workspace_name}:{relative}: {error}");
                        stats.failed += 1;
                    }
                    ProcessedFile::Unreadable {
                        workspace_name,
                        relative,
                        error,
                        ..
                    } => {
                        eprintln!("warning: skipped {workspace_name}:{relative}: {error}");
                        stats.failed += 1;
                    }
                    ProcessedFile::Binary => {}
                }
            }

            // Skip acquiring the IMMEDIATE write lock entirely when the batch
            // produced no deletes and no upserts (e.g. metadata-only events or
            // an all-`Unchanged`/`Binary` batch). Otherwise readers/writers
            // would be blocked on every debounced event for no reason.
            if to_delete.is_empty() && to_upsert.is_empty() {
                continue;
            }

            // Open the transaction only to apply the writes (deletes + upserts).
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

            for relative in &to_delete {
                deleted += delete_file_rows_or_prefix(&tx, repo_id, relative)?;
            }

            for result in to_upsert {
                if let ProcessedFile::Indexed {
                    repo_id,
                    relative,
                    spec_id,
                    sha,
                    mtime_ns,
                    byte_size,
                    parsed,
                } = result
                {
                    upsert_file(
                        &tx, repo_id, &relative, &spec_id, &sha, mtime_ns, byte_size, parsed,
                    )?;
                    stats.indexed += 1;
                    touched += 1;
                }
            }

            tx.commit()?;
        }

        eprintln!(
            "update complete: {} indexed, {} deleted, {} failed",
            touched, deleted, stats.failed
        );
        Ok(stats)
    }

    /// Resolve a set of changed paths within one workspace to `FileJob`s,
    /// applying the workspace's ignore rules and language filter. Uses the same
    /// `ignore::WalkBuilder` as `walk_source_files`, pruned via `filter_entry`
    /// so only the directories leading to a changed file are descended.
    fn resolve_changed_jobs(
        &self,
        workspace: &Workspace,
        repo_id: i64,
        allowed: &HashMap<String, LanguageSpec>,
        paths: &[PathBuf],
    ) -> Result<Vec<FileJob>> {
        // Set of changed files/dirs we care about, and the set of ancestor
        // directories we must descend into to reach them. Directory events are
        // expanded to all indexed source files below that directory.
        let targets: HashSet<PathBuf> = paths.iter().cloned().collect();
        let target_dirs: Vec<PathBuf> = targets
            .iter()
            .filter(|path| path.is_dir())
            .cloned()
            .collect();
        let mut needed_dirs: HashSet<PathBuf> = HashSet::new();
        for path in &targets {
            if path.is_dir() {
                needed_dirs.insert(path.clone());
            }
            let mut current = path.parent();
            while let Some(dir) = current {
                needed_dirs.insert(dir.to_path_buf());
                if dir == workspace.root {
                    break;
                }
                current = dir.parent();
            }
        }
        // Always allow the root itself so the walk can start.
        needed_dirs.insert(workspace.root.clone());

        let mut builder = WalkBuilder::new(&workspace.root);
        builder.hidden(false);
        builder.git_ignore(true);
        builder.git_exclude(true);
        builder.git_global(true);
        builder.add_custom_ignore_filename(".tsindexignore");
        for pattern in &workspace.ignore {
            builder.add_ignore(pattern);
        }
        let prune = needed_dirs.clone();
        let changed_dirs = target_dirs.clone();
        let foreign_roots = self.other_workspace_roots(workspace)?;
        builder.filter_entry(move |entry| {
            // Descend into directories on the path to a changed file, and into
            // changed directories themselves so directory events expand to all
            // source files below them. Always let files through so the ignore
            // engine still decides their fate. Never descend into another
            // configured repo's root: its files belong to that repo.
            if entry.file_type().is_some_and(|t| t.is_dir()) {
                !foreign_roots.contains(entry.path())
                    && (prune.contains(entry.path())
                        || changed_dirs.iter().any(|dir| entry.path().starts_with(dir)))
            } else {
                true
            }
        });

        let mut jobs = Vec::new();
        for result in builder.build() {
            let entry = match result {
                Ok(entry) => entry,
                Err(_) => continue,
            };
            let path = entry.path();
            if !path.is_file()
                || !(targets.contains(path) || target_dirs.iter().any(|dir| path.starts_with(dir)))
            {
                continue;
            }
            let Some(language_id) = detect_language_from_file(&workspace.root, path)? else {
                continue;
            };
            if !allowed.is_empty() && !allowed.contains_key(language_id) {
                continue;
            }
            let Some(spec) = allowed.get(language_id).cloned() else {
                continue;
            };
            jobs.push(FileJob {
                repo_id,
                workspace_name: workspace.name.clone(),
                path: path.to_path_buf(),
                relative: relative_path(&workspace.root, path),
                spec,
            });
        }
        Ok(jobs)
    }

    /// Continuously keep the index fresh.
    ///
    /// This is event-driven via the OS filesystem-notification backend
    /// (FSEvents on macOS, inotify on Linux, etc.) rather than polling. The
    /// previous implementation re-walked every repo tree once per second to
    /// fingerprint files; on a large multi-repo catalog that directory walk
    /// pinned a CPU core continuously even when nothing changed. With
    /// notifications the watcher sleeps at ~0% CPU until the OS reports a
    /// change, then runs a single incremental build.
    ///
    /// Events are debounced so that a burst (e.g. a `git checkout` touching
    /// thousands of files) collapses into one rebuild. The incremental
    /// `build` already skips unchanged files via mtime+sha, so triggering a
    /// rebuild on any event in a watched tree stays cheap and correct.
    pub fn watch(&self) -> Result<()> {
        use notify::{RecommendedWatcher, RecursiveMode};
        use notify_debouncer_full::{NoCache, new_debouncer_opt};
        use std::sync::mpsc;

        self.initialize()?;
        let workspaces = self.workspaces()?;
        if workspaces.is_empty() {
            return Err(anyhow!("no workspaces configured to watch"));
        }
        let db_dir = self
            .db_path
            .parent()
            .ok_or_else(|| anyhow!("invalid database path {}", self.db_path.display()))?
            .canonicalize()
            .with_context(|| {
                format!(
                    "failed to resolve database directory for {}",
                    self.db_path.display()
                )
            })?;

        let (tx, rx) = mpsc::channel::<DebounceEventResult>();
        // 2s debounce window coalesces bursts of events into one rebuild.
        //
        // Pass `NoCache` explicitly instead of the default `RecommendedCache`.
        // On non-Linux platforms `RecommendedCache` is `FileIdMap`, which
        // recursively scans the watched tree and stores a `FileId` per path to
        // correlate renames. Over a large repo, including dependency and Git
        // directories, that cache can consume substantial memory.
        // We never rely on rename correlation —
        // `update_paths` re-resolves each changed path independently — so the
        // cache is pure overhead. Linux already uses `NoCache`; this makes
        // every platform match.
        let mut debouncer = new_debouncer_opt::<_, RecommendedWatcher, NoCache>(
            Duration::from_secs(2),
            None,
            tx,
            NoCache::new(),
            notify::Config::default(),
        )
        .context("failed to start filesystem watcher")?;

        for workspace in &workspaces {
            if let Err(error) = debouncer.watch(&workspace.root, RecursiveMode::Recursive) {
                let message = error.to_string();
                if cfg!(target_os = "linux") && message.contains("No space left on device") {
                    return Err(anyhow!(
                        "failed to watch {}: inotify watch limit reached. \
                         Raise it with `sudo sysctl fs.inotify.max_user_watches=524288` \
                         (persist in /etc/sysctl.conf), or watch fewer/lower-level roots.",
                        workspace.root.display()
                    ));
                }
                return Err(error)
                    .with_context(|| format!("failed to watch {}", workspace.root.display()));
            }
            eprintln!("watching {} ({})", workspace.name, workspace.root.display());
        }
        eprintln!("watching {} repos for changes", workspaces.len());

        self.build(true, None)?;

        // Memoize each workspace's language allowlist for the lifetime of the
        // watch session. When no explicit language filter is configured this
        // would otherwise re-run a full-tree `detect_languages` walk on every
        // debounced event batch, defeating the point of incremental updates.
        let mut language_cache: Option<HashMap<String, HashMap<String, LanguageSpec>>> =
            Some(HashMap::new());

        // Block until the OS reports a (debounced) batch of changes, then run
        // one incremental build. Errors from the backend are logged but do not
        // stop the watcher.
        //
        // `notify` reports raw filesystem events and does NOT honor
        // `.gitignore`/`.tsindexignore`, so our own database writes would
        // otherwise trigger rebuild loops. Keep this pre-filter limited to the
        // actual DB directory and control directories that are never useful source
        // inputs; the incremental update still applies the full ignore rules when
        // it walks, so repos can intentionally index paths such as `build/` or
        // `dist/`.
        run_watch_event_loop(rx, &db_dir, &workspaces, |changed| {
            self.update_paths_cached(changed, &mut language_cache)
        });

        Ok(())
    }

    /// Open a read-only database connection and surface the stale-index
    /// warning if the on-disk schema version is older than this binary.
    ///
    /// All query-side methods (`get_symbol`, `list_file_outline`,
    /// `find_references`, `enclosing_symbol`) MUST go through this. A
    /// previous version of the code opened raw `Connection::open` on
    /// each read, which meant a v2 database silently returned stale
    /// symbol ranges with no warning at all — the warning only fired
    /// from `tsindex init` / `tsindex build`. Routing reads through this
    /// helper restores the safety net.
    fn open_read_conn(&self) -> Result<Connection> {
        let open = || {
            Connection::open_with_flags(
                &self.db_path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .with_context(|| format!("failed to open {}", self.db_path.display()))
        };
        let mut conn = open()?;
        check_schema_version_for_reads(&conn)?;
        self.legacy_partial.set(false);
        if table_exists(&conn, "files")? && !table_has_column(&conn, "files", "partial")? {
            drop(conn);
            match add_files_partial_column(&self.db_path) {
                // Migration ran (or a concurrent process added the column):
                // reopen and read normally.
                Ok(()) => conn = open()?,
                // The DB isn't writable from this process (read-only
                // filesystem, a mounted read-only index, or a lost migration
                // race). Rather than fail a read that a v3 index could still
                // serve, degrade: project `0 AS partial` so the SELECT parses
                // without the column and `partial` reports false.
                Err(AddPartialError::NotWritable) => {
                    self.legacy_partial.set(true);
                    conn = open()?;
                }
                // A genuine migration failure (corruption, unexpected SQL
                // error) stays an actionable error.
                Err(AddPartialError::Migration(error)) => return Err(error),
            }
        }
        ensure_index_ready(&conn)?;
        Ok(conn)
    }

    /// The `f.partial` projection for read queries: the real column on a
    /// current index, or `0 AS partial` when reading a pre-`partial` index
    /// that couldn't be migrated (see `legacy_partial`). The alias keeps the
    /// column at the same ordinal so `indexed_symbol_from_row` is unchanged.
    fn partial_projection(&self) -> &'static str {
        if self.legacy_partial.get() {
            "0 AS partial"
        } else {
            "f.partial"
        }
    }

    pub fn get_symbol(&self, args: GetSymbolArgs) -> Result<GetSymbolResponse> {
        let conn = self.open_read_conn()?;
        let matcher = args
            .file_glob
            .as_deref()
            .map(build_glob_matcher)
            .transpose()?;

        // Accept a single `name` and/or a `names` list; dedupe, preserve order.
        // Batching several names into one call is the point — it turns N
        // round-trips into one.
        let mut names: Vec<String> = Vec::new();
        let mut seen = HashSet::new();
        for name in args.name.iter().chain(args.names.iter().flatten()) {
            if seen.insert(name.clone()) {
                names.push(name.clone());
            }
        }
        if names.is_empty() {
            return Err(anyhow!(
                "get_symbol requires `name` or a non-empty `names` list"
            ));
        }

        // Build `s.name IN (?, ?, ...)` then bind repo/kind twice each for the
        // `(? IS NULL OR col = ?)` filters. All placeholders are positional in
        // bind order: names…, repo, repo, kind, kind.
        let placeholders = vec!["?"; names.len()].join(", ");
        let partial = self.partial_projection();
        let sql = format!(
            r#"
            SELECT
              s.id, s.file_id, r.name, f.path, f.language, s.kind, s.name, s.qualified,
              s.start_row, s.start_col, s.end_row, s.end_col, s.signature, s.docstring, f.sha, {partial}
            FROM symbols s
            JOIN files f ON f.id = s.file_id
            JOIN repos r ON r.id = f.repo_id
            WHERE s.name IN ({placeholders})
              AND (? IS NULL OR r.name = ?)
              AND (? IS NULL OR s.kind = ?)
            ORDER BY s.name, r.name, f.path, s.start_row, s.start_col
            "#
        );
        let mut stmt = conn.prepare(&sql)?;

        use rusqlite::types::Value;
        let mut binds: Vec<Value> = names.iter().cloned().map(Value::Text).collect();
        let repo_value = args.repo.clone().map_or(Value::Null, Value::Text);
        let kind_value = args.kind.clone().map_or(Value::Null, Value::Text);
        binds.push(repo_value.clone());
        binds.push(repo_value);
        binds.push(kind_value.clone());
        binds.push(kind_value);

        let mut rows = stmt.query(rusqlite::params_from_iter(binds))?;

        // Pull the full ordered result set into memory first so we can apply
        // a stable per-name offset + a *fair* per-name quota. The query is
        // already ordered by `s.name, repo, file, row, col`, so all rows for
        // one name are contiguous — bucketing them is a single pass. The
        // set is bounded by the index size and (in practice) by the few
        // names a caller requests, so materializing is cheap relative to the
        // body reads that follow.
        let mut buckets: Vec<Vec<IndexedSymbol>> = Vec::with_capacity(names.len());
        {
            // Map requested-name -> bucket index, preserving request order.
            let mut order: HashMap<String, usize> = HashMap::new();
            for (idx, name) in names.iter().enumerate() {
                order.entry(name.clone()).or_insert(idx);
            }
            buckets.resize(names.len(), Vec::new());
            while let Some(row) = rows.next()? {
                let symbol = indexed_symbol_from_row(row)?;
                if matcher
                    .as_ref()
                    .is_some_and(|matcher| !matcher.is_match(&symbol.file))
                {
                    continue;
                }
                if let Some(&bucket) = order.get(&symbol.name) {
                    buckets[bucket].push(symbol);
                }
            }
        }

        if names.len() > self.config.server.max_result_limit.max(1) {
            return Err(anyhow!(
                "get_symbol requested {} names, exceeding max_result_limit {}",
                names.len(),
                self.config.server.max_result_limit.max(1)
            ));
        }
        let limit = resolve_limit(&self.config.server, args.limit).max(names.len());
        let offset = args.offset.unwrap_or(0);
        let n_names = buckets.len().max(1);
        // Fair per-name quota: split `limit` across the N requested names so a
        // name with thousands of hits can't starve the others. Each name gets
        // `floor(limit / N)` rows, with the resolved limit first raised to at
        // least N. This guarantees one slot per name without exceeding the
        // configured response ceiling.
        let per_name = (limit / n_names).max(1);

        // `total` counts every matched symbol across all buckets (after the
        // file-glob filter), so a caller can tell how many remain past the
        // offset/quota window — independent of pagination.
        let total: usize = buckets.iter().map(Vec::len).sum();

        // Default body cap from config when the caller didn't opt out with
        // an explicit `max_body_lines`. Resolving it here (rather than
        // baking 120 into the SQL) keeps the limit configurable end-to-end.
        let effective_max_body_lines = resolve_body_limit(&self.config.server, args.max_body_lines);

        let mut page_buckets = Vec::with_capacity(buckets.len());
        for (requested_name, bucket) in names.into_iter().zip(buckets) {
            let bucket_len = bucket.len();
            let available = bucket_len.saturating_sub(offset);
            let mut bucket_matches = Vec::with_capacity(available.min(per_name));
            for symbol in bucket.into_iter().skip(offset).take(per_name) {
                let (body, stale) = if args.include_body {
                    let opts = BodyOpts {
                        context_lines: args.context_lines,
                        max_body_lines: effective_max_body_lines,
                    };
                    // Per-match stale tolerance: verify the indexed SHA against
                    // the live file before slicing. On a stale index, try to
                    // relocate the symbol by re-parsing live source and
                    // matching name/kind/range proximity; if that's ambiguous,
                    // omit only this body and expose `body_unavailable` rather
                    // than failing the whole batch. Clean matches are returned
                    // unchanged. See [`verified_slice_or_relocate`].
                    let path = self.source_path(&symbol.repo, &symbol.file)?;
                    let spec = lookup_language(&symbol.language);
                    let (body, omitted) = verified_slice_or_relocate(
                        spec.as_ref(),
                        &path,
                        &symbol.source_sha,
                        &StaleSymbolLocator {
                            name: &symbol.name,
                            kind: &symbol.kind,
                            start_row: symbol.start_row,
                            end_row: symbol.end_row,
                        },
                        args.context_lines,
                    );
                    let body = body.map(|body| elide_body(body, opts.max_body_lines));
                    (body, omitted)
                } else {
                    (None, false)
                };
                let body_unavailable = (stale && body.is_none()).then(|| {
                    "symbol could not be relocated in live source; run `tsindex build`".to_string()
                });
                bucket_matches.push(SymbolMatch {
                    repo: symbol.repo,
                    file: symbol.file,
                    language: symbol.language,
                    kind: symbol.kind,
                    name: symbol.name,
                    qualified: symbol.qualified,
                    signature: symbol.signature,
                    range: SourceRange {
                        start: RangePoint(symbol.start_row, symbol.start_col),
                        end: RangePoint(symbol.end_row, symbol.end_col),
                    },
                    docstring: symbol.docstring,
                    body,
                    stale,
                    body_unavailable,
                    partial: symbol.partial,
                });
            }
            page_buckets.push((requested_name, available, bucket_matches));
        }

        // Shrink the per-name quota uniformly when the serialized response is
        // over budget. A shared quota preserves the endpoint's per-name offset
        // contract across pages.
        let build_response = |quota: usize| {
            let matches = page_buckets
                .iter()
                .flat_map(|(_, _, matches)| matches.iter().take(quota).cloned())
                .collect::<Vec<_>>();
            let truncated_names = page_buckets
                .iter()
                .filter(|(_, available, _)| *available > quota)
                .map(|(name, _, _)| name.clone())
                .collect::<Vec<_>>();
            let truncated = !truncated_names.is_empty();
            GetSymbolResponse {
                matches,
                truncated,
                total,
                next_offset: truncated.then_some(offset + quota),
                truncated_names,
            }
        };

        let budget = self.config.server.max_response_chars;
        let mut quota = per_name;
        let mut response = build_response(quota);
        if budget > 0 {
            let mut guard = 0;
            while let Ok(serialized) = serde_json::to_string(&response) {
                if serialized.len() <= budget || quota <= 1 {
                    break;
                }
                quota = (quota * budget / serialized.len()).clamp(1, quota - 1);
                response = build_response(quota);
                guard += 1;
                if guard > 64 {
                    break;
                }
            }
            if let Ok(serialized) = serde_json::to_string(&response)
                && serialized.len() > budget
            {
                return Err(anyhow!(
                    "get_symbol response exceeds the configured max_response_chars \
                     ({} bytes) at the minimum one-match-per-name quota; narrow the \
                     request (fewer names, include_body=false, or a smaller \
                     max_body_lines) and retry",
                    budget
                ));
            }
        }

        Ok(response)
    }

    /// Replace a symbol's body in place, located by re-parsing the file
    /// live rather than trusting the (possibly stale) index. The write is
    /// refused if the replacement would introduce a syntax error the
    /// original file didn't already have, so a bad edit leaves the file
    /// untouched instead of corrupting it.
    pub fn replace_symbol(&self, args: ReplaceSymbolArgs) -> Result<ReplaceSymbolResponse> {
        let workspaces = self.workspaces()?;
        let workspace = match &args.repo {
            Some(repo) => workspaces
                .iter()
                .find(|workspace| &workspace.name == repo)
                .ok_or_else(|| anyhow!("repo {} is not configured", repo))?,
            None => match workspaces.as_slice() {
                [single] => single,
                _ => {
                    return Err(anyhow!(
                        "catalog has multiple repos; pass `repo` to choose one"
                    ));
                }
            },
        };

        // Resolve the path and confirm it stays inside the repo. This tool
        // writes files, so a `..`-laden `file` must not escape the repo root.
        let canonical_root = workspace
            .root
            .canonicalize()
            .with_context(|| format!("failed to resolve repo root {}", workspace.root.display()))?;
        let path = workspace.root.join(&args.file);
        let canonical_path = path
            .canonicalize()
            .with_context(|| format!("file {} not found", args.file))?;
        if !canonical_path.starts_with(&canonical_root) {
            return Err(anyhow!(
                "file {} resolves outside repo {}",
                args.file,
                workspace.name
            ));
        }

        let language_id = detect_language_from_file(&canonical_root, &canonical_path)?
            .ok_or_else(|| anyhow!("unsupported or unindexable file type: {}", args.file))?;
        let spec = lookup_language(language_id)
            .ok_or_else(|| anyhow!("unsupported language {language_id}"))?;

        let source = fs::read_to_string(&canonical_path)
            .with_context(|| format!("failed to read {}", args.file))?;
        let original_has_error = parse_tree(&spec, source.as_bytes())?
            .root_node()
            .has_error();
        let parsed = parse_file(&spec, source.as_bytes())?;

        // Select the symbols matching every selector the caller supplied.
        // `qualified` compares against the same dotted path get_symbol
        // reports (a top-level symbol has none, so its bare name matches).
        // Shared with the post-write re-derivation below so both agree;
        // `rows` is the caller's `row` here and the spliced region there.
        let select = |symbols: &[ExtractedSymbol],
                      rows: std::ops::Range<usize>|
         -> Vec<(ExtractedSymbol, Option<String>)> {
            let qualified = qualify_symbols(symbols);
            symbols
                .iter()
                .zip(qualified)
                .filter(|(symbol, _)| symbol.name == args.name)
                .filter(|(symbol, _)| args.kind.as_deref().is_none_or(|kind| symbol.kind == kind))
                .filter(|(symbol, qualified)| {
                    args.qualified
                        .as_deref()
                        .is_none_or(|wanted| qualified.as_deref().unwrap_or(&symbol.name) == wanted)
                })
                .filter(|(symbol, _)| rows.contains(&symbol.start_row))
                .map(|(symbol, qualified)| (symbol.clone(), qualified))
                .collect()
        };
        let requested_rows = args
            .row
            .map(|row| row.saturating_sub(1)..row)
            .unwrap_or(0..usize::MAX);
        let mut matches = select(&parsed.symbols, requested_rows);
        if matches.is_empty() {
            return Err(anyhow!(
                "no symbol named `{}`{}{}{} found in {}",
                args.name,
                args.kind
                    .as_deref()
                    .map(|kind| format!(" of kind `{kind}`"))
                    .unwrap_or_default(),
                args.qualified
                    .as_deref()
                    .map(|qualified| format!(" qualified `{qualified}`"))
                    .unwrap_or_default(),
                args.row
                    .map(|row| format!(" at row {row}"))
                    .unwrap_or_default(),
                args.file
            ));
        }
        if matches.len() > 1 {
            let locations = matches
                .iter()
                .map(|(symbol, qualified)| {
                    format!(
                        "{} `{}` at line {}",
                        symbol.kind,
                        qualified.as_deref().unwrap_or(&symbol.name),
                        symbol.start_row + 1
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            return Err(anyhow!(
                "symbol `{}` is ambiguous in {} ({}); pass `qualified` (the dotted \
                 name shown) or `row` (its 1-based start line) to pick one",
                args.name,
                args.file,
                locations
            ));
        }
        let (target, _) = matches.remove(0);
        let old_range = SourceRange {
            start: RangePoint(target.start_row, target.start_col),
            end: RangePoint(target.end_row, target.end_col),
        };
        let kind = target.kind.clone();

        // The body the agent sees from get_symbol is whole lines, so the
        // replacement is too; tolerate one accidental trailing newline (of
        // either flavor — the splice re-emits the file's own terminator).
        let new_body = args
            .new_body
            .strip_suffix("\r\n")
            .or_else(|| args.new_body.strip_suffix('\n'))
            .unwrap_or(&args.new_body);
        // get_symbol hands out bodies joined with `\n`, so an agent editing a
        // CRLF file sends LF back; re-terminate every line the file's way.
        let new_body = new_body
            .replace("\r\n", "\n")
            .replace('\n', line_terminator(&source));
        let new_body_rows = new_body.lines().count();
        let new_source = splice_symbol_lines(
            &source,
            target.start_row,
            target.start_col,
            target.end_row,
            target.end_col,
            &new_body,
        )?;

        // Syntax gate: refuse to write a replacement that breaks parsing,
        // unless the file was already unparseable (then we can't tell the
        // edit's errors from the pre-existing ones).
        let new_tree = parse_tree(&spec, new_source.as_bytes())?;
        if new_tree.root_node().has_error() && !original_has_error {
            let location = first_error(new_tree.root_node())
                .map(|(row, col)| format!(" near line {}, col {}", row + 1, col + 1))
                .unwrap_or_default();
            return Err(anyhow!(
                "replacement would introduce a syntax error{location}; {} left unchanged",
                args.file
            ));
        }

        // Atomic replace: write a sibling temp file, copy the target's
        // permissions, then rename over it. A crash (or the parent watchdog's
        // `process::exit`) mid-write can no longer leave a truncated file.
        // The temp file is created exclusively so a planted symlink at a
        // guessable name can never redirect the write outside the repo.
        let (tmp_path, mut tmp_file) = create_exclusive_sibling(&canonical_path)
            .with_context(|| format!("failed to write {}", args.file))?;
        let write = |tmp_file: &mut fs::File| -> Result<()> {
            use std::io::Write;
            tmp_file.write_all(new_source.as_bytes())?;
            tmp_file.sync_all()?;
            if let Ok(metadata) = fs::metadata(&canonical_path) {
                fs::set_permissions(&tmp_path, metadata.permissions())?;
            }
            fs::rename(&tmp_path, &canonical_path)?;
            Ok(())
        };
        if let Err(error) = write(&mut tmp_file) {
            let _ = fs::remove_file(&tmp_path);
            return Err(error).with_context(|| format!("failed to write {}", args.file));
        }

        // Re-derive the symbol's range from the written file so the caller
        // knows where it landed. Only the spliced rows are searched: the
        // symbol may have moved within them (a new leading comment), while
        // same-name duplicates elsewhere in the file must not match. `None`
        // when the written file no longer has a matching symbol (empty body
        // deleted it, or the replacement renamed it) — never echo `old_range`
        // as if the symbol were still there.
        let spliced_rows = target.start_row..target.start_row + new_body_rows;
        let new_range = parse_file(&spec, new_source.as_bytes())
            .ok()
            .and_then(
                |reparsed| match select(&reparsed.symbols, spliced_rows).as_slice() {
                    [(symbol, _)] => Some(SourceRange {
                        start: RangePoint(symbol.start_row, symbol.start_col),
                        end: RangePoint(symbol.end_row, symbol.end_col),
                    }),
                    _ => None,
                },
            );

        Ok(ReplaceSymbolResponse {
            repo: workspace.name.clone(),
            file: args.file,
            name: args.name,
            kind,
            old_range,
            new_range,
        })
    }

    pub fn list_file_outline(&self, args: OutlineArgs) -> Result<FileOutlineResponse> {
        let conn = self.open_read_conn()?;
        let partial = self.partial_projection();
        let mut file_stmt = conn.prepare(&format!(
            r#"
            SELECT f.id, r.name, f.path, f.language, f.sha, {partial}
            FROM files f
            JOIN repos r ON r.id = f.repo_id
            WHERE f.path = ?1
              AND (?2 IS NULL OR r.name = ?2)
            ORDER BY r.name
            "#,
        ))?;
        let files = file_stmt
            .query_map(params![args.file.clone(), args.repo.clone()], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, bool>(5)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        if files.is_empty() {
            return Err(anyhow!("file {} not found in index", args.file));
        }
        if args.repo.is_none() && files.len() > 1 {
            return Err(anyhow!(
                "file {} exists in multiple repos; rerun with --repo",
                args.file
            ));
        }

        let (file_id, repo, _, language, file_sha, file_partial) = files[0].clone();
        let mut stmt = conn.prepare(&format!(
            r#"
            SELECT
              s.id, s.file_id, r.name, f.path, f.language, s.kind, s.name, s.qualified,
              s.start_row, s.start_col, s.end_row, s.end_col, s.signature, s.docstring, f.sha, {partial}
            FROM symbols s
            JOIN files f ON f.id = s.file_id
            JOIN repos r ON r.id = f.repo_id
            WHERE s.file_id = ?1
            ORDER BY s.start_row, s.start_col, s.end_row, s.end_col
            "#,
        ))?;
        let mut symbols = stmt
            .query_map([file_id], indexed_symbol_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        // Imports are top-level and childless, so dropping them here doesn't
        // disturb the containment-based nesting of everything else.
        if !args.include_imports {
            symbols.retain(|symbol| symbol.kind != "import");
        }
        let mut symbols = build_outline(
            &symbols,
            args.depth,
            args.include_signatures,
            args.include_docstrings,
        );

        // Stable offset/limit pagination over the top-level outline entries.
        // Children of a kept top-level symbol come with it, so pagination
        // operates on the outer list only — a flat row count would split a
        // symbol from its nested definitions.
        let total = symbols.len();
        let limit = resolve_limit(&self.config.server, args.limit);
        let offset = args.offset.unwrap_or(0).min(total);
        let end = (offset + limit).min(total);
        let truncated = end < total;
        let kept: Vec<OutlineSymbol> = symbols.drain(offset..end).collect();
        symbols = kept;

        // Inline bodies for requested names so one call orients AND fetches
        // selected bodies. Bodies attach to whatever names appear in the tree
        // at the requested `depth`.
        let mut partial = file_partial;
        if let Some(wanted) = args
            .include_bodies_for
            .as_ref()
            .filter(|names| !names.is_empty())
        {
            let wanted: HashSet<&str> = wanted.iter().map(String::as_str).collect();
            let path = self.source_path(&repo, &args.file)?;
            // Verify the indexed SHA against the live file before slicing any
            // body — a stale index would attach the wrong source. On a
            // mismatch we surface the stale-index error rather than inline
            // the wrong text; the outline (without bodies) is still returned
            // and flagged `partial` so the caller knows bodies were skipped.
            // Note this is a body-omission signal, NOT pagination truncation:
            // the outline itself was fully returned, so `truncated`/`next_offset`
            // are left untouched (they describe the top-level symbol window).
            // Any read failure (stale SHA, missing or unreadable file) degrades
            // to `partial` with a per-symbol reason rather than failing the
            // whole call — the outline is still valid and useful on its own.
            if let Err(e) = attach_outline_bodies_verified(
                &mut symbols,
                &wanted,
                &path,
                &file_sha,
                resolve_body_limit(&self.config.server, args.max_body_lines),
            ) {
                partial = true;
                let reason = format!("body unavailable: {e:#}");
                eprintln!(
                    "warning: outline body inline skipped for {}: {e:#}",
                    args.file
                );
                mark_outline_bodies_unavailable(&mut symbols, &wanted, &reason);
            }
        }

        // Enforce the serialized-byte budget over the top-level symbols only.
        // Shrinking drops whole roots from the back — a root's children always
        // travel with it, so nested content is never silently pruned from a
        // kept root. `0` disables the budget. If a single root is itself too
        // large to fit, return a small actionable error (narrow with `depth`,
        // `include_signatures: false`, or `include_bodies_for`) rather than
        // emitting a payload the client will reject.
        let budget = self.config.server.max_response_chars;
        let next_offset = if end < total { Some(end) } else { None };
        let mut response = FileOutlineResponse {
            repo,
            file: args.file,
            language,
            symbols,
            total,
            truncated,
            next_offset,
            partial,
            warning: None,
        };
        // The warning is part of the measured payload, so (re)compute it
        // before every serialization inside the loop — not after it.
        let set_warning = |response: &mut FileOutlineResponse| {
            response.warning = crate::model::truncation_warning(
                response.symbols.len(),
                Some(response.total),
                response.next_offset,
                None,
            );
        };
        set_warning(&mut response);
        if budget > 0 {
            let mut guard = 0;
            while let Ok(serialized) = serde_json::to_string(&response) {
                if serialized.len() <= budget || response.symbols.len() <= 1 {
                    break;
                }
                // Drop a whole top-level root from the back; its children go
                // with it, so we never strip children from a kept root.
                response.symbols.pop();
                let returned_end = offset + response.symbols.len();
                response.truncated = returned_end < total;
                response.next_offset = if response.truncated {
                    Some(returned_end)
                } else {
                    None
                };
                set_warning(&mut response);
                guard += 1;
                if guard > 512 {
                    break;
                }
            }
            if let Ok(serialized) = serde_json::to_string(&response)
                && serialized.len() > budget
            {
                return Err(anyhow!(
                    "list_file_outline response for {} exceeds the configured \
                     max_response_chars ({} bytes) even with a single top-level \
                     symbol; narrow the request (lower depth, \
                     include_signatures=false, or drop include_bodies_for) and retry",
                    response.file,
                    budget
                ));
            }
        }

        Ok(response)
    }

    /// Run the shared `refs` query and apply the caller's filters (scope,
    /// exclude_tests, include_declarations), returning the filtered raw rows as
    /// `(name, repo, file, start_row, start_col, end_row, end_col, context, sha,
    /// partial)`. Shared by `find_references` (row-level detail) and
    /// `find_reference_counts` (aggregate-only) so both see the identical
    /// filtered set.
    #[allow(clippy::type_complexity)]
    fn query_reference_rows(
        &self,
        args: &FindReferencesArgs,
    ) -> Result<
        Vec<(
            String,
            String,
            String,
            usize,
            usize,
            usize,
            usize,
            String,
            String,
            bool,
        )>,
    > {
        let conn = self.open_read_conn()?;
        let scope = args.scope.as_deref().map(build_glob_matcher).transpose()?;

        // Combine `name` + `names`, dedupe, preserve order.
        let mut names: Vec<String> = Vec::new();
        let mut seen = HashSet::new();
        for name in args.name.iter().chain(args.names.iter().flatten()) {
            if seen.insert(name.clone()) {
                names.push(name.clone());
            }
        }
        if names.is_empty() {
            return Err(anyhow!(
                "find_references requires `name` or a non-empty `names` list"
            ));
        }

        let placeholders = vec!["?"; names.len()].join(", ");
        let partial = self.partial_projection();
        let sql = format!(
            r#"
            SELECT rf.name, rp.name, f.path, rf.start_row, rf.start_col, rf.end_row, rf.end_col, rf.context, f.sha, {partial}
            FROM refs rf
            JOIN files f ON f.id = rf.file_id
            JOIN repos rp ON rp.id = f.repo_id
            WHERE rf.name IN ({placeholders})
              AND (? IS NULL OR rp.name = ?)
            ORDER BY rf.name, rp.name, f.path, rf.start_row, rf.start_col
            "#
        );
        let mut stmt = conn.prepare(&sql)?;

        use rusqlite::types::Value;
        let mut binds: Vec<Value> = names.iter().cloned().map(Value::Text).collect();
        let repo_value = args.repo.clone().map_or(Value::Null, Value::Text);
        binds.push(repo_value.clone());
        binds.push(repo_value);

        let refs = stmt
            .query_map(rusqlite::params_from_iter(binds), |row| {
                Ok((
                    row.get::<_, String>(0)?, // referenced identifier
                    row.get::<_, String>(1)?, // repo
                    row.get::<_, String>(2)?, // file path
                    row.get::<_, i64>(3)? as usize,
                    row.get::<_, i64>(4)? as usize,
                    row.get::<_, i64>(5)? as usize,
                    row.get::<_, i64>(6)? as usize,
                    row.get::<_, String>(7)?, // context
                    row.get::<_, String>(8)?, // indexed source sha
                    row.get::<_, bool>(9)?,   // parse-recovery quality flag
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        // Apply the same in-memory filters both read paths honor.
        Ok(refs
            .into_iter()
            .filter(|(_, _, file, _, _, _, _, context, _, _)| {
                if scope
                    .as_ref()
                    .is_some_and(|matcher| !matcher.is_match(file))
                {
                    return false;
                }
                if args.exclude_tests && looks_like_test_path(file) {
                    return false;
                }
                if !args.include_declarations && context == "declaration" {
                    return false;
                }
                true
            })
            .collect())
    }

    /// Aggregate-only `find_references`: exact totals and per-file counts over
    /// the FULL filtered set in one pass, with no per-reference rows, snippets,
    /// or pagination. A fraction of the row-level payload. `total`/`total_files`
    /// /`names` are always exact; the `files` array is capped by the byte budget
    /// (top-N by count, with `files_truncated` set) so a many-file repo can't
    /// produce an oversized response — the invariant the row path enforces.
    pub fn find_reference_counts(&self, args: FindReferencesArgs) -> Result<RefCountsResponse> {
        let multi = args.name.iter().count() + args.names.as_ref().map_or(0, |n| n.len()) > 1;
        let rows = self.query_reference_rows(&args)?;

        let total = rows.len();
        // Staleness signal: any contributing file parse-recovered during
        // indexing. `counts_only` doesn't re-read files (that's what makes it
        // cheap), so it can't detect post-index source drift — surface the
        // parse-recovery flag it CAN see, same as the row path's parse_partial.
        let mut partial = false;
        // Per-(repo, file) counts.
        let mut by_file: HashMap<(String, String), usize> = HashMap::new();
        // Per-name totals (only meaningful when several names were requested).
        let mut by_name: HashMap<String, usize> = HashMap::new();
        for (name, repo, file, .., row_partial) in &rows {
            partial |= row_partial;
            *by_file.entry((repo.clone(), file.clone())).or_default() += 1;
            if multi {
                *by_name.entry(name.clone()).or_default() += 1;
            }
        }
        let total_files = by_file.len();

        // Sort files by count descending, then repo/file ascending so the top
        // users surface first and the order is deterministic.
        let mut files: Vec<FileRefCount> = by_file
            .into_iter()
            .map(|((repo, file), count)| FileRefCount { repo, file, count })
            .collect();
        files.sort_by(|a, b| {
            b.count
                .cmp(&a.count)
                .then_with(|| a.repo.cmp(&b.repo))
                .then_with(|| a.file.cmp(&b.file))
        });

        let mut name_counts: Vec<NameRefCount> = by_name
            .into_iter()
            .map(|(name, count)| NameRefCount { name, count })
            .collect();
        name_counts.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.name.cmp(&b.name)));

        // Bound the `files` array under the serialized-byte budget. counts_only
        // can't paginate without breaking its exact-total contract, but the
        // array is count-sorted, so dropping the tail keeps the most-referenced
        // files while `total`/`total_files`/`names` stay exact. Mirrors the
        // budget-shrink loops on the row-level read paths.
        let budget = self.config.server.max_response_chars;
        let build = |files: &[FileRefCount], files_truncated: bool| RefCountsResponse {
            total,
            total_files,
            files_returned: files.len(),
            files_truncated,
            partial,
            files: files.to_vec(),
            names: name_counts.clone(),
        };
        let mut kept = files.clone();
        let mut response = build(&kept, false);
        if budget > 0 {
            let mut guard = 0;
            while let Ok(serialized) = serde_json::to_string(&response) {
                if serialized.len() <= budget || kept.len() <= 1 {
                    break;
                }
                // Drop the tail proportionally to the overflow (count-sorted,
                // so the least-referenced files go first), then rebuild.
                let new_len = (kept.len() * budget / serialized.len()).clamp(1, kept.len() - 1);
                kept.truncate(new_len);
                response = build(&kept, true);
                guard += 1;
                if guard > 64 {
                    break;
                }
            }
            if let Ok(serialized) = serde_json::to_string(&response)
                && serialized.len() > budget
            {
                return Err(anyhow!(
                    "find_references counts_only response exceeds the configured \
                     max_response_chars ({} bytes) even with a single file entry; \
                     narrow the query (scope, fewer names) and retry",
                    budget
                ));
            }
        }

        Ok(response)
    }

    pub fn find_references(&self, args: FindReferencesArgs) -> Result<FindReferencesResponse> {
        // Several names => tag each ref with its name so the caller can tell
        // them apart; single-name responses stay byte-identical.
        let tag_names = args.names.as_ref().is_some_and(|n| n.len() > 1)
            || (args.name.is_some() && args.names.as_ref().is_some_and(|n| !n.is_empty()));

        // Shared query + filters (scope / exclude_tests / include_declarations)
        // live in `query_reference_rows`; the rows arrive pre-filtered.
        let refs = self.query_reference_rows(&args)?;

        // Build the flat, ordered reference list (the query already orders by
        // name, repo, file, row, col). Each row carries its repo/file, so the
        // grouped view can be derived from the same windowed rows later — one
        // source of truth for both shapes, and pagination is a simple slice.
        let mut rows: Vec<ReferenceRow> = Vec::new();
        let mut any_stale = false;
        let mut parse_partial = false;
        let snippet_lines = args.snippet_lines.clamp(1, 20);
        // Per-file cache of verified source lines so each file is read and
        // SHA-checked at most once, even when it carries many references. The
        // cache holds a short unavailability reason on error so every reference
        // in that file can carry an explicit `snippet_unavailable` instead of a
        // silently blank snippet.
        let mut file_lines: HashMap<(String, String), Result<Vec<String>, String>> = HashMap::new();
        for (ref_name, repo, file, start_row, start_col, end_row, end_col, context, sha, partial) in
            refs
        {
            parse_partial |= partial;
            let key = (repo.clone(), file.clone());
            let (snippet, snippet_start_row, snippet_unavailable): (
                String,
                Option<usize>,
                Option<String>,
            ) = match file_lines.get(&key) {
                Some(Ok(lines)) => {
                    let (snippet, first) = ref_snippet(lines, start_row, end_row, snippet_lines);
                    (snippet, first, None)
                }
                Some(Err(reason)) => (
                    String::new(),
                    None,
                    Some(reason.clone()).filter(|reason| !reason.is_empty()),
                ),
                None => {
                    let path = self.source_path(&repo, &file)?;
                    match read_verified(&path, &sha) {
                        Ok(bytes) => {
                            let source = std::str::from_utf8(&bytes).unwrap_or_default();
                            let lines: Vec<String> = source.lines().map(str::to_string).collect();
                            let (snippet, first) =
                                ref_snippet(&lines, start_row, end_row, snippet_lines);
                            file_lines.insert(key, Ok(lines));
                            (snippet, first, None)
                        }
                        Err(e) => {
                            any_stale = true;
                            // Stable, actionable reason: tell the caller the
                            // source diverged from the index and to rebuild.
                            let reason = if is_stale_index_error(&e) {
                                "source no longer matches the index; run `tsindex build` \
                                 before trusting snippets"
                                    .to_string()
                            } else {
                                format!("{e:#}")
                            };
                            file_lines.insert(key, Err(reason.clone()));
                            (String::new(), None, Some(reason))
                        }
                    }
                }
            };
            let tag = tag_names.then(|| ref_name.clone());
            rows.push(ReferenceRow {
                name: tag,
                repo: repo.clone(),
                file: file.clone(),
                range: SourceRange {
                    start: RangePoint(start_row, start_col),
                    end: RangePoint(end_row, end_col),
                },
                context,
                snippet,
                snippet_start_row,
                snippet_unavailable,
            });
        }

        // Stable offset/limit pagination over the ordered reference list.
        let total = rows.len();
        // Distinct (repo, file) pairs across the full filtered result set,
        // post-filter and pre-pagination. `groups` only covers the returned
        // slice when truncated, so its length is not the file count; this is.
        let total_files = rows
            .iter()
            .map(|row| (row.repo.as_str(), row.file.as_str()))
            .collect::<HashSet<_>>()
            .len();
        let limit = resolve_limit(&self.config.server, args.limit);
        let offset = args.offset.unwrap_or(0).min(total);
        // Requested window, before the byte budget may shrink it.
        let mut kept = limit.min(total - offset);

        // Build the grouped view + FindReferencesResponse for a given kept
        // count. Both wire shapes (`groups` populated when group_by_file, else
        // `refs`) are derived from the same windowed slice so they agree.
        let build = |kept: usize| -> FindReferencesResponse {
            let windowed: Vec<ReferenceRow> =
                rows.iter().skip(offset).take(kept).cloned().collect();
            let returned = windowed.len();
            let truncated = offset + returned < total;
            let next_offset = if truncated {
                Some(offset + returned)
            } else {
                None
            };
            let mut groups: HashMap<(String, String), Vec<ReferenceMatch>> = HashMap::new();
            for row in &windowed {
                groups
                    .entry((row.repo.clone(), row.file.clone()))
                    .or_default()
                    .push(ReferenceMatch {
                        name: row.name.clone(),
                        range: row.range.clone(),
                        context: row.context.clone(),
                        snippet: row.snippet.clone(),
                        snippet_start_row: row.snippet_start_row,
                        snippet_unavailable: row.snippet_unavailable.clone(),
                    });
            }
            let mut grouped: Vec<ReferenceGroup> = groups
                .into_iter()
                .map(|((repo, file), refs)| ReferenceGroup { repo, file, refs })
                .collect();
            grouped.sort_by(|a, b| a.repo.cmp(&b.repo).then_with(|| a.file.cmp(&b.file)));
            FindReferencesResponse {
                total,
                total_files,
                returned,
                truncated,
                next_offset,
                partial: any_stale || parse_partial,
                groups: if args.group_by_file {
                    grouped
                } else {
                    Vec::new()
                },
                refs: if args.group_by_file {
                    Vec::new()
                } else {
                    windowed
                },
            }
        };

        // Enforce the serialized-byte budget: shrink `kept` until the JSON
        // fits. `0` disables it. Converges in 2-3 iterations via a geometric
        // step; if even a single reference overruns the budget, return that
        // one anyway rather than erroring so the call stays useful.
        let budget = self.config.server.max_response_chars;
        let mut response = build(kept);
        if budget > 0 {
            let mut guard = 0;
            while let Ok(serialized) = serde_json::to_string(&response) {
                let len = serialized.len();
                if len <= budget || kept <= 1 {
                    break;
                }
                // Scale down proportionally; floor at 1.
                let next = (kept * budget / len).max(1);
                if next >= kept {
                    // No progress — force a decrement to avoid looping.
                    kept -= 1;
                } else {
                    kept = next;
                }
                response = build(kept);
                guard += 1;
                if guard > 64 {
                    kept = 1;
                    response = build(kept);
                    break;
                }
            }
        }

        Ok(response)
    }

    /// Given a `(file, row, [col])` position, return the symbols whose
    /// captured ranges contain that point, ordered outermost → innermost.
    ///
    /// `args.depth` caps the number of returned levels (1 = innermost
    /// only). When `args.col` is `None` the lookup is row-only — useful
    /// for stack-trace lines and commit hunks where you have a line
    /// number but not a column. When `args.col` is `Some`, symbols are
    /// filtered by exact `(row, col)` containment.
    ///
    /// Returns an error if the file isn't in the index. Returns an
    /// `Ok` with empty `matches` when the file is indexed but the
    /// position sits outside any captured symbol (e.g. the user
    /// pointed at a top-level comment or blank line).
    pub fn enclosing_symbol(&self, args: EnclosingSymbolArgs) -> Result<EnclosingSymbolResponse> {
        let conn = self.open_read_conn()?;

        // Resolve which repo this lookup applies to BEFORE querying
        // symbols. Without this, a path that exists in two configured
        // repos would interleave symbols from both repos in the SQL
        // result, the depth-trim would pick from whichever repo
        // happened to have the smallest range, and `response.repo`
        // would silently reflect just the first one. `list_file_outline`
        // already enforces this invariant — mirror its check so the two
        // primitives behave consistently when paths collide.
        let mut files_stmt = conn.prepare(
            r#"
            SELECT r.name
            FROM files f
            JOIN repos r ON r.id = f.repo_id
            WHERE f.path = ?1 AND (?2 IS NULL OR r.name = ?2)
            "#,
        )?;
        let candidate_repos: Vec<String> = files_stmt
            .query_map(params![args.file.clone(), args.repo.clone()], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        if candidate_repos.is_empty() {
            return Err(anyhow!("file {} not found in index", args.file));
        }
        if args.repo.is_none() && candidate_repos.len() > 1 {
            return Err(anyhow!(
                "file {} exists in multiple repos; rerun with --repo",
                args.file
            ));
        }
        let resolved_repo = candidate_repos[0].clone();

        let depth = args.depth.max(1);

        let effective_max_body_lines = resolve_body_limit(&self.config.server, args.max_body_lines);
        let budget = self.config.server.max_response_chars;

        // Batch path: one EnclosingHit per requested row (row-only lookups).
        // A stack trace or a set of diff hunks resolves to its enclosing
        // symbols in a single call instead of one round-trip per line. The
        // row list is paginated (offset/limit) and budget-shrunk like the
        // other read endpoints — this is the path that can otherwise produce
        // an unbounded `rows.len() * depth` payload.
        if let Some(rows) = args.rows.as_ref().filter(|rows| !rows.is_empty()) {
            let total = rows.len();
            let limit = resolve_limit(&self.config.server, args.limit);
            let offset = args.offset.unwrap_or(0).min(total);

            // Resolve rows into EnclosingHits for a given window of the
            // request. Returned as a closure so the budget-shrink loop can
            // rebuild smaller pages without duplicating the lookup logic.
            let build = |window: &[usize]| -> Result<Vec<EnclosingHit>> {
                let mut results = Vec::with_capacity(window.len());
                for &row in window {
                    // Input rows are 1-based (the wire convention); the
                    // internal lookup is 0-based. Reject 0 rather than
                    // silently saturating it to row 0 — that would mask an
                    // off-by-one in the caller. Echo the original 1-based row
                    // back so the caller sees the row it asked for.
                    let internal_row = row
                        .checked_sub(1)
                        .ok_or_else(|| anyhow!("enclosing_symbol rows are 1-based; received 0"))?;
                    let matches = self.enclosing_matches(
                        &conn,
                        &args.file,
                        &resolved_repo,
                        internal_row,
                        None,
                        depth,
                        args.include_body,
                        args.context_lines,
                        effective_max_body_lines,
                    )?;
                    results.push(EnclosingHit { row, matches });
                }
                Ok(results)
            };

            // Requested window, before the byte budget may shrink it.
            let mut kept = limit.min(total - offset);
            let mut results = build(&rows[offset..offset + kept])?;
            if budget > 0 {
                let mut guard = 0;
                loop {
                    let response = EnclosingSymbolResponse {
                        repo: resolved_repo.clone(),
                        file: args.file.clone(),
                        matches: Vec::new(),
                        results,
                        total,
                        truncated: offset + kept < total,
                        next_offset: (offset + kept < total).then_some(offset + kept),
                    };
                    let serialized = serde_json::to_string(&response)?;
                    if serialized.len() <= budget || kept <= 1 {
                        return Ok(response);
                    }
                    // Shrink the page proportionally to the overflow, then
                    // rebuild. Mirrors find_references' budget loop.
                    kept = (kept * budget / serialized.len()).clamp(1, kept - 1);
                    results = build(&rows[offset..offset + kept])?;
                    guard += 1;
                    if guard > 64 {
                        return Ok(response);
                    }
                }
            }
            return Ok(EnclosingSymbolResponse {
                repo: resolved_repo,
                file: args.file,
                matches: Vec::new(),
                results,
                total,
                truncated: offset + kept < total,
                next_offset: (offset + kept < total).then_some(offset + kept),
            });
        }

        // Single-position path, supporting the optional precise column. The
        // payload is bounded by `depth`, but a large depth with bodies can
        // still overflow the budget — shrink depth in that case.
        let row = args
            .row
            .ok_or_else(|| anyhow!("enclosing_symbol requires `row` or a non-empty `rows` list"))?;
        // Input row is 1-based (wire convention); lookup is 0-based. Reject 0
        // rather than saturating it, so an off-by-one caller gets an error.
        let internal_row = row
            .checked_sub(1)
            .ok_or_else(|| anyhow!("enclosing_symbol row is 1-based; received 0"))?;
        let mut depth_kept = depth;
        let mut guard = 0;
        loop {
            let matches = self.enclosing_matches(
                &conn,
                &args.file,
                &resolved_repo,
                internal_row,
                args.col,
                depth_kept,
                args.include_body,
                args.context_lines,
                effective_max_body_lines,
            )?;
            let response = EnclosingSymbolResponse {
                repo: resolved_repo.clone(),
                file: args.file.clone(),
                matches,
                results: Vec::new(),
                total: 0,
                truncated: false,
                next_offset: None,
            };
            if budget == 0 {
                return Ok(response);
            }
            let serialized = serde_json::to_string(&response)?;
            if serialized.len() <= budget || depth_kept <= 1 {
                return Ok(response);
            }
            depth_kept -= 1;
            guard += 1;
            if guard > 64 {
                return Ok(response);
            }
        }
    }

    /// Symbols whose captured range encloses `(row, col)` in `file`/`repo`,
    /// trimmed to the innermost `depth` and returned outermost → innermost.
    /// Empty when the position sits outside every captured symbol. Shared by
    /// the single- and batch-position paths of `enclosing_symbol`.
    ///
    /// End-positions in tree-sitter are exclusive (the point AFTER the last
    /// character), so `end_col` is checked with strict `>`: a position exactly
    /// at `(end_row, end_col)` sits one past the symbol and is NOT enclosed.
    /// The `?4 IS NULL` gate lets one query serve both row-only and
    /// point-precise lookups. The ORDER BY puts the smallest range first so a
    /// `take(depth)` yields the innermost N; ties fall through to
    /// start DESC / end ASC for deterministic innermost-first ordering.
    #[allow(clippy::too_many_arguments)]
    fn enclosing_matches(
        &self,
        conn: &Connection,
        file: &str,
        repo: &str,
        row: usize,
        col: Option<usize>,
        depth: usize,
        include_body: bool,
        context_lines: usize,
        max_body_lines: Option<usize>,
    ) -> Result<Vec<SymbolMatch>> {
        let row_param: i64 = row as i64;
        let col_param: Option<i64> = col.map(|value| value as i64);

        let partial = self.partial_projection();
        let mut stmt = conn.prepare(&format!(
            r#"
            SELECT
              s.id, s.file_id, r.name, f.path, f.language, s.kind, s.name, s.qualified,
              s.start_row, s.start_col, s.end_row, s.end_col, s.signature, s.docstring, f.sha, {partial}
            FROM symbols s
            JOIN files f ON f.id = s.file_id
            JOIN repos r ON r.id = f.repo_id
            WHERE f.path = ?1
              AND r.name = ?2
              AND s.start_row <= ?3
              AND s.end_row   >= ?3
              AND (?4 IS NULL OR (
                    (s.start_row < ?3 OR s.start_col <= ?4)
                AND (s.end_row   > ?3 OR s.end_col   >  ?4)
              ))
            ORDER BY (s.end_row - s.start_row) ASC,
                     s.start_row DESC,
                     s.start_col DESC,
                     s.end_row ASC,
                     s.end_col ASC
            "#,
        ))?;
        let symbols = stmt
            .query_map(
                params![file, repo, row_param, col_param],
                indexed_symbol_from_row,
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        let mut taken: Vec<IndexedSymbol> = symbols.into_iter().take(depth.max(1)).collect();
        taken.reverse();

        let mut matches = Vec::with_capacity(taken.len());
        for symbol in taken {
            let (body, stale_omitted) = if include_body {
                // Same per-match stale tolerance as get_symbol: relocate from
                // live source on a stale index, else omit just this body and
                // expose `body_unavailable`. Never fail the whole enclosing set
                // because one file drifted.
                let path = self.source_path(&symbol.repo, &symbol.file)?;
                let spec = lookup_language(&symbol.language);
                let (body, omitted) = verified_slice_or_relocate(
                    spec.as_ref(),
                    &path,
                    &symbol.source_sha,
                    &StaleSymbolLocator {
                        name: &symbol.name,
                        kind: &symbol.kind,
                        start_row: symbol.start_row,
                        end_row: symbol.end_row,
                    },
                    context_lines,
                );
                (body.map(|body| elide_body(body, max_body_lines)), omitted)
            } else {
                (None, false)
            };
            let body_unavailable = (stale_omitted && body.is_none()).then(|| {
                "symbol could not be relocated in live source; run `tsindex build`".to_string()
            });
            matches.push(SymbolMatch {
                repo: symbol.repo,
                file: symbol.file,
                language: symbol.language,
                kind: symbol.kind,
                name: symbol.name,
                qualified: symbol.qualified,
                signature: symbol.signature,
                range: SourceRange {
                    start: RangePoint(symbol.start_row, symbol.start_col),
                    end: RangePoint(symbol.end_row, symbol.end_col),
                },
                docstring: symbol.docstring,
                body,
                stale: stale_omitted,
                body_unavailable,
                partial: symbol.partial,
            });
        }
        Ok(matches)
    }

    pub fn query(&self, args: QueryArgs) -> Result<QueryResponse> {
        // Raw queries read live files (via `walk_source_files`), not the index,
        // so they must NOT be gated on the initial build completing — a query
        // against a brand-new or never-indexed root is still meaningful and
        // should return live results, not "index is not ready". We only
        // bootstrap the schema (cheap, idempotent) so the DB file is valid and
        // no database initialization or readiness check is needed.
        let spec = lookup_language(&args.language)
            .ok_or_else(|| anyhow!("unsupported language {}", args.language))?;
        let query = Query::new(&(spec.language)(), &args.query)
            .with_context(|| format!("invalid query for {}", args.language))?;
        let matcher = args
            .file_glob
            .as_deref()
            .map(build_glob_matcher)
            .transpose()?;
        let limit = resolve_limit(&self.config.server, args.limit);
        let offset = args.offset.unwrap_or(0);
        let capture_char_limit = self.config.server.capture_char_limit;
        let timeout = Duration::from_millis(self.config.server.query_timeout_ms);
        let started = Instant::now();
        let mut captures = Vec::new();
        let mut truncated = false;
        let mut timed_out = false;
        let mut partial = false;
        // Position in the overall matched-capture stream (after the
        // `capture` filter). Offset skips the first `offset` matches; the
        // window then emits up to `limit`. `truncated`/`timed_out` record that
        // the walk stopped before exhausting the stream so `next_offset` is
        // correct even when the timeout cuts it short.
        let mut stream_pos = 0usize;

        for workspace in self.workspaces()? {
            if truncated || timed_out {
                break;
            }
            if args
                .repo
                .as_ref()
                .is_some_and(|repo| repo != &workspace.name)
            {
                continue;
            }
            for path in self.walk_source_files(&workspace)? {
                if started.elapsed() > timeout {
                    timed_out = true;
                    break;
                }
                let Some(language_id) = detect_language_from_file(&workspace.root, &path)? else {
                    continue;
                };
                if language_id != spec.id {
                    continue;
                }
                let relative = relative_path(&workspace.root, &path);
                if matcher
                    .as_ref()
                    .is_some_and(|matcher| !matcher.is_match(&relative))
                {
                    continue;
                }
                // Match build's file gate: skip oversized files outright, and
                // let one unreadable file degrade the response to `partial`
                // instead of failing the whole query.
                if fs::metadata(&path).is_ok_and(|meta| meta.len() > MAX_FILE_SIZE) {
                    continue;
                }
                let source = match fs::read(&path) {
                    Ok(source) => source,
                    Err(error) => {
                        eprintln!("warning: query skipped {}: {error}", path.display());
                        partial = true;
                        continue;
                    }
                };
                if is_binary(&source) {
                    continue;
                }
                let parsed = parse_tree(&spec, &source)?;
                // Track this file's parse-recovery state, but only fold it into
                // the response `partial` when a capture from this file is
                // actually returned — otherwise a malformed file that matched
                // nothing would flag the whole response partial for no reason
                // visible to the caller.
                let file_has_error = parsed.root_node().has_error();
                let mut emitted_from_file = false;
                let mut cursor = QueryCursor::new();
                let names = query.capture_names();
                let mut capture_iter =
                    cursor.captures(&query, parsed.root_node(), source.as_slice());
                while let Some(capture) = capture_iter.next() {
                    if started.elapsed() > timeout {
                        timed_out = true;
                        break;
                    }
                    let (matched, index) = capture;
                    let QueryCapture { node, .. } = matched.captures[*index];
                    let name = names[matched.captures[*index].index as usize].to_string();
                    if args.capture.as_ref().is_some_and(|needle| needle != &name) {
                        continue;
                    }
                    // Advance the stream position for every matched capture
                    // so the offset window is stable across the whole walk,
                    // not per file.
                    if stream_pos < offset {
                        stream_pos += 1;
                        continue;
                    }
                    if captures.len() >= limit {
                        // This match exists past the window — stop emitting.
                        truncated = true;
                        break;
                    }
                    let raw_text = node.utf8_text(&source).unwrap_or_default().to_string();
                    // Cap the capture text at `capture_char_limit` chars so a
                    // single huge capture (e.g. a whole-file string) can't
                    // dominate the response. The cap counts Unicode scalar
                    // values, not bytes, and marks the capture `text_truncated`
                    // so the caller knows it was trimmed.
                    let (text, text_truncated) = truncate_chars(&raw_text, capture_char_limit);
                    captures.push(RawQueryCapture {
                        repo: workspace.name.clone(),
                        name,
                        text,
                        file: relative.clone(),
                        range: SourceRange {
                            start: RangePoint(
                                node.start_position().row,
                                node.start_position().column,
                            ),
                            end: RangePoint(node.end_position().row, node.end_position().column),
                        },
                        text_truncated,
                    });
                    emitted_from_file = true;
                    stream_pos += 1;
                }
                // Only fold this file's parse-recovery flag in when it
                // actually contributed a returned capture.
                if emitted_from_file {
                    partial |= file_has_error;
                }
                if truncated || timed_out {
                    break;
                }
            }
        }

        // A timeout that fires before this page emitted anything would hand
        // back `next_offset == offset`: a page the caller cannot advance past.
        // Paging is O(offset) because the walk restarts from the first file,
        // so say so instead of looping the caller.
        if timed_out && captures.is_empty() {
            return Err(anyhow!(
                "query timed out before reaching offset {offset} (no captures emitted \
                 within {} ms); raise `query_timeout_ms` or narrow with `file_glob`",
                self.config.server.query_timeout_ms
            ));
        }

        let mut truncated = truncated || timed_out;
        // `next_offset` lets a caller resume: it's the count of captures
        // consumed up to and including the window (offset + returned). We
        // only expose it when the walk saw more matches (truncated or
        // timed_out) — otherwise the stream is exhausted.
        // Query streams files, so the total is unknown — the warning just says
        // more results exist when truncated/timed_out.
        let build = |captures: Vec<RawQueryCapture>, truncated: bool| {
            let next_offset = truncated.then_some(offset + captures.len());
            QueryResponse {
                language: args.language.clone(),
                warning: crate::model::truncation_warning(captures.len(), None, next_offset, None),
                captures,
                truncated,
                timed_out,
                next_offset,
                partial,
            }
        };

        // Enforce the same serialized-byte budget as the indexed tools: drop
        // captures from the tail until the JSON fits (floor at one capture),
        // moving the window end back so `next_offset` resumes at the first
        // dropped capture. `0` disables the budget.
        let budget = self.config.server.max_response_chars;
        let mut response = build(captures, truncated);
        if budget > 0 {
            let mut guard = 0;
            while let Ok(serialized) = serde_json::to_string(&response) {
                let len = serialized.len();
                let kept = response.captures.len();
                if len <= budget || kept <= 1 {
                    break;
                }
                let next = (kept * budget / len).clamp(1, kept - 1);
                let mut captures = response.captures;
                captures.truncate(next);
                truncated = true;
                response = build(captures, truncated);
                guard += 1;
                if guard > 64 {
                    break;
                }
            }
        }

        Ok(response)
    }

    pub fn detected_languages(&self) -> Result<Vec<RepoLanguageReport>> {
        self.workspaces()?
            .into_iter()
            .map(|workspace| {
                Ok(RepoLanguageReport {
                    repo: workspace.name.clone(),
                    root: workspace.root.to_string_lossy().to_string(),
                    languages: detect_languages(&workspace.root)?,
                })
            })
            .collect()
    }

    pub fn repos(&self) -> Result<Vec<RepoInfo>> {
        Ok(self
            .workspaces()?
            .into_iter()
            .map(|workspace| RepoInfo {
                name: workspace.name,
                path: workspace.root.to_string_lossy().to_string(),
            })
            .collect())
    }

    pub fn workspaces(&self) -> Result<Vec<Workspace>> {
        if self.config.repos.is_empty() {
            return Ok(vec![Workspace {
                name: default_repo_name(&self.root),
                root: self.root.clone(),
                languages: self.config.languages.include.clone(),
                ignore: self.config.ignore.extra.clone(),
            }]);
        }

        let mut workspaces = Vec::with_capacity(self.config.repos.len());
        for repo in &self.config.repos {
            match workspace_from_config(&self.root, repo, &self.config)? {
                Some(workspace) => workspaces.push(workspace),
                None => warn_missing_repo_once(&repo.name, &repo.path),
            }
        }
        Ok(workspaces)
    }

    /// Names of every repo in the loaded config, whether or not its clone is
    /// currently on disk. `workspaces()` drops missing roots, so pruning must
    /// not key off it: a temporarily unmounted clone is not a removed repo.
    fn configured_repo_names(&self) -> HashSet<String> {
        if self.config.repos.is_empty() {
            return HashSet::from([default_repo_name(&self.root)]);
        }
        self.config
            .repos
            .iter()
            .map(|repo| repo.name.clone())
            .collect()
    }

    /// Canonical roots of every configured workspace other than `workspace`.
    /// Walks prune these so a nested repo's files are indexed only under the
    /// repo that owns them (deepest root wins, matching `best_workspace_for_path`).
    fn other_workspace_roots(&self, workspace: &Workspace) -> Result<HashSet<PathBuf>> {
        Ok(self
            .workspaces()?
            .into_iter()
            .filter(|other| other.root != workspace.root)
            .map(|other| other.root)
            .collect())
    }

    pub fn source_path(&self, repo: &str, file: &str) -> Result<PathBuf> {
        let workspace = self
            .workspaces()?
            .into_iter()
            .find(|workspace| workspace.name == repo)
            .ok_or_else(|| anyhow!("repo {} is not configured", repo))?;
        Ok(workspace.root.join(file))
    }

    fn allowed_languages(&self, workspace: &Workspace) -> Result<HashMap<String, LanguageSpec>> {
        let explicit = if self.languages.is_empty() {
            workspace.languages.clone()
        } else {
            self.languages.clone()
        };
        if explicit.is_empty() {
            // No explicit filter means "index whatever languages are present",
            // and membership is already decided per file by
            // `detect_language_from_file` during the indexing walk. Running
            // `detect_languages` here only to prefilter cost a second full
            // tree walk (plus shebang file reads) per repo on every build.
            let mut map = HashMap::new();
            for spec in crate::lang::all_languages() {
                map.insert(spec.id.to_string(), spec);
            }
            Ok(map)
        } else {
            let mut map = HashMap::new();
            for language in explicit {
                let spec = lookup_language(&language)
                    .ok_or_else(|| anyhow!("unsupported language {}", language))?;
                map.insert(language, spec);
            }
            Ok(map)
        }
    }

    fn walk_source_files(&self, workspace: &Workspace) -> Result<Vec<PathBuf>> {
        let mut builder = WalkBuilder::new(&workspace.root);
        builder.hidden(false);
        builder.git_ignore(true);
        builder.git_exclude(true);
        builder.git_global(true);
        builder.add_custom_ignore_filename(".tsindexignore");
        for pattern in &workspace.ignore {
            builder.add_ignore(pattern);
        }
        let foreign_roots = self.other_workspace_roots(workspace)?;
        builder.filter_entry(move |entry| {
            !(entry.file_type().is_some_and(|t| t.is_dir()) && foreign_roots.contains(entry.path()))
        });
        let mut files = Vec::new();
        for result in builder.build() {
            let entry = match result {
                Ok(entry) => entry,
                Err(_) => continue,
            };
            if entry.path().is_file() {
                files.push(entry.into_path());
            }
        }
        files.sort();
        Ok(files)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct BuildStats {
    pub indexed: usize,
    pub skipped: usize,
    pub failed: usize,
}

#[derive(Debug)]
struct ParsedFile {
    symbols: Vec<ExtractedSymbol>,
    refs: Vec<ExtractedRef>,
    partial: bool,
}

#[derive(Debug, Clone)]
struct ExtractedSymbol {
    kind: String,
    name: String,
    start_row: usize,
    start_col: usize,
    end_row: usize,
    end_col: usize,
    signature: Option<String>,
    docstring: Option<String>,
}

struct StaleSymbolLocator<'a> {
    name: &'a str,
    kind: &'a str,
    start_row: usize,
    end_row: usize,
}

#[derive(Debug, Clone)]
struct ExtractedRef {
    name: String,
    start_row: usize,
    start_col: usize,
    end_row: usize,
    end_col: usize,
    context: String,
}

struct FileJob {
    repo_id: i64,
    workspace_name: String,
    path: PathBuf,
    relative: String,
    spec: LanguageSpec,
}

enum ProcessedFile {
    Indexed {
        repo_id: i64,
        relative: String,
        spec_id: String,
        sha: String,
        mtime_ns: i64,
        byte_size: i64,
        parsed: ParsedFile,
    },
    Unchanged {
        repo_id: i64,
        relative: String,
    },
    ParseFailed {
        repo_id: i64,
        relative: String,
        workspace_name: String,
        error: String,
    },
    /// stat/read failed (permissions, vanished mid-build, FIFO). Counted as
    /// failed but kept in `active_paths` so a transient EBUSY/permission blip
    /// never purges rows that were fine a moment ago; never aborts the batch.
    /// A file that is really gone is purged by `purge_stale_files`'s
    /// existence check instead.
    Unreadable {
        repo_id: i64,
        relative: String,
        workspace_name: String,
        error: String,
    },
    Binary,
}

fn effective_thread_count(jobs: usize) -> usize {
    if jobs == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    } else {
        jobs
    }
}

/// Load the stored file state (language, sha, mtime, byte size) for only the
/// paths in the current build chunk, grouped by repo.
///
/// A full build of a large repo used to prefetch the entire
/// `files` table into memory up front so every chunk could be diffed against
/// it. That nested map dominated peak RSS. Instead we fetch state lazily, one
/// chunk at a time: each chunk holds at most `BUILD_CHUNK_SIZE` paths, so the
/// returned map never grows beyond a chunk's worth of rows regardless of repo
/// size. The map is dropped once the chunk is processed.
/// Max path placeholders per `IN (...)` query. Stays under SQLite's default
/// `SQLITE_MAX_VARIABLE_NUMBER` of 999 (the query also binds `repo_id`).
const SQLITE_MAX_IN_PARAMS: usize = 900;

type StoredFileState = (String, String, i64, i64);
type StoredFileStates = HashMap<i64, HashMap<String, StoredFileState>>;

fn prefetch_file_state_for_chunk(conn: &Connection, chunk: &[FileJob]) -> Result<StoredFileStates> {
    // Group the chunk's relative paths by repo so each repo gets a single
    // `path IN (...)` query.
    let mut paths_by_repo: HashMap<i64, Vec<&str>> = HashMap::new();
    for job in chunk {
        paths_by_repo
            .entry(job.repo_id)
            .or_default()
            .push(job.relative.as_str());
    }

    let mut map: StoredFileStates = HashMap::new();
    for (repo_id, paths) in paths_by_repo {
        let repo_map = map.entry(repo_id).or_default();
        // The build path bounds a chunk to `BUILD_CHUNK_SIZE`, but the watch
        // path calls this with every changed path in a debounced batch, which
        // is unbounded. Split each repo's paths into sub-batches so the bound
        // `IN (?, ?, ...)` query never exceeds SQLite's host-parameter limit
        // (`SQLITE_MAX_VARIABLE_NUMBER`, commonly 999) — the `+ repo_id` keeps
        // us one under the round number.
        for path_batch in paths.chunks(SQLITE_MAX_IN_PARAMS) {
            let placeholders: String = path_batch.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let sql = format!(
                "SELECT path, language, sha, mtime_ns, byte_size FROM files \
                 WHERE repo_id = ? AND path IN ({})",
                placeholders
            );
            let mut stmt = conn.prepare(&sql)?;
            let mut params: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(path_batch.len() + 1);
            params.push(&repo_id);
            for path in path_batch {
                params.push(path);
            }
            let mut rows = stmt.query(params.as_slice())?;
            while let Some(row) = rows.next()? {
                let path: String = row.get(0)?;
                let language: String = row.get(1)?;
                let sha: String = row.get(2)?;
                let mtime_ns: i64 = row.get(3)?;
                let byte_size: i64 = row.get(4)?;
                repo_map.insert(path, (language, sha, mtime_ns, byte_size));
            }
        }
    }
    Ok(map)
}

const MAX_FILE_SIZE: u64 = 2 * 1024 * 1024;

/// Number of files processed per build chunk. Bounds peak memory: parsed
/// symbols/refs and the per-chunk prefetched file state both scale with this.
const BUILD_CHUNK_SIZE: usize = 128;

/// Parse and persist a single bounded batch of file jobs.
///
/// This is the streaming counterpart to the old "collect every job, then
/// iterate chunks" loop: `build` now fills a fixed-size buffer as the walker
/// yields files and hands each full buffer here, so peak memory is bounded by
/// `BUILD_CHUNK_SIZE` jobs plus their parsed symbols/refs rather than by the
/// whole repo. Updates `stats` and `active_paths` in place.
fn process_batch(
    conn: &mut Connection,
    pool: &rayon::ThreadPool,
    batch: &[FileJob],
    incremental: bool,
    stats: &mut BuildStats,
    active_paths: &mut HashMap<i64, HashSet<String>>,
) -> Result<()> {
    let existing = if incremental {
        prefetch_file_state_for_chunk(conn, batch)?
    } else {
        HashMap::new()
    };
    let results: Vec<ProcessedFile> = pool.install(|| {
        batch
            .par_iter()
            .map(|job| process_file_job(job, incremental, false, &existing))
            .collect::<Result<Vec<_>>>()
    })?;

    let has_writes = results
        .iter()
        .any(|r| matches!(r, ProcessedFile::Indexed { .. }));
    let tx = if has_writes {
        Some(conn.transaction_with_behavior(TransactionBehavior::Immediate)?)
    } else {
        None
    };
    for result in results {
        match result {
            ProcessedFile::Indexed {
                repo_id,
                relative,
                spec_id,
                sha,
                mtime_ns,
                byte_size,
                parsed,
            } => {
                active_paths
                    .entry(repo_id)
                    .or_default()
                    .insert(relative.clone());
                upsert_file(
                    tx.as_ref().unwrap(),
                    repo_id,
                    &relative,
                    &spec_id,
                    &sha,
                    mtime_ns,
                    byte_size,
                    parsed,
                )?;
                stats.indexed += 1;
            }
            ProcessedFile::Unchanged { repo_id, relative } => {
                active_paths.entry(repo_id).or_default().insert(relative);
                stats.skipped += 1;
            }
            ProcessedFile::ParseFailed {
                repo_id,
                relative,
                workspace_name,
                error,
            } => {
                eprintln!("warning: failed to parse {workspace_name}:{relative}: {error}");
                active_paths.entry(repo_id).or_default().insert(relative);
                stats.failed += 1;
            }
            ProcessedFile::Unreadable {
                repo_id,
                relative,
                workspace_name,
                error,
            } => {
                eprintln!("warning: skipped {workspace_name}:{relative}: {error}");
                active_paths.entry(repo_id).or_default().insert(relative);
                stats.failed += 1;
            }
            ProcessedFile::Binary => {}
        }
    }
    if let Some(tx) = tx {
        tx.commit()?;
    }
    Ok(())
}

fn process_file_job(
    job: &FileJob,
    incremental: bool,
    force_hash: bool,
    existing: &StoredFileStates,
) -> Result<ProcessedFile> {
    let unreadable = |error: std::io::Error| ProcessedFile::Unreadable {
        repo_id: job.repo_id,
        relative: job.relative.clone(),
        workspace_name: job.workspace_name.clone(),
        error: error.to_string(),
    };
    let metadata = match fs::metadata(&job.path) {
        Ok(metadata) => metadata,
        Err(error) => return Ok(unreadable(error)),
    };
    if metadata.len() > MAX_FILE_SIZE {
        return Ok(ProcessedFile::Binary);
    }

    let mtime_ns = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos() as i64)
        .unwrap_or_default();

    let stored = if incremental {
        existing
            .get(&job.repo_id)
            .and_then(|repo_files| repo_files.get(&job.relative))
    } else {
        None
    };

    // Fast path: a matching language + mtime + byte size means the file is
    // taken as unchanged without reading it, so a no-op refresh costs one
    // stat() per file instead of read + sha256 per file. `mark_all_files_stale`
    // poisons stored mtimes with -1 and an unreadable mtime reports 0 here, so
    // both fall through to the content check below.
    //
    // The watcher path passes `force_hash = true` to skip this: a `touch`
    // (mtime/size unchanged but content replaced in place) must still be
    // detected, so it falls through to the content-hash check that follows.
    // A full `build` keeps the cheap fast path.
    if !force_hash
        && let Some((lang, _, stored_mtime, stored_size)) = stored
        && *lang == job.spec.id
        && mtime_ns > 0
        && *stored_mtime == mtime_ns
        && *stored_size == metadata.len() as i64
    {
        return Ok(ProcessedFile::Unchanged {
            repo_id: job.repo_id,
            relative: job.relative.clone(),
        });
    }

    let bytes = match fs::read(&job.path) {
        Ok(bytes) => bytes,
        Err(error) => return Ok(unreadable(error)),
    };

    if is_binary(&bytes) {
        return Ok(ProcessedFile::Binary);
    }

    let sha = sha256_hex(&bytes);

    if let Some((lang, existing_sha, existing_mtime, _)) = stored
        && *lang == job.spec.id
        && *existing_sha == sha
        && *existing_mtime == mtime_ns
    {
        return Ok(ProcessedFile::Unchanged {
            repo_id: job.repo_id,
            relative: job.relative.clone(),
        });
    }

    match parse_file(&job.spec, &bytes) {
        Ok(parsed) => Ok(ProcessedFile::Indexed {
            repo_id: job.repo_id,
            relative: job.relative.clone(),
            spec_id: job.spec.id.to_string(),
            sha,
            mtime_ns,
            byte_size: metadata.len() as i64,
            parsed,
        }),
        Err(e) => Ok(ProcessedFile::ParseFailed {
            repo_id: job.repo_id,
            relative: job.relative.clone(),
            workspace_name: job.workspace_name.clone(),
            error: format!("{e:#}"),
        }),
    }
}

fn workspace_from_config(
    catalog_root: &Path,
    repo: &RepoConfig,
    config: &TsIndexConfig,
) -> Result<Option<Workspace>> {
    let root = if Path::new(&repo.path).is_absolute() {
        PathBuf::from(&repo.path)
    } else {
        catalog_root.join(&repo.path)
    };
    if !root.exists() {
        // A deleted or renamed clone must not disable the rest of the catalog;
        // the caller warns and skips it.
        return Ok(None);
    }
    let root = root
        .canonicalize()
        .with_context(|| format!("failed to resolve repo path {}", root.display()))?;
    Ok(Some(Workspace {
        name: repo.name.clone(),
        root,
        languages: if repo.languages.is_empty() {
            config.languages.include.clone()
        } else {
            repo.languages.clone()
        },
        ignore: if repo.ignore.is_empty() {
            config.ignore.extra.clone()
        } else {
            repo.ignore.clone()
        },
    }))
}

/// Warn once per process per missing repo; `workspaces()` runs on every read,
/// so an unconditional warning would spam an MCP session's stderr.
fn warn_missing_repo_once(name: &str, path: &str) {
    static WARNED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    let mut warned = WARNED
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if warned.insert(name.to_string()) {
        eprintln!(
            "warning: repo {name} path {path} does not exist; skipping it (run `tsindex repos remove {name}` to drop it)"
        );
    }
}

fn default_repo_name(path: &Path) -> String {
    path.file_name()
        .and_then(|value| value.to_str())
        .map(|value| value.to_string())
        .unwrap_or_else(|| "default".to_string())
}

fn indexed_symbol_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<IndexedSymbol> {
    Ok(IndexedSymbol {
        id: row.get(0)?,
        file_id: row.get(1)?,
        repo: row.get(2)?,
        file: row.get(3)?,
        language: row.get(4)?,
        kind: row.get(5)?,
        name: row.get(6)?,
        qualified: row.get(7)?,
        start_row: row.get::<_, i64>(8)? as usize,
        start_col: row.get::<_, i64>(9)? as usize,
        end_row: row.get::<_, i64>(10)? as usize,
        end_col: row.get::<_, i64>(11)? as usize,
        signature: row.get(12)?,
        docstring: row.get(13)?,
        source_sha: row.get(14)?,
        partial: row.get(15)?,
    })
}

fn create_schema(conn: &Connection, workspaces: &[Workspace]) -> Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    let stale = migrate_schema(conn, workspaces)?;
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS repos (
          id          INTEGER PRIMARY KEY,
          name        TEXT UNIQUE NOT NULL,
          root_path   TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS files (
          id          INTEGER PRIMARY KEY,
          repo_id     INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
          path        TEXT NOT NULL,
          language    TEXT NOT NULL,
          sha         TEXT NOT NULL,
          mtime_ns    INTEGER NOT NULL,
          byte_size   INTEGER NOT NULL,
          partial     INTEGER NOT NULL DEFAULT 0,
          UNIQUE(repo_id, path)
        );

        CREATE TABLE IF NOT EXISTS symbols (
          id          INTEGER PRIMARY KEY,
          file_id     INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
          kind        TEXT NOT NULL,
          name        TEXT NOT NULL,
          qualified   TEXT,
          start_row   INTEGER NOT NULL,
          start_col   INTEGER NOT NULL,
          end_row     INTEGER NOT NULL,
          end_col     INTEGER NOT NULL,
          signature   TEXT,
          docstring   TEXT
        );

        CREATE TABLE IF NOT EXISTS refs (
          id          INTEGER PRIMARY KEY,
          file_id     INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
          name        TEXT NOT NULL,
          start_row   INTEGER NOT NULL,
          start_col   INTEGER NOT NULL,
          end_row     INTEGER NOT NULL,
          end_col     INTEGER NOT NULL,
          context     TEXT
        );

        CREATE TABLE IF NOT EXISTS index_state (
          id          INTEGER PRIMARY KEY CHECK(id = 1),
          ready       INTEGER NOT NULL DEFAULT 0,
          refreshing  INTEGER NOT NULL DEFAULT 0,
          generation  INTEGER NOT NULL DEFAULT 0,
          updated_at  INTEGER NOT NULL DEFAULT 0
        );
        "#,
    )?;

    conn.execute(
        "INSERT OR IGNORE INTO index_state(id, ready) VALUES(1, CASE WHEN EXISTS(SELECT 1 FROM files) THEN 1 ELSE 0 END)",
        [],
    )?;

    conn.execute_batch(
        r#"
        CREATE INDEX IF NOT EXISTS idx_symbols_name ON symbols(name);
        CREATE INDEX IF NOT EXISTS idx_symbols_file ON symbols(file_id);
        CREATE INDEX IF NOT EXISTS idx_refs_name ON refs(name);
        CREATE INDEX IF NOT EXISTS idx_refs_file ON refs(file_id);
        CREATE INDEX IF NOT EXISTS idx_files_repo_path ON files(repo_id, path);
        "#,
    )?;

    sync_repos(conn, workspaces)?;
    // A stale index keeps its old version (so reads keep warning) until
    // `build` has re-extracted every file and stamps the current one.
    if !stale {
        set_user_version(conn, SCHEMA_VERSION)?;
    }
    Ok(())
}

/// Bring the on-disk layout up to date. Returns `true` when the stored data
/// was extracted by an older binary and must be rebuilt before it is trusted.
fn migrate_schema(conn: &Connection, workspaces: &[Workspace]) -> Result<bool> {
    // Capture the on-disk version BEFORE any migration steps run so the
    // stale-version warning below can distinguish a brand-new database
    // (version == 0) from one that was indexed by an older binary.
    let pre_migrate_version = user_version(conn)?;

    // The v0 (pre-repo) layout was already extracted with v0 ranges
    // (e.g. without decorator-aware Python class ranges). Treat any
    // legacy-shape database as data-stale even though pre_migrate_version
    // reads as 0; otherwise the cleanup below would skip it.
    let legacy_layout =
        table_exists(conn, "files")? && !table_has_column(conn, "files", "repo_id")?;
    if legacy_layout {
        migrate_legacy_files_table(conn, workspaces)?;
        // Park the migrated database at a known-stale version (< current)
        // rather than leaving it at 0: version 0 is indistinguishable from a
        // fresh database on the next open, which would then stamp the current
        // version without any rebuild having happened.
        set_user_version(conn, 1)?;
    }
    if table_exists(conn, "files")? && !table_has_column(conn, "files", "partial")? {
        conn.execute(
            "ALTER TABLE files ADD COLUMN partial INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
    }

    if pre_migrate_version > SCHEMA_VERSION {
        return Err(anyhow!(
            "database schema version {} is newer than supported version {}; \
             upgrade tsindex, or delete the index database and rebuild it with this binary",
            pre_migrate_version,
            SCHEMA_VERSION
        ));
    }

    // Stale data: either the on-disk version is older than this binary,
    // or we just migrated a v0 layout whose rows were extracted before
    // any of the v3 range-shape changes (decorator-aware Python ranges,
    // etc.). In both cases the captured ranges in `symbols` and `refs`
    // are out of date. Force the next incremental build to re-extract
    // every file by zeroing mtime_ns; file_is_unchanged() compares
    // sha+mtime_ns and a fresh stat() will never reproduce -1.
    let stale = legacy_layout || (pre_migrate_version > 0 && pre_migrate_version < SCHEMA_VERSION);
    if stale {
        mark_all_files_stale(conn)?;
        emit_stale_schema_warning_once(pre_migrate_version);
    }

    Ok(stale)
}

/// Read-only schema check for indexed query paths.
///
/// Read-only callers (`get_symbol`, `enclosing_symbol`, `find_references`,
/// `list_file_outline`) MUST NOT run the full `migrate_schema`
/// because that mutates the database (legacy migration, mtime dirtying).
/// They still need to surface the stale-index warning so a user running
/// `tsindex symbol` against a v2 database is told their results may be
/// out of date. Emits the warning at most once per process via OnceLock.
fn check_schema_version_for_reads(conn: &Connection) -> Result<()> {
    let pre = user_version(conn)?;
    if pre > SCHEMA_VERSION {
        bail!(
            "index schema version {pre} is newer than this binary supports ({SCHEMA_VERSION}); \
             upgrade tsindex, or delete the index database (.tsindex/index.db) and run \
             `tsindex build` with this binary"
        );
    }
    if pre > 0 && pre < SCHEMA_VERSION {
        emit_stale_schema_warning_once(pre);
    }
    Ok(())
}

/// Additively add the `files.partial` column to a v3 (pre-`partial`) index so
/// indexed reads do not fail with "no such column: f.partial" after upgrading
/// across the schema bump. Read-path migration is strictly additive and
/// idempotent: it only ever `ALTER TABLE files ADD COLUMN partial ...`, which
/// is a no-op once the column exists. Every read query selects `f.partial`, so
/// a v3 index lacking the column is unusable without this.
///
/// The read connection itself is read-only, so this reopens the DB read-write
/// just long enough to run the migration, then re-checks on a fresh read-only
/// connection. A read-only filesystem (or any write failure) is surfaced as an
/// actionable error naming the fix rather than a silent "no such column".
/// Why `add_files_partial_column` couldn't make the column available. The
/// read path distinguishes "this process can't write the DB" (degrade to a
/// tolerant read) from "the migration itself failed" (a hard, actionable
/// error).
enum AddPartialError {
    /// The DB couldn't be opened read-write, or the `ALTER TABLE` was refused
    /// for a permissions/read-only reason. The caller should degrade to a
    /// read that tolerates the missing column.
    NotWritable,
    /// The migration ran (or attempted to) and failed for a real reason.
    Migration(anyhow::Error),
}

fn add_files_partial_column(db_path: &Path) -> std::result::Result<(), AddPartialError> {
    let write_conn = match Connection::open_with_flags(
        db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) {
        Ok(conn) => conn,
        // A read-only filesystem (or read-only mount) surfaces here as an
        // open failure. Degrade rather than error: the v3 index is still
        // readable without the new column.
        Err(_) => return Err(AddPartialError::NotWritable),
    };
    if let Err(error) = write_conn.execute(
        "ALTER TABLE files ADD COLUMN partial INTEGER NOT NULL DEFAULT 0",
        [],
    ) {
        // Someone else may have added the column concurrently (multi-session
        // catalogs race here). Re-check on a read conn; if it's present now,
        // the race resolved in our favor and there's nothing to do.
        let recheck = Connection::open_with_flags(
            db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|e| AddPartialError::Migration(anyhow!(e)));
        if let Ok(recheck) = recheck
            && let Ok(true) = table_has_column(&recheck, "files", "partial")
        {
            return Ok(());
        }
        // A read-only / permission error on the ALTER is the same degraded
        // case as a failed RW open: the column simply isn't there and can't
        // be added by this process.
        if is_readonly_sqlite_error(&error) {
            return Err(AddPartialError::NotWritable);
        }
        return Err(AddPartialError::Migration(anyhow!(
            "index at {} predates the `files.partial` column and the read-path \
             migration could not add it ({}); run `tsindex build` to rebuild the index \
             with the current schema",
            db_path.display(),
            error
        )));
    }
    Ok(())
}

/// True when a SQLite error indicates the database (or its filesystem) is
/// read-only, so a write is impossible regardless of correctness.
fn is_readonly_sqlite_error(error: &rusqlite::Error) -> bool {
    use rusqlite::ffi;
    match error {
        rusqlite::Error::SqliteFailure(code, _) => {
            matches!(
                code.code,
                ffi::ErrorCode::ReadOnly | ffi::ErrorCode::PermissionDenied
            )
        }
        _ => false,
    }
}

fn ensure_index_ready(conn: &Connection) -> Result<()> {
    if !table_exists(conn, "index_state")? {
        return Ok(());
    }
    // `create_schema` creates the table and inserts the singleton row in two
    // statements; a reader landing between them sees no row, which is the
    // same "initial build has not completed" state as `ready = 0`.
    let ready: Option<bool> = conn
        .query_row("SELECT ready FROM index_state WHERE id = 1", [], |row| {
            row.get(0)
        })
        .optional()?;
    if ready != Some(true) {
        return Err(anyhow!(
            "index is not ready: the initial build is still running or has not completed; retry shortly or run `tsindex build`"
        ));
    }
    Ok(())
}

fn mark_index_refreshing(conn: &Connection) -> Result<()> {
    conn.execute("UPDATE index_state SET refreshing = 1 WHERE id = 1", [])?;
    Ok(())
}

fn mark_index_ready(conn: &Connection) -> Result<()> {
    conn.execute(
        "UPDATE index_state SET ready = 1, refreshing = 0, generation = generation + 1, updated_at = unixepoch() WHERE id = 1",
        [],
    )?;
    Ok(())
}

fn emit_stale_schema_warning_once(pre_migrate_version: i64) {
    // OnceLock gates the warning to one emission per process. Without
    // this, every read-path call (potentially many per session for MCP
    // and HTTP servers) would re-print the same warning.
    static WARNED: OnceLock<()> = OnceLock::new();
    WARNED.get_or_init(|| {
        eprintln!(
            "warning: tsindex database was indexed with schema version {} but this \
             binary expects version {}; symbol ranges and references may be stale. \
             Run `tsindex build` to refresh the index.",
            pre_migrate_version, SCHEMA_VERSION
        );
    });
}

/// Mark every indexed file as stale so the next incremental build
/// re-extracts it.
///
/// `file_is_unchanged()` decides whether to skip a file based on
/// `sha + mtime_ns`. Setting `mtime_ns = -1` guarantees no live file
/// can match (real `stat()` mtime is non-negative), forcing
/// re-extraction. We deliberately keep `sha` so that if the file truly
/// hasn't changed on disk, the rebuild is the only difference — no
/// wasted re-hash.
fn mark_all_files_stale(conn: &Connection) -> Result<()> {
    conn.execute("UPDATE files SET mtime_ns = -1", [])?;
    Ok(())
}

fn user_version(conn: &Connection) -> Result<i64> {
    Ok(conn.pragma_query_value(None, "user_version", |row| row.get(0))?)
}

fn set_user_version(conn: &Connection, version: i64) -> Result<()> {
    conn.pragma_update(None, "user_version", version)?;
    Ok(())
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1 LIMIT 1",
            [table],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

fn table_has_column(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows.iter().any(|name| name == column))
}

fn migrate_legacy_files_table(conn: &Connection, workspaces: &[Workspace]) -> Result<()> {
    if !table_exists(conn, "files")? {
        return Ok(());
    }

    let default_workspace = workspaces.first().cloned().unwrap_or(Workspace {
        name: "default".to_string(),
        root: PathBuf::from("."),
        languages: Vec::new(),
        ignore: Vec::new(),
    });
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS repos (
          id          INTEGER PRIMARY KEY,
          name        TEXT UNIQUE NOT NULL,
          root_path   TEXT NOT NULL
        );
        "#,
    )?;
    conn.execute(
        "INSERT INTO repos(name, root_path) VALUES(?1, ?2) ON CONFLICT(name) DO UPDATE SET root_path = excluded.root_path",
        params![default_workspace.name, default_workspace.root.to_string_lossy().to_string()],
    )?;
    let repo_id = repo_id_by_name(conn, &default_workspace.name)?
        .ok_or_else(|| anyhow!("failed to create default repo during migration"))?;

    conn.execute_batch("BEGIN IMMEDIATE")?;
    let migration = (|| -> Result<()> {
        conn.execute_batch(
            r#"
            ALTER TABLE files RENAME TO files_legacy;
            CREATE TABLE files (
              id          INTEGER PRIMARY KEY,
              repo_id     INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
              path        TEXT NOT NULL,
              language    TEXT NOT NULL,
              sha         TEXT NOT NULL,
              mtime_ns    INTEGER NOT NULL,
              byte_size   INTEGER NOT NULL,
              UNIQUE(repo_id, path)
            );
            "#,
        )?;
        conn.execute(
            r#"
            INSERT INTO files(id, repo_id, path, language, sha, mtime_ns, byte_size)
            SELECT id, ?1, path, language, sha, mtime_ns, byte_size
            FROM files_legacy
            "#,
            [repo_id],
        )?;
        conn.execute_batch(
            r#"
            DROP TABLE files_legacy;
            "#,
        )?;
        Ok(())
    })();

    match migration {
        Ok(()) => conn.execute_batch("COMMIT")?,
        Err(error) => {
            let _ = conn.execute_batch("ROLLBACK");
            return Err(error);
        }
    }

    // Intentionally do NOT bump user_version here. The legacy v0 layout
    // had v0-extracted symbol ranges (no decorator-aware Python ranges,
    // pre-multirepo schema). The caller (`migrate_schema`) treats a
    // legacy migration as data-stale and dirties every file's mtime_ns
    // to force re-extraction; `build` stamps SCHEMA_VERSION once that
    // rebuild has completed.
    Ok(())
}

/// Register every configured workspace. Never deletes: pruning repos that
/// left the config is a `build`-only step (`prune_missing_repos`).
fn sync_repos(conn: &Connection, workspaces: &[Workspace]) -> Result<()> {
    for workspace in workspaces {
        conn.execute(
            "INSERT INTO repos(name, root_path) VALUES(?1, ?2) ON CONFLICT(name) DO UPDATE SET root_path = excluded.root_path",
            params![workspace.name, workspace.root.to_string_lossy().to_string()],
        )?;
    }
    Ok(())
}

/// Delete `repos` rows (cascading to their files/symbols/refs) for repos that
/// are no longer in the loaded config. `configured` is the config's name list,
/// not the reachable workspaces, so an unavailable clone keeps its rows.
fn prune_missing_repos(conn: &Connection, configured: &HashSet<String>) -> Result<()> {
    let active = configured;
    let mut stmt = conn.prepare("SELECT name FROM repos")?;
    let existing = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for name in existing {
        if !active.contains(&name) {
            conn.execute("DELETE FROM repos WHERE name = ?1", [name])?;
        }
    }
    Ok(())
}

fn repo_id_by_name(conn: &Connection, repo: &str) -> Result<Option<i64>> {
    Ok(conn
        .query_row("SELECT id FROM repos WHERE name = ?1", [repo], |row| {
            row.get(0)
        })
        .optional()?)
}

/// Return the index of the workspace that best contains `path`, or `None` if no
/// workspace does. When workspaces are nested, the deepest (longest-root) match
/// wins so a file is attributed to the most specific repo.
fn best_workspace_for_path(workspaces: &[Workspace], path: &Path) -> Option<usize> {
    let mut best: Option<(usize, usize)> = None;
    for (idx, workspace) in workspaces.iter().enumerate() {
        if path.starts_with(&workspace.root) {
            let depth = workspace.root.components().count();
            if best.is_none_or(|(_, best_depth)| depth > best_depth) {
                best = Some((idx, depth));
            }
        }
    }
    best.map(|(idx, _)| idx)
}

/// Whether `relative` currently has any rows in the index, either as an exact
/// file path or as a directory prefix of one. Used to decide if a deletion
/// candidate is worth a write transaction: an ignored-but-never-indexed path
/// (e.g. `target/`/`node_modules/` churn) has no rows, so deleting it would be
/// a needless 0-row write.
///
/// This queries the DB directly rather than consulting an in-memory snapshot so
/// the watch path never has to prefetch the whole repo's file list — it mirrors
/// the `WHERE` clause of [`delete_file_rows_or_prefix`] so the pre-check and the
/// delete agree on what counts as a match.
fn path_has_indexed_rows(conn: &Connection, repo_id: i64, relative: &str) -> Result<bool> {
    let prefix = dir_prefix(relative);
    // `substr(...) = ?3` is a binary comparison; `LIKE` is case-insensitive
    // for ASCII by default and would match `src/foo/` for `src/Foo`.
    let found: Option<i64> = conn
        .query_row(
            r#"
            SELECT 1 FROM files
            WHERE repo_id = ?1
              AND (path = ?2 OR substr(path, 1, length(?3)) = ?3)
            LIMIT 1
            "#,
            params![repo_id, relative, prefix],
            |row| row.get(0),
        )
        .optional()?;
    Ok(found.is_some())
}

/// Indexed `(path, language)` pairs at or below `relative` (a directory; `""`
/// for the repo root). Used to purge files a directory re-walk no longer yields.
fn indexed_paths_under(
    conn: &Connection,
    repo_id: i64,
    relative: &str,
) -> Result<Vec<(String, String)>> {
    let prefix = dir_prefix(relative);
    let mut stmt = conn.prepare(
        "SELECT path, language FROM files WHERE repo_id = ?1 AND (?2 = '' OR substr(path, 1, length(?2)) = ?2)",
    )?;
    let rows = stmt
        .query_map(
            params![
                repo_id,
                if relative.is_empty() {
                    ""
                } else {
                    prefix.as_str()
                }
            ],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn dir_prefix(relative: &str) -> String {
    format!("{}/", relative.trim_end_matches('/'))
}

/// Delete rows for a changed path that no longer resolves to an indexed source
/// file. The path may be an exact file or a directory reported by the watcher;
/// in the directory case, purge all indexed files below that prefix. Returns the
/// number of file rows removed.
fn delete_file_rows_or_prefix(conn: &Connection, repo_id: i64, relative: &str) -> Result<usize> {
    let prefix = dir_prefix(relative);
    // `symbols` and `refs` both declare `ON DELETE CASCADE` on `file_id`, and
    // `foreign_keys=ON` is set on every writing connection (`create_schema`,
    // `update_paths_cached`), so deleting the `files` rows
    // purges their children automatically. For directory deletes/renames this is
    // a single statement instead of one DELETE per matched row.
    let deleted = conn.execute(
        r#"
        DELETE FROM files
        WHERE repo_id = ?1
          AND (path = ?2 OR substr(path, 1, length(?3)) = ?3)
        "#,
        params![repo_id, relative, prefix],
    )?;
    Ok(deleted)
}

#[allow(clippy::too_many_arguments)]
fn upsert_file(
    conn: &Connection,
    repo_id: i64,
    path: &str,
    language: &str,
    sha: &str,
    mtime_ns: i64,
    byte_size: i64,
    mut parsed: ParsedFile,
) -> Result<()> {
    conn.execute(
        r#"
        INSERT INTO files(repo_id, path, language, sha, mtime_ns, byte_size, partial)
        VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
        ON CONFLICT(repo_id, path) DO UPDATE SET
          language = excluded.language,
          sha = excluded.sha,
          mtime_ns = excluded.mtime_ns,
          byte_size = excluded.byte_size,
          partial = excluded.partial
        "#,
        params![
            repo_id,
            path,
            language,
            sha,
            mtime_ns,
            byte_size,
            parsed.partial
        ],
    )?;
    let file_id: i64 = conn.query_row(
        "SELECT id FROM files WHERE repo_id = ?1 AND path = ?2",
        params![repo_id, path],
        |row| row.get(0),
    )?;
    conn.execute("DELETE FROM symbols WHERE file_id = ?1", [file_id])?;
    conn.execute("DELETE FROM refs WHERE file_id = ?1", [file_id])?;

    parsed.symbols.sort_by_key(|symbol| {
        (
            symbol.start_row,
            symbol.start_col,
            symbol.end_row,
            symbol.end_col,
        )
    });
    let qualified = qualify_symbols(&parsed.symbols);
    for (symbol, qualified) in parsed.symbols.into_iter().zip(qualified) {
        conn.execute(
            r#"
            INSERT INTO symbols(
              file_id, kind, name, qualified, start_row, start_col, end_row, end_col, signature, docstring
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
            "#,
            params![
                file_id,
                symbol.kind,
                symbol.name,
                qualified,
                symbol.start_row as i64,
                symbol.start_col as i64,
                symbol.end_row as i64,
                symbol.end_col as i64,
                symbol.signature,
                symbol.docstring
            ],
        )?;
    }

    for reference in parsed.refs {
        conn.execute(
            r#"
            INSERT INTO refs(file_id, name, start_row, start_col, end_row, end_col, context)
            VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#,
            params![
                file_id,
                reference.name,
                reference.start_row as i64,
                reference.start_col as i64,
                reference.end_row as i64,
                reference.end_col as i64,
                reference.context
            ],
        )?;
    }
    Ok(())
}

fn purge_stale_files(
    conn: &Connection,
    active_paths: &HashMap<i64, HashSet<String>>,
    purge_languages: &HashMap<i64, Option<HashSet<String>>>,
    repo_roots: &HashMap<i64, PathBuf>,
) -> Result<()> {
    let mut stmt = conn.prepare(
        r#"
        SELECT id, repo_id, path, language
        FROM files
        "#,
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (file_id, repo_id, path, language) in rows {
        if !repo_roots.contains_key(&repo_id) {
            continue;
        }
        let source_exists = repo_roots
            .get(&repo_id)
            .is_some_and(|root| root.join(&path).exists());
        let is_active = active_paths
            .get(&repo_id)
            .is_some_and(|paths| paths.contains(&path));
        let in_purge_scope = match purge_languages.get(&repo_id) {
            Some(Some(languages)) => languages.contains(&language),
            Some(None) => true,
            None => true,
        };
        if !source_exists || (!is_active && in_purge_scope) {
            conn.execute("DELETE FROM files WHERE id = ?1", [file_id])?;
        }
    }
    Ok(())
}

fn parse_file(spec: &LanguageSpec, source: &[u8]) -> Result<ParsedFile> {
    let tree = parse_tree(spec, source)?;
    let (mut symbols, name_spans) = extract_symbols(spec, tree.root_node(), source)?;
    if spec.id == "markdown" {
        expand_markdown_heading_ranges(&mut symbols, source);
    }
    let refs = extract_refs(spec, tree.root_node(), source, &name_spans)?;
    Ok(ParsedFile {
        symbols,
        refs,
        partial: tree.root_node().has_error(),
    })
}

fn expand_markdown_heading_ranges(symbols: &mut [ExtractedSymbol], source: &[u8]) {
    let text = String::from_utf8_lossy(source);
    let lines: Vec<&str> = text.lines().collect();
    let heading_level = |symbol: &ExtractedSymbol| -> usize {
        let line = lines.get(symbol.start_row).copied().unwrap_or_default();
        let hashes = line.chars().take_while(|ch| *ch == '#').count();
        if hashes > 0 {
            return hashes;
        }
        match lines.get(symbol.start_row + 1).map(|line| line.trim()) {
            Some(underline) if underline.starts_with('=') => 1,
            Some(underline) if underline.starts_with('-') => 2,
            _ => 1,
        }
    };
    let levels: Vec<usize> = symbols.iter().map(heading_level).collect();
    let last_row = lines.len().saturating_sub(1);
    for index in 0..symbols.len() {
        if symbols[index].kind != "heading" {
            continue;
        }
        let next_boundary = ((index + 1)..symbols.len())
            .find(|next| symbols[*next].kind == "heading" && levels[*next] <= levels[index])
            .map(|next| symbols[next].start_row.saturating_sub(1))
            .unwrap_or(last_row);
        symbols[index].end_row = next_boundary;
        symbols[index].end_col = lines
            .get(next_boundary)
            .map(|line| line.len())
            .unwrap_or_default();
    }
}

fn parse_tree(spec: &LanguageSpec, source: &[u8]) -> Result<tree_sitter::Tree> {
    let mut parser: Parser = spec.parser()?;
    let tree = parser
        .parse(source, None)
        .ok_or_else(|| anyhow!("parser returned no tree"))?;
    Ok(tree)
}

/// Byte spans `(start, end)` of symbol-name nodes in one file.
type NameSpans = HashSet<(usize, usize)>;

/// Extract the symbols a language's symbol query captures, plus the byte
/// spans of their name nodes. `extract_refs` uses those spans to tag the
/// declaring occurrence of each symbol as a `declaration` regardless of what
/// the grammar calls the surrounding node.
fn extract_symbols(
    spec: &LanguageSpec,
    root: Node<'_>,
    source: &[u8],
) -> Result<(Vec<ExtractedSymbol>, NameSpans)> {
    let query = spec.symbol_query()?;
    let mut cursor = QueryCursor::new();
    let capture_names = query.capture_names();
    let mut symbols = Vec::new();
    let mut name_spans = HashSet::new();
    // Decoded once per file; `extract_docstring` used to redo this (and
    // re-split into lines) for every symbol, which made indexing a file
    // quadratic in its symbol count.
    let text = String::from_utf8_lossy(source);
    let lines: Vec<&str> = text.lines().collect();
    let mut matches = cursor.matches(&query, root, source);
    while let Some(matched) = matches.next() {
        let mut name_node = None;
        let mut name_capture = None;
        let mut def_node = None;
        let mut kind = None;
        for capture in matched.captures {
            let capture_name = &capture_names[capture.index as usize];
            if capture_name.ends_with(".name") {
                name_node = Some(capture.node);
                name_capture = Some(capture_name.to_string());
            } else if capture_name.ends_with(".def") || *capture_name == "import" {
                def_node = Some(capture.node);
                kind = Some(capture_name.trim_end_matches(".def").to_string());
            }
        }

        if let Some(node) = def_node
            && kind.as_deref() == Some("import")
        {
            let text = node
                .utf8_text(source)
                .unwrap_or_default()
                .trim()
                .to_string();
            symbols.push(ExtractedSymbol {
                kind: "import".to_string(),
                name: text.clone(),
                start_row: node.start_position().row,
                start_col: node.start_position().column,
                end_row: node.end_position().row,
                end_col: node.end_position().column,
                signature: Some(text),
                docstring: None,
            });
            continue;
        }

        let Some(node) = def_node else {
            continue;
        };
        let Some(name_node) = name_node else {
            continue;
        };
        let Some(kind_capture) = name_capture else {
            continue;
        };
        let name = name_node.utf8_text(source).unwrap_or_default().to_string();
        let kind = kind_capture.trim_end_matches(".name").to_string();
        // Value bindings keep their defining occurrence in find_references:
        // a JSON/YAML key, a Make target, or a variable assignment *is* the
        // hit a "where is X set" search wants, and every assignment is a
        // symbol so tagging them would hide all writes. Only declarations of
        // named entities (functions, types, classes, …) are echo noise.
        if !matches!(
            kind.as_str(),
            "variable" | "key" | "anchor" | "target" | "property" | "constant"
        ) {
            name_spans.insert((name_node.start_byte(), name_node.end_byte()));
        }
        // Some grammars wrap a definition in an outer node carrying its
        // leading decorators / annotations (e.g. Python's
        // `decorated_definition` wraps `function_definition` and
        // `class_definition`). When the captured `*.def` node sits inside
        // such a wrapper, expand its range upward so the recorded symbol
        // includes the decorator block. signature extraction continues to
        // use the inner node so it remains decorator-free.
        let range_node = expand_to_decorator_wrapper(spec, node);
        let mut start = range_node.start_position();
        // ponytail: Rust is the one grammar that puts outer attributes
        // (`#[derive]`, `#[test]`, …) as preceding *siblings* rather than in
        // a wrapper node, so the wrapper walk above can't see them. Hardcoded
        // here instead of a LanguageSpec field until a second grammar needs it.
        // `prev_named_sibling` is O(child index) in tree-sitter 0.25, so only
        // pay for it where it can match.
        let mut previous = if spec.id == "rust" {
            range_node.prev_named_sibling()
        } else {
            None
        };
        while let Some(sibling) = previous.filter(|sibling| sibling.kind() == "attribute_item") {
            start = sibling.start_position();
            previous = sibling.prev_named_sibling();
        }
        let signature = first_line(node, source);
        // Leading-comment docstrings are scanned from the first line the
        // symbol owns (decorators/attributes included), so a `///` block above
        // a `#[derive]` still attaches to the struct below it.
        let docstring = extract_docstring(spec, node, start.row, source, &lines);
        symbols.push(ExtractedSymbol {
            kind,
            name,
            start_row: start.row,
            start_col: start.column,
            end_row: range_node.end_position().row,
            end_col: range_node.end_position().column,
            signature,
            docstring,
        });
    }
    Ok((symbols, name_spans))
}

/// Walk up the AST while the parent node's kind is one of the language's
/// `decorator_wrappers`, returning the outermost matching ancestor (or
/// `node` itself if there is no wrapper). This lets us record symbol
/// ranges that include leading decorators/annotations whenever the grammar
/// puts them in a separate wrapper node.
fn expand_to_decorator_wrapper<'tree>(spec: &LanguageSpec, node: Node<'tree>) -> Node<'tree> {
    if spec.decorator_wrappers.is_empty() {
        return node;
    }
    let mut current = node;
    while let Some(parent) = current.parent() {
        if spec.decorator_wrappers.contains(&parent.kind()) {
            current = parent;
        } else {
            break;
        }
    }
    current
}

/// Extract identifier occurrences. `declaration_spans` holds the byte spans of
/// the file's symbol-name nodes (from `extract_symbols`): an occurrence at
/// exactly such a span is the symbol's own declaration and is tagged
/// `declaration` so `include_declarations: false` hides it in every language,
/// not only those whose declaring node happens to be named `*_declaration`.
fn extract_refs(
    spec: &LanguageSpec,
    root: Node<'_>,
    source: &[u8],
    declaration_spans: &HashSet<(usize, usize)>,
) -> Result<Vec<ExtractedRef>> {
    let query = spec.ref_query()?;
    let mut cursor = QueryCursor::new();
    let mut refs = Vec::new();
    // Some grammars capture the same reference twice when an inner node is
    // nested inside an outer one that both match the ref query — e.g. bash's
    // `command_name` wraps a `word`, and both are captured at the identical
    // span for a call. Dedupe by exact span + name so such overlaps yield one
    // reference; the first capture (the outer, more specific node) wins.
    let mut seen: HashSet<(usize, usize, usize, usize, String)> = HashSet::new();
    let mut captures = cursor.captures(&query, root, source);
    while let Some(capture) = captures.next() {
        let (matched, index) = capture;
        let node = matched.captures[*index].node;
        let name = node
            .utf8_text(source)
            .unwrap_or_default()
            .trim()
            .to_string();
        if name.is_empty() {
            continue;
        }
        let span = (
            node.start_position().row,
            node.start_position().column,
            node.end_position().row,
            node.end_position().column,
            name.clone(),
        );
        if !seen.insert(span) {
            continue;
        }
        let context = if declaration_spans.contains(&(node.start_byte(), node.end_byte())) {
            "declaration"
        } else {
            classify_reference(node)
        };
        refs.push(ExtractedRef {
            name,
            start_row: node.start_position().row,
            start_col: node.start_position().column,
            end_row: node.end_position().row,
            end_col: node.end_position().column,
            context: context.to_string(),
        });
    }
    Ok(refs)
}

fn classify_reference(node: Node<'_>) -> &'static str {
    if let Some(parent) = node.parent()
        && parent
            .child_by_field_name("name")
            .is_some_and(|name| name.id() == node.id())
        && matches!(
            parent.kind(),
            kind if kind.contains("declaration")
                || kind.contains("definition")
                || kind.ends_with("_item")
                || kind.ends_with("_specifier")
        )
    {
        return "declaration";
    }
    let mut current = Some(node);
    while let Some(item) = current {
        let kind = item.kind();
        if kind.contains("import") || kind.contains("use_declaration") {
            return "import";
        }
        // Java `method_invocation` / C# `invocation_expression` are calls too.
        if kind.contains("call") || kind.contains("invocation") || kind == "command" {
            return "call";
        }
        if kind.contains("type") || kind.contains("annotation") {
            return "type-ref";
        }
        if kind.contains("assignment") || kind.contains("declarator") {
            return "assignment";
        }
        current = item.parent();
    }
    "identifier"
}

/// Dotted container path for each symbol (`Outer.Inner.name`), or `None` for
/// a top-level symbol. Symbol ranges nest as a tree, so one pass over the
/// symbols in start order with a stack of open containers finds every
/// enclosing symbol — linear instead of the former all-predecessors scan,
/// which was quadratic in the symbol count of a file.
fn qualify_symbols(symbols: &[ExtractedSymbol]) -> Vec<Option<String>> {
    let mut order: Vec<usize> = (0..symbols.len()).collect();
    // Start ascending; for equal starts the longer range first, so a
    // container precedes the symbols it holds.
    order.sort_by_key(|&i| {
        let s = &symbols[i];
        (
            s.start_row,
            s.start_col,
            std::cmp::Reverse((s.end_row, s.end_col)),
        )
    });
    let mut qualified = vec![None; symbols.len()];
    let mut stack: Vec<usize> = Vec::new();
    for &index in &order {
        let symbol = &symbols[index];
        while let Some(&top) = stack.last() {
            if contains(&symbols[top], symbol) {
                break;
            }
            stack.pop();
        }
        if !stack.is_empty() {
            let mut parts: Vec<&str> = stack.iter().map(|&i| symbols[i].name.as_str()).collect();
            parts.push(&symbol.name);
            qualified[index] = Some(parts.join("."));
        }
        if symbol.kind != "import" {
            stack.push(index);
        }
    }
    qualified
}

fn contains(parent: &ExtractedSymbol, child: &ExtractedSymbol) -> bool {
    (parent.start_row, parent.start_col) <= (child.start_row, child.start_col)
        && (parent.end_row, parent.end_col) >= (child.end_row, child.end_col)
        && (parent.start_row, parent.start_col) != (child.start_row, child.start_col)
}

/// Docstring for `node`: a Python body docstring when the first statement is
/// a string literal (any prefix/quote style; only the literal contents are
/// kept), otherwise the run of comment lines immediately above `scan_row`.
/// `lines` is the file split once by the caller.
fn extract_docstring(
    spec: &LanguageSpec,
    node: Node<'_>,
    scan_row: usize,
    source: &[u8],
    lines: &[&str],
) -> Option<String> {
    if spec.id == "python"
        && let Some(body) = node.child_by_field_name("body")
        && let Some(first) = body.named_child(0)
        && first.kind() == "expression_statement"
        && let Some(string) = first.named_child(0)
        && matches!(string.kind(), "string" | "concatenated_string")
    {
        // Only a string literal is a docstring; the previous check accepted
        // any expression, so `self.x = x` or `print(...)` as a first
        // statement was stored as documentation. Read the `string_content`
        // nodes so prefixes (`r`, `b`, `f`) and quotes never leak into it.
        let mut content = String::new();
        let mut stack = vec![string];
        while let Some(current) = stack.pop() {
            if current.kind() == "string_content" {
                content.push_str(current.utf8_text(source).unwrap_or_default());
                continue;
            }
            let mut cursor = current.walk();
            let children: Vec<Node<'_>> = current.named_children(&mut cursor).collect();
            stack.extend(children.into_iter().rev());
        }
        if !content.is_empty() {
            return Some(content);
        }
    }

    if scan_row == 0 {
        return None;
    }
    let mut collected = Vec::new();
    let mut row = scan_row;
    while row > 0 {
        row -= 1;
        let line = lines.get(row)?.trim();
        if line.is_empty() {
            if collected.is_empty() {
                continue;
            }
            break;
        }
        if spec
            .comment_prefixes
            .iter()
            .any(|prefix| line.starts_with(prefix))
        {
            collected.push(line.to_string());
        } else {
            break;
        }
    }

    if collected.is_empty() {
        None
    } else {
        collected.reverse();
        Some(
            collected
                .into_iter()
                .map(|line| {
                    // Strip the comment marker plus any run of its last
                    // character, so `///` and `//!` (Rust docs) or `##` don't
                    // leak a stray `/`/`#` into the docstring.
                    match spec
                        .comment_prefixes
                        .iter()
                        .filter(|prefix| line.starts_with(*prefix))
                        .max_by_key(|prefix| prefix.len())
                    {
                        Some(prefix) => {
                            let rest = &line[prefix.len()..];
                            let marker = prefix.chars().last().unwrap_or(' ');
                            rest.trim_start_matches(marker).trim().to_string()
                        }
                        None => line,
                    }
                })
                .collect::<Vec<_>>()
                .join("\n"),
        )
    }
}

fn first_line(node: Node<'_>, source: &[u8]) -> Option<String> {
    let text = node.utf8_text(source).ok()?;
    text.lines().next().map(|line| line.trim().to_string())
}

fn relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut sha = Sha256::new();
    sha.update(bytes);
    format!("{:x}", sha.finalize())
}

fn is_binary(bytes: &[u8]) -> bool {
    bytes.iter().take(1024).any(|byte| *byte == 0)
}

fn build_glob_matcher(pattern: &str) -> Result<GlobMatcher> {
    Ok(Glob::new(pattern)?.compile_matcher())
}

/// Options that govern how a symbol body is sliced out of its source file.
///
/// Phase A introduces this struct as the single arg shape for `extract_body`
/// so future enhancements (leading-comment scan, dedent, head+tail
/// truncation, byte-precise ranges) can be added without rippling new
/// parameters through every call site.
#[derive(Debug, Clone, Default)]
pub struct BodyOpts {
    /// Extra lines of context to include before and after the symbol body.
    pub context_lines: usize,
    /// When set, bodies longer than this many lines have their middle elided
    /// (head + tail kept) with a marker reporting the true line count. The
    /// read endpoints resolve `None` to the server's `default_body_line_limit`
    /// so bodies are bounded by default; pass a large value (e.g.
    /// `max_body_line_limit`) for a full body.
    pub max_body_lines: Option<usize>,
}

/// Collapse the middle of a body that exceeds `max_lines`, keeping `max_lines`
/// total source lines split between head and tail, plus a single marker line
/// that reports the true line count. Returns the body unchanged when
/// `max_lines` is `None`/`0` or the body already fits. The marker keeps
/// truncation explicit so an agent never mistakes an elided body for a complete
/// short one.
fn elide_body(body: String, max_lines: Option<usize>) -> String {
    let Some(max) = max_lines.filter(|m| *m > 0) else {
        return body;
    };
    let lines: Vec<&str> = body.lines().collect();
    let total = lines.len();
    if total <= max {
        return body;
    }
    let head = max / 2;
    let tail = max - head;
    let elided = total - head - tail;
    let marker = format!("… {elided} lines elided ({total} total) …");
    let mut parts: Vec<&str> = Vec::with_capacity(max + 1);
    parts.extend_from_slice(&lines[..head]);
    parts.push(&marker);
    parts.extend_from_slice(&lines[total - tail..]);
    parts.join("\n")
}

/// Walk an outline tree and inline the source body of every node whose name
/// is in `wanted`. Recurses into children so nested symbols (methods, etc.)
/// at the requested depth are covered too. The file is read once and sliced
/// in memory for every match, so inlining K bodies costs one read, not K.
///
/// SHA-verified: read the file once, confirm its SHA matches `expected_sha`
/// (so a stale index can't attach the wrong source), then slice every wanted
/// body from the in-memory lines. One read + one hash covers all inlined
/// bodies for the file.
fn attach_outline_bodies_verified(
    symbols: &mut [OutlineSymbol],
    wanted: &HashSet<&str>,
    path: &Path,
    expected_sha: &str,
    max_body_lines: Option<usize>,
) -> Result<()> {
    let bytes = read_verified(path, expected_sha)?;
    let source = std::str::from_utf8(&bytes).unwrap_or_default();
    let lines: Vec<String> = source.lines().map(str::to_string).collect();
    attach_outline_bodies_from_lines(symbols, wanted, &lines, path, max_body_lines)
}

fn attach_outline_bodies_from_lines(
    symbols: &mut [OutlineSymbol],
    wanted: &HashSet<&str>,
    lines: &[String],
    path: &Path,
    max_body_lines: Option<usize>,
) -> Result<()> {
    for symbol in symbols.iter_mut() {
        if wanted.contains(symbol.name.as_str())
            && let Some(range) = &symbol.range
        {
            let body = slice_source_lines(lines, range.start.0, range.end.0, 0, path)?;
            symbol.body = Some(elide_body(body, max_body_lines));
        }
        if !symbol.children.is_empty() {
            attach_outline_bodies_from_lines(
                &mut symbol.children,
                wanted,
                lines,
                path,
                max_body_lines,
            )?;
        }
    }
    Ok(())
}

/// Record on every requested symbol that its body could not be inlined, so
/// the caller sees the reason next to the symbol instead of a silent gap.
fn mark_outline_bodies_unavailable(
    symbols: &mut [OutlineSymbol],
    wanted: &HashSet<&str>,
    reason: &str,
) {
    for symbol in symbols.iter_mut() {
        if wanted.contains(symbol.name.as_str()) && symbol.body.is_none() {
            symbol.body_unavailable = Some(reason.to_string());
        }
        mark_outline_bodies_unavailable(&mut symbol.children, wanted, reason);
    }
}

#[cfg(test)]
fn read_source_lines(path: &Path) -> Result<Vec<String>> {
    let file =
        fs::File::open(path).with_context(|| format!("failed to read {}", path.display()))?;
    BufReader::new(file)
        .lines()
        .collect::<std::io::Result<Vec<_>>>()
        .with_context(|| format!("failed to read {}", path.display()))
}

/// True when an error message was produced by a SHA-mismatch (stale-index)
/// check, so callers can downgrade a body/snippet failure to a partial result
/// while still surfacing the cause. The check is string-based because the
/// errors flow through `anyhow` and don't carry a typed variant.
fn is_stale_index_error(error: &anyhow::Error) -> bool {
    format!("{error:#}").contains("index is stale")
}

/// Join lines `[start_row - context .. end_row + context]`, clamped to the
/// file's bounds. The DB can hold stale ranges — e.g. after a schema bump
/// where the file shrunk since indexing — so start_row may point past the
/// file's current end. Without clamping, `lines[start..=end]` would panic with
/// "slice index starts at X but ends at Y" when saturating_sub leaves
/// start > end. Clamp both ends; if the clamped start is still past the clamped
/// end — i.e. the recorded range no longer overlaps the file — return a typed
/// error rather than panicking, so callers can surface "run `tsindex build`".
fn slice_source_lines(
    lines: &[String],
    start_row: usize,
    end_row: usize,
    context_lines: usize,
    path: &Path,
) -> Result<String> {
    if lines.is_empty() {
        return Ok(String::new());
    }
    let last = lines.len().saturating_sub(1);
    let start = start_row.saturating_sub(context_lines).min(last);
    let end = end_row.saturating_add(context_lines).min(last);
    if start > end {
        return Err(anyhow!(
            "stored symbol range [{start_row}..={end_row}] for {} is outside the \
             current file (now {} line(s)); index appears stale, run `tsindex build`",
            path.display(),
            lines.len()
        ));
    }
    Ok(lines[start..=end].join("\n"))
}

/// Read `path` and return the slice of lines covering the symbol's range,
/// padded by `opts.context_lines` on each side.
///
/// This is the single chokepoint for "given a symbol's line range, give me
/// its source text." Behavior is identical to the older `slice_with_context`
/// helper today; Phase C will extend `BodyOpts` with `include_leading_comments`,
/// `dedent`, and head+tail truncation, and that work lands here.
///
/// Only the unit tests exercise the unverified path now; every live read
/// endpoint routes through [`verified_slice`] instead so a stale index can't
/// return the wrong body.
#[cfg(test)]
fn extract_body(path: &Path, start_row: usize, end_row: usize, opts: &BodyOpts) -> Result<String> {
    let lines = read_source_lines(path)?;
    let body = slice_source_lines(&lines, start_row, end_row, opts.context_lines, path)?;
    Ok(elide_body(body, opts.max_body_lines))
}

/// Verify the file on disk still matches the SHA captured at index time
/// before trusting a stored line range to slice it. Returns the live bytes
/// (hashed and verified) so callers can slice from memory without a second
/// read. A mismatch means the index is stale — the recorded range may no
/// longer point at the right text — so this surfaces a clear, actionable
/// error rather than silently returning the wrong body/snippet.
fn read_verified(path: &Path, expected_sha: &str) -> Result<Vec<u8>> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let actual = sha256_hex(&bytes);
    if actual != expected_sha {
        return Err(anyhow!(
            "index is stale for {}: stored source sha {} no longer matches the file on disk \
             (now {}); run `tsindex build` before reading bodies/snippets",
            path.display(),
            expected_sha,
            actual
        ));
    }
    Ok(bytes)
}

/// Read `path`, confirm its SHA matches `expected_sha`, then return the slice
/// of lines covering `[start_row..=end_row]` padded by `context_lines`. The
/// SHA check is the guard against slicing a live file with a stale index
/// range: a mismatch returns a typed "run `tsindex build`" error instead of
/// the wrong text.
fn verified_slice(
    path: &Path,
    expected_sha: &str,
    start_row: usize,
    end_row: usize,
    context_lines: usize,
) -> Result<String> {
    let bytes = read_verified(path, expected_sha)?;
    let source = std::str::from_utf8(&bytes).unwrap_or_default();
    let lines: Vec<String> = source.lines().map(str::to_string).collect();
    let body = slice_source_lines(&lines, start_row, end_row, context_lines, path)?;
    Ok(body)
}

/// Try to relocate a symbol whose indexed range is stale by re-parsing the live
/// file and matching on name (required), kind, qualified name, and range
/// proximity. Returns the relocated `(start_row, end_row)` so the caller can
/// slice the body from live source, or `None` when no confident match exists.
///
/// Disambiguation: candidates must share the symbol's `name`. When the index
/// recorded a `kind`, candidates of a different kind are demoted. When a
/// `qualified` name was recorded, an exact qualified match wins outright. Among
/// the remaining candidates, the one whose indexed start row is closest to the
/// stale start row wins (edits usually move a symbol by a few lines, not
/// across the file) — but a kind mismatch can only win when nothing else is
/// available, so we never relocate a `function` onto a `class` of the same name
/// when a same-kind candidate exists. Ties (same name, same kind, equidistant)
/// are ambiguous and return `None` rather than guessing.
fn relocate_symbol_range(
    spec: &LanguageSpec,
    bytes: &[u8],
    locator: &StaleSymbolLocator<'_>,
) -> Option<(usize, usize)> {
    let parsed = parse_file(spec, bytes).ok()?;
    let candidates: Vec<&ExtractedSymbol> = parsed
        .symbols
        .iter()
        .filter(|symbol| symbol.name == locator.name)
        .collect();
    if candidates.is_empty() {
        return None;
    }
    let kind_matches = |c: &&ExtractedSymbol| locator.kind.is_empty() || c.kind == locator.kind;
    let same_kind: Vec<&ExtractedSymbol> =
        candidates.iter().copied().filter(kind_matches).collect();
    let pool: Vec<&ExtractedSymbol> = if same_kind.is_empty() {
        candidates.to_vec()
    } else {
        same_kind
    };

    let stale_mid = (locator.start_row + locator.end_row) / 2;
    // Score by proximity of the candidate's midpoint to the stale midpoint.
    // Smaller is better. Track the best and whether it is uniquely best.
    let mut best: Option<(usize, (usize, usize))> = None;
    let mut tied = false;
    for candidate in pool {
        let cand_mid = (candidate.start_row + candidate.end_row) / 2;
        let dist = cand_mid.abs_diff(stale_mid);
        match best {
            None => best = Some((dist, (candidate.start_row, candidate.end_row))),
            Some((bd, _)) if dist < bd => {
                best = Some((dist, (candidate.start_row, candidate.end_row)));
                tied = false;
            }
            Some((bd, _)) if dist == bd => tied = true,
            _ => {}
        }
    }
    match best {
        Some((_, range)) if !tied => Some(range),
        _ => None,
    }
}

/// Resolve a symbol body for an indexed match, tolerating a stale index per
/// match rather than failing the whole batch. On a SHA mismatch we re-parse the
/// live file and try to relocate the symbol (see [`relocate_symbol_range`]);
/// if relocation succeeds the live body is sliced. If it fails (ambiguous or
/// not found) the body is omitted; callers expose `stale` plus an actionable
/// `body_unavailable` reason instead of wrong text. Clean matches are preserved
/// unchanged.
fn verified_slice_or_relocate(
    spec: Option<&LanguageSpec>,
    path: &Path,
    expected_sha: &str,
    locator: &StaleSymbolLocator<'_>,
    context_lines: usize,
) -> (Option<String>, bool) {
    match verified_slice(
        path,
        expected_sha,
        locator.start_row,
        locator.end_row,
        context_lines,
    ) {
        Ok(body) => (Some(body), false),
        Err(e) if is_stale_index_error(&e) => {
            // Stale index: try to relocate by re-parsing live source. A failure
            // to read the file at all means we can't relocate either — omit the
            // body with a per-match partial signal. Read the bytes once and
            // reuse for both relocation and (on success) slicing the live body.
            let bytes = fs::read(path).ok();
            let relocated = bytes.as_ref().and_then(|bytes| {
                spec.as_ref()
                    .and_then(|spec| relocate_symbol_range(spec, bytes, locator))
            });
            match relocated {
                Some((start_row, end_row)) => {
                    // Slice from the freshly read bytes (no SHA check — we
                    // already know the index is stale; the live text IS the
                    // source of truth now). Reparse confirmed the relocated
                    // range is a real symbol, so this is safe.
                    let body = bytes.as_ref().and_then(|bytes| {
                        let source = std::str::from_utf8(bytes).unwrap_or_default();
                        let lines: Vec<String> = source.lines().map(str::to_string).collect();
                        slice_source_lines(&lines, start_row, end_row, context_lines, path).ok()
                    });
                    match body {
                        Some(body) => (Some(body), true),
                        None => (None, true),
                    }
                }
                None => (None, true),
            }
        }
        Err(e) => {
            // Non-stale errors (e.g. file vanished) still shouldn't kill the
            // batch — omit the body with a partial signal. Log once so the
            // cause is discoverable without bloating the response.
            eprintln!("warning: body omitted for {}: {e}", path.display());
            (None, true)
        }
    }
}

/// Byte offset at which each 0-based line starts. `offsets[row]` is the
/// index just past the previous line's `\n` (0 for the first line). Has
/// one entry per line, so `offsets.len()` is the line count.
fn line_start_offsets(source: &str) -> Vec<usize> {
    let mut offsets = vec![0usize];
    for (index, byte) in source.bytes().enumerate() {
        if byte == b'\n' {
            offsets.push(index + 1);
        }
    }
    offsets
}

/// Replace whole lines `start_row..=end_row` of `source` with `new_body`,
/// preserving the terminating newline. Refuses (rather than silently
/// clobbering) when the symbol shares its first or last line with other
/// code, so a sibling statement on the same line is never destroyed.
fn splice_symbol_lines(
    source: &str,
    start_row: usize,
    start_col: usize,
    end_row: usize,
    end_col: usize,
    new_body: &str,
) -> Result<String> {
    let line_starts = line_start_offsets(source);
    if start_row >= line_starts.len() || end_row >= line_starts.len() || start_row > end_row {
        return Err(anyhow!(
            "symbol range [{start_row}..={end_row}] is outside the file; \
             the source changed under us"
        ));
    }
    let start_off = line_starts[start_row];
    // Anything before the symbol on its start line must be whitespace
    // (i.e. indentation), or a whole-line replace would eat it.
    let prefix_end = (start_off + start_col).min(source.len());
    if !source[start_off..prefix_end].trim().is_empty() {
        return Err(anyhow!(
            "symbol shares its start line with other code; replace_symbol \
             only edits symbols that own their lines"
        ));
    }
    // Byte just before the terminator of `end_row` (or EOF for the last
    // line). The suffix begins here so the terminator is preserved — `\r\n`
    // included, so a CRLF file does not end up with one LF-only line.
    let mut end_content_off = line_starts
        .get(end_row + 1)
        .map(|next| next - 1)
        .unwrap_or(source.len());
    if end_content_off > line_starts[end_row] && source.as_bytes()[end_content_off - 1] == b'\r' {
        end_content_off -= 1;
    }
    let suffix_start = (line_starts[end_row] + end_col).min(end_content_off);
    if !source[suffix_start..end_content_off].trim().is_empty() {
        return Err(anyhow!(
            "symbol shares its end line with other code; replace_symbol \
             only edits symbols that own their lines"
        ));
    }

    let mut out =
        String::with_capacity(start_off + new_body.len() + (source.len() - end_content_off));
    out.push_str(&source[..start_off]);
    out.push_str(new_body);
    out.push_str(&source[end_content_off..]);
    Ok(out)
}

/// The line terminator a file predominantly uses: `\r\n` when most of its
/// newlines are CRLF, `\n` otherwise (including files with no newline).
fn line_terminator(source: &str) -> &'static str {
    let crlf = source.matches("\r\n").count();
    let lf = source.matches('\n').count() - crlf;
    if crlf > lf { "\r\n" } else { "\n" }
}

/// Open a fresh temp file next to `target` with `create_new`, so an existing
/// path (in particular a symlink planted at a guessable name) is refused
/// rather than followed. Returns the path for the later rename/cleanup.
fn create_exclusive_sibling(target: &Path) -> Result<(PathBuf, fs::File)> {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let base = target
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    for _ in 0..32 {
        let nonce = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = target.with_file_name(format!(
            ".{base}.tsindex-tmp.{}.{nonce}",
            std::process::id()
        ));
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    bail!("could not create a temp file next to {}", target.display())
}

/// First ERROR or MISSING node in pre-order, as a 0-based `(row, col)`.
/// Used to point the caller at where a rejected edit broke parsing.
fn first_error(node: Node<'_>) -> Option<(usize, usize)> {
    if node.is_error() || node.is_missing() {
        let point = node.start_position();
        return Some((point.row, point.column));
    }
    if !node.has_error() {
        return None;
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if let Some(found) = first_error(child) {
            return Some(found);
        }
    }
    None
}

fn one_line_snippet(snippet: &str) -> String {
    snippet
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or_default()
        .to_string()
}

/// Slice the reference's line range from already-verified source lines and
/// shape it per `snippet_lines`. The default of 1 keeps the original one-line
/// behavior — collapse the slice to the reference's own (first non-empty)
/// line, so no padding leaks in as a leading context line. A larger value
/// pads the slice with `snippet_lines - 1` surrounding lines and keeps them
/// verbatim, giving multi-line context without flattening. A stale range
/// (start past the clamped end) degrades to an empty snippet rather than
/// panicking.
/// Returns the snippet plus, for multi-line snippets, the row its FIRST line
/// sits on — in the same coordinate system as the reference's `range`.
///
/// Without that anchor a caller receiving `snippet_lines: 3` gets a five-line
/// block with the reference somewhere in the middle and a single `range`, and
/// cannot tell which snippet line the range denotes. Observed failure: an agent
/// assumed `range` pointed at the snippet's first line and reported every call
/// site two lines off. Single-line snippets need no anchor — `range` is the line.
fn ref_snippet(
    lines: &[String],
    start_row: usize,
    end_row: usize,
    snippet_lines: usize,
) -> (String, Option<usize>) {
    let context = snippet_lines.saturating_sub(1);
    match slice_source_lines(lines, start_row, end_row, context, Path::new("<verified>")) {
        Ok(slice) => {
            if snippet_lines <= 1 || slice.is_empty() {
                (one_line_snippet(&slice), None)
            } else {
                // Mirror slice_source_lines' own clamping so the anchor always
                // matches the text actually returned.
                let last = lines.len().saturating_sub(1);
                let first = start_row.saturating_sub(context).min(last);
                (slice, Some(first))
            }
        }
        Err(_) => (String::new(), None),
    }
}

/// Truncate `text` to at most `max_chars` Unicode scalar values, appending an
/// ellipsis marker when something was cut. Returns `(truncated_text, was_cut)`.
/// `max_chars` of 0 disables the cap (returns the text whole). Counting scalar
/// values (not bytes) keeps the cap meaningful for non-ASCII source without
/// splitting a multi-byte character mid-codepoint.
fn truncate_chars(text: &str, max_chars: usize) -> (String, bool) {
    if max_chars == 0 {
        return (text.to_string(), false);
    }
    // Byte length is a cheap upper bound on char count: a string with
    // `len <= max_chars` bytes cannot have more than `max_chars` chars, so
    // the common small-capture case short-circuits without any decoding.
    if text.len() <= max_chars {
        return (text.to_string(), false);
    }
    // `chars().count()` compiles to a vectorized continuation-byte scan
    // (~16 bytes/cycle) but always scans the whole string; a capped
    // `char_indices` walk costs ~1-2ns/char but stops at the cap. The
    // vectorized count wins whenever the string is within ~16x the cap;
    // beyond that (e.g. a 50KB whole-file capture cut at 1000 chars),
    // walking to the cap is cheaper than scanning the tail.
    if text.len() <= max_chars.saturating_mul(16) {
        if text.chars().count() <= max_chars {
            return (text.to_string(), false);
        }
        let end = text
            .char_indices()
            .nth(max_chars.saturating_sub(1))
            .map(|(i, _)| i)
            .unwrap_or(text.len());
        return (format!("{}…", &text[..end]), true);
    }
    // Single capped walk: record the start of the max_chars-th char (the
    // slice end if truncation is needed), then peek one position further to
    // decide whether a (max_chars+1)-th char exists — one pass instead of
    // two `nth()` walks over the same prefix.
    let mut iter = text.char_indices();
    let Some((end, _)) = iter.nth(max_chars.saturating_sub(1)) else {
        return (text.to_string(), false);
    };
    if iter.next().is_none() {
        (text.to_string(), false)
    } else {
        (format!("{}…", &text[..end]), true)
    }
}

fn looks_like_test_path(path: &str) -> bool {
    let path = path.replace('\\', "/");
    let file_name = path.rsplit('/').next().unwrap_or(path.as_str());
    path.starts_with("test/")
        || path.starts_with("tests/")
        || path.contains("/test/")
        || path.contains("/tests/")
        || file_name.ends_with("_test.py")
        || (file_name.starts_with("test_") && file_name.ends_with(".py"))
        || file_name.ends_with("_test.go")
        || file_name.ends_with("_test.rs")
        || file_name.ends_with(".spec.ts")
        || file_name.ends_with(".test.ts")
        || file_name.ends_with(".spec.tsx")
        || file_name.ends_with(".test.tsx")
        || file_name.ends_with(".spec.js")
        || file_name.ends_with(".test.js")
        || file_name.ends_with(".spec.jsx")
        || file_name.ends_with(".test.jsx")
}

/// Control common generated directories and files that should not trigger a
/// rebuild when the OS reports activity inside them. `notify` does not apply
/// `.gitignore`/`.tsindexignore`, so this suppresses events from Git metadata,
/// dependency caches, framework build output, and our own local tool state.
const WATCH_IGNORED_DIR_COMPONENTS: &[&str] = &[
    ".cache",
    ".git",
    ".logs",
    ".next",
    ".nuxt",
    ".parcel-cache",
    ".pids",
    ".playwright-mcp",
    ".pnpm-store",
    ".svelte-kit",
    ".sst",
    ".tmp",
    ".turbo",
    ".vercel",
    ".vite",
    ".yarn",
    "coverage",
    "node_modules",
    "tmp",
];

const WATCH_IGNORED_FILE_NAMES: &[&str] = &[".DS_Store"];

const WATCH_IGNORED_FILE_SUFFIXES: &[&str] = &[".tsbuildinfo"];

const WATCH_IGNORED_COMPONENT_PATHS: &[&[&str]] = &[&[".claude", "worktrees"]];

/// Returns true if a changed path could plausibly affect the index. Used to
/// suppress rebuild triggers for internal metadata; the actual ignore rules are
/// still enforced by the indexer's walk.
///
/// Ignore-dir components are matched against the path *relative to the root of
/// the workspace that contains it*, not relative to the database directory.
/// `db_dir` is the catalog root only for the default DB layout; with
/// `config.repos` or a custom `--db` location the catalog and the watched repo
/// need not be parent/child at all, so anchoring on `db_dir.parent()` would
/// either fall back to the absolute path (dropping every event whose absolute
/// prefix contains an ignored component like `/tmp`) or strip a prefix that
/// isn't actually the repo root.
fn watch_path_is_relevant(path: &Path, db_dir: &Path, workspaces: &[Workspace]) -> bool {
    if path.starts_with(db_dir) {
        return false;
    }
    if path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            WATCH_IGNORED_FILE_NAMES.contains(&name)
                || WATCH_IGNORED_FILE_SUFFIXES
                    .iter()
                    .any(|suffix| name.ends_with(suffix))
        })
    {
        return false;
    }
    // Match ignored components against the path *relative to the repo root*.
    // Absolute system prefixes (e.g. `/tmp` on Linux, where tests place
    // tempdirs) are outside the repo and must not trigger repo-level ignore
    // rules; the watch filter exists to suppress churn *inside* watched repos.
    // Paths outside every watched workspace keep their full absolute path —
    // their components are genuinely not repo-relative.
    let relative = best_workspace_for_path(workspaces, path)
        .and_then(|idx| path.strip_prefix(&workspaces[idx].root).ok())
        .unwrap_or(path);
    let components: Vec<&str> = relative
        .components()
        .filter_map(|component| component.as_os_str().to_str())
        .collect();
    if WATCH_IGNORED_COMPONENT_PATHS.iter().any(|needle| {
        components
            .windows(needle.len())
            .any(|window| window == *needle)
    }) {
        return false;
    }
    !components
        .iter()
        .any(|name| WATCH_IGNORED_DIR_COMPONENTS.contains(name))
}

fn run_watch_event_loop(
    rx: std::sync::mpsc::Receiver<DebounceEventResult>,
    db_dir: &Path,
    workspaces: &[Workspace],
    mut update: impl FnMut(&[PathBuf]) -> Result<BuildStats>,
) {
    for result in rx {
        match result {
            Ok(events) => {
                // Collect the distinct changed paths, dropping those under
                // high-churn ignored directories. We then update only those
                // paths instead of re-walking every repo.
                let mut changed: Vec<PathBuf> = events
                    .iter()
                    .flat_map(|event| event.paths.iter())
                    .filter(|path| watch_path_is_relevant(path, db_dir, workspaces))
                    .cloned()
                    .collect();
                changed.sort();
                changed.dedup();
                if !changed.is_empty()
                    && let Err(error) = update(&changed)
                {
                    eprintln!("watch update failed: {error}");
                }
            }
            Err(errors) => {
                for error in errors {
                    eprintln!("watch error: {error}");
                }
            }
        }
    }
}

fn build_outline(
    symbols: &[IndexedSymbol],
    depth: usize,
    include_signatures: bool,
    include_docstrings: bool,
) -> Vec<OutlineSymbol> {
    fn build_node(
        symbol: &IndexedSymbol,
        children: &[IndexedSymbol],
        depth: usize,
        include_signatures: bool,
        include_docstrings: bool,
    ) -> OutlineSymbol {
        OutlineSymbol {
            kind: symbol.kind.clone(),
            name: symbol.name.clone(),
            signature: include_signatures
                .then(|| symbol.signature.clone())
                .flatten(),
            docstring: include_docstrings
                .then(|| symbol.docstring.clone())
                .flatten(),
            range: Some(SourceRange {
                start: RangePoint(symbol.start_row, symbol.start_col),
                end: RangePoint(symbol.end_row, symbol.end_col),
            }),
            body: None,
            body_unavailable: None,
            children: if depth > 1 {
                build_outline(children, depth - 1, include_signatures, include_docstrings)
            } else {
                Vec::new()
            },
        }
    }

    // `symbols` arrive ordered by (start, end) from SQL, so the nesting tree
    // falls out of one pass with a stack of open containers: each symbol is a
    // direct child of the innermost open symbol that still encloses it, or a
    // root when none does. Linear, where the previous all-pairs containment
    // scan was quadratic in the symbol count of a file.
    fn encloses(outer: &IndexedSymbol, inner: &IndexedSymbol) -> bool {
        (outer.start_row, outer.start_col) <= (inner.start_row, inner.start_col)
            && (outer.end_row, outer.end_col) >= (inner.end_row, inner.end_col)
            && (outer.start_row, outer.start_col) != (inner.start_row, inner.start_col)
    }
    let mut direct_children: Vec<Vec<IndexedSymbol>> = vec![Vec::new(); symbols.len()];
    let mut root_indices = Vec::new();
    let mut stack: Vec<usize> = Vec::new();
    for (index, symbol) in symbols.iter().enumerate() {
        while let Some(&top) = stack.last() {
            if encloses(&symbols[top], symbol) {
                break;
            }
            stack.pop();
        }
        match stack.last() {
            Some(&parent) => direct_children[parent].push(symbol.clone()),
            None => root_indices.push(index),
        }
        stack.push(index);
    }
    root_indices
        .into_iter()
        .map(|index| {
            build_node(
                &symbols[index],
                &direct_children[index],
                depth,
                include_signatures,
                include_docstrings,
            )
        })
        .collect()
}

pub fn print_json<T: serde::Serialize>(value: &T) -> Result<()> {
    // Compact, not pretty: `--json` is for machine/agent consumption, where
    // indentation is pure context bloat (see issue #38). Humans wanting a
    // readable view can pipe through `jq`.
    println!("{}", serde_json::to_string(value)?);
    Ok(())
}

pub fn print_language_reports(reports: &[RepoLanguageReport], min_share: f64) {
    for report in reports {
        println!("{}  {}", report.repo, report.root);
        for row in &report.languages {
            if row.confidence < min_share {
                continue;
            }
            println!(
                "  {:<12} {:>8} {:>7.2}%",
                row.language,
                row.files,
                row.confidence * 100.0
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::{Event, EventKind};
    use notify_debouncer_full::DebouncedEvent;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };
    use std::thread;
    use tempfile::tempdir;

    // --- Storage / build / watch regressions (review findings F02-F33) -------

    // --- PR 71 review follow-ups (fork B) ------------------------------------

    fn unfiltered_runtime(root: &Path) -> Runtime {
        Runtime::new(
            root.to_path_buf(),
            root.join(".tsindex").join("index.db"),
            TsIndexConfig::default(),
            Vec::new(),
        )
    }

    /// B2: a configured repo whose clone is temporarily missing keeps its rows.
    #[test]
    fn build_keeps_rows_of_configured_repo_whose_clone_is_missing() -> Result<()> {
        let dir = tempdir()?;
        let root = dir.path().canonicalize()?;
        let (a, b) = (root.join("a"), root.join("b"));
        fs::create_dir_all(&a)?;
        fs::create_dir_all(&b)?;
        fs::write(a.join("a.py"), "def alpha():\n    return 1\n")?;
        fs::write(b.join("b.py"), "def beta():\n    return 2\n")?;
        let runtime = catalog_runtime(&root, &[("a", &a), ("b", &b)]);
        runtime.build(false, None)?;
        assert_eq!(file_paths(&runtime.db_path).len(), 2);

        fs::remove_dir_all(&b)?;
        runtime.build(false, None)?;
        assert!(
            file_paths(&runtime.db_path).contains(&("b".to_string(), "b.py".to_string())),
            "an unavailable clone is still configured and must not be pruned"
        );

        // Dropping it from the config is what prunes it.
        let pruned = catalog_runtime(&root, &[("a", &a)]);
        pruned.build(false, None)?;
        assert_eq!(
            file_paths(&pruned.db_path),
            vec![("a".to_string(), "a.py".to_string())]
        );
        Ok(())
    }

    /// B3: an `index_state` table with no singleton row yet reads as not ready.
    #[test]
    fn missing_index_state_row_is_not_ready() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            "CREATE TABLE index_state (id INTEGER PRIMARY KEY CHECK(id = 1), ready INTEGER NOT NULL DEFAULT 0)",
        )?;
        let error =
            ensure_index_ready(&conn).expect_err("no row means the build has not completed");
        assert!(format!("{error:#}").contains("not ready"), "{error:#}");
        Ok(())
    }

    /// B4: a directory event under a `--languages` filter leaves other
    /// languages' rows alone, as the full build does via `purge_languages`.
    #[test]
    fn directory_event_under_language_filter_keeps_other_languages() -> Result<()> {
        let dir = tempdir()?;
        let root = dir.path().canonicalize()?;
        fs::create_dir_all(root.join("src"))?;
        fs::write(root.join("src/a.py"), "def alpha():\n    return 1\n")?;
        fs::write(root.join("src/lib.rs"), "pub fn lib() {}\n")?;
        unfiltered_runtime(&root).build(false, None)?;
        assert_eq!(file_paths(&root.join(".tsindex/index.db")).len(), 2);

        python_runtime(&root).update_paths_cached(&[root.join("src")], &mut None)?;
        assert_eq!(
            file_paths(&root.join(".tsindex/index.db"))
                .iter()
                .map(|(_, p)| p.as_str())
                .collect::<Vec<_>>(),
            vec!["src/a.py", "src/lib.rs"],
            "rust rows are out of scope for a python-only watcher, not stale"
        );
        Ok(())
    }

    /// B5: a previously indexed file that becomes unreadable keeps its rows
    /// on a full build (only a file that is really gone is purged).
    #[cfg(unix)]
    #[test]
    fn full_build_keeps_rows_of_file_that_became_unreadable() -> Result<()> {
        let dir = tempdir()?;
        let root = dir.path().canonicalize()?;
        fs::write(root.join("a.py"), "def alpha():\n    return 1\n")?;
        fs::write(root.join("b.py"), "def beta():\n    return 2\n")?;
        let runtime = python_runtime(&root);
        runtime.build(false, None)?;
        if !make_unreadable(&root.join("b.py")) {
            return Ok(());
        }
        let stats = runtime.build(false, None)?;
        assert_eq!(stats.failed, 1);
        assert_eq!(
            file_paths(&runtime.db_path)
                .iter()
                .map(|(_, p)| p.as_str())
                .collect::<Vec<_>>(),
            vec!["a.py", "b.py"],
            "an unreadable file is a failure, not a deletion"
        );
        Ok(())
    }

    /// B6: a `--languages`-scoped build re-extracts only part of the index and
    /// so must not claim the whole index matches SCHEMA_VERSION.
    #[test]
    fn language_filtered_build_does_not_stamp_schema_version() -> Result<()> {
        let dir = tempdir()?;
        let root = dir.path().canonicalize()?;
        fs::write(root.join("a.py"), "def alpha():\n    return 1\n")?;
        fs::write(root.join("lib.rs"), "pub fn lib() {}\n")?;
        unfiltered_runtime(&root).build(false, None)?;
        let db = root.join(".tsindex/index.db");
        set_user_version(&Connection::open(&db)?, SCHEMA_VERSION - 1)?;

        python_runtime(&root).build(false, None)?;
        assert_eq!(
            user_version(&Connection::open(&db)?)?,
            SCHEMA_VERSION - 1,
            "rust rows were not re-extracted, so the index is still stale"
        );

        unfiltered_runtime(&root).build(false, None)?;
        assert_eq!(user_version(&Connection::open(&db)?)?, SCHEMA_VERSION);
        Ok(())
    }

    /// B8: files reached by expanding a directory event (e.g. a root
    /// `.gitignore` edit) take the mtime/size fast path; only files the
    /// watcher named are force-hashed.
    #[test]
    fn directory_event_takes_fast_path_but_named_file_is_hashed() -> Result<()> {
        let dir = tempdir()?;
        let root = dir.path().canonicalize()?;
        fs::write(root.join("a.py"), "def alpha():\n    return 1\n")?;
        let runtime = python_runtime(&root);
        runtime.build(false, None)?;

        // Same size, same mtime, different content: only a content hash sees it.
        let mtime = fs::metadata(root.join("a.py"))?.modified()?;
        fs::write(root.join("a.py"), "def alpha():\n    return 2\n")?;
        fs::File::options()
            .write(true)
            .open(root.join("a.py"))?
            .set_modified(mtime)?;
        fs::write(root.join(".gitignore"), "target/\n")?;

        let stats = runtime.update_paths(&[root.join(".gitignore")])?;
        assert_eq!(
            (stats.indexed, stats.skipped),
            (0, 1),
            "an ignore edit must not re-hash the whole repo"
        );
        let stats = runtime.update_paths(&[root.join("a.py")])?;
        assert_eq!(
            (stats.indexed, stats.skipped),
            (1, 0),
            "a named file is still hashed so an in-place `touch` is caught"
        );
        Ok(())
    }

    /// B9: a nested repo whose clone did not exist when the watcher started is
    /// registered on its first event instead of failing the batch.
    #[test]
    fn watch_registers_repo_that_appeared_after_startup() -> Result<()> {
        let dir = tempdir()?;
        let root = dir.path().canonicalize()?;
        let (outer, inner) = (root.join("outer"), root.join("outer/inner"));
        fs::create_dir_all(&outer)?;
        fs::write(outer.join("o.py"), "def outer():\n    return 1\n")?;
        let runtime = catalog_runtime(&root, &[("outer", &outer), ("inner", &inner)]);
        runtime.initialize()?; // registers `outer` only: `inner` is missing

        fs::create_dir_all(&inner)?;
        fs::write(inner.join("i.py"), "def inner():\n    return 2\n")?;
        let stats = runtime.update_paths_cached(&[inner.join("i.py")], &mut None)?;
        assert_eq!(stats.indexed, 1);
        assert!(
            file_paths(&runtime.db_path).contains(&("inner".to_string(), "i.py".to_string())),
            "{:?}",
            file_paths(&runtime.db_path)
        );
        Ok(())
    }

    fn python_runtime(root: &Path) -> Runtime {
        Runtime::new(
            root.to_path_buf(),
            root.join(".tsindex").join("index.db"),
            TsIndexConfig::default(),
            vec!["python".to_string()],
        )
    }

    fn catalog_runtime(root: &Path, repos: &[(&str, &Path)]) -> Runtime {
        Runtime::new(
            root.to_path_buf(),
            root.join(".tsindex").join("index.db"),
            TsIndexConfig {
                repos: repos
                    .iter()
                    .map(|(name, path)| RepoConfig {
                        name: name.to_string(),
                        path: path.to_string_lossy().to_string(),
                        languages: Vec::new(),
                        ignore: Vec::new(),
                    })
                    .collect(),
                ..TsIndexConfig::default()
            },
            vec!["python".to_string()],
        )
    }

    fn file_paths(db: &Path) -> Vec<(String, String)> {
        let conn = Connection::open(db).unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT r.name, f.path FROM files f JOIN repos r ON r.id = f.repo_id ORDER BY 1, 2",
            )
            .unwrap();
        stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    }

    /// chmod 000 `path`; returns false (test should bail) when the file is
    /// still readable, e.g. running as root.
    #[cfg(unix)]
    fn make_unreadable(path: &Path) -> bool {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o000)).unwrap();
        fs::read(path).is_err()
    }

    #[cfg(unix)]
    #[test]
    fn build_continues_past_unreadable_file() -> Result<()> {
        let dir = tempdir()?;
        let root = dir.path().canonicalize()?;
        fs::write(root.join("a.py"), "def alpha():\n    return 1\n")?;
        fs::write(root.join("b.py"), "def beta():\n    return 2\n")?;
        if !make_unreadable(&root.join("b.py")) {
            return Ok(());
        }
        let runtime = python_runtime(&root);
        let stats = runtime.build(false, None)?;
        assert_eq!(stats.failed, 1, "the unreadable file counts as failed");
        assert_eq!(stats.indexed, 1, "the readable file is still indexed");
        let conn = Connection::open(&runtime.db_path)?;
        let (ready, refreshing): (i64, i64) = conn.query_row(
            "SELECT ready, refreshing FROM index_state WHERE id = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(
            (ready, refreshing),
            (1, 0),
            "a build with unreadable files still completes"
        );
        assert_eq!(
            file_paths(&runtime.db_path)
                .iter()
                .map(|(_, p)| p.as_str())
                .collect::<Vec<_>>(),
            vec!["a.py"]
        );
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn update_paths_reports_unreadable_without_dropping_batch() -> Result<()> {
        let dir = tempdir()?;
        let root = dir.path().canonicalize()?;
        fs::write(root.join("a.py"), "def alpha():\n    return 1\n")?;
        fs::write(root.join("b.py"), "def beta():\n    return 2\n")?;
        let runtime = python_runtime(&root);
        runtime.build(false, None)?;
        assert_eq!(file_paths(&runtime.db_path).len(), 2);

        fs::write(root.join("a.py"), "def alpha():\n    return 10\n")?;
        if !make_unreadable(&root.join("b.py")) {
            return Ok(());
        }
        let stats = runtime.update_paths(&[root.join("a.py"), root.join("b.py")])?;
        assert_eq!(
            stats.indexed, 1,
            "the readable change in the batch is applied"
        );
        assert_eq!(stats.failed, 1);
        assert_eq!(
            file_paths(&runtime.db_path)
                .iter()
                .map(|(_, p)| p.as_str())
                .collect::<Vec<_>>(),
            vec!["a.py", "b.py"],
            "rows of a file that became unreadable are kept"
        );
        Ok(())
    }

    #[test]
    fn initialize_does_not_bump_user_version_before_rebuild() -> Result<()> {
        let dir = tempdir()?;
        fs::write(dir.path().join("a.py"), "def alpha():\n    return 1\n")?;
        // Unfiltered: a `--languages`-scoped build deliberately never stamps.
        let runtime = unfiltered_runtime(dir.path());
        runtime.build(false, None)?;
        let conn = Connection::open(&runtime.db_path)?;
        assert_eq!(user_version(&conn)?, SCHEMA_VERSION);
        set_user_version(&conn, SCHEMA_VERSION - 1)?;
        drop(conn);

        runtime.initialize()?;
        let conn = Connection::open(&runtime.db_path)?;
        assert_eq!(
            user_version(&conn)?,
            SCHEMA_VERSION - 1,
            "bootstrapping the schema must not claim the data was re-extracted"
        );
        drop(conn);

        runtime.build(true, None)?;
        let conn = Connection::open(&runtime.db_path)?;
        assert_eq!(
            user_version(&conn)?,
            SCHEMA_VERSION,
            "a completed build stamps the version"
        );
        Ok(())
    }

    #[test]
    fn update_with_foreign_db_does_not_delete_other_repos() -> Result<()> {
        let dir = tempdir()?;
        let catalog = dir.path().canonicalize()?;
        let repo_a = catalog.join("a");
        let repo_b = catalog.join("b");
        fs::create_dir_all(&repo_a)?;
        fs::create_dir_all(&repo_b)?;
        fs::write(repo_a.join("a.py"), "def alpha():\n    return 1\n")?;
        fs::write(repo_b.join("b.py"), "def beta():\n    return 2\n")?;
        let shared = catalog_runtime(&catalog, &[("a", &repo_a), ("b", &repo_b)]);
        shared.build(false, None)?;
        assert_eq!(file_paths(&shared.db_path).len(), 2);

        // `tsindex --root a --db <catalog db> update`: the root's own config
        // lists only `a`, but it must not prune `b` from the shared index.
        let foreign = Runtime::new(
            repo_a.clone(),
            shared.db_path.clone(),
            TsIndexConfig::default(),
            vec!["python".to_string()],
        )
        .with_repo_pruning(false);
        foreign.build(true, None)?;
        let repos: Vec<String> = file_paths(&shared.db_path)
            .into_iter()
            .map(|(r, _)| r)
            .collect();
        assert_eq!(repos, vec!["a".to_string(), "b".to_string()]);

        // Without the override (a genuine `repos remove` followed by a build),
        // the missing repo is pruned.
        let pruning = catalog_runtime(&catalog, &[("a", &repo_a)]);
        pruning.build(true, None)?;
        let repos: Vec<String> = file_paths(&shared.db_path)
            .into_iter()
            .map(|(r, _)| r)
            .collect();
        assert_eq!(repos, vec!["a".to_string()]);
        Ok(())
    }

    #[test]
    fn update_paths_cached_no_op_batch_performs_no_write() -> Result<()> {
        let dir = tempdir()?;
        let root = dir.path().canonicalize()?;
        fs::write(root.join("a.py"), "def alpha():\n    return 1\n")?;
        let runtime = python_runtime(&root);
        runtime.build(false, None)?;

        // `data_version` changes only when ANOTHER connection commits.
        let probe = Connection::open(&runtime.db_path)?;
        let before: i64 = probe.pragma_query_value(None, "data_version", |row| row.get(0))?;
        let mut cache = Some(HashMap::new());
        let stats = runtime.update_paths_cached(&[root.join("a.py")], &mut cache)?;
        assert_eq!(stats.indexed, 0);
        let after: i64 = probe.pragma_query_value(None, "data_version", |row| row.get(0))?;
        assert_eq!(
            before, after,
            "an unchanged batch must not commit a write transaction"
        );
        Ok(())
    }

    #[test]
    fn prefix_delete_is_case_sensitive() -> Result<()> {
        let dir = tempdir()?;
        fs::write(dir.path().join("x.py"), "def x():\n    return 1\n")?;
        let runtime = python_runtime(dir.path());
        runtime.build(false, None)?;
        let conn = Connection::open(&runtime.db_path)?;
        let repo_id: i64 = conn.query_row("SELECT id FROM repos", [], |row| row.get(0))?;
        for path in ["src/Foo/a.py", "src/foo/b.py"] {
            conn.execute(
                "INSERT INTO files(repo_id, path, language, sha, mtime_ns, byte_size) VALUES(?1, ?2, 'python', '', 0, 0)",
                params![repo_id, path],
            )?;
        }
        assert!(path_has_indexed_rows(&conn, repo_id, "src/Foo")?);
        assert_eq!(delete_file_rows_or_prefix(&conn, repo_id, "src/Foo")?, 1);
        assert!(!path_has_indexed_rows(&conn, repo_id, "src/Foo")?);
        assert!(
            path_has_indexed_rows(&conn, repo_id, "src/foo")?,
            "src/foo/b.py must survive"
        );
        Ok(())
    }

    #[test]
    fn build_skips_missing_repo_with_warning() -> Result<()> {
        let dir = tempdir()?;
        let catalog = dir.path().canonicalize()?;
        let present = catalog.join("present");
        fs::create_dir_all(&present)?;
        fs::write(present.join("p.py"), "def present():\n    return 1\n")?;
        let runtime = catalog_runtime(
            &catalog,
            &[("gone", &catalog.join("gone")), ("present", &present)],
        );
        let stats = runtime.build(false, None)?;
        assert_eq!(stats.indexed, 1);
        assert_eq!(
            runtime
                .repos()?
                .iter()
                .map(|r| r.name.as_str())
                .collect::<Vec<_>>(),
            vec!["present"]
        );
        assert!(
            runtime.build(false, Some("gone")).is_err(),
            "explicitly targeting the missing repo is still an error"
        );
        Ok(())
    }

    #[test]
    fn nested_repo_files_are_indexed_once_under_deepest_repo() -> Result<()> {
        let dir = tempdir()?;
        let outer = dir.path().canonicalize()?;
        let inner = outer.join("sub");
        fs::create_dir_all(&inner)?;
        fs::write(outer.join("top.py"), "def top():\n    return 1\n")?;
        fs::write(inner.join("s.py"), "def s():\n    return 2\n")?;
        let runtime = catalog_runtime(&outer, &[("outer", &outer), ("sub", &inner)]);
        runtime.build(false, None)?;
        assert_eq!(
            file_paths(&runtime.db_path),
            vec![
                ("outer".to_string(), "top.py".to_string()),
                ("sub".to_string(), "s.py".to_string()),
            ]
        );
        // The watch path resolves changed files through the same pruned walk.
        fs::write(inner.join("s.py"), "def s():\n    return 3\n")?;
        runtime.update_paths(&[inner.join("s.py")])?;
        assert_eq!(file_paths(&runtime.db_path).len(), 2);
        Ok(())
    }

    #[test]
    fn watch_update_purges_files_newly_ignored_by_gitignore_edit() -> Result<()> {
        let dir = tempdir()?;
        let root = dir.path().canonicalize()?;
        // The `ignore` crate only honors .gitignore inside a git repository.
        fs::create_dir_all(root.join(".git"))?;
        fs::write(root.join("a.py"), "def alpha():\n    return 1\n")?;
        fs::write(root.join("b.py"), "def beta():\n    return 2\n")?;
        let runtime = python_runtime(&root);
        runtime.build(false, None)?;
        assert_eq!(file_paths(&runtime.db_path).len(), 2);

        fs::write(root.join(".gitignore"), "b.py\n")?;
        runtime.update_paths(&[root.join(".gitignore")])?;
        assert_eq!(
            file_paths(&runtime.db_path)
                .iter()
                .map(|(_, p)| p.as_str())
                .collect::<Vec<_>>(),
            vec!["a.py"],
            "a file excluded by the edited .gitignore is purged"
        );

        fs::write(root.join(".gitignore"), "")?;
        runtime.update_paths(&[root.join(".gitignore")])?;
        assert_eq!(
            file_paths(&runtime.db_path).len(),
            2,
            "un-ignoring re-indexes it"
        );
        Ok(())
    }

    #[test]
    fn truncate_chars_handles_ascii_multibyte_and_boundary_cases() {
        // (input, max_chars, expected_output, expected_truncated). When
        // truncating, the result keeps max_chars-1 chars plus the ellipsis,
        // so it never exceeds max_chars chars in total.
        let cases: Vec<(String, usize, String, bool)> = vec![
            ("".into(), 0, "".into(), false),
            ("".into(), 5, "".into(), false),
            ("hello".into(), 0, "hello".into(), false), // 0 disables the cap
            ("hello".into(), 5, "hello".into(), false), // exact fit
            ("hello".into(), 4, "hel…".into(), true),
            ("hello".into(), 1, "…".into(), true),
            ("hello".into(), 100, "hello".into(), false),
            // Long ASCII: over the cap, and over the 16x hybrid threshold.
            (
                "a".repeat(10_000),
                100,
                format!("{}…", "a".repeat(99)),
                true,
            ),
            ("a".repeat(50), 100, "a".repeat(50), false),
            // Multi-byte: byte length exceeds the cap while char count does
            // not — the len() short-circuit must not truncate these.
            ("héllo wörld".into(), 5, "héll…".into(), true),
            ("héllo wörld".into(), 11, "héllo wörld".into(), false),
            ("日本語のテキストです".into(), 4, "日本語…".into(), true),
            ("🦀🦀🦀🦀🦀".into(), 3, "🦀🦀…".into(), true),
            ("é".repeat(5000), 100, format!("{}…", "é".repeat(99)), true),
            ("é".repeat(50), 100, "é".repeat(50), false),
            // 600 chars / 1800 bytes: under cap in chars, over in bytes, and
            // inside the 16x hybrid threshold (count path, not capped walk).
            ("日本語".repeat(200), 1000, "日本語".repeat(200), false),
            ("日本語".repeat(200), 600, "日本語".repeat(200), false),
            (
                "日本語".repeat(200),
                599,
                format!("{}…", "日本語".repeat(199) + "日"),
                true,
            ),
            // Mixed widths never split a codepoint mid-character.
            ("mixed ASCII and é🦀".into(), 10, "mixed ASC…".into(), true),
        ];
        for (input, max_chars, expected, expected_truncated) in cases {
            let (out, truncated) = truncate_chars(&input, max_chars);
            assert_eq!(
                (out.as_str(), truncated),
                (expected.as_str(), expected_truncated),
                "input len={} cap={max_chars}",
                input.len()
            );
            // A truncated result must never exceed max_chars chars.
            if truncated {
                assert!(out.chars().count() <= max_chars);
            }
        }
    }

    #[test]
    fn watch_filter_skips_internal_metadata_and_keeps_source() -> Result<()> {
        // Internal metadata and common generated output must not trigger
        // rebuilds; real source paths and tsindex config still must.
        let dir = tempdir()?;
        let base = dir.path().canonicalize()?;
        let repo = base.join("repo");
        let db_dir = repo.join("custom-index");
        let workspaces = vec![Workspace {
            name: "repo".to_string(),
            root: repo.clone(),
            languages: Vec::new(),
            ignore: Vec::new(),
        }];

        assert!(!watch_path_is_relevant(
            &db_dir.join("index.db-wal"),
            &db_dir,
            &workspaces
        ));
        assert!(!watch_path_is_relevant(
            &repo.join(".git/HEAD"),
            &db_dir,
            &workspaces
        ));
        assert!(!watch_path_is_relevant(
            &repo.join("node_modules/pkg/index.js"),
            &db_dir,
            &workspaces
        ));
        assert!(!watch_path_is_relevant(
            &repo.join(".next/server/app.js"),
            &db_dir,
            &workspaces
        ));
        assert!(!watch_path_is_relevant(
            &repo.join(".sst/platform/index.js"),
            &db_dir,
            &workspaces
        ));
        assert!(!watch_path_is_relevant(
            &repo.join(".claude/worktrees/task-1/src/index.ts"),
            &db_dir,
            &workspaces
        ));
        assert!(!watch_path_is_relevant(
            &repo.join("tmp/debug.log"),
            &db_dir,
            &workspaces
        ));
        assert!(!watch_path_is_relevant(
            &repo.join("src/.DS_Store"),
            &db_dir,
            &workspaces
        ));
        assert!(!watch_path_is_relevant(
            &repo.join("tsconfig.tsbuildinfo"),
            &db_dir,
            &workspaces
        ));
        assert!(watch_path_is_relevant(
            &repo.join(".tsindex/config.toml"),
            &db_dir,
            &workspaces
        ));
        assert!(watch_path_is_relevant(
            &repo.join(".claude/settings.json"),
            &db_dir,
            &workspaces
        ));
        assert!(watch_path_is_relevant(
            &repo.join("src/main.rs"),
            &db_dir,
            &workspaces
        ));
        assert!(watch_path_is_relevant(
            &repo.join("target/debug/build/foo.rs"),
            &db_dir,
            &workspaces
        ));
        assert!(watch_path_is_relevant(
            &repo.join("dist/index.js"),
            &db_dir,
            &workspaces
        ));
        assert!(watch_path_is_relevant(
            &repo.join("app/components/Button.tsx"),
            &db_dir,
            &workspaces
        ));
        Ok(())
    }

    #[test]
    fn watch_filter_anchors_on_workspace_root_not_db_parent() -> Result<()> {
        // Regression: `config.repos` and `--db` allow the watched repo and the
        // DB/catalog root to sit in unrelated directories. Anchoring the ignore
        // filter on `db_dir.parent()` breaks both directions:
        //  - catalog outside /tmp watching a repo under /tmp: strip_prefix
        //    fails, the absolute path's `/tmp` component matches the `tmp`
        //    ignore entry, and every source event is silently dropped.
        //  - catalog parent inside the repo: a repo-local ignored dir could
        //    slip through because the stripped prefix isn't the repo root.
        // The filter must anchor on the matching Workspace.root instead.
        let dir = tempdir()?;
        let base = dir.path().canonicalize()?;
        let repo = base.join("payments");
        // DB lives in a disjoint directory — the default layout this used to
        // assume (db_dir = repo/.tsindex) does not hold here.
        let db_dir = base.join("catalog").join("db");
        let workspaces = vec![Workspace {
            name: "payments".to_string(),
            root: repo.clone(),
            languages: Vec::new(),
            ignore: Vec::new(),
        }];

        // Source files inside the watched repo stay relevant even though the
        // DB root is not an ancestor of the repo — and even though the
        // tempdir's absolute prefix may contain components (e.g. `/tmp` on
        // Linux) that are also repo-level ignore entries.
        assert!(watch_path_is_relevant(
            &repo.join("src/main.rs"),
            &db_dir,
            &workspaces
        ));
        // Repo-local ignored dirs still get filtered relative to the
        // workspace root.
        assert!(!watch_path_is_relevant(
            &repo.join("tmp/debug.log"),
            &db_dir,
            &workspaces
        ));
        assert!(!watch_path_is_relevant(
            &repo.join("node_modules/pkg/index.js"),
            &db_dir,
            &workspaces
        ));
        // The DB's own churn is still suppressed even though db_dir is not
        // inside any workspace.
        assert!(!watch_path_is_relevant(
            &db_dir.join("index.db-wal"),
            &db_dir,
            &workspaces
        ));
        // Churn in the catalog dir that is NOT the db_dir is not a source
        // input; it is outside every workspace, so it falls back to the
        // absolute path. Its components are legitimately not repo-relative.
        // (notify never reports these paths in practice — workspaces are the
        // only watched roots — but the filter must not misclassify them.)
        Ok(())
    }

    #[test]
    fn watch_event_loop_does_not_update_while_idle() -> Result<()> {
        // CPU regression guard: the event-driven watcher must block while idle.
        // We assert the direct cause of idle CPU use (spurious update calls), not
        // an OS-specific CPU percentage that would be flaky in CI.
        let dir = tempdir()?;
        let db_dir = dir.path().join("db");
        let (tx, rx) = mpsc::channel();
        let update_count = Arc::new(AtomicUsize::new(0));
        let loop_update_count = Arc::clone(&update_count);
        let handle = thread::spawn(move || {
            run_watch_event_loop(rx, &db_dir, &[], |_| {
                loop_update_count.fetch_add(1, Ordering::SeqCst);
                Ok(BuildStats::default())
            });
        });

        thread::sleep(Duration::from_millis(50));
        assert!(
            !handle.is_finished(),
            "idle watcher loop should remain blocked waiting for events"
        );
        assert_eq!(
            update_count.load(Ordering::SeqCst),
            0,
            "idle watcher loop must not call update without filesystem events"
        );

        drop(tx);
        handle.join().expect("watch loop exits when channel closes");
        Ok(())
    }

    #[test]
    fn watch_event_loop_updates_once_per_non_empty_event_batch() -> Result<()> {
        let dir = tempdir()?;
        // Canonicalize for the same reason as
        // watch_filter_skips_internal_metadata_and_keeps_source: macOS
        // tempdirs live under the /tmp symlink.
        let repo = dir.path().canonicalize()?.join("repo");
        let db_dir = repo.join(".tsindex");
        let source = repo.join("src/main.rs");
        let ignored_db = db_dir.join("index.db-wal");
        let workspaces = vec![Workspace {
            name: "repo".to_string(),
            root: repo.clone(),
            languages: Vec::new(),
            ignore: Vec::new(),
        }];
        let (tx, rx) = mpsc::channel();
        let updates = Arc::new(Mutex::new(Vec::<Vec<PathBuf>>::new()));
        let loop_updates = Arc::clone(&updates);
        let handle = thread::spawn(move || {
            run_watch_event_loop(rx, &db_dir, &workspaces, |changed| {
                loop_updates
                    .lock()
                    .expect("updates lock")
                    .push(changed.to_vec());
                Ok(BuildStats::default())
            });
        });

        tx.send(Ok(vec![DebouncedEvent::new(
            Event::new(EventKind::Any)
                .add_path(source.clone())
                .add_path(source.clone())
                .add_path(ignored_db),
            Instant::now(),
        )]))?;
        drop(tx);
        handle.join().expect("watch loop exits when channel closes");

        let updates = updates.lock().expect("updates lock");
        assert_eq!(updates.as_slice(), &[vec![source]]);
        Ok(())
    }

    #[test]
    fn extract_body_does_not_panic_on_stale_range_past_eof() -> Result<()> {
        // Regression: stale DB rows can reference start_row past the
        // file's current line count (e.g. after a v2 -> v3 schema bump
        // where the file shrunk since indexing). Before the fix,
        // start_row.saturating_sub(ctx) wasn't clamped against
        // lines.len(), so when end clamped to last but start stayed
        // past EOF, lines[start..=end] panicked with
        // "slice index starts at X but ends at Y." The fix clamps both
        // ends so the worst case is a truncated "last-line-only" body
        // rather than a process panic.
        let dir = tempdir()?;
        let path = dir.path().join("shrunk.txt");
        fs::write(&path, "line0\nline1\n")?;

        let opts = BodyOpts::default();
        // start_row = 100 against a 2-line file used to panic. With the
        // clamp it returns the last line — degraded but safe.
        let body = extract_body(&path, 100, 105, &opts)?;
        assert_eq!(body, "line1");
        Ok(())
    }

    #[test]
    fn extract_body_returns_typed_error_for_inverted_range() -> Result<()> {
        // A recorded start_row > end_row (inverted range, possible after
        // a corrupted or partially-rolled-back migration) would clamp
        // to start > end after my fix. Surface that as a typed error
        // pointing the user at `tsindex build`, so callers don't have
        // to guess.
        let dir = tempdir()?;
        let path = dir.path().join("inverted.txt");
        fs::write(
            &path,
            "0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n13\n14\n15\n",
        )?;

        let opts = BodyOpts::default();
        // start_row=10 > end_row=5 with last=15: start clamps to 10,
        // end clamps to 5 — start > end.
        let result = extract_body(&path, 10, 5, &opts);
        let error = result.expect_err("expected a typed error for inverted range");
        let message = format!("{error:#}");
        assert!(
            message.contains("stale"),
            "expected error to mention staleness so callers can hint at \
             `tsindex build`; got: {message}"
        );
        Ok(())
    }

    #[test]
    fn extract_body_clamps_end_row_past_eof_without_panic() -> Result<()> {
        // The other half of the panic scenario: end_row past EOF is
        // clamped to last line. This was already working pre-fix, but
        // the clamp pattern got reworked, so guard it.
        let dir = tempdir()?;
        let path = dir.path().join("ok.txt");
        fs::write(&path, "alpha\nbeta\ngamma\n")?;

        let opts = BodyOpts::default();
        let body = extract_body(&path, 0, 999, &opts)?;
        assert_eq!(body, "alpha\nbeta\ngamma");
        Ok(())
    }

    #[test]
    fn elide_body_leaves_short_bodies_and_disabled_caps_untouched() {
        let body = "l1\nl2\nl3".to_string();
        // No cap configured.
        assert_eq!(elide_body(body.clone(), None), body);
        // Cap larger than the body.
        assert_eq!(elide_body(body.clone(), Some(10)), body);
        // A zero cap is treated as "no cap" rather than eliding everything.
        assert_eq!(elide_body(body.clone(), Some(0)), body);
    }

    #[test]
    fn elide_body_collapses_middle_and_reports_true_count() {
        // 10 lines, cap at 4: keep 2 head + 2 tail, elide the 6 in between.
        let body = (1..=10)
            .map(|n| format!("line{n}"))
            .collect::<Vec<_>>()
            .join("\n");
        let elided = elide_body(body, Some(4));
        let lines: Vec<&str> = elided.lines().collect();
        assert_eq!(lines.len(), 5, "2 head + marker + 2 tail");
        assert_eq!(&lines[..2], &["line1", "line2"]);
        assert_eq!(&lines[3..], &["line9", "line10"]);
        // The marker reports both the elided count and the true total so an
        // agent never mistakes a truncated body for a complete one.
        assert_eq!(lines[2], "… 6 lines elided (10 total) …");
    }

    #[test]
    fn extract_body_applies_max_body_lines() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("big.txt");
        let source = (1..=20)
            .map(|n| format!("row{n}"))
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(&path, &source)?;

        let opts = BodyOpts {
            context_lines: 0,
            max_body_lines: Some(6),
        };
        let body = extract_body(&path, 0, 19, &opts)?;
        assert!(body.contains("lines elided (20 total)"), "got: {body}");
        // Far fewer lines than the original 20.
        assert!(body.lines().count() <= 7);
        Ok(())
    }

    fn replace_runtime(root: &Path, language: &str) -> Runtime {
        Runtime::new(
            root.to_path_buf(),
            root.join(".tsindex").join("index.db"),
            TsIndexConfig::default(),
            vec![language.to_string()],
        )
    }

    #[test]
    fn splice_replaces_whole_lines_preserving_newline() -> Result<()> {
        let source = "a\nbb\nccc\n";
        let out = splice_symbol_lines(source, 1, 0, 1, 2, "BB")?;
        assert_eq!(out, "a\nBB\nccc\n");
        Ok(())
    }

    #[test]
    fn splice_refuses_symbol_sharing_its_line() {
        // `y` starts at column 7, after `x = 1; ` — a whole-line replace
        // would clobber the `x` assignment, so it must refuse.
        let source = "x = 1; y = 2\n";
        let result = splice_symbol_lines(source, 0, 7, 0, 12, "y = 99");
        assert!(
            result.is_err(),
            "expected refusal when a sibling shares the line"
        );
    }

    #[test]
    fn replace_symbol_rewrites_function_body() -> Result<()> {
        let dir = tempdir()?;
        fs::write(
            dir.path().join("mod.py"),
            "def greet():\n    return 1\n\n\ndef other():\n    return 2\n",
        )?;
        let runtime = replace_runtime(dir.path(), "python");

        let response = runtime.replace_symbol(ReplaceSymbolArgs {
            file: "mod.py".to_string(),
            name: "greet".to_string(),
            new_body: "def greet():\n    return 42".to_string(),
            repo: None,
            kind: None,
            qualified: None,
            row: None,
        })?;

        assert_eq!(response.name, "greet");
        assert_eq!(response.kind, "function");
        // Only the symbol's lines change; surrounding code is preserved.
        assert_eq!(
            fs::read_to_string(dir.path().join("mod.py"))?,
            "def greet():\n    return 42\n\n\ndef other():\n    return 2\n"
        );
        Ok(())
    }

    #[test]
    fn replace_symbol_preserves_indentation_of_nested_symbol() -> Result<()> {
        let dir = tempdir()?;
        fs::write(
            dir.path().join("s.rs"),
            "struct S;\nimpl S {\n    fn m(&self) -> i32 {\n        1\n    }\n}\n",
        )?;
        let runtime = replace_runtime(dir.path(), "rust");

        runtime.replace_symbol(ReplaceSymbolArgs {
            file: "s.rs".to_string(),
            name: "m".to_string(),
            new_body: "    fn m(&self) -> i32 {\n        2\n    }".to_string(),
            repo: None,
            kind: None,
            qualified: None,
            row: None,
        })?;

        assert_eq!(
            fs::read_to_string(dir.path().join("s.rs"))?,
            "struct S;\nimpl S {\n    fn m(&self) -> i32 {\n        2\n    }\n}\n"
        );
        Ok(())
    }

    #[test]
    fn replace_symbol_refuses_edit_that_breaks_syntax() -> Result<()> {
        let dir = tempdir()?;
        let original = "fn add(a: i32, b: i32) -> i32 {\n    a + b\n}\n";
        fs::write(dir.path().join("m.rs"), original)?;
        let runtime = replace_runtime(dir.path(), "rust");

        let result = runtime.replace_symbol(ReplaceSymbolArgs {
            file: "m.rs".to_string(),
            name: "add".to_string(),
            // Missing the closing brace.
            new_body: "fn add(a: i32, b: i32) -> i32 {\n    a + b".to_string(),
            repo: None,
            kind: None,
            qualified: None,
            row: None,
        });

        let error = result.expect_err("expected the broken edit to be refused");
        assert!(
            format!("{error:#}").contains("syntax error"),
            "expected a syntax-error message, got: {error:#}"
        );
        assert_eq!(
            fs::read_to_string(dir.path().join("m.rs"))?,
            original,
            "the file must be left untouched when the edit is refused"
        );
        Ok(())
    }

    #[test]
    fn replace_symbol_rejects_ambiguous_name() -> Result<()> {
        let dir = tempdir()?;
        fs::write(
            dir.path().join("dup.py"),
            "def f():\n    return 1\n\ndef f():\n    return 2\n",
        )?;
        let runtime = replace_runtime(dir.path(), "python");

        let result = runtime.replace_symbol(ReplaceSymbolArgs {
            file: "dup.py".to_string(),
            name: "f".to_string(),
            new_body: "def f():\n    return 3".to_string(),
            repo: None,
            kind: None,
            qualified: None,
            row: None,
        });

        let error = result.expect_err("expected ambiguity to be rejected");
        assert!(format!("{error:#}").contains("ambiguous"));
        Ok(())
    }

    #[test]
    fn replace_symbol_errors_when_symbol_absent() -> Result<()> {
        let dir = tempdir()?;
        fs::write(dir.path().join("mod.py"), "def greet():\n    return 1\n")?;
        let runtime = replace_runtime(dir.path(), "python");

        let result = runtime.replace_symbol(ReplaceSymbolArgs {
            file: "mod.py".to_string(),
            name: "missing".to_string(),
            new_body: "def missing():\n    return 0".to_string(),
            repo: None,
            kind: None,
            qualified: None,
            row: None,
        });

        assert!(
            format!("{:#}", result.expect_err("expected not-found error")).contains("no symbol")
        );
        Ok(())
    }

    #[test]
    fn replace_symbol_refuses_paths_outside_the_repo() -> Result<()> {
        let dir = tempdir()?;
        let repo = dir.path().join("repo");
        fs::create_dir_all(&repo)?;
        // A file the caller should not be able to reach via `..`.
        fs::write(dir.path().join("secret.py"), "def s():\n    return 1\n")?;
        let runtime = replace_runtime(&repo, "python");

        let result = runtime.replace_symbol(ReplaceSymbolArgs {
            file: "../secret.py".to_string(),
            name: "s".to_string(),
            new_body: "def s():\n    return 666".to_string(),
            repo: None,
            kind: None,
            qualified: None,
            row: None,
        });

        assert!(
            format!(
                "{:#}",
                result.expect_err("expected traversal to be refused")
            )
            .contains("outside repo")
        );
        // The out-of-repo file is untouched.
        assert_eq!(
            fs::read_to_string(dir.path().join("secret.py"))?,
            "def s():\n    return 1\n"
        );
        Ok(())
    }

    #[test]
    fn outline_excludes_imports_by_default() -> Result<()> {
        let dir = tempdir()?;
        fs::write(
            dir.path().join("m.rs"),
            "use std::io;\nuse std::fmt;\n\nfn go() -> i32 {\n    1\n}\n",
        )?;
        let runtime = replace_runtime(dir.path(), "rust");
        runtime.build(false, None)?;

        let outline_args = |include_imports| OutlineArgs {
            file: "m.rs".to_string(),
            repo: None,
            depth: 2,
            include_signatures: true,
            include_docstrings: true,
            include_imports,
            include_bodies_for: None,
            max_body_lines: None,
            limit: None,
            offset: None,
        };

        let default = runtime.list_file_outline(outline_args(false))?;
        assert!(
            default.symbols.iter().all(|s| s.kind != "import"),
            "imports must be excluded from the outline by default"
        );
        assert!(
            default.symbols.iter().any(|s| s.name == "go"),
            "non-import symbols must still appear"
        );

        let with_imports = runtime.list_file_outline(outline_args(true))?;
        assert!(
            with_imports.symbols.iter().any(|s| s.kind == "import"),
            "include_imports=true must bring import rows back"
        );
        Ok(())
    }

    #[test]
    fn build_with_explicit_parallel_jobs_indexes_and_skips_incrementally() -> Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src");
        fs::create_dir_all(&src)?;
        for index in 0..8 {
            fs::write(
                src.join(format!("module_{index}.py")),
                format!(
                    r#"
def handler_{index}():
    return {index}
"#
                ),
            )?;
        }

        let runtime = Runtime::new(
            dir.path().to_path_buf(),
            dir.path().join(".tsindex").join("index.db"),
            TsIndexConfig::default(),
            vec!["python".to_string()],
        )
        .with_jobs(2);
        assert_eq!(effective_thread_count(runtime.jobs), 2);

        let stats = runtime.build(false, None)?;
        assert_eq!(stats.indexed, 8);
        assert_eq!(stats.skipped, 0);
        assert_eq!(stats.failed, 0);

        let symbol = runtime.get_symbol(GetSymbolArgs {
            name: Some("handler_3".to_string()),
            names: None,
            repo: None,
            kind: None,
            file_glob: None,
            include_body: false,
            context_lines: 0,
            max_body_lines: None,
            limit: None,
            offset: None,
        })?;
        assert_eq!(symbol.matches.len(), 1);

        let update_stats = runtime.build(true, None)?;
        assert_eq!(update_stats.indexed, 0);
        assert_eq!(update_stats.skipped, 8);
        assert_eq!(update_stats.failed, 0);
        Ok(())
    }

    #[test]
    fn get_symbol_batches_multiple_names_in_one_call() -> Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src");
        fs::create_dir_all(&src)?;
        fs::write(
            src.join("m.py"),
            "def alpha():\n    return 1\n\ndef beta():\n    return 2\n\ndef gamma():\n    return 3\n",
        )?;
        let runtime = Runtime::new(
            dir.path().to_path_buf(),
            dir.path().join(".tsindex").join("index.db"),
            TsIndexConfig::default(),
            vec!["python".to_string()],
        );
        runtime.build(false, None)?;

        let args = |name: Option<&str>, names: Option<Vec<&str>>| GetSymbolArgs {
            name: name.map(str::to_string),
            names: names.map(|v| v.into_iter().map(str::to_string).collect()),
            repo: None,
            kind: None,
            file_glob: None,
            include_body: false,
            context_lines: 0,
            max_body_lines: None,
            limit: None,
            offset: None,
        };
        let sorted = |resp: GetSymbolResponse| {
            let mut n: Vec<String> = resp.matches.into_iter().map(|m| m.name).collect();
            n.sort();
            n
        };

        // `names` fetches several symbols in one call.
        assert_eq!(
            sorted(runtime.get_symbol(args(None, Some(vec!["alpha", "gamma"])))?),
            vec!["alpha", "gamma"]
        );
        // `name` + `names` combine and dedupe.
        assert_eq!(
            sorted(runtime.get_symbol(args(Some("alpha"), Some(vec!["alpha", "beta"])))?),
            vec!["alpha", "beta"]
        );
        // The single-name path still works.
        assert_eq!(
            sorted(runtime.get_symbol(args(Some("beta"), None))?),
            vec!["beta"]
        );
        // Neither provided is an error.
        assert!(runtime.get_symbol(args(None, None)).is_err());
        // An empty `names` list is also an error (matches the schema's minItems).
        assert!(runtime.get_symbol(args(None, Some(vec![]))).is_err());
        Ok(())
    }

    #[test]
    fn outline_inlines_bodies_only_for_requested_names() -> Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src");
        fs::create_dir_all(&src)?;
        fs::write(
            src.join("m.py"),
            "def alpha():\n    return 1\n\ndef beta():\n    return 2\n",
        )?;
        let runtime = Runtime::new(
            dir.path().to_path_buf(),
            dir.path().join(".tsindex").join("index.db"),
            TsIndexConfig::default(),
            vec!["python".to_string()],
        );
        runtime.build(false, None)?;

        let outline = runtime.list_file_outline(OutlineArgs {
            file: "src/m.py".to_string(),
            repo: None,
            depth: 2,
            include_signatures: true,
            include_docstrings: true,
            include_imports: false,
            include_bodies_for: Some(vec!["alpha".to_string()]),
            max_body_lines: None,
            limit: None,
            offset: None,
        })?;
        let find = |name| outline.symbols.iter().find(|s| s.name == name);
        let alpha = find("alpha").expect("alpha in outline");
        assert!(
            alpha
                .body
                .as_deref()
                .unwrap_or_default()
                .contains("return 1"),
            "requested name's body should be inlined, got {:?}",
            alpha.body
        );
        assert!(
            find("beta").expect("beta in outline").body.is_none(),
            "unrequested name's body should stay None"
        );
        Ok(())
    }

    #[test]
    fn find_references_batches_names_and_tags_each() -> Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src");
        fs::create_dir_all(&src)?;
        fs::write(
            src.join("m.py"),
            "def alpha():\n    return 1\n\ndef beta():\n    return 2\n\n\
             def main():\n    alpha()\n    beta()\n    alpha()\n",
        )?;
        let runtime = Runtime::new(
            dir.path().to_path_buf(),
            dir.path().join(".tsindex").join("index.db"),
            TsIndexConfig::default(),
            vec!["python".to_string()],
        );
        runtime.build(false, None)?;

        let refs = |name: Option<&str>, names: Option<Vec<&str>>| FindReferencesArgs {
            name: name.map(str::to_string),
            names: names.map(|v| v.into_iter().map(str::to_string).collect()),
            repo: None,
            scope: None,
            exclude_tests: false,
            include_declarations: false,
            group_by_file: false,
            snippet_lines: 1,
            limit: None,
            offset: None,
            counts_only: false,
        };

        // Batch: references to both names come back in one call, each tagged.
        let batched = runtime.find_references(refs(None, Some(vec!["alpha", "beta"])))?;
        assert!(batched.total >= 3, "expected alpha (x2) + beta refs");
        assert!(
            batched
                .refs
                .iter()
                .all(|r| matches!(r.name.as_deref(), Some("alpha" | "beta"))),
            "every ref in a batch should be tagged with its name"
        );
        assert!(
            batched
                .refs
                .iter()
                .any(|r| r.name.as_deref() == Some("alpha"))
        );
        assert!(
            batched
                .refs
                .iter()
                .any(|r| r.name.as_deref() == Some("beta"))
        );

        // Single name: untagged (byte-identical to pre-batch output).
        let single = runtime.find_references(refs(Some("alpha"), None))?;
        assert!(single.total >= 2);
        assert!(
            single.refs.iter().all(|r| r.name.is_none()),
            "single-name references stay untagged"
        );

        // Neither provided is an error.
        assert!(runtime.find_references(refs(None, None)).is_err());
        Ok(())
    }

    #[test]
    fn find_references_snippet_is_the_reference_line_not_a_neighbor() -> Result<()> {
        // Regression: the one-line snippet must be the reference's OWN line.
        // It was padded with `snippet_lines` of context and then collapsed to
        // the first non-empty line, yielding the line *above* the reference.
        let dir = tempdir()?;
        let src = dir.path().join("src");
        fs::create_dir_all(&src)?;
        let source = "def target():\n    return 1\n\ndef main():\n    target()\n";
        fs::write(src.join("m.py"), source)?;
        let runtime = Runtime::new(
            dir.path().to_path_buf(),
            dir.path().join(".tsindex").join("index.db"),
            TsIndexConfig::default(),
            vec!["python".to_string()],
        );
        runtime.build(false, None)?;

        let resp = runtime.find_references(FindReferencesArgs {
            name: Some("target".to_string()),
            names: None,
            repo: None,
            scope: None,
            exclude_tests: false,
            include_declarations: false,
            group_by_file: false,
            snippet_lines: 1,
            limit: None,
            offset: None,
            counts_only: false,
        })?;
        assert!(resp.total >= 1, "expected at least one reference to target");

        let lines: Vec<&str> = source.lines().collect();
        for r in &resp.refs {
            let own_line = lines[r.range.start.0].trim();
            assert_eq!(
                r.snippet, own_line,
                "snippet for ref on row {} must be its own line `{}`, not a neighbor",
                r.range.start.0, own_line
            );
        }
        Ok(())
    }

    #[test]
    fn find_references_multiline_snippet_carries_its_start_row() -> Result<()> {
        // Regression: with `snippet_lines > 1` the snippet is a block with the
        // reference somewhere in the middle, and `range` alone does not say
        // which block line that is. A live agent assumed `range` pointed at the
        // snippet's FIRST line and reported every call site two rows off.
        // `snippet_start_row` anchors the block; single-line snippets omit it
        // because `range` already is the line.
        let dir = tempdir()?;
        let src = dir.path().join("src");
        fs::create_dir_all(&src)?;
        let source = "def target():\n    return 1\n\n# pad\n# pad\ndef main():\n    target()\n    return 0\n";
        fs::write(src.join("m.py"), source)?;
        let runtime = Runtime::new(
            dir.path().to_path_buf(),
            dir.path().join(".tsindex").join("index.db"),
            TsIndexConfig::default(),
            vec!["python".to_string()],
        );
        runtime.build(false, None)?;

        let args = |snippet_lines| FindReferencesArgs {
            name: Some("target".to_string()),
            names: None,
            repo: None,
            scope: None,
            exclude_tests: false,
            include_declarations: false,
            group_by_file: false,
            snippet_lines,
            limit: None,
            offset: None,
            counts_only: false,
        };

        // Single line: no anchor needed.
        for r in &runtime.find_references(args(1))?.refs {
            assert!(
                r.snippet_start_row.is_none(),
                "single-line snippet must not carry an anchor"
            );
        }

        // Multi-line: the anchor must land the reference on its own line.
        for snippet_lines in [3, 5] {
            let resp = runtime.find_references(args(snippet_lines))?;
            assert!(resp.total >= 1, "expected a reference to target");
            for r in &resp.refs {
                let first = r
                    .snippet_start_row
                    .expect("multi-line snippet must carry snippet_start_row");
                let block: Vec<&str> = r.snippet.lines().collect();
                let idx = r
                    .range
                    .start
                    .0
                    .checked_sub(first)
                    .expect("anchor must not be after the reference row");
                assert!(
                    block.get(idx).is_some_and(|line| line.contains("target")),
                    "snippet_lines={snippet_lines}: range row {} minus anchor {first} = {idx}, \
                     which is not the reference line in {block:?}",
                    r.range.start.0
                );
            }

            // Same invariant ON THE WIRE. Rows are stored 0-based and become
            // 1-based only at serialization; the anchor must take the same +1
            // or `range[0] - snippet_start_row` is off by one for callers, even
            // though the in-process fields agree.
            let wire: serde_json::Value = serde_json::to_value(&resp)?;
            for r in wire["refs"].as_array().expect("refs array") {
                let row = r["range"][0].as_u64().expect("range row") as usize;
                let first = r["snippet_start_row"]
                    .as_u64()
                    .expect("wire snippet_start_row") as usize;
                let block: Vec<&str> = r["snippet"].as_str().expect("snippet").lines().collect();
                let idx = row
                    .checked_sub(first)
                    .expect("wire anchor must not be after the reference row");
                assert!(
                    block.get(idx).is_some_and(|line| line.contains("target")),
                    "wire snippet_lines={snippet_lines}: row {row} - anchor {first} = {idx}, \
                     not the reference line in {block:?}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn enclosing_symbol_batches_rows() -> Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src");
        fs::create_dir_all(&src)?;
        // 1-based: alpha is lines 1-2, beta is lines 4-5. Query the def
        // line of each (rows 1 and 4) — input rows are 1-based.
        fs::write(
            src.join("m.py"),
            "def alpha():\n    return 1\n\ndef beta():\n    return 2\n",
        )?;
        let runtime = Runtime::new(
            dir.path().to_path_buf(),
            dir.path().join(".tsindex").join("index.db"),
            TsIndexConfig::default(),
            vec!["python".to_string()],
        );
        runtime.build(false, None)?;

        let args = |row: Option<usize>, rows: Option<Vec<usize>>| EnclosingSymbolArgs {
            file: "src/m.py".to_string(),
            row,
            rows,
            col: None,
            repo: None,
            include_body: false,
            context_lines: 0,
            max_body_lines: None,
            depth: 1,
            limit: None,
            offset: None,
        };

        // Batch: each row resolves to its enclosing function in one call.
        let batch = runtime.enclosing_symbol(args(None, Some(vec![1, 4])))?;
        assert!(
            batch.matches.is_empty(),
            "batch uses `results`, not `matches`"
        );
        assert_eq!(batch.results.len(), 2);
        let row_match = |r: usize| {
            batch
                .results
                .iter()
                .find(|hit| hit.row == r)
                .and_then(|hit| hit.matches.first())
                .map(|m| m.name.as_str())
        };
        assert_eq!(row_match(1), Some("alpha"));
        assert_eq!(row_match(4), Some("beta"));

        // Single position: unchanged shape (matches present, results empty).
        let single = runtime.enclosing_symbol(args(Some(1), None))?;
        assert!(single.results.is_empty());
        assert_eq!(
            single.matches.first().map(|m| m.name.as_str()),
            Some("alpha")
        );

        // Rows are 1-based: 0 is invalid and must error rather than silently
        // resolving to internal row 0 (which would mask a caller off-by-one).
        assert!(runtime.enclosing_symbol(args(Some(0), None)).is_err());
        assert!(runtime.enclosing_symbol(args(None, Some(vec![0]))).is_err());

        // Neither row nor rows is an error.
        assert!(runtime.enclosing_symbol(args(None, None)).is_err());
        Ok(())
    }

    /// When no explicit language filter is configured, `allowed_languages`
    /// runs a full-tree `detect_languages` walk. `update_paths_cached` must
    /// memoize that result per workspace so the watch loop computes it once
    /// instead of on every event batch. This verifies the cache is populated
    /// on first use and that a pre-seeded cache is honored on the next call.
    #[test]
    fn update_paths_cached_memoizes_language_allowlist() -> Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src");
        fs::create_dir_all(&src)?;
        fs::write(dir.path().join("pyproject.toml"), "[project]\nname='f'\n")?;
        fs::write(src.join("a.py"), "def alpha():\n    return 1\n")?;

        // Empty `languages` => the no-explicit-filter branch (detect_languages).
        let runtime = Runtime::new(
            dir.path().to_path_buf(),
            dir.path().join(".tsindex").join("index.db"),
            TsIndexConfig::default(),
            Vec::new(),
        );
        runtime.build(false, None)?;

        let workspace_name = default_repo_name(dir.path());

        // First call with an empty cache: it must populate the memo.
        let mut cache: Option<HashMap<String, HashMap<String, LanguageSpec>>> =
            Some(HashMap::new());
        runtime.update_paths_cached(&[src.join("a.py")], &mut cache)?;

        let populated = cache.as_ref().expect("cache stays Some");
        assert!(
            populated.contains_key(&workspace_name),
            "first update must memoize the workspace allowlist"
        );
        assert!(
            populated
                .get(&workspace_name)
                .is_some_and(|langs| langs.contains_key("python")),
            "memoized allowlist should include the detected language"
        );

        // Seed the cache with a bogus (empty) allowlist for this workspace. If
        // the cache is honored, the next update resolves no jobs (no language
        // matches) and indexes nothing — proving the memo is read, not the
        // fresh detect_languages result.
        cache = Some(HashMap::from([(workspace_name.clone(), HashMap::new())]));
        fs::write(src.join("a.py"), "def alpha():\n    return 1\n# touch\n")?;
        let stats = runtime.update_paths_cached(&[src.join("a.py")], &mut cache)?;
        assert_eq!(
            stats.indexed, 0,
            "an empty memoized allowlist must suppress indexing, proving the cache is consulted"
        );
        Ok(())
    }

    #[test]
    fn path_has_indexed_rows_matches_exact_and_prefix_only() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE files(repo_id INTEGER NOT NULL, path TEXT NOT NULL);")
            .unwrap();
        let insert = |path: &str| {
            conn.execute(
                "INSERT INTO files(repo_id, path) VALUES (1, ?1)",
                params![path],
            )
            .unwrap();
        };
        insert("src/a.py");
        insert("src/pkg/one.py");
        // A row in a different repo must never match repo 1's lookups.
        conn.execute(
            "INSERT INTO files(repo_id, path) VALUES (2, ?1)",
            params!["build/out.py"],
        )
        .unwrap();

        let has = |relative: &str| path_has_indexed_rows(&conn, 1, relative).unwrap();
        // Exact file match.
        assert!(has("src/a.py"));
        // Directory prefix of an indexed file (with and without trailing slash).
        assert!(has("src/pkg"));
        assert!(has("src/pkg/"));
        // Indexed only under repo 2, so it must not match repo 1 (also covers the
        // ignored-but-never-indexed case: no row in repo 1 => no write).
        assert!(!has("build/out.py"));
        assert!(!has("build"));
        // Prefix must be path-segment aware: "src/p" must not match "src/pkg/...".
        assert!(!has("src/p"));
    }

    #[test]
    fn prefetch_file_state_for_chunk_fetches_only_chunk_paths_grouped_by_repo() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE files(\
                repo_id INTEGER NOT NULL, path TEXT NOT NULL, language TEXT NOT NULL, \
                sha TEXT NOT NULL, mtime_ns INTEGER NOT NULL, byte_size INTEGER NOT NULL);",
        )
        .unwrap();
        let insert = |repo_id: i64, path: &str, sha: &str, mtime: i64| {
            conn.execute(
                "INSERT INTO files(repo_id, path, language, sha, mtime_ns, byte_size) \
                 VALUES (?1, ?2, 'rust', ?3, ?4, ?4 * 10)",
                params![repo_id, path, sha, mtime],
            )
            .unwrap();
        };
        insert(1, "src/a.rs", "sha-a", 10);
        insert(1, "src/b.rs", "sha-b", 20);
        // Same repo, but NOT part of the chunk below: must be excluded from the
        // result. This is the whole point of chunk-scoped prefetch — we must not
        // load the entire repo's file state.
        insert(1, "src/untouched.rs", "sha-x", 99);
        insert(2, "lib/c.rs", "sha-c", 30);

        let spec = crate::lang::lookup_language("rust").unwrap();
        let job = |repo_id: i64, relative: &str| FileJob {
            repo_id,
            workspace_name: format!("repo{repo_id}"),
            path: PathBuf::from(relative),
            relative: relative.to_string(),
            spec: spec.clone(),
        };
        // Chunk spans two repos and includes one path with no stored row yet.
        let chunk = vec![
            job(1, "src/a.rs"),
            job(1, "src/b.rs"),
            job(1, "src/new.rs"),
            job(2, "lib/c.rs"),
        ];

        let state = prefetch_file_state_for_chunk(&conn, &chunk).unwrap();

        let repo1 = state.get(&1).unwrap();
        assert_eq!(
            repo1.get("src/a.rs"),
            Some(&("rust".into(), "sha-a".into(), 10, 100))
        );
        assert_eq!(
            repo1.get("src/b.rs"),
            Some(&("rust".into(), "sha-b".into(), 20, 200))
        );
        // New file (no row) is simply absent — process_file_job treats absence as
        // "changed".
        assert!(repo1.get("src/new.rs").is_none());
        // The repo-1 row outside the chunk must NOT be fetched.
        assert!(repo1.get("src/untouched.rs").is_none());

        let repo2 = state.get(&2).unwrap();
        assert_eq!(
            repo2.get("lib/c.rs"),
            Some(&("rust".into(), "sha-c".into(), 30, 300))
        );
        assert!(!repo2.contains_key("src/a.rs"));
    }

    #[test]
    fn prefetch_file_state_for_chunk_handles_chunks_over_sqlite_param_limit() {
        // The watch path calls this with a whole debounced batch, which can
        // exceed SQLite's ~999 host-parameter limit. A chunk larger than
        // SQLITE_MAX_IN_PARAMS must be split into multiple IN queries rather
        // than failing with "too many SQL variables".
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE files(\
                repo_id INTEGER NOT NULL, path TEXT NOT NULL, language TEXT NOT NULL, \
                sha TEXT NOT NULL, mtime_ns INTEGER NOT NULL, byte_size INTEGER NOT NULL);",
        )
        .unwrap();

        let count = SQLITE_MAX_IN_PARAMS * 2 + 5;
        for i in 0..count {
            conn.execute(
                "INSERT INTO files(repo_id, path, language, sha, mtime_ns, byte_size) \
                 VALUES (1, ?1, 'rust', ?2, ?3, ?3)",
                params![format!("src/f{i}.rs"), format!("sha{i}"), i as i64],
            )
            .unwrap();
        }

        let spec = crate::lang::lookup_language("rust").unwrap();
        let chunk: Vec<FileJob> = (0..count)
            .map(|i| {
                let relative = format!("src/f{i}.rs");
                FileJob {
                    repo_id: 1,
                    workspace_name: "repo1".to_string(),
                    path: PathBuf::from(&relative),
                    relative,
                    spec: spec.clone(),
                }
            })
            .collect();

        let state = prefetch_file_state_for_chunk(&conn, &chunk).unwrap();
        let repo1 = state.get(&1).unwrap();
        assert_eq!(
            repo1.len(),
            count,
            "every path across all sub-batches must be fetched"
        );
        assert_eq!(
            repo1.get("src/f0.rs"),
            Some(&("rust".into(), "sha0".into(), 0, 0))
        );
        assert_eq!(
            repo1.get(&format!("src/f{}.rs", count - 1)),
            Some(&(
                "rust".into(),
                format!("sha{}", count - 1),
                (count - 1) as i64,
                (count - 1) as i64
            ))
        );
    }

    #[test]
    fn process_file_job_skips_on_mtime_and_size_without_hashing() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("a.rs");
        fs::write(&path, "fn a() {}\n")?;
        let metadata = fs::metadata(&path)?;
        let mtime_ns = metadata
            .modified()?
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos() as i64;

        let spec = crate::lang::lookup_language("rust").unwrap();
        let job = FileJob {
            repo_id: 1,
            workspace_name: "repo1".to_string(),
            path: path.clone(),
            relative: "a.rs".to_string(),
            spec: spec.clone(),
        };
        let stored = |sha: &str, mtime: i64| {
            HashMap::from([(
                1,
                HashMap::from([(
                    "a.rs".to_string(),
                    (
                        "rust".to_string(),
                        sha.to_string(),
                        mtime,
                        metadata.len() as i64,
                    ),
                )]),
            )])
        };

        // A stored sha that cannot match the file's content proves the skip
        // decision came from mtime + size alone, without reading the file.
        // `force_hash = false` mirrors the `build` path, which keeps the
        // mtime+size fast path.
        let existing = stored("not-the-real-sha", mtime_ns);
        assert!(matches!(
            process_file_job(&job, true, false, &existing)?,
            ProcessedFile::Unchanged { .. }
        ));

        // A poisoned mtime (`mark_all_files_stale` writes -1) must fall through
        // to the content check and re-extract.
        let existing = stored("not-the-real-sha", -1);
        assert!(matches!(
            process_file_job(&job, true, false, &existing)?,
            ProcessedFile::Indexed { .. }
        ));
        Ok(())
    }

    // ---- regression tests for F07, F14, F17, F20, F27 ----

    #[test]
    fn python_docstring_ignores_non_string_first_statement() -> Result<()> {
        let spec = lookup_language("python").unwrap();
        let src = b"def h():\n    print(\"hi\")\n\ndef g():\n    s = \"x\"\n    return s\n\n\
def d():\n    \"\"\"Doc.\"\"\"\n    return 1\n\ndef r():\n    r\"\"\"Raw doc.\"\"\"\n    return 1\n";
        let parsed = parse_file(&spec, src)?;
        let doc = |name: &str| {
            parsed
                .symbols
                .iter()
                .find(|s| s.name == name && s.kind == "function")
                .and_then(|s| s.docstring.clone())
        };
        assert_eq!(doc("h"), None, "a call is not a docstring");
        assert_eq!(doc("g"), None, "an assignment is not a docstring");
        assert_eq!(doc("d").as_deref(), Some("Doc."));
        assert_eq!(
            doc("r").as_deref(),
            Some("Raw doc."),
            "prefix and quotes stripped"
        );
        Ok(())
    }

    #[test]
    fn rust_symbol_range_includes_outer_attributes_and_doc() -> Result<()> {
        let spec = lookup_language("rust").unwrap();
        let src = b"/// Doc for Foo.\n#[derive(Debug, Clone)]\n#[serde(default)]\npub struct Foo;\n\n#[test]\nfn t() {}\n";
        let parsed = parse_file(&spec, src)?;
        let foo = parsed.symbols.iter().find(|s| s.name == "Foo").unwrap();
        assert_eq!(foo.start_row, 1, "range starts at the first attribute");
        assert_eq!(foo.end_row, 3);
        assert_eq!(foo.docstring.as_deref(), Some("Doc for Foo."));
        let t = parsed.symbols.iter().find(|s| s.name == "t").unwrap();
        assert_eq!(t.start_row, 5, "#[test] belongs to the fn");
        Ok(())
    }

    #[test]
    fn qualify_symbols_matches_naive_nesting() {
        fn sym(kind: &str, name: &str, range: (usize, usize, usize, usize)) -> ExtractedSymbol {
            ExtractedSymbol {
                kind: kind.into(),
                name: name.into(),
                start_row: range.0,
                start_col: range.1,
                end_row: range.2,
                end_col: range.3,
                signature: None,
                docstring: None,
            }
        }
        // Start-sorted, properly nested: imports, two classes with methods,
        // a nested class, and locals inside methods.
        let symbols = vec![
            sym("import", "os", (0, 0, 0, 9)),
            sym("class", "A", (2, 0, 20, 0)),
            sym("function", "m1", (3, 4, 6, 0)),
            sym("variable", "x", (4, 8, 4, 13)),
            sym("class", "Inner", (7, 4, 15, 0)),
            sym("function", "deep", (8, 8, 12, 0)),
            sym("variable", "y", (9, 12, 9, 17)),
            sym("function", "m2", (16, 4, 19, 0)),
            sym("class", "B", (22, 0, 30, 0)),
            sym("function", "m1", (23, 4, 29, 0)),
            sym("variable", "top", (32, 0, 32, 7)),
        ];
        // Reference: the former all-predecessors scan.
        let naive: Vec<Option<String>> = symbols
            .iter()
            .enumerate()
            .map(|(index, symbol)| {
                let mut parents: Vec<String> = symbols
                    .iter()
                    .take(index)
                    .filter(|c| c.kind != "import" && contains(c, symbol))
                    .map(|c| c.name.clone())
                    .collect();
                parents.push(symbol.name.clone());
                (parents.len() > 1).then(|| parents.join("."))
            })
            .collect();
        assert_eq!(qualify_symbols(&symbols), naive);
        assert_eq!(naive[6].as_deref(), Some("A.Inner.deep.y"));
    }

    #[test]
    fn replace_symbol_preserves_crlf_line_endings() -> Result<()> {
        let dir = tempdir()?;
        fs::write(
            dir.path().join("crlf.py"),
            "def a():\r\n    return 1\r\n\r\ndef other():\r\n    return 2\r\n",
        )?;
        let runtime = replace_runtime(dir.path(), "python");
        runtime.replace_symbol(ReplaceSymbolArgs {
            file: "crlf.py".to_string(),
            name: "a".to_string(),
            // LF-only, as an agent round-tripping a get_symbol body sends it.
            new_body: "def a():\n    return 9".to_string(),
            repo: None,
            kind: None,
            qualified: None,
            row: None,
        })?;
        let out = fs::read_to_string(dir.path().join("crlf.py"))?;
        assert_eq!(
            out,
            "def a():\r\n    return 9\r\n\r\ndef other():\r\n    return 2\r\n"
        );
        assert!(
            !out.replace("\r\n", "").contains('\n'),
            "no bare LF introduced"
        );
        Ok(())
    }

    #[test]
    fn replace_symbol_empty_body_reports_missing_new_range() -> Result<()> {
        let dir = tempdir()?;
        fs::write(
            dir.path().join("del.py"),
            "def alpha():\n    return 1\n\ndef beta():\n    return 2\n",
        )?;
        let runtime = replace_runtime(dir.path(), "python");
        let response = runtime.replace_symbol(ReplaceSymbolArgs {
            file: "del.py".to_string(),
            name: "beta".to_string(),
            new_body: String::new(),
            repo: None,
            kind: None,
            qualified: None,
            row: None,
        })?;
        assert!(
            response.new_range.is_none(),
            "deleted symbol has no new range"
        );
        assert_eq!(response.old_range.start.0, 3);
        let out = fs::read_to_string(dir.path().join("del.py"))?;
        assert!(!out.contains("beta"));
        assert!(out.contains("def alpha()"));
        Ok(())
    }

    #[test]
    fn replace_symbol_write_is_atomic() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("atomic.py");
        fs::write(&path, "def a():\n    return 1\n")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o640))?;
        }
        let runtime = replace_runtime(dir.path(), "python");
        let response = runtime.replace_symbol(ReplaceSymbolArgs {
            file: "atomic.py".to_string(),
            name: "a".to_string(),
            new_body: "def a():\n    return 2".to_string(),
            repo: None,
            kind: None,
            qualified: None,
            row: None,
        })?;
        assert_eq!(fs::read_to_string(&path)?, "def a():\n    return 2\n");
        assert!(response.new_range.is_some());
        let names: Vec<String> = fs::read_dir(dir.path())?
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            !names.iter().any(|name| name.contains("tsindex-tmp")),
            "temp file must not be left behind: {names:?}"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(fs::metadata(&path)?.permissions().mode() & 0o777, 0o640);
        }
        Ok(())
    }

    #[test]
    fn replace_symbol_disambiguates_by_qualified_name() -> Result<()> {
        let dir = tempdir()?;
        let src = "class K1:\n    def dup(self):\n        return 1\n\n\
class K2:\n    def dup(self):\n        return 2\n";
        fs::write(dir.path().join("q.py"), src)?;
        let runtime = replace_runtime(dir.path(), "python");

        let ambiguous = runtime
            .replace_symbol(ReplaceSymbolArgs {
                file: "q.py".to_string(),
                name: "dup".to_string(),
                new_body: "    def dup(self):\n        return 9".to_string(),
                repo: None,
                kind: None,
                qualified: None,
                row: None,
            })
            .expect_err("two same-kind methods must be ambiguous");
        let message = format!("{ambiguous:#}");
        assert!(
            message.contains("K1.dup") && message.contains("K2.dup"),
            "{message}"
        );
        assert!(message.contains("`qualified`"), "{message}");

        let response = runtime.replace_symbol(ReplaceSymbolArgs {
            file: "q.py".to_string(),
            name: "dup".to_string(),
            new_body: "    def dup(self):\n        return 9".to_string(),
            repo: None,
            kind: None,
            qualified: Some("K2.dup".to_string()),
            row: None,
        })?;
        let out = fs::read_to_string(dir.path().join("q.py"))?;
        assert_eq!(
            out,
            "class K1:\n    def dup(self):\n        return 1\n\n\
class K2:\n    def dup(self):\n        return 9\n"
        );
        assert_eq!(response.new_range.map(|r| r.start.0), Some(5));
        Ok(())
    }

    #[test]
    fn replace_symbol_disambiguates_by_row() -> Result<()> {
        let dir = tempdir()?;
        fs::write(
            dir.path().join("dup.py"),
            "def f():\n    return 1\n\ndef f():\n    return 2\n",
        )?;
        let runtime = replace_runtime(dir.path(), "python");
        let response = runtime.replace_symbol(ReplaceSymbolArgs {
            file: "dup.py".to_string(),
            name: "f".to_string(),
            new_body: "def f():\n    return 3".to_string(),
            repo: None,
            kind: None,
            qualified: None,
            row: Some(4),
        })?;
        let out = fs::read_to_string(dir.path().join("dup.py"))?;
        assert_eq!(out, "def f():\n    return 1\n\ndef f():\n    return 3\n");
        assert_eq!(
            response.new_range.map(|r| r.start.0),
            Some(3),
            "duplicate at row 1 must not be mistaken for the edited one"
        );
        Ok(())
    }

    #[test]
    fn replace_symbol_new_range_follows_shifted_start_row() -> Result<()> {
        let dir = tempdir()?;
        fs::write(
            dir.path().join("shift.py"),
            "def a():\n    return 1\n\ndef other():\n    return 2\n",
        )?;
        let runtime = replace_runtime(dir.path(), "python");
        let response = runtime.replace_symbol(ReplaceSymbolArgs {
            file: "shift.py".to_string(),
            name: "a".to_string(),
            new_body: "# documented\ndef a():\n    return 9".to_string(),
            repo: None,
            kind: None,
            qualified: None,
            row: Some(1),
        })?;
        let range = response
            .new_range
            .expect("symbol still exists, one row lower");
        assert_eq!(range.start.0, 1);
        Ok(())
    }

    #[test]
    fn create_exclusive_sibling_never_reuses_a_path() -> Result<()> {
        let dir = tempdir()?;
        let target = dir.path().join("t.py");
        fs::write(&target, "")?;
        let (first, _) = create_exclusive_sibling(&target)?;
        let (second, _) = create_exclusive_sibling(&target)?;
        assert_ne!(first, second);
        assert_eq!(first.parent(), Some(dir.path()));
        assert!(first.exists() && second.exists());
        Ok(())
    }

    #[test]
    fn line_terminator_picks_dominant() {
        assert_eq!(line_terminator("a\r\nb\r\nc\n"), "\r\n");
        assert_eq!(line_terminator("a\nb\r\nc\n"), "\n");
        assert_eq!(line_terminator("no newline"), "\n");
    }
}
