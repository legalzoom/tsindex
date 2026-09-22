use std::env;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use clap::{ArgAction, Args, Parser, Subcommand};
use serde_json::json;

use crate::config::{RepoConfig, TsIndexConfig, config_path, db_path};
use crate::index::{
    BuildStats, EnclosingSymbolArgs, FindReferencesArgs, GetSymbolArgs, OutlineArgs, QueryArgs,
    Runtime, print_json, print_language_reports,
};
use crate::mcp::{serve_http, serve_mcp, spawn_parent_watchdog};

#[derive(Debug, Parser)]
#[command(name = "tsindex")]
#[command(version)]
#[command(about = "Tree-sitter codebase index and MCP retrieval server")]
#[command(
    long_about = "Build a lightweight tree-sitter index for a codebase, query symbols and references from the CLI, and expose the same retrieval primitives over MCP or HTTP."
)]
pub struct Cli {
    #[arg(
        long,
        global = true,
        help = "Workspace root to index or query. Defaults to the current directory."
    )]
    root: Option<PathBuf>,
    #[arg(
        long,
        global = true,
        help = "Path to the SQLite index database. Defaults to .tsindex/index.db under --root."
    )]
    db: Option<PathBuf>,
    #[arg(
        long = "languages",
        global = true,
        help = "Restrict indexing and queries to one or more explicit languages. \
                (Named --languages so it does not collide with `query --language`.)"
    )]
    languages: Vec<String>,
    /// Pre-1.6 spelling of `--languages`. Deliberately not `global` so it
    /// cannot collide with `query --language`; it is therefore only accepted
    /// before the subcommand, which is where existing automation puts it.
    #[arg(long = "language", hide = true, conflicts_with = "languages")]
    legacy_language: Vec<String>,
    #[arg(long, global = true, help = "Print command results as JSON.")]
    json: bool,
    #[arg(
        long = "no-refresh",
        global = true,
        help = "Do not run the one-time incremental refresh before starting serve."
    )]
    no_refresh: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    #[command(about = "Detect languages and create .tsindex/config.toml for a workspace.")]
    Init(InitArgs),
    #[command(about = "Run a full index build over the workspace.")]
    Build(BuildArgs),
    #[command(about = "Run an incremental index update, skipping unchanged files.")]
    Update(BuildArgs),
    #[command(about = "Continuously watch the workspace and refresh the index on changes.")]
    Watch(WatchArgs),
    #[command(about = "Show the detected language breakdown for the workspace.")]
    Languages {
        #[arg(
            long,
            default_value_t = 0.02,
            help = "Hide languages below this share in non-JSON output."
        )]
        min_share: f64,
    },
    #[command(about = "Look up indexed symbols by name, with optional kind and file filters.")]
    Symbol(SymbolCommand),
    #[command(about = "Show a file outline without reading the full file body.")]
    Outline(OutlineCommand),
    #[command(
        about = "Find the symbol(s) whose range encloses a (file, row, col) position.",
        long_about = "Useful for stack-trace lines, commit hunks, or any time you have a position and need to know what symbol you're standing in. Returns matches outermost \u{2192} innermost; use --depth to retrieve a module/class/method trail in one call."
    )]
    Enclosing(EnclosingCommand),
    #[command(about = "Find syntactic references to a symbol name across the workspace.")]
    Refs(RefsCommand),
    #[command(about = "Run a raw tree-sitter query against indexed files for one language.")]
    Query(QueryCommand),
    #[command(about = "List, add, or remove repos in the multi-repo catalog.")]
    Repos(ReposCommand),
    #[command(about = "Start the MCP server or the HTTP JSON server.")]
    Serve(ServeCommand),
}

#[derive(Debug, Subcommand)]
pub enum RepoAction {
    #[command(about = "List configured repos.")]
    List,
    #[command(about = "Add a repo to the catalog.")]
    Add(RepoAddCommand),
    #[command(about = "Remove a repo from the catalog.")]
    Remove(RepoRemoveCommand),
}

#[derive(Debug, Args)]
pub struct BuildArgs {
    #[arg(
        short = 'j',
        long,
        default_value_t = 0,
        help = "Parallel indexing jobs (0 = auto-detect CPU count)."
    )]
    jobs: usize,
}

#[derive(Debug, Args)]
pub struct WatchArgs {
    #[arg(
        short = 'j',
        long,
        default_value_t = 0,
        help = "Parallel indexing jobs (0 = auto-detect CPU count)."
    )]
    jobs: usize,
    #[arg(
        long,
        help = "Exit when the parent process that spawned this watcher dies. Lets editor integrations spawn a detached watcher that cleans itself up without relying on the editor to send a signal."
    )]
    exit_with_parent: bool,
}

#[derive(Debug, Args)]
pub struct InitArgs {
    #[arg(help = "Optional path to initialize. If omitted, uses --root or the current directory.")]
    path: Option<PathBuf>,
    #[arg(
        long,
        help = "Overwrite an existing .tsindex/config.toml. Without it, init refuses to \
                clobber a catalog (the next build would drop every repo not re-listed)."
    )]
    force: bool,
}

#[derive(Debug, Args)]
pub struct SymbolCommand {
    #[arg(num_args = 1.., help = "One or more symbol names to look up (batched into one query).")]
    names: Vec<String>,
    #[arg(long, help = "Limit matches to one configured repo.")]
    repo: Option<String>,
    #[arg(
        long,
        help = "Filter matches to a symbol kind such as function, class, or method."
    )]
    kind: Option<String>,
    #[arg(long = "file", help = "Filter matches to files matching this glob.")]
    file_glob: Option<String>,
    #[arg(long, help = "Include the full symbol body in the response.")]
    include_body: bool,
    #[arg(
        long,
        default_value_t = 0,
        help = "Extra lines of context to include before and after the symbol body."
    )]
    context_lines: usize,
    #[arg(
        long,
        value_parser = parse_nonzero_usize,
        help = "Keep this many source lines per returned body; elision adds one marker line."
    )]
    max_body_lines: Option<usize>,
    #[arg(long, help = "Cap on matches returned per name before truncation.")]
    limit: Option<usize>,
    #[arg(
        long,
        help = "Skip this many matches per name before emitting (pagination)."
    )]
    offset: Option<usize>,
}

#[derive(Debug, Args)]
pub struct EnclosingCommand {
    #[arg(help = "Repository-relative file path.")]
    file: String,
    #[arg(
        num_args = 1..,
        help = "One or more 1-based rows to look up (several rows are batched into one call)."
    )]
    rows: Vec<usize>,
    #[arg(
        long,
        help = "Optional 0-based column for point-precise lookup. Omit for row-only matching."
    )]
    col: Option<usize>,
    #[arg(
        long,
        help = "Repo name when the same file path exists in multiple repos."
    )]
    repo: Option<String>,
    #[arg(long, help = "Include the full symbol body in the response.")]
    include_body: bool,
    #[arg(
        long,
        default_value_t = 0,
        help = "Extra lines of context to include before and after the symbol body."
    )]
    context_lines: usize,
    #[arg(
        long,
        default_value_t = 1,
        help = "How many enclosing levels to return. 1 = innermost only; 2 adds its parent; etc."
    )]
    depth: usize,
    #[arg(
        long,
        value_parser = parse_nonzero_usize,
        help = "Keep this many source lines per returned body; elision adds one marker line."
    )]
    max_body_lines: Option<usize>,
    #[arg(
        long,
        value_parser = parse_nonzero_usize,
        help = "Cap on how many rows are resolved in one batch call (batch path only)."
    )]
    limit: Option<usize>,
    #[arg(
        long,
        help = "Stable pagination offset over the requested rows list (batch path only)."
    )]
    offset: Option<usize>,
}

#[derive(Debug, Args)]
pub struct OutlineCommand {
    #[arg(help = "Repository-relative file path to inspect.")]
    file: String,
    #[arg(
        long,
        help = "Repo name when the same file path exists in multiple repos."
    )]
    repo: Option<String>,
    #[arg(
        long,
        default_value_t = 2,
        help = "Outline depth. 1 shows top-level items only."
    )]
    depth: usize,
    #[arg(
        long,
        default_value_t = true,
        hide = true,
        help = "Include signature snippets when available."
    )]
    include_signatures: bool,
    #[arg(
        long = "no-include-signatures",
        action = ArgAction::SetTrue,
        help = "Omit signature snippets from the outline."
    )]
    no_include_signatures: bool,
    #[arg(
        long,
        default_value_t = true,
        hide = true,
        help = "Include docstrings or leading comments when available."
    )]
    include_docstrings: bool,
    #[arg(
        long = "no-include-docstrings",
        action = ArgAction::SetTrue,
        help = "Omit docstrings or leading comments from the outline."
    )]
    no_include_docstrings: bool,
    #[arg(
        long = "include-imports",
        action = ArgAction::SetTrue,
        help = "Include import/use rows in the outline (off by default)."
    )]
    include_imports: bool,
    #[arg(
        long = "body-for",
        value_name = "NAME",
        help = "Inline the full body of these symbol names into the outline (repeatable)."
    )]
    include_bodies_for: Vec<String>,
    #[arg(
        long,
        value_parser = parse_nonzero_usize,
        help = "Keep this many source lines per inlined body; elision adds one marker line."
    )]
    max_body_lines: Option<usize>,
    #[arg(
        long,
        help = "Cap on top-level outline symbols returned before truncation."
    )]
    limit: Option<usize>,
    #[arg(
        long,
        help = "Skip this many top-level symbols before emitting (pagination)."
    )]
    offset: Option<usize>,
}

#[derive(Debug, Args)]
pub struct RefsCommand {
    #[arg(num_args = 1.., help = "One or more identifier names to search for (batched into one query).")]
    names: Vec<String>,
    #[arg(long, help = "Limit matches to one configured repo.")]
    repo: Option<String>,
    #[arg(long, help = "Limit matches to files matching this glob.")]
    scope: Option<String>,
    #[arg(long, help = "Exclude common test file paths from the results.")]
    exclude_tests: bool,
    #[arg(long, help = "Include declaration-name occurrences in results.")]
    include_declarations: bool,
    #[arg(
        long,
        default_value_t = true,
        hide = true,
        help = "Group results by file instead of returning a flat list."
    )]
    group_by_file: bool,
    #[arg(
        long = "no-group-by-file",
        action = ArgAction::SetTrue,
        help = "Return a flat list instead of grouping results by file."
    )]
    no_group_by_file: bool,
    #[arg(
        long,
        default_value_t = 1,
        help = "Number of surrounding lines to include in each reference snippet."
    )]
    snippet_lines: usize,
    #[arg(long, help = "Cap on references returned before truncation.")]
    limit: Option<usize>,
    #[arg(long, help = "Skip this many references before emitting (pagination).")]
    offset: Option<usize>,
    #[arg(
        long = "counts-only",
        action = ArgAction::SetTrue,
        help = "Return exact per-file reference counts and the total, without per-reference rows or snippets. Cheap aggregate for 'how many / which files' questions."
    )]
    counts_only: bool,
}

#[derive(Debug, Args)]
pub struct QueryCommand {
    #[arg(help = "Raw tree-sitter S-expression query.")]
    query: String,
    #[arg(
        long,
        help = "Language to run the query against, such as python or typescript."
    )]
    language: String,
    #[arg(long, help = "Limit query execution to one configured repo.")]
    repo: Option<String>,
    #[arg(
        long = "file",
        help = "Limit query execution to files matching this glob."
    )]
    file_glob: Option<String>,
    #[arg(long, help = "Return only one capture name from the query results.")]
    capture: Option<String>,
    #[arg(long, help = "Maximum number of captures to return.")]
    limit: Option<usize>,
    #[arg(long, help = "Skip this many captures before emitting (pagination).")]
    offset: Option<usize>,
}

#[derive(Debug, Args)]
pub struct ServeCommand {
    #[arg(
        long,
        help = "Serve the four retrieval primitives over MCP on stdin/stdout."
    )]
    mcp: bool,
    #[arg(long, help = "Serve the same primitives over a small HTTP JSON API.")]
    http: bool,
    #[arg(long, help = "Port for --http. Defaults to the configured HTTP port.")]
    port: Option<u16>,
    #[arg(
        long,
        default_value = "127.0.0.1",
        help = "Address for --http to bind. Non-loopback binds expose the \
                server to the network; pair with --allowed-host so clients \
                can pass the Host check."
    )]
    bind: String,
    #[arg(
        long = "allowed-host",
        help = "Additional Host header hostname to accept, besides the \
                localhost forms. Repeatable. The port must still match the \
                bound port."
    )]
    allowed_hosts: Vec<String>,
}

#[derive(Debug, Args)]
pub struct ReposCommand {
    #[command(subcommand)]
    action: RepoAction,
}

#[derive(Debug, Args)]
pub struct RepoAddCommand {
    // Accepts either `<PATH>` (name derived from the path's final segment) or
    // `<NAME> <PATH>`. A single positional with num_args=1..=2 sidesteps clap's
    // inability to disambiguate which slot a lone argument fills when both
    // `name` and `path` are declared as separate optional positionals.
    //
    // `required = true` is *not* redundant with `num_args = 1..=2`: clap-derive
    // treats `Vec<T>` as inherently optional, so without it the field accepts
    // zero values. Removing it changes the help text from `<NAME_OR_PATH>` to
    // `[NAME_OR_PATH]` and trades clap's standard "required arguments not
    // provided" error for the ad-hoc fallback in `manage_repos`. Keep both.
    #[arg(
        required = true,
        num_args = 1..=2,
        value_names = ["NAME_OR_PATH", "PATH"],
        help = "Either <PATH> alone (name is derived from the path's final segment) or <NAME> <PATH>."
    )]
    args: Vec<String>,
}

#[derive(Debug, Args)]
pub struct RepoRemoveCommand {
    #[arg(help = "Repo name to remove from the catalog.")]
    name: String,
}

pub fn run() -> Result<()> {
    let mut cli = Cli::parse();
    merge_legacy_language_flag(&mut cli);
    if should_start_parent_watchdog(&cli.command) {
        spawn_parent_watchdog();
    }
    match cli.command {
        Command::Init(ref args) => init(&cli, args),
        Command::Build(ref args) => with_runtime(&cli, |runtime| {
            let stats = runtime.with_jobs(args.jobs).build(false, None)?;
            if cli.json {
                print_json(&build_stats_json(&stats))?;
            } else {
                println!(
                    "indexed {} files (skipped {}, failed {})",
                    stats.indexed, stats.skipped, stats.failed
                );
            }
            check_full_build_failures(&stats)
        }),
        Command::Update(ref args) => with_runtime(&cli, |runtime| {
            let stats = runtime.with_jobs(args.jobs).build(true, None)?;
            if cli.json {
                print_json(&build_stats_json(&stats))
            } else {
                println!(
                    "updated index: {} changed, {} unchanged, {} failed",
                    stats.indexed, stats.skipped, stats.failed
                );
                Ok(())
            }
        }),
        Command::Watch(ref args) => {
            with_runtime(&cli, |runtime| runtime.with_jobs(args.jobs).watch())
        }
        Command::Languages { min_share } => with_runtime(&cli, |runtime| {
            let rows = runtime.detected_languages()?;
            if cli.json {
                print_json(&rows)
            } else {
                print_language_reports(&rows, min_share);
                Ok(())
            }
        }),
        Command::Symbol(ref command) => with_runtime(&cli, |runtime| {
            let response = runtime.get_symbol(GetSymbolArgs {
                name: None,
                names: Some(command.names.clone()),
                repo: command.repo.clone(),
                kind: command.kind.clone(),
                file_glob: command.file_glob.clone(),
                include_body: command.include_body,
                context_lines: command.context_lines,
                max_body_lines: command.max_body_lines,
                limit: command.limit,
                offset: command.offset,
            })?;
            print_json(&response)
        }),
        Command::Outline(ref command) => with_runtime(&cli, |runtime| {
            let response = runtime.list_file_outline(OutlineArgs {
                file: command.file.clone(),
                repo: command.repo.clone(),
                depth: command.depth,
                include_signatures: command.include_signatures && !command.no_include_signatures,
                include_docstrings: command.include_docstrings && !command.no_include_docstrings,
                include_imports: command.include_imports,
                include_bodies_for: (!command.include_bodies_for.is_empty())
                    .then(|| command.include_bodies_for.clone()),
                max_body_lines: command.max_body_lines,
                limit: command.limit,
                offset: command.offset,
            })?;
            print_json(&response)
        }),
        Command::Enclosing(ref command) => with_runtime(&cli, |runtime| {
            // One row keeps the precise-column single path; several rows batch
            // (row-only, so `col` is ignored).
            let (row, rows, col) = match command.rows.as_slice() {
                [single] => (Some(*single), None, command.col),
                many => (None, Some(many.to_vec()), None),
            };
            let response = runtime.enclosing_symbol(EnclosingSymbolArgs {
                file: command.file.clone(),
                row,
                rows,
                col,
                repo: command.repo.clone(),
                include_body: command.include_body,
                context_lines: command.context_lines,
                depth: command.depth,
                max_body_lines: command.max_body_lines,
                limit: command.limit,
                offset: command.offset,
            })?;
            print_json(&response)
        }),
        Command::Refs(ref command) => with_runtime(&cli, |runtime| {
            if command.counts_only {
                let response = runtime.find_reference_counts(FindReferencesArgs {
                    name: None,
                    names: Some(command.names.clone()),
                    repo: command.repo.clone(),
                    scope: command.scope.clone(),
                    exclude_tests: command.exclude_tests,
                    include_declarations: command.include_declarations,
                    group_by_file: false,
                    snippet_lines: 1,
                    limit: None,
                    offset: None,
                    counts_only: true,
                })?;
                return print_json(&response);
            }
            let response = runtime.find_references(FindReferencesArgs {
                name: None,
                names: Some(command.names.clone()),
                repo: command.repo.clone(),
                scope: command.scope.clone(),
                exclude_tests: command.exclude_tests,
                include_declarations: command.include_declarations,
                group_by_file: command.group_by_file && !command.no_group_by_file,
                snippet_lines: command.snippet_lines,
                limit: command.limit,
                offset: command.offset,
                counts_only: false,
            })?;
            print_json(&response)
        }),
        Command::Query(ref command) => with_runtime(&cli, |runtime| {
            // `query` reads live files, not the index, so it does not refresh
            // the index first (unlike the indexed read commands). This keeps a
            // raw query usable against a brand-new root without a prior build.
            let response = runtime.query(QueryArgs {
                language: command.language.clone(),
                repo: command.repo.clone(),
                query: command.query.clone(),
                file_glob: command.file_glob.clone(),
                capture: command.capture.clone(),
                limit: command.limit,
                offset: command.offset,
            })?;
            print_json(&response)
        }),
        Command::Repos(ref command) => manage_repos(&cli, command),
        Command::Serve(ref command) => with_runtime(&cli, |runtime| {
            let runtime = runtime.with_jobs(0);
            if command.mcp {
                spawn_background_watch(&cli, &runtime)?;
                return serve_mcp(runtime);
            }
            refresh_if_needed(&cli, &runtime)?;
            // `--mcp` returned above; everything else (`--http` or no mode at
            // all) serves the HTTP dashboard.
            let port = command.port.unwrap_or(runtime.config.server.http_port);
            serve_http(runtime, &command.bind, port, &command.allowed_hosts)
        }),
    }
}

fn build_stats_json(stats: &BuildStats) -> serde_json::Value {
    json!({
        "indexed": stats.indexed,
        "skipped": stats.skipped,
        "failed": stats.failed,
    })
}

fn should_start_parent_watchdog(command: &Command) -> bool {
    match command {
        Command::Serve(command) => command.mcp,
        Command::Watch(args) => args.exit_with_parent,
        _ => false,
    }
}

fn parse_nonzero_usize(value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("`{value}` is not a valid positive integer"))?;
    if parsed == 0 {
        Err("value must be at least 1".into())
    } else {
        Ok(parsed)
    }
}

fn init(cli: &Cli, args: &InitArgs) -> Result<()> {
    let raw = args.path.as_deref().or(cli.root.as_deref());
    if let Some(p) = raw
        && !p.exists()
    {
        std::fs::create_dir_all(p)
            .with_context(|| format!("failed to create root directory {}", p.display()))?;
    }
    let root = resolve_root(raw)?;
    let existing = config_path(&root);
    if existing.exists() && !args.force {
        return Err(anyhow!(
            "{} already exists; re-run with --force to overwrite it (this rewrites the \
             catalog to a single repo and drops [server] settings), or use `tsindex repos add`",
            existing.display()
        ));
    }
    let detected = Runtime::new(
        root.clone(),
        db_path(&root),
        TsIndexConfig {
            repos: vec![RepoConfig {
                name: default_repo_name(&root),
                path: root.to_string_lossy().to_string(),
                languages: Vec::new(),
                ignore: Vec::new(),
            }],
            ..TsIndexConfig::default()
        },
        Vec::new(),
    )
    .detected_languages()?;
    let mut config = TsIndexConfig::default();
    config.root.path = ".".to_string();
    config.repos = vec![RepoConfig {
        name: default_repo_name(&root),
        path: root.to_string_lossy().to_string(),
        languages: Vec::new(),
        ignore: Vec::new(),
    }];
    config.server.http_port = 7337;
    config.server.query_timeout_ms = 2_000;
    config.server.default_result_limit = 200;
    let path = config.write(&root)?;
    if cli.json {
        print_json(&json!({
            "root": root,
            "config": path,
            "languages": detected,
        }))
    } else {
        println!("wrote {}", path.display());
        print_language_reports(&detected, 0.0);
        Ok(())
    }
}

/// `--language` (pre-1.6) is accepted as a hidden alias of `--languages`.
fn merge_legacy_language_flag(cli: &mut Cli) {
    if cli.languages.is_empty() {
        cli.languages = std::mem::take(&mut cli.legacy_language);
    }
}

/// A full `build` that could not read or parse some files still completes
/// (their existing rows are kept) but exits non-zero so scripts and CI notice
/// instead of shipping an index that is silently missing symbols.
fn check_full_build_failures(stats: &BuildStats) -> Result<()> {
    if stats.failed > 0 {
        return Err(anyhow!(
            "{} file(s) failed to index (unreadable or unparseable); see warnings above",
            stats.failed
        ));
    }
    Ok(())
}

fn with_runtime(cli: &Cli, f: impl FnOnce(Runtime) -> Result<()>) -> Result<()> {
    let root = resolve_root(cli.root.as_deref())?;
    let config = TsIndexConfig::load_or_default(&root)?;
    let db = cli.db.clone().unwrap_or_else(|| db_path(&root));
    // A `--db` override may point at another catalog's index; never let this
    // root's repo list decide which of that catalog's repos get deleted.
    let runtime =
        Runtime::new(root, db, config, cli.languages.clone()).with_repo_pruning(cli.db.is_none());
    f(runtime)
}

fn refresh_if_needed(cli: &Cli, runtime: &Runtime) -> Result<()> {
    if cli.no_refresh {
        return Ok(());
    }
    let stats = runtime.build(true, None)?;
    eprintln!(
        "refreshed index: {} changed, {} unchanged, {} failed",
        stats.indexed, stats.skipped, stats.failed
    );
    Ok(())
}

/// Index maintenance for `serve --mcp`, run off the startup path. The watch
/// loop performs the startup refresh (a synchronous refresh of a large catalog
/// root can take minutes, which blows the MCP client's startup timeout before
/// `initialize` is ever answered) and then keeps the index fresh for the
/// server's lifetime, so MCP sessions need no external watcher process. Only
/// the cheap schema bootstrap stays synchronous, so queries against a
/// brand-new root return empty results instead of "no such table" while the
/// first build runs.
fn spawn_background_watch(cli: &Cli, runtime: &Runtime) -> Result<()> {
    // Bootstrap the schema even under --no-refresh: the flag opts out of
    // refreshing the index, not of having a database, and skipping it would
    // turn every query on a brand-new root into "no such table".
    runtime.initialize()?;
    if cli.no_refresh {
        return Ok(());
    }
    let runtime = runtime.clone();
    std::thread::spawn(move || {
        // The startup build inside `watch()` can lose a lock race with a
        // concurrent `tsindex build`/second server or hit a transient I/O
        // error; a one-shot failure would leave this session serving a stale
        // index for its whole lifetime. Retry a few times before giving up.
        const ATTEMPTS: u32 = 5;
        for attempt in 1..=ATTEMPTS {
            match runtime.watch() {
                Ok(()) => return,
                Err(e) if attempt < ATTEMPTS => {
                    eprintln!(
                        "warning: background index watch failed (attempt {attempt}/{ATTEMPTS}): {e:#}"
                    );
                    std::thread::sleep(std::time::Duration::from_secs(2 * u64::from(attempt)));
                }
                Err(e) => eprintln!("warning: background index watch failed; giving up: {e:#}"),
            }
        }
    });
    Ok(())
}

fn manage_repos(cli: &Cli, command: &ReposCommand) -> Result<()> {
    let root = resolve_root(cli.root.as_deref())?;
    let mut config = TsIndexConfig::load_or_default(&root)?;
    match &command.action {
        RepoAction::List => {
            let runtime = Runtime::new(
                root.clone(),
                cli.db.clone().unwrap_or_else(|| db_path(&root)),
                config,
                cli.languages.clone(),
            );
            let repos = runtime.repos()?;
            if cli.json {
                print_json(&repos)
            } else {
                for repo in repos {
                    println!("{:<20} {}", repo.name, repo.path);
                }
                Ok(())
            }
        }
        RepoAction::Add(args) => {
            // Disambiguate the 1-vs-2 positional form: a single argument is the
            // path (name auto-derived); two arguments are <NAME> <PATH>.
            let (provided_name, raw_path) = match args.args.as_slice() {
                [path] => (None, PathBuf::from(path)),
                [name, path] => (Some(name.clone()), PathBuf::from(path)),
                // Clap is configured to enforce 1..=2 args, but match must be
                // exhaustive — surface a proper error instead of panicking if
                // the parser config and this arm ever drift apart.
                _ => return Err(anyhow!("repos add expects either <PATH> or <NAME> <PATH>")),
            };
            let path = raw_path.canonicalize().map_err(|error| {
                anyhow!(
                    "failed to resolve repo path {}: {error}",
                    raw_path.display()
                )
            })?;
            // Derive from the canonicalized path so `.`, `..`, and `./foo`
            // resolve to the actual directory name instead of falling back
            // to the "default" placeholder in default_repo_name.
            let name = provided_name.unwrap_or_else(|| default_repo_name(&path));
            if config.repos.iter().any(|repo| repo.name == name) {
                return Err(anyhow!("repo {} already exists", name));
            }
            config.repos.push(RepoConfig {
                name: name.clone(),
                path: path.to_string_lossy().to_string(),
                languages: Vec::new(),
                ignore: Vec::new(),
            });
            config.write(&root)?;
            if cli.json {
                print_json(&config.repos)
            } else {
                println!("added repo {} -> {}", name, path.display());
                Ok(())
            }
        }
        RepoAction::Remove(args) => {
            let before = config.repos.len();
            config.repos.retain(|repo| repo.name != args.name);
            if before == config.repos.len() {
                return Err(anyhow!("repo {} does not exist", args.name));
            }
            config.write(&root)?;
            if cli.json {
                print_json(&config.repos)
            } else {
                println!("removed repo {}", args.name);
                Ok(())
            }
        }
    }
}

fn resolve_root(root: Option<&Path>) -> Result<PathBuf> {
    let path = match root {
        Some(p) => p.to_path_buf(),
        None => env::current_dir()?,
    };
    Ok(path.canonicalize()?)
}

fn default_repo_name(path: &Path) -> String {
    path.file_name()
        .and_then(|value| value.to_str())
        .map(|value| value.to_string())
        .unwrap_or_else(|| "default".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // --- Review regressions (F06, F09, F34) ----------------------------------

    #[test]
    fn init_refuses_to_overwrite_existing_catalog() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().to_string_lossy().to_string();
        let parse = |argv: &[&str]| match Cli::try_parse_from(argv).unwrap().command {
            Command::Init(args) => (Cli::try_parse_from(argv).unwrap(), args),
            other => panic!("expected init, got {other:?}"),
        };
        let (cli, args) = parse(&["tsindex", "--json", "init", &root]);
        init(&cli, &args).expect("first init writes the config");
        std::fs::write(
            config_path(dir.path()),
            "[root]\npath = \".\"\n[server]\nmax_response_chars = 1\n",
        )
        .unwrap();

        let (cli, args) = parse(&["tsindex", "--json", "init", &root]);
        let err = init(&cli, &args).expect_err("second init must refuse");
        assert!(
            err.to_string().contains("--force"),
            "error names the override: {err}"
        );
        assert!(
            std::fs::read_to_string(config_path(dir.path()))
                .unwrap()
                .contains("max_response_chars = 1"),
            "the existing config is untouched"
        );

        let (cli, args) = parse(&["tsindex", "--json", "init", &root, "--force"]);
        init(&cli, &args).expect("--force overwrites");
        assert!(
            !std::fs::read_to_string(config_path(dir.path()))
                .unwrap()
                .contains("max_response_chars = 1")
        );
    }

    #[test]
    fn query_language_flag_does_not_collide_with_global_languages() {
        let cli = Cli::try_parse_from(["tsindex", "query", "(x)", "--language", "python"])
            .expect("query --language parses");
        match cli.command {
            Command::Query(command) => assert_eq!(command.language, "python"),
            other => panic!("expected query, got {other:?}"),
        }
        let cli = Cli::try_parse_from(["tsindex", "--languages", "python", "build"])
            .expect("global --languages parses");
        assert_eq!(cli.languages, vec!["python".to_string()]);
        // Both together, in either position.
        let cli = Cli::try_parse_from([
            "tsindex",
            "--languages",
            "rust",
            "query",
            "(x)",
            "--language",
            "python",
        ])
        .unwrap();
        assert_eq!(cli.languages, vec!["rust".to_string()]);
    }

    #[test]
    fn legacy_global_language_flag_still_parses() {
        let mut cli = Cli::try_parse_from(["tsindex", "--language", "python", "build"])
            .expect("pre-1.6 --language parses before the subcommand");
        merge_legacy_language_flag(&mut cli);
        assert_eq!(cli.languages, vec!["python".to_string()]);
        let mut cli =
            Cli::try_parse_from(["tsindex", "--languages", "rust", "build"]).expect("parses");
        merge_legacy_language_flag(&mut cli);
        assert_eq!(cli.languages, vec!["rust".to_string()]);
    }

    #[test]
    fn full_build_with_failures_is_an_error() {
        let ok = BuildStats {
            indexed: 1,
            skipped: 0,
            failed: 0,
        };
        assert!(check_full_build_failures(&ok).is_ok());
        let failed = BuildStats {
            indexed: 1,
            skipped: 0,
            failed: 2,
        };
        let error = check_full_build_failures(&failed).expect_err("failed > 0 exits non-zero");
        assert!(error.to_string().contains("2 file(s) failed"));
    }

    #[test]
    fn build_json_reports_failed_count() {
        let stats = BuildStats {
            indexed: 1,
            skipped: 2,
            failed: 3,
        };
        assert_eq!(
            build_stats_json(&stats),
            json!({ "indexed": 1, "skipped": 2, "failed": 3 })
        );
    }

    // --- Build/update flag wiring (from main, PR #16) ------------------------

    #[test]
    fn build_command_accepts_parallel_jobs_flag() {
        let cli =
            Cli::try_parse_from(["tsindex", "build", "-j", "2"]).expect("build -j should parse");
        match cli.command {
            Command::Build(args) => assert_eq!(args.jobs, 2),
            other => panic!("expected build command, got {other:?}"),
        }
    }

    #[test]
    fn update_command_accepts_parallel_jobs_flag() {
        let cli = Cli::try_parse_from(["tsindex", "update", "--jobs", "3"])
            .expect("update --jobs should parse");
        match cli.command {
            Command::Update(args) => assert_eq!(args.jobs, 3),
            other => panic!("expected update command, got {other:?}"),
        }
    }

    #[test]
    fn serve_command_accepts_no_refresh_flag() {
        let cli = Cli::try_parse_from(["tsindex", "--no-refresh", "serve", "--mcp"])
            .expect("--no-refresh should parse before serve");
        assert!(cli.no_refresh);
        match cli.command {
            Command::Serve(command) => assert!(command.mcp),
            other => panic!("expected serve command, got {other:?}"),
        }
    }

    #[test]
    fn max_body_lines_rejects_zero() {
        let result = Cli::try_parse_from(["tsindex", "symbol", "foo", "--max-body-lines", "0"]);
        assert!(result.is_err(), "symbol max_body_lines=0 should fail");

        let result = Cli::try_parse_from([
            "tsindex",
            "enclosing",
            "src/app.rs",
            "1",
            "--max-body-lines",
            "0",
        ]);
        assert!(result.is_err(), "enclosing max_body_lines=0 should fail");

        let result =
            Cli::try_parse_from(["tsindex", "outline", "src/app.rs", "--max-body-lines", "0"]);
        assert!(result.is_err(), "outline max_body_lines=0 should fail");
    }

    #[test]
    fn global_flags_parse_before_and_after_subcommand() {
        // The global flags (--json, --root, --db, --languages, --no-refresh)
        // must parse in ANY position — before the subcommand, after it, or
        // interleaved with its args — so agents don't trip over flag ordering.
        // (Regression: an agent running `refs path --json` used to get
        // "unexpected argument '--json'".)
        // Each case: argv, whether --json should be set, expected --root.
        let cases: &[(&[&str], bool, Option<&str>)] = &[
            // before the subcommand (canonical)
            (&["tsindex", "--json", "refs", "path"], true, None),
            // after the subcommand's positionals
            (&["tsindex", "refs", "path", "--json"], true, None),
            // between subcommand and positional
            (&["tsindex", "refs", "--json", "path"], true, None),
            // --root after the subcommand
            (
                &["tsindex", "refs", "path", "--root", "/tmp/x"],
                false,
                Some("/tmp/x"),
            ),
            // --json + --counts-only both after
            (
                &["tsindex", "refs", "path", "--json", "--counts-only"],
                true,
                None,
            ),
            // mixed: root before, json after
            (
                &["tsindex", "--root", "/tmp/x", "refs", "path", "--json"],
                true,
                Some("/tmp/x"),
            ),
        ];
        for (argv, want_json, want_root) in cases {
            let cli =
                Cli::try_parse_from(*argv).unwrap_or_else(|e| panic!("should parse {argv:?}: {e}"));
            assert_eq!(cli.json, *want_json, "--json flag for {argv:?}");
            let got_root = cli.root.as_ref().map(|p| p.to_string_lossy().to_string());
            assert_eq!(got_root.as_deref(), *want_root, "--root value for {argv:?}");
        }
    }

    #[test]
    fn serve_command_without_mode_defaults_to_http_dashboard() {
        let cli = Cli::try_parse_from(["tsindex", "serve"])
            .expect("serve should parse without an explicit mode");
        match cli.command {
            Command::Serve(command) => {
                assert!(!command.mcp);
                assert!(!command.http);
                assert_eq!(command.port, None);
            }
            other => panic!("expected serve command, got {other:?}"),
        }
    }

    #[test]
    fn parent_watchdog_starts_for_mcp_serve_and_watch_exit_with_parent() {
        for argv in [
            ["tsindex", "serve", "--mcp"].as_slice(),
            ["tsindex", "watch", "--exit-with-parent"].as_slice(),
        ] {
            let cli = Cli::try_parse_from(argv).expect("watchdog-eligible command should parse");
            assert!(
                should_start_parent_watchdog(&cli.command),
                "watchdog should start early for {argv:?}"
            );
        }

        for argv in [
            ["tsindex", "watch"].as_slice(),
            ["tsindex", "serve", "--http"].as_slice(),
            ["tsindex", "build"].as_slice(),
            ["tsindex", "update"].as_slice(),
            ["tsindex", "languages"].as_slice(),
            ["tsindex", "symbol", "foo"].as_slice(),
        ] {
            let cli = Cli::try_parse_from(argv).expect("short-lived command should parse");
            assert!(
                !should_start_parent_watchdog(&cli.command),
                "watchdog should not start for {argv:?}"
            );
        }
    }

    // --- `repos add` helpers -------------------------------------------------

    // Build a Cli for the `repos add` flow against a given catalog root.
    fn add_cli(root: &Path, args: &[&str]) -> Cli {
        let mut argv = vec![
            "tsindex",
            "--root",
            root.to_str().expect("catalog path is valid UTF-8"),
            "repos",
            "add",
        ];
        argv.extend_from_slice(args);
        Cli::try_parse_from(argv).expect("parse")
    }

    fn repos_command(cli: &Cli) -> &ReposCommand {
        match &cli.command {
            Command::Repos(repos) => repos,
            other => panic!("expected Repos command, got {other:?}"),
        }
    }

    // --- `repos add` parser arity (verifies the conclusion from review #1) ---

    #[test]
    fn parser_rejects_zero_positional_args() {
        // `required = true` is what makes this fail at the clap layer rather
        // than falling through to the runtime fallback.
        let result = Cli::try_parse_from(["tsindex", "repos", "add"]);
        assert!(result.is_err(), "zero args should be a clap error");
    }

    #[test]
    fn parser_accepts_one_positional_arg() {
        let cli = Cli::try_parse_from(["tsindex", "repos", "add", "/tmp/foo"])
            .expect("one positional should parse");
        let RepoAction::Add(add) = &repos_command(&cli).action else {
            panic!("expected Add action");
        };
        assert_eq!(add.args, vec!["/tmp/foo"]);
    }

    #[test]
    fn parser_accepts_two_positional_args() {
        let cli = Cli::try_parse_from(["tsindex", "repos", "add", "payments", "/tmp/foo"])
            .expect("two positionals should parse");
        let RepoAction::Add(add) = &repos_command(&cli).action else {
            panic!("expected Add action");
        };
        assert_eq!(add.args, vec!["payments", "/tmp/foo"]);
    }

    #[test]
    fn parser_rejects_three_positional_args() {
        let result = Cli::try_parse_from(["tsindex", "repos", "add", "a", "b", "c"]);
        assert!(result.is_err(), "three args should be a clap error");
    }

    // --- `repos add` disambiguation (the four cases from review #3) ----------

    #[test]
    fn lone_path_derives_name_from_final_segment() {
        let catalog = TempDir::new().expect("catalog tempdir");
        let target = catalog.path().join("payments-service");
        std::fs::create_dir_all(&target).expect("create target dir");

        let cli = add_cli(catalog.path(), &[target.to_str().expect("utf8")]);
        manage_repos(&cli, repos_command(&cli)).expect("manage_repos add");

        let config = TsIndexConfig::load_or_default(catalog.path()).expect("load config");
        assert_eq!(config.repos.len(), 1);
        assert_eq!(config.repos[0].name, "payments-service");
    }

    #[test]
    fn two_args_preserve_explicit_name() {
        let catalog = TempDir::new().expect("catalog tempdir");
        let target = catalog.path().join("auth-service");
        std::fs::create_dir_all(&target).expect("create target dir");

        let cli = add_cli(catalog.path(), &["my-auth", target.to_str().expect("utf8")]);
        manage_repos(&cli, repos_command(&cli)).expect("manage_repos add");

        let config = TsIndexConfig::load_or_default(catalog.path()).expect("load config");
        assert_eq!(config.repos.len(), 1);
        assert_eq!(config.repos[0].name, "my-auth");
        // Explicit name wins even when the path's final segment would have
        // produced a different default.
        assert_ne!(config.repos[0].name, "auth-service");
    }

    #[test]
    fn duplicate_detection_runs_on_derived_name() {
        let catalog = TempDir::new().expect("catalog tempdir");
        let target = catalog.path().join("dupe-name");
        std::fs::create_dir_all(&target).expect("create target dir");

        let target_str = target.to_str().expect("utf8").to_string();
        let cli = add_cli(catalog.path(), &[&target_str]);
        manage_repos(&cli, repos_command(&cli)).expect("first add");

        let cli = add_cli(catalog.path(), &[&target_str]);
        let err = manage_repos(&cli, repos_command(&cli))
            .expect_err("re-adding same path should error on derived name collision");
        let msg = err.to_string();
        assert!(
            msg.contains("dupe-name"),
            "error mentions derived name: {msg}"
        );
        assert!(msg.contains("already exists"), "error: {msg}");
    }

    #[test]
    fn canonicalize_before_deriving_avoids_default_fallback() {
        // Inputs like `.`, `..`, or `tempdir/..` have `file_name() == None`,
        // so deriving from the raw path falls through to default_repo_name's
        // "default" placeholder. Canonicalizing first resolves them to a real
        // directory whose file_name is a usable name. This is the case
        // `repos add .` exercises in the wild — we use `tempdir/..` here
        // instead of `.` to avoid `set_current_dir`, which fights cargo's
        // parallel test runner.
        let temp = TempDir::new().expect("tempdir");
        let raw = temp.path().join("..");
        assert!(
            raw.file_name().is_none(),
            "test premise: `tempdir/..` lacks a file_name segment"
        );

        // Pre-canonicalization derivation hits the placeholder.
        assert_eq!(
            default_repo_name(&raw),
            "default",
            "raw path with no file_name should fall back to \"default\""
        );

        // Post-canonicalization derivation recovers a real name (the parent
        // directory's name) — which is exactly what `repos add` ends up with.
        let canonical = raw.canonicalize().expect("canonicalize `tempdir/..`");
        assert_ne!(
            default_repo_name(&canonical),
            "default",
            "canonicalized path should yield a real directory name, not the placeholder"
        );
    }
}
