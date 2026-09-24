# tsindex

`tsindex` is a Rust CLI and server for building a lightweight tree-sitter index over one repo or a catalog of many repos. It stores symbols, outlines, and syntactic references in SQLite, then exposes a small retrieval surface for local CLI use, MCP clients, or a simple HTTP API.

## What it does

- Detects the dominant languages in each configured repo.
- Walks the repo while honoring `.gitignore` and `.tsindexignore`.
- Indexes symbols and references into `.tsindex/index.db` with repo-aware storage.
- Lets you query symbols, file outlines, references, and raw tree-sitter patterns.
- Supports multiple repos in one catalog with per-query `--repo` filters.
- Supports Python, JavaScript/TypeScript/TSX, HTML, CSS/SCSS, Rust, Go, Java,
  Kotlin, Swift, C/C++/C#, PHP, Ruby, Bash, SQL, Makefiles, JSON, YAML, and Markdown.
- Can serve the same primitives over MCP or HTTP.

SQL files (`.sql`, case-insensitive) use the
[tree-sitter-sequel](https://github.com/DerekStride/tree-sitter-sql) grammar.
Symbols include databases, schemas, tables, columns, views, indexes, functions,
sequences, types, triggers, and CTEs. Schema-qualified objects are looked up by
their final name; identifier case and quoting are preserved. Dialect coverage
follows the grammar, so unsupported syntax can produce partial results; SQL
inside string literals is not indexed separately. The grammar treats unqualified
double-quoted expressions as string literals; qualify column references such as
`users."User ID"` to distinguish them.

---

## Install

### Prebuilt binaries

When a release is available, download the macOS binary for your architecture
(arm64 or x86_64) and `checksums.txt` from the
[Releases page](https://github.com/legalzoom/tsindex/releases).
Verify its checksum with `shasum -a 256`, make it executable with `chmod +x`,
and place it in a directory on your `PATH`. Releases also include the MIT and
Apache-2.0 license texts, third-party notices, and build provenance when available.
See [SECURITY.md](SECURITY.md) for verification details.

### From source

Requires the Rust toolchain. If you don't have one, install via [rustup](https://rustup.rs):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Add cargo to your PATH in your shell profile. For example, add this line to `~/.bashrc` or `~/.zshrc`:
```bash
export PATH="$HOME/.cargo/bin:$PATH"
```

Then clone and install the binary:

```bash
git clone https://github.com/legalzoom/tsindex.git
cd tsindex
cargo install --path . --locked
```

The binary lands in `~/.cargo/bin/tsindex`, which rustup already added to your `PATH`.
To upgrade later, pull and re-run `cargo install --path . --locked`.
To remove: `cargo uninstall tsindex`.

## Codex skills

This repo includes shareable Codex skills at `skills/tsindex` and
`skills/goal-tsindex`. They refresh the local index on demand, help configure
the MCP command, and tell Codex and spawned agents to prefer tsindex MCP tools
for code navigation. The `goal-tsindex` skill is the same workflow exposed
through the `/goal-tsindex` alias.

Install or update the skills locally:

```bash
skills_dir="${CODEX_HOME:-$HOME/.codex}/skills"
mkdir -p "$skills_dir"
cp -R skills/tsindex "$skills_dir/"
cp -R skills/goal-tsindex "$skills_dir/"
```

Start a new Codex session after installing so the skill metadata is loaded.
Then invoke `$tsindex` or `/goal-tsindex` from a repo, or from `~/dev` for a
multi-repo catalog. Set `TSINDEX_DEV_ROOT` if your multi-repo root lives
somewhere other than `~/dev`. The skills expect the `tsindex` binary on `PATH`;
set `TSINDEX_BIN` if you keep the binary somewhere else.

### RTK alongside tsindex

RTK (`https://github.com/rtk-ai/rtk`) and tsindex solve complementary context
problems: RTK reduces noisy shell-command output before it reaches the model,
while tsindex avoids whole-file reads by serving structured code-navigation
results over MCP. Use both in the same agent session: keep your RTK command hooks
enabled, and configure tsindex as an MCP server with:

```bash
tsindex --root /path/to/repo serve --mcp
```

For a multi-repo catalog, point `--root` at the catalog root (for example
`~/dev`) and keep passing `repo="<name>"` in tsindex tool calls. The HTTP
dashboard's Adoption panel also reports whether repo-local or home RTK config is
present and shows the matching tsindex MCP command for the current root.

The skill startup scripts do not install persistent watchers. By default they
run `tsindex --root <target> update` as an eager warm-up and exit.
`tsindex --root <target> serve --mcp` runs its own in-process watcher for the
lifetime of the server (startup refresh, then filesystem notifications), so MCP
sessions stay fresh without an external `watch`. Use `TSINDEX_START_MODE=watch`
only when you want a foreground watcher for CLI-only sessions that never start
`serve --mcp`.

---

## Quick start

Initialize a catalog rooted at one repo:

```bash
tsindex --root /path/to/repo init
```

`init` refuses to overwrite an existing `.tsindex/config.toml`; pass `--force`
to regenerate it (this discards `[server]` tuning and extra `[[repos]]`).

Indexing honors `.gitignore` by default, so common dependency, build, cache,
coverage, and runtime output paths usually stay excluded without tsindex-specific
configuration. Add `.tsindexignore` only for extra index-only excludes.

Independently of ignore files, any path containing one of these directory
components is never indexed and cannot currently be whitelisted:
`node_modules`, `vendor`, `target`, `.venv`, `.git`, `dist`, `build`, `.next`,
`.turbo`, `.parcel-cache`. Committed `vendor/` trees or packages named `build`
or `dist` are therefore invisible to the index.

For PHP projects, keep common generated paths such as `vendor/`,
`.phpunit.cache/`, `.phpstan.cache/`, `.psalm/cache/`, `bootstrap/cache/`, and
`storage/logs/` in `.gitignore` so both Git and tsindex skip them.

Build the first index:

```bash
tsindex --root /path/to/repo build
```

Update incrementally after edits:

```bash
tsindex --root /path/to/repo update
```

`update` is the command to run when `watch` was not running. It walks the
configured repo(s), compares files against the existing index, indexes new or
changed files, skips unchanged files, and purges rows for files that were
deleted or are no longer indexable. Index-backed CLI commands (`symbol`,
`outline`, `enclosing`, and `refs`) read the existing SQLite index; they do not
refresh it on every query. `query` is the exception: it reads live source files,
not the index, so it works without a prior `build` and does not gate on the
initial index completing.

`serve` (HTTP) runs one incremental refresh at startup so sessions begin from a
fresh index; keep `watch` running alongside it for long-lived HTTP sessions.
`serve --mcp` instead runs a watcher in-process for the lifetime of the server:
it performs the startup refresh in the background (so the MCP `initialize`
handshake is not delayed by a large catalog) and then keeps the index fresh from
filesystem notifications, so no separate `watch` is needed. Pass `--no-refresh`
to skip the startup refresh; under `--mcp` this also disables the in-process
watcher, so the index is then only as fresh as the last `build`/`update`.

Watch for changes and refresh the index on save:

```bash
tsindex --root /path/to/repo watch
```

## Multi-repo catalog workflow

After `init`, the root repo is registered as the first catalog entry. You can add more repos:

```bash
tsindex --root /path/to/catalog repos add payments /path/to/payments-repo
tsindex --root /path/to/catalog repos add auth /path/to/auth-repo
tsindex --root /path/to/catalog repos list
```

Build the shared catalog index:

```bash
tsindex --root /path/to/catalog build
```

Query across every repo:

```bash
tsindex --root /path/to/catalog symbol authenticate_user
```

Or limit a query to one repo:

```bash
tsindex --root /path/to/catalog symbol authenticate_user --repo auth
tsindex --root /path/to/catalog refs authenticate_user --repo auth
tsindex --root /path/to/catalog query '(function_definition name: (identifier) @fn)' --language python --repo auth
```

---

## CLI commands

- `init`: Detects languages and writes `.tsindex/config.toml`.
- `build`: Runs a full index build over the workspace.
- `update`: Runs an incremental build, indexing only changed/new files and purging stale files.
- `watch`: Uses filesystem notifications to keep the index up to date.
- `languages`: Prints the detected language breakdown.
- `symbol`: Looks up indexed symbols by name. Accepts several names to batch the lookup into one query.
- `outline`: Returns the structure of one file without dumping the full file. Pass `--body-for <name>` (repeatable) to inline selected symbol bodies in the same call.
- `enclosing`: Finds the symbol range that contains a file position. Accepts several rows to resolve a whole stack trace / hunk set in one call.
- `refs`: Finds syntactic identifier occurrences (not binding-aware semantic references), batched and paginated.
- `query`: Runs a raw tree-sitter query for one language.
- `repos`: Lists, adds, and removes repos in the catalog.
- `serve`: Starts the MCP or HTTP server.

You can inspect per-command help with:

```bash
tsindex <command> --help
```

Examples:

```bash
tsindex symbol authenticate_user --include-body
tsindex symbol authenticate_user --repo auth --include-body
tsindex symbol authenticate_user create_session list_orders   # batch several names in one query
tsindex outline src/auth/login.py --repo auth
tsindex outline src/auth/login.py --body-for authenticate_user --body-for create_session  # structure + selected bodies in one call
tsindex refs authenticate_user --scope 'src/**'
tsindex refs authenticate_user create_session list_orders   # references for several names in one query
tsindex enclosing src/server.py 42 87 130                   # enclosing symbol for several lines (e.g. a stack trace)
tsindex query '(function_definition name: (identifier) @fn)' --language python --capture fn
```

## Global flags

- `--root`: Workspace root to index or query.
- `--db`: Override the SQLite database path.
- `--languages <lang>`: Restrict work to one or more explicit languages
  (repeatable). `query` has its own required `--language <lang>` selecting the
  grammar to run the pattern with.
- `--json`: Emit JSON output for CLI commands.
- `--no-refresh`: Skip the one-time incremental refresh before `serve` starts
  (and, under `serve --mcp`, the in-process watcher).

Several query commands also support `--repo` to restrict results to one configured repo.

## MCP server

Start the MCP server on stdio:

```bash
tsindex --root /path/to/repo serve --mcp
```

The server exposes five read tools:

- `get_symbol` — pass `names: [...]` to fetch several symbols in one call (one round-trip; results keyed by each match's `name`)
- `list_file_outline` — pass `include_bodies_for: [...]` to inline selected symbol bodies in the same call (orient + fetch in one round-trip)
- `enclosing_symbol` — pass `rows: [...]` to resolve several positions (a whole stack trace / hunk set) in one call; answers come back per row under `results`
- `find_references` — bounded, paginated syntactic occurrences for one or more identifiers; its MCP input requires `names: ["identifier"]` even for a single identifier, avoiding ambiguous conditional schemas in tool-calling clients; verify before dependency-sensitive refactors
- `query`

Over the MCP/stdio transport it also exposes one write tool:

- `replace_symbol`: rewrites a function/class/method in place by name. It
  locates the symbol by re-parsing the file live (so it is not fooled by a
  stale index), replaces the symbol's lines with `new_body`, and refuses the
  write if that would introduce a syntax error the file didn't already have —
  leaving the file untouched on rejection.
  When several symbols share a name and kind (two `fn new` in different
  `impl` blocks, the same method on two classes), pass `qualified`
  (e.g. `"K2.dup"`, as returned by `get_symbol`) or `row` (1-based start
  row) to pick one; the ambiguity error lists both for each candidate.
  The write goes through a temp file and `rename`, so a crash mid-write
  cannot truncate the source. `new_range` is omitted when the named symbol
  no longer exists after the edit (empty body, rename). This removes the need to quote the
  old code just to anchor a whole-symbol replacement. Unlike the built-in
  editor — which requires reading a file before editing it — `replace_symbol`
  needs no prior read, so it is the cheapest way to rewrite a known symbol body
  in a large file (it carries the symbol, not the whole file, into context). It
  is intentionally *not* offered over HTTP, which stays read-only.

Source ranges are compact arrays `[start_row, start_col, end_row, end_col]`.
Rows are 1-based for editor/grep compatibility; columns are 0-based **UTF-8
byte offsets** (tree-sitter's convention, not character counts — they differ on
lines containing non-ASCII text) and end coordinates follow tree-sitter's
exclusive-end convention. The `col` input of `enclosing_symbol` uses the same
byte convention.
`list_file_outline` omits `import`/`use` rows by default; pass
`include_imports: true` to include them.
When `include_bodies_for` names a symbol whose source cannot be read or no
longer matches the index, the node carries `body_unavailable` with the reason
and the response is marked `partial: true` instead of failing the whole call.

`find_references` drops the declaring occurrence by default
(`include_declarations: false`) for classes, functions, methods, types, and
similar named declarations in every language; value bindings (variables,
properties, constants, JSON/YAML keys, Make targets) are still reported because
their name is the reference site too.
In C/C++ a function prototype is indexed with kind `prototype` while its
definition keeps `function`, so `kind` alone distinguishes them in `get_symbol`
and `replace_symbol`.

`find_references` with `snippet_lines` greater than 1 returns a block of source
with the reference in the *middle*, so the single `range` does not by itself say
which block line is the reference. Those results carry `snippet_start_row` (the
row of the block's first line, same 1-based convention as `range`), and the
reference sits at `range[0] - snippet_start_row` within `snippet`. Single-line
snippets omit the field, because `range` already is the line. Assuming `range`
points at the block's first line silently reports every hit N rows off — that is
a real failure observed in benchmarking, not a hypothetical.

Read result counts and bodies are bounded by default, and paginated responses
expose `truncated` plus `next_offset` when more results are available.
`list_file_outline` paginates whole top-level roots, so a single root carries its
entire requested subtree; the serialized-byte budget rejects an oversized lone
root with guidance to lower `depth` rather than silently dropping children.
Server limits are configurable under
`[server]` in `.tsindex/config.toml`:

```toml
[server]
http_port = 7337
query_timeout_ms = 2000
default_result_limit = 200
max_result_limit = 300
default_body_line_limit = 120
max_body_line_limit = 1000
capture_char_limit = 2000
max_response_chars = 45000
```

`http_port` is the default port for `serve --http`; `query_timeout_ms` bounds
the wall-clock time a raw `query` spends before returning `timed_out: true`.

The reference-page limits are sized against the MCP client's tool-result cap,
measured on a 1,607-reference identifier at `snippet_lines: 0` (~134 chars per
reference): 200 refs (28 KB) and 300 refs (45 KB) are accepted, 500 refs (67 KB)
is hard-rejected by Claude Code with `exceeds maximum allowed tokens` and spilled
to a file — which makes the call useless to the agent. Raising
`max_result_limit` past 300 re-opens that failure. Note that `limit` alone is
not a guarantee: `snippet_lines: 3` roughly doubles bytes per reference, so a
300-reference page can still exceed the cap. `max_response_chars` is the real
guard: the response is additionally capped by a serialized-byte budget which,
when a page would otherwise exceed it, shrinks the page (adjusting
`returned`/`next_offset`, or the per-name quota for `get_symbol`) so the payload
stays within the configured budget. Outlines shrink only by whole top-level
roots.
Set it to `0` to disable the budget — page size is then governed by `limit`
alone.

Files parsed with tree-sitter error recovery mark their results `partial: true`.
During the initial background build, the indexed read tools (`get_symbol`,
`list_file_outline`, `enclosing_symbol`, `find_references`) return an explicit
`index is not ready` error instead of treating an incomplete index as an
authoritative empty result. `query` is exempt — it reads live files, so it
returns results immediately even before the first build finishes.

Raw `query` responses set `truncated` when execution stops before all captures are returned. If this
was caused by the configured query timeout instead of the result limit, the response also includes
`timed_out: true`.

`find_references` returns a `total_files` field: the count of **distinct** `(repo, file)` pairs
across the full filtered result set — post-filter (scope, `exclude_tests`, `include_declarations`)
and pre-pagination. The `groups` array covers only the returned slice, so its length is not the
distinct-file count when the response is truncated; `total_files` is the fix for that. It is
identical whether the caller asks for `limit=3` or `limit=5000`.

When a reference's source no longer matches the indexed SHA (the index is stale
for that file), `find_references` cannot safely slice a snippet from the wrong
text. Instead of emitting a silently blank `snippet`, the row carries a
`snippet_unavailable` string with an actionable reason (e.g. "source no longer
matches the index; run `tsindex build` before trusting snippets"), and the
response is flagged `partial: true`. Re-run after `tsindex build` to restore
snippets.

## HTTP server

Start the local dashboard and HTTP API:

```bash
tsindex --root /path/to/repo serve
```

By default, `serve` starts the HTTP dashboard/API on the configured HTTP port
(`7337` unless changed in `.tsindex/config.toml`). You can also pass the mode
and port explicitly:

```bash
tsindex --root /path/to/repo serve --http --port 7337
```

Then open `http://127.0.0.1:7337/dashboard`.

The server binds `127.0.0.1` and only answers requests whose `Host` header is a
localhost form on the bound port (a DNS-rebinding mitigation). `--bind <addr>`
binds another address and `--allowed-host <name>` (repeatable) accepts an
additional `Host` name, for example a Kubernetes Service DNS name. **The Host
check is not authentication**: a non-loopback bind exposes the read API and the
`POST /repos/*` mutation endpoints to anyone who can reach the port and set a
matching `Host` header. Keep the default loopback bind unless the network path
is already access-controlled.

Available routes:

- `GET /health`
- `GET /dashboard`
- `GET /dashboard/data`
- `GET /repos`
- `POST /repos/add`
- `POST /repos/remove`
- `POST /repos/rebuild`
- `POST /tools/list`
- `POST /tools/call`

Example:

```bash
curl -s http://127.0.0.1:7337/health
curl -s http://127.0.0.1:7337/tools/list -X POST
curl -s http://127.0.0.1:7337/tools/call \
  -H 'Content-Type: application/json' \
  -d '{"name":"get_symbol","arguments":{"name":"authenticate_user","repo":"auth","include_body":true}}'
```

The dashboard reports savings two ways. Both are **heuristic estimates**, not
accounting totals, and neither measures your actual model billing or any
realized saving.

The **potential** estimate is an indexed-codebase projection: indexed file bytes
are approximated as tokens at 4 bytes per token, multiplied by a heuristic
cache-read reduction rate (30.8%), then priced at a notional $3 per million
input tokens. These are fixed illustrative assumptions, not a validated
performance guarantee across languages, repositories, models, or tasks.

The **usage-tally** figure is also a heuristic estimate: every indexed tool call
appends an event (the source bytes the response delivered vs. the bytes
returned) to `~/.tsindex/<repo>/savings.jsonl` (set `TSINDEX_HOME` to relocate
that directory; `cargo test`/`cargo run` point it at `target/` via
`.cargo/config.toml` so development never touches your real home), and the dashboard rolls those up
per repo and globally across all past sessions, priced at the cache-read
$0.30-per-million notional rate. It is a conservative lower bound on the bytes
tsindex returned instead of a full read — an estimate of what you might
otherwise have carried into context, not a realized or observed saving against
your bill.

---

## Benchmarks

`bench/payload_size_old_vs_new.py` compares deterministic MCP response sizes
between two locally built versions. Its docstring contains setup instructions.
Build a separate index with each binary, and use a repository whose contents
you are permitted to process. The output measures bytes and approximate token
counts, not model accuracy, billing, or realized savings.

---

## Developing

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution and validation guidance.

For iterative work, run the CLI through Cargo so you don't have to reinstall on every change:

```bash
cargo run -- --help
cargo run -- --root /path/to/repo build
```

Build without installing:

```bash
cargo build              # debug build at ./target/debug/tsindex
cargo build --release    # release build at ./target/release/tsindex
```

### Validation

```bash
cargo fmt
cargo test
```

## Releases and license

Maintainers publish a release by pushing a `vX.Y.Z` tag matching `Cargo.toml`
from the reviewed `main` branch. GitHub Actions builds the macOS binaries,
generates checksums and provenance, and attaches the project license texts and
third-party notices. Existing release versions are not overwritten.

tsindex is available under either the [MIT license](LICENSE-MIT) or the
[Apache License, Version 2.0](LICENSE-APACHE), at your option
(`MIT OR Apache-2.0`). Dependency licenses and copyright notices are listed in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

### Project layout

- [src/app.rs](./src/app.rs): CLI definition and command dispatch
- [src/index.rs](./src/index.rs): indexing, SQLite schema, and query API
- [src/lang.rs](./src/lang.rs): language registry and tree-sitter queries
- [src/mcp.rs](./src/mcp.rs): MCP and HTTP serving layer
- [tests/integration.rs](./tests/integration.rs): fixture-based integration tests
