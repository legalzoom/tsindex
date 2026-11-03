# Agent-safe filters and per-worktree indexes

- **Status:** Proposed
- **Date:** 2026-11-02
- **Baseline:** tsindex 1.8.0 (`08a6eff`)

## Summary

Two changes, shipped separately:

1. **No silent empties.** When an argument filters out every candidate, say so
   in the response instead of returning an empty result that looks normal.
   This covers five cases, all reproduced against 1.8.0.
2. **Per-worktree indexes.** The main checkout keeps its `.tsindex/`. Each
   linked Git worktree gets its own `<worktree>/.tsindex/`, created on first
   use and seeded from the main index. A server started in the main checkout
   can answer for any of its worktrees through `repo`.

## Motivation

We audited 60 Claude Code sessions in `iq-flow` (11,865 indexed files): 1,429
Bash calls against 32 tsindex calls. Agents fall back to `rg`/`sed` mostly by
habit, but these bugs push them the same way. A wrong filter returns
`{"matches": [], "total": 0}`, which an agent reads as "no usages" and then
stops trusting the tool.

191 of those Bash calls `cd`'d into another checkout of the same repository,
usually a linked worktree. The server only indexes the checkout it was started
in, so `rg` was the only option there.

## Part 1: No silent empties

### Shared mechanism

Add an optional `diagnostics` array to every read-tool response:

```json
"diagnostics": [
  { "code": "kind_filtered_out", "message": "…", "hint": "…" }
]
```

The field is additive: leave it out when empty, so existing clients see no
change. Use a diagnostic when the call is valid but probably not what the
caller meant. Use `InvalidParams` (-32602) when the call cannot be right.

### G1. A bare directory in `scope`/`file_glob` matches nothing (P0)

**Repro:**
`find_references(names=["useLlcCp2Enabled"], scope="apps/iq-flow/src/llc")`
returns 0 matches, while `scope="apps/iq-flow/src/llc/**"` returns 4 files.
`get_symbol(file_glob="apps/iq-flow/src/llc/q1")` returns 0 the same way.

**Cause:** `build_glob_matcher` (`src/index.rs:4469`) passes the string
straight to `Glob::new`, so a directory path compiles to a literal that
matches only a file with that exact path. The callers are `get_symbol`
(`:1178`), `find_references`/counts (`:1829`) and `query` (`:2496`).

**Change:** replace `build_glob_matcher` with `build_path_filter(pattern)`:

- Normalize: strip a leading `./` and a trailing `/`. Turn an absolute path
  under the workspace root into a repo-relative one; reject one outside the
  root with `InvalidParams`.
- No glob metacharacters (`* ? [ {`): match `path == p` or a path that starts
  with `p/`. A directory and a single file then both work.
- Otherwise compile as a glob, as today.
- If the filter matches no indexed file in the selected repos, add
  `filter_matched_no_files` with the pattern, so a typo is distinguishable
  from "no usages". This costs one scan of `files.path` per repo.

### G2. `kind` must match exactly, and a mismatch looks like "not found" (P1)

**Repro:** `get_symbol(name="Container", kind="variable",
file_glob="apps/iq-flow/src/llc/q1/**")` returns 0, because the component is
recorded as `function`. Arrow-function components are recorded as `variable`,
so callers can't predict the kind.

**Change:**

- Reject a kind that never appears in the index with `InvalidParams`, listing
  the valid kinds (`SELECT DISTINCT kind FROM symbols`).
- When `kind` removes every match for a name that does exist, add
  `kind_filtered_out` with the kinds that were found, for example
  `{"function": 1}`.
- Out of scope: changing how arrow functions are classified.

### G3. Unscoped `query` times out, and one language per call (P2)

**Repro:**
`query(language="tsx", query='(call_expression function: (identifier) @fn (#eq? @fn "useAutoRouteSubmit"))')`
fails with "query timed out before reaching offset 0" after 2,000 ms. With
`file_glob="apps/iq-flow/src/llc/q2/**"` it returns 24 captures. A `tsx` query
also never sees `.ts` files.

**Cause:** `query` walks the live tree (`walk_source_files`, `:2876`). For
each file it runs `detect_language_from_file` (`:2532`) **before** applying
the glob, then reads and parses every file of that language.

**Change:**

- Take candidates from the `files` table (`WHERE language = ?`, plus the glob)
  when the index is ready. Keep the live walk only for a root that has never
  been indexed; the comment at `:2485` gives the reason.
- Apply the path filter before language detection on the fallback walk.
- Literal prefilter: for each `#eq? @capture "literal"` predicate, skip any
  file whose bytes don't contain the literal (use `memchr`), before parsing.
  This covers the common "calls to X" query.
- Accept `languages: [..]` alongside `language`. Compile once per language;
  if one fails to compile, record `query_compile_failed` for that language
  and keep going. `language: "typescript"` alone keeps today's behavior.
- Add `files_scanned` to the timeout error.

**Target:** the unscoped repro above finishes within the default 2,000 ms on
`iq-flow`. Add it to `bench/`.

### G4. `enclosing_symbol` with a column past the end returns nothing (P1)

**Repro:** `enclosing_symbol(file="…/experimentUtilities.ts", row=170, col=4)`
returns `{"file": …, "repo": …}`: no `matches` key at all. Row 170 is the
closing `};` of `isLlcCp2ActiveForSession` (range ends at `[170, 2]`).
`row=155, col=4` works.

**Cause:** the column clause `(s.end_row > ?3 OR s.end_col > ?4)`
(`src/index.rs:2415`) drops any symbol whose last line ends before `col`.
`col` is already optional.

**Change:**

- Always include `matches` (possibly `[]`).
- If the column-qualified match is empty, retry by row only and add
  `col_ignored`.
- If it's still empty, add `no_enclosing_symbol` ("row is at top level").
- In the schema, describe `col` as optional and suggest leaving it out unless
  the line holds more than one symbol.

### G5. An unknown `repo` returns empty (P0)

**Repro:** `get_symbol(name="useAutoRouteSubmit", repo="iq-flow-cp2-cross-device")`
returns `{"matches": [], "total": 0}`. Agents that compute the repo name from
`basename $(git rev-parse --show-toplevel)` inside a worktree hit this every
time.

**Cause:** every read path filters with `repo != workspace.name` or
`(? IS NULL OR r.name = ?)` and never validates the name.

**Change:** validate `repo` once in `call_tool` (`src/mcp.rs:709`) before
dispatch. If unknown, return `InvalidParams` naming the known repos, including
the worktrees from Part 2 and whether each is indexed. This also fixes the
same failure on the HTTP transport.

### Compatibility

All new fields are additive. G1 changes what a bare-path pattern matches. It
used to match only an exact file path, and still does, so nothing that
returned results before returns different results now. G2 and G5 turn
responses that used to be silently empty into errors; we know of no client
that relies on the empty result.

## Part 2: Per-worktree indexes

### Today

- `serve` resolves its root from `--root` or the cwd (`resolve_root`,
  `src/app.rs:835`). The database is `<root>/.tsindex/index.db`
  (`db_path`, `src/config.rs:190`).
- One `Runtime` per server (`serve_mcp`, `src/mcp.rs:25`). `call_tool`
  dispatches every tool to it.
- Agent launchers pass `--root "$(git rev-parse --show-toplevel)"` at startup,
  so a session started in the main checkout can't see a worktree created
  later. Naming that worktree in `repo` hits G5.
- A session started inside a worktree with no `.tsindex/` falls back to
  `load_or_default` and does a cold build. For `iq-flow` that took 17.3 s and
  produced a 250 MB database (measured 2026-11-02).
- Seeding a worktree database from the main one doesn't help yet. The
  content-hash path only treats a file as unchanged when the hash **and** the
  mtime both match (`src/index.rs:3227`), and a fresh checkout gives every
  file a new mtime, so every file is parsed again.

### Goals

- The main checkout keeps `<main>/.tsindex/`. Each linked worktree that's
  used gets `<worktree>/.tsindex/`.
- A server started in any checkout can answer for every worktree of the same
  repository through `repo`.
- A worktree's first query is ready in under 5 s on `iq-flow`, compared with
  17.3 s for a cold build.
- Unused worktrees cost nothing: no index and no watcher.

### Non-goals

- Indexing every worktree at startup.
- Merging results across worktrees.
- Historical revisions (`git show <rev>:path`).

### W1. Discovery

Follow the same rule as ADR 0001: read `.git` files directly and never run
the Git CLI.

1. Find the common dir from the server's root. If `<root>/.git` is a
   directory, it is the common dir. If it's a file, follow `gitdir:` and then
   that directory's `commondir`.
2. The main worktree root is the parent of the common dir.
3. Linked worktrees: for each `<common>/worktrees/<id>/gitdir`, the worktree
   root is the parent of the `.git` file it points to. Skip entries whose
   target is missing (prunable).
4. Discover again on any unknown-repo lookup, and when
   `<common>/worktrees/` changes. `ignore_inputs` already watches shared Git
   metadata, so add this directory as an input.

This finds worktrees wherever they live: siblings
(`~/dev/iq-flow-cp2-cross-device`), under the repo (`.claude/worktrees/*`,
`playpen/*`), or anywhere else.

### W2. Naming

- A worktree's repo name is its directory basename. That's what agent hooks
  compute with `basename "$(git rev-parse --show-toplevel)"`.
- On a collision with the main repo or another worktree, use
  `<basename>@<id>` for each colliding entry, where `<id>` is the
  `.git/worktrees/<id>` directory name. The G5 error lists the full names.
- `repo` also accepts an absolute worktree root.

### W3. Routing

Replace the single `Runtime` in `serve_mcp`/`serve_http` with:

```rust
struct Runtimes {
    primary: Runtime,
    worktrees: Mutex<HashMap<String, WorktreeEntry>>, // name -> path + lazy Runtime
}
```

`call_tool` resolves `args.repo` before dispatch:

- `repo` omitted, or naming the primary repo: use `primary` (unchanged).
- A known worktree: open its runtime lazily (W4) and dispatch to it.
- Unknown: discover again once (W1), then fail with the G5 error.

`replace_symbol` routes the same way, so edits land in the worktree's files.

### W4. Creating a worktree's index

On the first request for a worktree:

1. If `<wt>/.tsindex/index.db` exists, open it and run an incremental update.
2. Otherwise create `<wt>/.tsindex/`:
   - Write `config.toml` copied from the primary's, with `repos` replaced by
     one entry: `name = <W2 name>`, `path = "."`. Keep the server settings.
   - Write `.tsindex/.gitignore` containing `*` if the worktree doesn't
     already ignore `.tsindex`.
   - Seed `index.db` from the main checkout's database with the SQLite online
     backup API, which gives a consistent copy while the primary is writing.
     Then run `UPDATE repos SET name = ?, root_path = ?`.
   - Run an incremental update.
3. Requests block until the update finishes, using the existing
   index-not-ready handling, with a 30 s ceiling. After that the request
   fails with a retryable error.

**Required change C1:** in the content-hash check
(`src/index.rs:3222-3230`), a file whose language and hash match but whose
mtime differs becomes `ProcessedFile::Touched`. That updates
`mtime_ns`/`byte_size` with no parse. Seeding then costs a read and a hash per
file instead of a parse. It also stops the watcher's forced-hash path from
re-parsing files that were only touched.

Only seed from a database whose common dir matches; W1 guarantees this.

### W5. Freshness

v1 adds no watchers for worktrees. Before serving a worktree request, run an
incremental update if the last one is more than 2 s old (`worktree_refresh_ms`,
default 2000). The stat-only fast path makes a no-op update one `stat()` per
file. Watchers bounded by an LRU (`max_active_worktrees`) can come later if
the 2 s refresh measurably costs too much.

### W6. Lifecycle

- `git worktree remove` deletes the directory, including `.tsindex/`, so
  there's nothing to clean up.
- If a cached entry's root disappears, drop it and fail with
  `worktree <name> no longer exists at <path>`.
- A server started inside a linked worktree applies W1–W4 to itself. If its
  own `.tsindex/` is missing, it seeds from the main worktree's index instead
  of doing a cold build.

### W7. Surface

- CLI: `tsindex worktrees` lists name, path, `indexed` and the last update
  time, with `--json` supported.
- MCP: an optional `list_repos` tool returning the same data as the HTTP
  repos handler (`handle_repos_list`, `src/mcp.rs:781`), plus worktrees.
  The G5 error already gives agents the list, so this can wait.
- Agent rules, outside tsindex: once this ships, change "tsindex indexes only
  the checkout the session started in" to "in a linked worktree, pass
  `repo=<worktree directory name>`."

## Test plan

Unit and integration tests in `tests/` build Git fixtures by hand (a `.git`
directory, `worktrees/<id>/gitdir`, `commondir`), so they don't depend on the
Git CLI at runtime.

| Item | Test |
|---|---|
| G1 | `scope`/`file_glob` with `dir`, `dir/`, `./dir`, an absolute dir, and an exact file give the same results as `dir/**` or the file itself. A pattern matching no files emits `filter_matched_no_files`. |
| G2 | An unknown kind gives `InvalidParams` listing the kinds. A kind mismatch on an existing name emits `kind_filtered_out` with the found kinds. |
| G3 | The candidate list comes from the index (assert no walk once the index is ready). The literal prefilter skips non-matching files (counter). `languages: ["typescript","tsx"]` returns captures from both. A query that fails to compile for one language returns the other's captures plus `query_compile_failed`. |
| G4 | A closing-line `col` past the end gives `col_ignored` plus the symbol. A top-level row gives `matches: []` plus `no_enclosing_symbol`. |
| G5 | An unknown repo gives `InvalidParams` listing the repos, over both stdio and HTTP. |
| C1 | Copy the database, touch every file with the same content, update: 0 parsed and N touched. |
| W1 | Main, two linked worktrees, one prunable: discovery returns the two live ones. Started from inside a worktree, it finds the main checkout and its sibling. |
| W2 | Basename collisions produce `<basename>@<id>` names. An absolute path resolves. |
| W3/W4 | The first call to a worktree creates `<wt>/.tsindex/{config.toml,index.db}`, seeds it, and returns worktree-local results. A file that exists only in the worktree is found. `replace_symbol` writes inside the worktree. |
| W5 | An edit in the worktree shows up on a query more than 2 s later, with no watcher. |
| W6 | After the worktree is deleted, the next call gives the "no longer exists" error. |

## Acceptance on `iq-flow`

1. `find_references(names=["useLlcCp2Enabled"], scope="apps/iq-flow/src/llc")`
   returns the same 4 files as `scope="apps/iq-flow/src/llc/**"`.
2. `get_symbol(name="Container", kind="variable", file_glob="apps/iq-flow/src/llc/q1")`
   returns 0 matches plus `kind_filtered_out: {"function": 1}`.
3. `enclosing_symbol(row=170, col=4)` on `experimentUtilities.ts` returns
   `isLlcCp2ActiveForSession` plus `col_ignored`.
4. `get_symbol(repo="does-not-exist")` returns `InvalidParams` listing
   `iq-flow`.
5. The unscoped `useAutoRouteSubmit` call-expression query finishes within
   2,000 ms.
6. From a server started in `~/dev/iq-flow`, after
   `git worktree add ../iq-flow-wt-test`:
   `get_symbol(name="useAutoRouteSubmit", repo="iq-flow-wt-test")` returns a
   match in under 5 s, and `../iq-flow-wt-test/.tsindex/index.db` exists.

## Rollout

1. G5 and G1 (P0): the bugs that cause wrong conclusions. Small diff.
2. G4 and G2.
3. C1, a prerequisite for W4 and useful on its own.
4. W1–W6.
5. G3 (performance), measured against `bench/`.

## Open questions

1. Naming: plain basename (matches today's agent hooks) or always
   `<main>@<worktree>` (unambiguous, but agents must learn it)? This spec uses
   the basename and falls back to `@<id>` on collision.
2. Disk: each used worktree adds about 250 MB on `iq-flow`. Is that
   acceptable, or should seeding be opt-in (`worktree_seed = true`)?
3. Should `repo` be required whenever the server knows more than one
   checkout, so a call meant for a worktree can't silently fall back to the
   main checkout?
