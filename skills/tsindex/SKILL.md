---
name: tsindex
description: Start, refresh, and use the local tsindex code-navigation index from Codex. Use when the user invokes $tsindex, asks to enable tsindex, start the tsindex MCP/code navigation server, explicitly watch the current repository, index all repositories under ~/dev, troubleshoot stale or missing tsindex results, or wants Codex and spawned agents to use mcp__tsindex__ tools for structural code reading instead of broad file reads.
---

# tsindex

Use this skill to prepare tsindex for the current working area and put
tsindex-first code-reading rules into the session.

This skill is generic for Codex. It does not use product-specific paths and
does not assume any fixed repository list.

## Startup

Run the bundled startup script from the current shell context:

```bash
skill_root="${CODEX_HOME:-$HOME/.codex}/skills/tsindex"
bash "$skill_root/scripts/start-tsindex.sh"
```

The script resolves the target this way:

- If the current directory matches `${TSINDEX_DEV_ROOT:-$HOME/dev}` (defaults to
  `~/dev`), configure a multi-repo catalog at that path by discovering every
  immediate child directory with a `.git/` folder. Set `TSINDEX_DEV_ROOT` to
  point at a different catalog root.
- Otherwise, use the current Git repository root from
  `git rev-parse --show-toplevel`; if the directory is not in a Git repo, use
  the current directory.

The script initializes or refreshes `.tsindex/config.toml`, runs an on-demand
index refresh as an eager warm-up, and exits:

```bash
tsindex --root <target> update
```

It does not install LaunchAgents, use `nohup`, or leave a watcher running after
the startup command exits. `serve --mcp` runs its own in-process watcher for the
lifetime of the server (startup refresh, then filesystem notifications), so this
warm-up is helpful but not required for freshness once the MCP server is up. To
keep a foreground watcher attached to the current shell for CLI-only sessions
that never start `serve --mcp`, run:

```bash
TSINDEX_START_MODE=watch bash "$skill_root/scripts/start-tsindex.sh"
```

The explicit watcher is not daemonized and exits when the shell process stops.
Treat it as an optional freshness optimization, not a requirement for the MCP
server to recover from stale indexes.

MCP stdio servers are owned by Codex config and cannot be attached to a running
session by a shell script. Set the Codex MCP command to:

```bash
tsindex --root <target> serve --mcp
```

## RTK

RTK (`https://github.com/rtk-ai/rtk`) is complementary to tsindex. Keep RTK's
shell-output reduction hooks enabled if the session has them; tsindex should
still be configured as the MCP code-navigation server with the command above.
RTK trims command output, while tsindex avoids broad file reads through
structured MCP tools.

If `mcp__tsindex__*` tools are missing or clearly pointed at the wrong root,
update `${CODEX_HOME:-$HOME/.codex}/config.toml` after user approval and tell
the user they may need to restart Codex for the MCP server change to apply.

Recommended Codex MCP server root:

- Use `~/dev` when the user commonly works across several repos under `~/dev`.
- Use the current repo root for isolated one-repo sessions.

## Verify

After startup, verify the index through the MCP tools when they are available.

For a single-repo target:

Always scope to the active repo by passing `repo="<repo-name>"` (the basename
of the workspace root). The index typically spans many repos, so an unscoped
call fans out across all of them and returns a huge, mostly-irrelevant blob that
wastes tokens. Only omit `repo` when deliberately searching across repos.

```text
mcp__tsindex__list_file_outline(file="README.md", repo="<repo-name>")
```

If `README.md` is absent, use another known source file. If the MCP check fails
but the CLI script succeeded, continue with `rg` and targeted reads for the
immediate task and report that the MCP server likely needs a Codex restart or
config-root correction.

## Session Rules

Print or summarize these rules in the conversation after startup:

```text
## tsindex session rules active

Code reading in this session uses tsindex MCP tools when available. These rules
apply to Codex and to every spawned agent that reads code.

Tool priority:
- Find a function, class, type, or symbol by name: mcp__tsindex__get_symbol
- Orient to a file's structure: mcp__tsindex__list_file_outline
- Find callers/users of a symbol: mcp__tsindex__find_references
- Run structural tree-sitter queries: mcp__tsindex__query
- Find enclosing symbol for a line/column: mcp__tsindex__enclosing_symbol
- Search literals, config keys, env vars, log strings, or prose: rg
- Count occurrences or list which files use a symbol ("how many", "which
  files"): mcp__tsindex__find_references with counts_only=true — exact totals
  and per-file counts in one small response, no paging. Use rg --count-matches
  (not rg -c, which counts lines) only for raw substring counts across file
  types tsindex does not index; always say which you counted and over what scope.
- Orient and fetch selected bodies in one call: list_file_outline with
  include_bodies_for=[...]; resolve a whole stack trace: enclosing_symbol with
  rows=[...]
- Rewrite a whole named function/class/method without reading the file first:
  mcp__tsindex__replace_symbol (refuses edits that break parsing)

Workflow: outline first -> symbol lookup -> targeted read/edit.
Avoid full-file reads when an outline or symbol lookup gives enough context.
```

When spawning agents that read code, include
`references/agent-prompt-block.md` in the prompt. In a multi-repo catalog, also
tell the agent the active repo name and require it to pass `repo="<repo-name>"`
to tsindex tools.

## Stale Or Missing Results

Do not treat an empty language result as proof that tsindex cannot parse the
language. tsindex supports C#, Kotlin, Swift, Ruby, SCSS, C, and C++ in addition
to TS/JS/Python/Go/Java/Bash. Empty results in a repo that clearly contains
that language usually mean the index is stale or was built with a language
filter.

Fix stale coverage by running the startup script again, restarting
`serve --mcp`, or run:

```bash
tsindex --root <target> build
```

The startup script leaves no watcher running unless `TSINDEX_START_MODE=watch`
is used explicitly; `serve --mcp` keeps its own in-process watcher, so a
separate `watch` only matters for CLI-only sessions.

## Guardrails

- Keep this skill scoped to tsindex startup and code-reading behavior.
- Do not install persistent daemons or background watchers.
- Do not add product-specific paths, repo names, patterns, or MCP tools.
- Use tsindex for code structure, not string search.
- Fall back gracefully to `rg` and targeted reads when MCP tools are
  unavailable.
