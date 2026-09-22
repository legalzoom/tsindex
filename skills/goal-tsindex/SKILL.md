---
name: goal-tsindex
description: Start, refresh, and use the local tsindex code-navigation index from Codex through the /goal-tsindex alias. Use when the user invokes /goal-tsindex or $goal-tsindex, asks to enable tsindex, start the tsindex MCP/code navigation server, explicitly watch the current repository, index all repositories under ~/dev, troubleshoot stale or missing tsindex results, or wants Codex and spawned agents to use mcp__tsindex__ tools for structural code reading instead of broad file reads.
---

# /goal-tsindex

This skill is the `/goal-tsindex` invocation path for the same tsindex workflow
as the `tsindex` skill. It prepares tsindex for the current working area and
puts tsindex-first code-reading rules into the session.

The workflow is identical to the `tsindex` skill — same startup, RTK guidance,
verification, session rules, stale-result handling, and guardrails — with two
differences specific to this alias:

1. **Startup script path.** The bundled script lives under this skill's
   directory, not `tsindex`'s:

   ```bash
   skill_root="${CODEX_HOME:-$HOME/.codex}/skills/goal-tsindex"
   bash "$skill_root/scripts/start-tsindex.sh"
   ```

2. **Session-rule heading.** When you print the session rules, use the
   `/goal-tsindex` heading:

   ```text
   ## /goal-tsindex session rules active

   Code reading in this session uses tsindex MCP tools when available. These
   rules apply to Codex and to every spawned agent that reads code.

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

Everything else — how the startup script resolves its target (single repo vs
`~/dev` multi-repo catalog via `TSINDEX_DEV_ROOT`), the on-demand `update`
warm-up, `TSINDEX_START_MODE=watch`, the Codex MCP server command, RTK guidance,
verify step (always scope calls with `repo="<repo-name>"`), stale/missing-result
handling, and the guardrails — follows the `tsindex` skill exactly. When
spawning agents that read code, include this skill's
`references/agent-prompt-block.md` in the prompt (it is the same code-reading
rules under the `/goal-tsindex` heading); in a multi-repo catalog, also tell the
agent the active repo name and require it to pass `repo="<repo-name>"` to tsindex
tools.
