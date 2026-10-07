# Experimental Codex session-cleanup adapter

This hook-only local plugin registers ownership on an **existing** MCP server
named `tsindex` and releases it at session end. It does not bundle, launch,
reconfigure, or replace an MCP server. See the
[native contract](../../docs/session-lifecycle.md) and
[official hook documentation](https://learn.chatgpt.com/docs/hooks).

## Opt-in setup

1. Build/install a binary containing this implementation. Review the manifest
   and `hooks/hooks.json` in this directory. Use a local Codex workflow that
   supports command hooks; cloud orchestration does not support this adapter's
   local `SessionEnd` command hook.
2. Explicitly migrate the existing `tsindex` MCP command to add
   `--session-lifecycle`, retaining its root, DB, language, and other settings:
   `tsindex --root /path/to/repo serve --mcp --session-lifecycle`.
   Do not replace someone else's existing MCP command or stack this with a
   stdio-wrapper ownership adapter.
3. Install this directory using Codex's local-plugin flow, or merge its three
   event groups into an active `hooks.json` next to your Codex config. Preserve
   all existing hooks. Choose one source: hooks from multiple sources accumulate
   rather than override one another.
4. Ensure the hook's `tsindex` on `PATH` is the same implementation as the MCP
   binary. If using an absolute MCP binary path, explicitly edit the command
   hook to use that path as well. If setting `TSINDEX_SESSION_DIR`, pass the same
   absolute private directory to both the MCP server and command hook. The
   default is `$HOME/.tsindex/sessions-v1`; plugin data directories are not used.
5. Start/restart a Codex session. Review and trust the exact hooks through
   Codex's normal UI (`/hooks` in the CLI). Changed hooks need new review. Do not
   write trust hashes or bypass trust. Respect managed policies that disable
   local hooks or permit only managed hooks.

`PreToolUse` registers `${session_id}` before work; `Stop` registers it after a
tool-free turn as well. MCP hooks do not recursively trigger hooks and return
the non-blocking JSON object `{}`. Missing/unready servers and hook errors do
not block work. No registration is invented from an environment variable,
model-supplied repository, or process scan. There is no `SessionStart`-only
dependency, cleanup at `Stop`, or cleanup at `SubagentStop`.

`SessionEnd` uses `tsindex session end --from-stdin` with a three-second timeout.
It runs for the main thread, not subagents. Subagent hook events carry the
parent session ID so their connections register under the same owner.
Switching conversations does not immediately end a session. The documented
triggers are archiving/deleting an open conversation, normal host shutdown,
and 30 minutes idle with no connected client.

To undo: disable/remove only this plugin's hooks through the normal flow and
remove `--session-lifecycle` from the migrated MCP command, preserving its
other settings. Restart/reconnect. Ordinary EOF/parent-watchdog behavior is
restored; existing indexes and untracked servers are untouched. Removing hooks
alone stops registration/release and leaves ordinary transport teardown intact.

## Required real-host validation before advertising support

The repository's native process tests and synthetic events are not a substitute
for this checklist. This implementation has not yet passed it.

- Observe whether the host actually retains an ordinary tsindex server after a
  documented session-end event; distinguish idle chats from ended sessions.
- Confirm new/changed hooks are skipped until reviewed/trusted, and that
  `PreToolUse` registration succeeds once the MCP connection is ready.
- Finish a tool-free turn and verify `Stop` registers its existing connection,
  returning valid JSON without requesting another turn.
- Exercise subagents: verify their connections register under the parent ID;
  stopping an agent or one turn must not release the parent's connections.
- Open two conversations for the same repository. End one and confirm its
  connections close while the other remains usable. Test shared ownership if
  the host shares a connection, and multiple connections for one owner.
- Test archive/delete of an open chat, normal host shutdown, and the documented
  idle/no-client timeout. Confirm actual `SessionEnd` command execution and
  acknowledged releases within its deadline. Switching tabs must not release.
- Test readiness failure/reconnect, hook failure reporting, and removal of the
  adapter. Record host version, tested platforms, and outcomes without copying
  nonce-bearing records or raw private transcripts.

Keep this adapter experimental and opt-in until these checks pass. Local-only
Unix transport is implemented for Linux/macOS; Windows is explicitly deferred.
