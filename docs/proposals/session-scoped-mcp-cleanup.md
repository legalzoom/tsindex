# Proposal: session-scoped MCP cleanup

Status: proposed. This document changes no runtime behavior. API names below
are illustrative, not commands or tools available in the current release.

## Problem and existing behavior

The [stdio server](../../src/mcp.rs) exits when its input reaches EOF and has a
parent watchdog that exits when its original parent disappears. Those remain
the appropriate default lifecycle controls. A long-lived host daemon can,
however, remain alive and retain a connection after a conversation no longer
needs its server. Parent liveness alone cannot identify that conversation.

Multiple servers, including servers for the same repository, can be legitimate.
Their age or identical workspace roots are not proof of a leak. Before enabling
additional cleanup, verify whether the host actually retains servers after its
documented session-end event, rather than merely while conversations are idle.

This proposal adds an opt-in, explicit ownership/release path. It must never
infer ownership from a repository, process age, parent PID, or process name.

## Proposed native lifecycle

Keep ordinary `serve --mcp` unchanged. An explicit lifecycle option would enable
a small local control endpoint and a hook-facing registration tool in the Rust
server, replacing the need for a separate stdio proxy.

1. A hook invokes a proposed `register_session` MCP tool with the host's
   `session_id` on that session's existing tsindex connection. Registration is
   idempotent and does not alter the index. The server records that session as
   an owner of its own connection.
2. The server maintains a private record for each owner. Records identify a
   random connection instance and its local endpoint, authenticated by a
   per-instance nonce. They do not authorize a PID-based signal. One session
   may own several connections; a shared connection may have several owners.
3. A proposed `tsindex session end --from-stdin` command reads the hook event,
   validates it, and releases that session through its registered endpoints.
   The server verifies the nonce and owner, then exits only after the last
   registered owner releases the connection.

The shutdown path must wake a blocked stdio reader, stop its watcher, cancel
any initial refresh, and terminate within a bounded deadline. A flag that the
stdio loop checks only after the next request is insufficient. Protocol stdout
must remain JSON-RPC only; diagnostics belong on stderr. Existing EOF and
parent-exit behavior must continue to work without hooks or registration.

Use one consistent, per-user state-directory resolution for the server and
cleanup command. Initially support authenticated Unix-domain sockets on Linux
and macOS. Do not claim Windows support until an equivalent local transport,
permissions model, and tests are implemented.

## Codex adapter and packaging

Codex is an adapter, not an assumption embedded in the indexing runtime. Ship
reviewable hook configuration and setup instructions alongside the existing
[Codex skills](../../skills/tsindex/SKILL.md). This repository currently does
not ship a Codex plugin hook package; packaging is part of the proposed work.

- Register through an MCP-tool `PreToolUse` hook before work and a `Stop` hook
  when a turn finishes. The latter covers conversations that use no tools.
  Expand `session_id` from the hook event, rather than guessing an environment
  variable or using a model-supplied repository argument. Registration must
  return valid, non-blocking hook output and avoid recursive hook invocation.
- Use a command hook for `SessionEnd` to invoke the cleanup command. Codex
  does not support MCP-tool hooks for `SessionEnd`.
- Do not rely exclusively on `SessionStart` registration: its MCP connection
  may not be ready. Until registration succeeds, retain the ordinary transport
  lifecycle; never invent an ownership record from a process scan.
- `SessionEnd` runs for the main thread, not subagents. Subagent hook inputs use
  the parent session ID, allowing their connections to register under that
  owner. Ending a subagent or stopping one turn must not trigger cleanup.
- Document that switching conversations does not immediately end a session.
  The current documented triggers include archiving/deleting an open chat,
  normal host shutdown, and 30 minutes idle with no connected client.
- Installation is opt-in. Preserve existing configuration and require the
  user to review/trust new hooks through the host's normal flow. Do not write
  trust hashes, bypass trust, or silently replace an existing MCP command.

These constraints follow the [official OpenAI hook documentation][hooks]. A
real host-driven registration/release check is required before advertising this
adapter; protocol tests with synthetic hook events are not sufficient alone.

## Safety and compatibility requirements

- Never use `pkill`, workspace-wide kills, or a recorded PID without a live
  authenticated endpoint. PID reuse must not be able to select a target.
- Scope records to random connection instances in a user-private directory.
  Reject unexpected record/endpoint paths and symlinks; do not accept arbitrary
  filesystem paths from an MCP registration request. Keep nonces out of model
  output and diagnostics, and bound input sizes and owner-record growth.
- Release is idempotent. Unknown sessions and already-closed endpoints are
  harmless. Malformed, mismatched, or forged records must not shut down a live
  server. Remove only records that are still owned by the instance being
  released; avoid clobbering concurrent registrations.
- Bound the total cleanup time within Codex's current three-second maximum
  for `SessionEnd` command hooks. Report genuine failures without blocking the
  host, and do not claim successful release when the endpoint did not confirm it.
- Untracked servers are not retroactively terminated. Existing installations
  continue to use their current lifecycle until explicitly migrated.
- Do not introduce a persistent supervisor, external watcher, or cleanup of
  SQLite indexes, user files, unrelated servers, or other clients' sessions.

## Validation and rollout

A local stdio-wrapper proof of concept exercised registration, protocol
passthrough, and release using synthetic session-end events. It included
overlapping sessions, shared ownership, stale records, forged control requests,
EOF, and child termination, plus a smoke test with the actual tsindex binary.
That validates the control-path concept, not a native implementation, an
observed host leak, or end-to-end Codex hook execution.

Before releasing native support, add Rust/unit and real-process integration
tests for:

- Two sessions in the same repository: ending either leaves the other usable.
- Multiple connections for one session, and multiple owners for one connection.
- Duplicate registration/release, unknown sessions, and stale/forged records.
- Registration/release races, reconnects, instance replacement, and bounded
  shutdown while the stdio reader is blocked or initial indexing is running.
- EOF, client/parent death, and unchanged behavior with lifecycle support off.
- Normal MCP discovery, requests, notifications, malformed input, and stdout
  framing. Hook-facing output must obey the host's hook-output schema.

Then verify the adapter against a real Codex session, including hook readiness,
trust review, tool-free turns, subagents, and the actual `SessionEnd` triggers.
Run the existing checks in [CONTRIBUTING.md](../../CONTRIBUTING.md). Initially
roll out as opt-in on tested platforms. Keep any local wrapper until the native
binary and adapter have passed those checks; removing the hook configuration
must restore ordinary EOF/parent-watchdog behavior without changing indexes.

## Decisions requested

- Is a retained server after a real host session-end event reproducible, or
  does the host's existing transport teardown make this integration unnecessary?
- Should the lifecycle API remain host-neutral, with a separately packaged
  Codex adapter and explicit enablement? This proposal recommends that split.
- What should the state-directory and endpoint naming contract be, and how
  should reconnects and session-ID reuse be handled across host generations?
- Should Linux/macOS be the initial supported scope, with Windows deferred?

[hooks]: https://learn.chatgpt.com/docs/hooks
