# Opt-in session lifecycle (experimental)

Ordinary `tsindex serve --mcp` is unchanged: EOF and the original-parent
watchdog end the server. Session cleanup is an additional, explicit ownership
path for local hosts that keep a connection after a conversation ends. It does
not prove that a host leaks servers, and switching conversations is not evidence
of a leak.

Linux and macOS are the initial transport scope. Other platforms reject the
option; no Windows lifecycle support is claimed. No supervisor, stdio proxy,
process scan, PID signal, or external index watcher is installed.

## Native interface

Enable ownership only on connections whose host will register and release them:

```sh
tsindex --root /path/to/repo serve --mcp --session-lifecycle
```

The additional MCP tool `register_session` accepts only `session_id` (a
nonempty string of at most 256 UTF-8 bytes, without control characters).
Lifecycle hooks, not the model, should call it on the connection they own.
Registration is idempotent and returns `{}` in `content[0].text`, without
credentials, paths, or hook decisions. It does not change the index. The tool
is absent when the flag is off and is never available over HTTP.

Release a host-neutral session explicitly with:

```sh
tsindex session end --session-id '<host-session-id>'
```

Command-hook adapters can instead pipe their JSON event to:

```sh
tsindex session end --from-stdin
```

The event must contain string fields `hook_event_name: "SessionEnd"` and
`session_id`; other event fields are ignored. Wrong events, malformed JSON,
invalid IDs, and input over 64 KiB fail without releasing anything. A turn stop
or subagent stop is not a session end.

One session may own several connections, and one connection may have several
owners. Only release of the last registered owner initiates shutdown. An
unregistered connection retains the normal transport lifecycle. Repeated release
and unknown sessions are harmless. Cleanup is silent on stdout; genuine
failures are reported on stderr with a nonzero exit code, without nonces.

Cleanup has one 2.4-second budget, including stdin, scanning, and endpoint I/O.
Each endpoint has at most 150 ms of that budget. A response must confirm release;
timeout or forged input is not success. Already-closed endpoints are harmless
and are not represented as confirmed releases. Very large sets of connections
or an unresponsive endpoint can produce a partial cleanup failure. The host's
three-second hook deadline remains the outer limit.

Last-owner release wakes the scoped stdio loop without requiring EOF or another
request. Index maintenance cooperatively cancels its build/walk and watcher.
A 750 ms process-exit fallback terminates all in-process work if a parse, DB
lock, or protocol write is blocked. Ordinary EOF and parent death still apply.

## Private state and reconnects

Both commands resolve state to `$TSINDEX_SESSION_DIR`, or
`$HOME/.tsindex/sessions-v1` when unset or empty. An override must be an absolute
path without `..` or symlink components. This directory must be owned by the
current user and private (normally mode `0700`). Newly created directories are
private; an existing unsafe directory is rejected, not silently chmodded.
Use the same environment for the MCP server and cleanup hook. `TSINDEX_HOME`
controls telemetry, not this ownership state.

Each connection creates a random 128-bit instance directory containing a
mode-`0600` Unix socket `control.sock` and one mode-`0600`
`s-<SHA256(session_id)>.json` record per owner. The record contains a version,
instance ID, session ID, and random 256-bit per-instance nonce. Endpoint paths
are derived from the instance directory, never supplied by an MCP caller or
read from an arbitrary record field. Socket-path length limits may require a
shorter private `TSINDEX_SESSION_DIR` on macOS.

The server authenticates the nonce and instance, verifies the owner, and
serializes release with registration. There are at most 128 simultaneous
owners per connection and at most 4096 entries scanned per cleanup. Control
messages and records are bounded to 2 KiB; scoped MCP input is capped at 1 MiB.
Symlink/non-regular records, hardlinks, insecure permissions, mismatched
instances, and unexpected endpoint types are rejected. Do not print or share
record contents: they contain local control credentials.

The server removes only matching records and its original socket, and checks
the directory's identity before removal. It never recursively removes state or
touches indexes. Crashes, forced-deadline exit, and parent death can leave stale state; cleanup
does not delete unauthenticated stale records. A forged/replaced record may also
be left behind. These are inert, not authority to signal a process.

Reconnects create a new instance and nonce, so a captured old-instance request
cannot select the replacement connection. Hosts must use IDs unique to a
session lifetime (namespace them by host generation if necessary) and finish
registration before dispatching that session's end event. Reusing an ID for a
new lifetime while an old end event can still arrive is unsupported: an
ID-only end event cannot distinguish those lifetimes. There is no repository,
age, PID, or parent-based ownership inference.

## Codex adapter and rollout gate

The reviewable hook-only plugin lives in
[`plugins/codex-session-cleanup`](../plugins/codex-session-cleanup/README.md).
Installation and migration are manual and opt-in; no existing MCP command,
hooks, or trust hashes are changed by the native binary or skill startup.

Rust/unit and real-process protocol tests cover the native interface. The
adapter remains experimental until the checklist in its README has been run
against a real Codex host, including actual session-end triggers and trust
review. Synthetic events alone do not establish end-to-end host support or a
reproducible retained-server problem. Keep any existing local wrapper until that
gate passes; do not wrap a lifecycle-enabled native connection in a second
ownership adapter.
