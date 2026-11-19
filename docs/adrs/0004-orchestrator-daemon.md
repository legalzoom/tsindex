# Manage MCP session processes from a login-started orchestrator daemon

Each agent harness previously launched a standalone `tsindex serve --mcp`
process directly. That forced every MCP server to carry its own parent-PID
polling thread, wrapper-PID flags, and ad hoc orphan recovery, and left no
central place to inspect active sessions, track which harness owns each
checkout, or terminate stuck workers cleanly.

Run a single user-level orchestrator daemon (`tsindex daemon run`) started at
login via `launchd` on macOS and `systemd --user` on Linux. The daemon starts
idle: it opens no databases and registers no filesystem watchers on boot. Its
sole responsibility at rest is listening on a private, user-owned Unix domain
socket and managing the lifecycle of sessions requested by agent harnesses.

When a harness invokes `tsindex --root <path> serve --mcp`, the CLI acts as a
thin stdio proxy that connects to the orchestrator socket and requests a
session. If the daemon is not running, the proxy fails immediately with an
actionable error telling the harness/user to start or install the daemon; it
does not silently fall back to an unmanaged standalone server.

For each accepted session, the orchestrator spawns an isolated per-session MCP
worker child process (`tsindex daemon worker`) in its own process group. Using
one worker per session preserves isolation across concurrent sessions working
on the same repository in different Git worktrees (or with different language
and refresh settings), while keeping fault and memory boundaries per session.
Existing cross-process writer leases, watcher leases, and immutable worktree
overlays continue to coordinate shared disk state cleanly across workers.

The orchestrator sits in the newline-delimited MCP stream between the proxy's
Unix socket and the worker's stdio pipes:

- **Deterministic teardown:** Harness exit or proxy termination immediately
  closes the Unix socket, causing the orchestrator to close the worker's stdin,
  allow a bounded graceful drain on normal EOF, and escalate to `SIGTERM` and
  `SIGKILL` against the worker's process group before reaping the child. On
  Linux, workers also request `PR_SET_PDEATHSIG(SIGTERM)` so they cannot
  outlive a hard-killed daemon.
- **Centralized owner monitoring:** One background sweeper in the orchestrator
  watches captured session-owner PIDs (including `--session-pid` overrides)
  using the existing start-time and zombie checks, replacing per-server
  watchdog threads.
- **Control plane and future extensibility:** Because the orchestrator owns
  every session handle and passively inspects control metadata (`initialize`
  client identity, request/tool counters, recent worker stderr), operators and
  future integrations gain `daemon status`, `daemon sessions`, `daemon kill`,
  and clean room for unified dashboards or additional transports without
  changing the worker protocol.

This trades one local Unix-socket hop per JSON-RPC message and a required
login daemon for deterministic process supervision, zero orphaned MCP servers,
and a single lifecycle control plane across macOS and Linux.
