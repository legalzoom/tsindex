# Share warm indexes across trusted sessions and preserve harness sandboxes

Each MCP session needs a source index, coordinated watcher ownership, and a
bounded process lifetime. Some harnesses restrict filesystem access with a
kernel sandbox. A login daemon runs in a separate launch context and holds its
own filesystem authority, so delegating work to it changes the session's
permissions.

The default `tsindex --root <path> serve --mcp` executes directly inside its
launching environment. Harness configuration must place this executable inside
the intended sandbox. Hosts that launch MCP externally can wrap the command in
a sandbox launcher. The Linux bubblewrap example in the README grants one
workspace, the executable, and runtime libraries. Index databases are part of
the source access policy because they can contain cached snippets.

Trusted clients select `serve --mcp --via-daemon` explicitly. This grants the
session the user daemon's source-read and source-write authority. The daemon
socket belongs outside the mount view of untrusted processes. Unix peer
credentials authenticate accepted clients as the daemon's user. Server
connections also require an owned socket leaf and private user directory,
rejecting symlinks at the leaf and its immediate parent. On macOS, server
authentication accepts the trusted root creator cached for launchd's listener
after handoff to the user daemon. Kernel restrictions stay with the process
launched inside them.

For managed sessions, use a foreground orchestrator with manager-owned socket
activation. `systemd --user` installs a socket unit and associated service on
Linux. `launchd` registers the `Listeners` Unix socket on macOS. The manager
passes the listener to `daemon run --socket-activated` and retains it through
supervisor restart. Installation, service start, and launchd bootstrap first
validate the reserved endpoint's ownership and file type. The Linux socket
unit repeats validation in a pre-bind hook on activation. The orchestrator
starts without databases or watchers. A manual foreground command can also
bind a validated private endpoint.

The shared unit is a compatible checkout's indexing runtime. It owns watcher
registration, primary freshness, and the cache of linked-checkout refreshes.
A dedicated process per runtime gives those resources a finite lifetime.
Process acquisition is serialized and concurrent clients can attach while the
owner initializes. Settings changes create a new generation. File leases still
coordinate durable state with direct MCP, CLI writers, and older installations.

Each connection has a separate MCP worker so closing a session can interrupt
its running edits and discard its queued requests. Workers inherit a private
runtime channel. A request carries its own reply descriptor, and MCP request
IDs never enter the shared channel. A stopped or crashed index owner affects
only its attached sessions. The pool retains at most four idle owners for a
default 60 seconds to cover short reconnects. The daemon releases all index
owners on shutdown.

The supervisor reserves a session slot during startup and holds it through
teardown. Workers acknowledge successful initialization before the client
receives a session acknowledgement. Startup failures reach the client with a
failing exit status and bounded diagnostics.

A direct stop event starts teardown independently of relay-pipe progress. Normal
stdin EOF drains accepted requests while the proxy can receive results. A forced
stop or lost client gets bounded teardown, escalation, and reaping. Linux and
manual foreground daemons assign a process group per worker and clear remaining
members. Linux adds parent-death signaling for the direct child and
service-level control-group escalation with `KillMode=mixed`. Socket-activated
macOS workers remain in launchd's job group for cleanup on daemon failure.
Per-session teardown targets their direct worker PID. Current MCP worker
operations run in-process. Future helpers on every platform require explicit
containment, including helpers that leave their original process group. Linux's
cgroup backstop covers whole-service stops, and manual daemons rely on their own
process supervision. The proxy registers a dedicated outcome connection before
forwarding MCP. The supervisor sends the reaped worker's status through that
existing connection even while shutdown stops accepting new clients. Session
listings are paginated, and stderr detail is retained by bytes.

The daemon centralizes inspection and termination for trusted managed sessions.
The direct path keeps sandboxed sessions inside the harness's existing kernel
boundary. Both paths retain process-identity owner monitoring and the same MCP
retrieval and editing semantics.


The lifetime observed by this design is the transport connection and its
owner process. A host can keep both alive while switching conversations. A
logical conversation lease requires an explicit release signal from that host.
