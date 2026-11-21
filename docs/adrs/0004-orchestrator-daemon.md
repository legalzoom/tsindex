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
owner initializes. Settings changes select a new generation for future
connections. File leases still coordinate durable state with direct MCP, CLI
writers, and older installations.

Startup first launches a cancellable session worker. That child computes its
configuration key and requests a matching owner. The daemon passes a private
channel to the worker through a bootstrap socket. The worker's first lookup
connection waits for initialization before the worker announces readiness.
A blocked configuration read stays inside the inspectable session child. Both
children check their loaded settings and executable identity. Linux uses the
mapped executable inode, and macOS uses its mapped UUID when available.

Each connection has a separate MCP worker so closing a session can interrupt
its running edits and discard its queued requests. A session reuses at most
four private lookup connections. The owner acknowledges each new connection
through its independent intake loop and bounds total connections to four times
the effective session maximum. MCP request IDs stay within each worker.
Accepted lookups that time out release their connection while unfinished work
continues counting against that session until it returns. Other sessions and
the index owner continue. A failed intake acknowledgement retires the owner
for future attachments while existing peers retain their generation.

Actual owner loss ends its attached sessions. A stopped owner is terminated
and replaced on the next attachment, and its old sessions end when it exits.
The pool retains at most four idle owners for a default 60 seconds. Exiting
owners keep their capacity until reaped, within a pool that reserves one extra
replacement slot. Shutdown terminates index owners and bounds its final wait.
Linked-checkout reconciliation reloads that checkout's configuration.

The supervisor reserves a session slot during startup and holds it through
teardown. Its effective session cap derives from the operator's inherited soft
descriptor limit and the requested maximum. Admission reserves bootstrap
pairs, idle owners, one replacement owner, and lifecycle-command capacity.
Measured Linux foreground starts admitted 8 sessions with a soft limit of 256
and 39 with 1024. Existing descriptors affect capacity. Status reports the
computed maximum so operators can size launch limits within the OS hard limit.

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
