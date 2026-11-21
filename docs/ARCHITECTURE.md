# Architecture

The CLI and MCP server share the indexing runtime rather than maintaining
separate indexes or source-discovery rules.

- `src/app.rs` implements CLI operations. `src/mcp.rs` exposes code navigation
  and editing tools through the MCP server.
- `src/index.rs` resolves configured workspaces, walks eligible sources,
  parses files, and updates SQLite rows. Nested workspaces own their files;
  the containing workspace prunes those roots from its own walk.
- `src/lang.rs`, `src/detect.rs`, and the tree-sitter queries define language
  support and the symbols and references extracted from each source file.
- `src/config.rs` loads project and catalog configuration. `src/model.rs`
  defines the data returned by queries and tools.
- `src/worktrees.rs` discovers checkouts from Git metadata and coordinates lazy
  ingestion without holding the registry lock while a checkout is refreshing.
- `src/overlay.rs` owns immutable base publication, private staging, and merged
  read views. `src/maintenance.rs` coordinates database writers, watcher
  ownership, and successful full-refresh markers across server processes.
- `src/daemon/` implements the login-started user orchestrator (`orchestrator.rs`),
  the warm index pool (`runtime.rs`), the stdio proxy (`proxy.rs`), and service
  management (`service.rs`) for macOS `launchd` and Linux `systemd --user`.

## Checkout storage

The primary catalog uses the existing materialized SQLite schema. A fresh
linked checkout with one workspace rooted at that checkout may reuse a
consistent snapshot of the main checkout's ready index. The snapshot is copied
to a private file, closed in DELETE journal mode, then published as
`.tsindex/bases/extraction-v<schema>.db` with a create-only hard link. This
links the completed immutable copy, never the mutable primary database.
Concurrent creators can safely reuse the winner. The snapshot is fixed even
when later primary builds change the materialized index. Publication records a
SHA-derived snapshot identity in the base, and every overlay persists that
identity with its base path. Every attach compares the identities before
exposing extraction: a pathname and schema version alone cannot identify a
generation if someone deletes and recreates the base at the same path.

If a ready main index is unavailable on first use, or its shared-base store is
read-only, the checkout instead builds a standalone materialized index.
Snapshots with schema-stale file markers are also excluded: matching content
does not make old extraction ranges compatible. This preserves availability
without requiring the main checkout to finish indexing first. A later ready
main index does not
automatically convert that existing cache to an overlay.

The linked checkout keeps `.tsindex/index.db` containing its base pin, per-path
observations and hidden markers, and extraction only for changed or added
files. Reusing a base file requires matching relative path, language, and
content SHA. A private file shadows the entire base extraction even when it
contains no symbols. Deletions and ignore exclusions hide base paths, and a
restored matching file can return to the base without private extraction.

Every read opens one SQLite transaction before resolving the base metadata.
Connection-local views combine visible private and base rows, using positive
private IDs and negative base IDs. This applies file visibility before tool
filters, counts, ordering, and pagination. Source bodies and edits resolve
against the selected live checkout, with the existing SHA verification and
stale-body behavior. A logical read is a consistent index snapshot, not an
atomic snapshot of the filesystem.

Private-file authority probes include repository ID and relative path, matching
the composite unique index instead of scanning the entire delta for each base
file.

The base branches of the symbol and reference views join base files directly
and apply the same visibility predicates, avoiding a second effective-files
materialization inside those views. Each private overlay also persists an
`overlay_file_directory` with resolved file metadata, indexed by signed logical
file ID and relative path. The `files` read view selects that directory when
its version and generation match the extraction state. Existing tool SQL can
then seek matching files rather than materialize the full file catalog for
each named lookup. Symbols, references, and visibility predicates are unchanged.

The directory is derived from private files, base files, and authority records.
File upserts, metadata observations, restores, and deletions maintain it in the
same private-stage transactions. Preparation seeds an unready directory;
a completed refresh stamps its generation before the existing online backup
publishes the directory with the facts. Readers
continue to pin one consistent generation. Existing overlays, or overlays
published by an older writer that does not maintain the directory, fall back
to the original merged file view when the directory is absent or stale. The
next new writer rebuilds it before publication without reparsing unchanged
files. This adds per-checkout file metadata and staging bytes while retaining
the existing extraction format, reconciliation, and source verification. See
the [directory ADR](adrs/0003-effective-file-directory.md) and
[measurement](benchmarks/worktree-file-directory.md).

All writers for one database take the same cross-process lease before reading
input state or parsing. Refresh copies only the thin overlay into a private
stage, reuses the existing walker and extraction pipeline, and validates source
timestamps, sizes, and content hashes before publication. A source that changes
during preparation aborts publication and leaves the old generation available
for a retry. Serializing this complete cycle per checkout prevents an earlier
parse from publishing after a newer accepted update. Different checkouts use
independent leases and can parse and publish concurrently. Cooperative symbol
edits take the same checkout lease; external editors remain outside that
advisory protocol and are covered by source verification.

Reconciliation verifies content hashes across the complete logical checkout,
including no-op scans; parsing only the delta does not make the refresh cost
proportional only to the delta. Source changes during preparation discard the
stage and return a retryable tool error. An encountered file-read or extraction
failure also prevents overlay publication and freshness success. Managed reads
then return an explicit freshness error until a complete refresh succeeds,
rather than publish partial replacement rows as a successful generation.
Continuously changing external sources can keep requiring retries. This is an
availability and latency tradeoff; the primary's explicit `--no-refresh`
opt-out remains available, and a logical generation still is not an atomic
filesystem snapshot.

SQLite's online backup API publishes the completed stage into the stable live
WAL database in one destination transaction. Existing readers retain their old
snapshot, and an interrupted backup rolls back; no live database pathname is
renamed or replaced. No-op reconciliation skips redundant publication. Bases
are retained indefinitely, protecting persistent checkout pins and readers in
other processes until an explicit reclamation protocol exists.

Overlay publication reconciles all configured languages, even when a command
temporarily selects fewer languages: validation requires a complete logical
checkout generation. Query and result filters still apply, and invalid
repository or language selectors retain their normal errors.

Published overlays use an overlay-specific schema version and physical table
names. Older indexed readers reject the format; their raw-query path sees no
complete `files` table and falls back to a live walk. A downgrade requires
rebuilding derived overlay caches. Existing materialized worktree indexes are
preserved without conversion. Missing or incompatible bases, or a changed
configuration with several workspaces or an outside source root, rebuild the
selected checkout into a standalone private stage and publish only on success.
An explicitly changed configuration whose repository name or source root no
longer matches the stored workspace also takes that cold-rebuild path.
A moved checkout whose stored root no longer exists is such a mismatch.
When repository pruning is disabled for an explicit external database,
configuration-changing recovery refuses publication rather than discard
repositories absent from its new stage. The caller can choose a separate
database destination. Existing overlays backed by schema-stale extraction
also rebuild independently, even when their file directory is current.
A changed or absent base identity similarly refuses reads with rebuild
guidance until ordinary reconciliation or an explicit build recovers it.
Existing readers that already attached the previous base retain that snapshot.
A base from an older experiment without an identity record is preserved
untouched; a fresh checkout uses standalone storage instead of stamping it in
place.

The [execution trace](specs/worktree-index-overlays.md) describes each path and
the [storage ADR](adrs/0002-worktree-index-overlays.md) records the tradeoff.

## Maintenance and request concurrency

Automatic MCP servers coordinate watcher ownership with a lease keyed by the
database and effective configuration. One compatible owner receives filesystem
notifications. Lease handles release ownership on exit; failed startup or
runtime watchers retry, and another server can take over. HTTP uses on-demand
maintenance without allocating a watcher. `--no-refresh` opts the primary out
of automatic maintenance; explicit linked checkout selection still reconciles.

Watcher leases, refresh coordination, and refresh markers append their suffixes
to the complete database filename, so `cache.db` and `cache.sqlite` keep separate
maintenance state. Marker identity also includes the canonical database path.
The writer lease retains its original name to coordinate with older binaries;
same-stem custom databases can contend on that lease without sharing freshness.

Before a managed primary indexed read, a configuration-matched full-refresh
marker can be reused for `worktree_refresh_ms` (default 2000 ms). Its generation
must still match the database. A local coordinator and a cross-process lease
coalesce scans; the marker is checked again after taking the lease. Only a
successful full build advances it. Partial path updates cannot conceal a gap
in notification coverage. Reconciliation runs even with a healthy watcher,
covering missed or incomplete registrations; failure returns a freshness
error instead of silently treating old indexed data as current.

Primary live syntax queries resolve sources without this reconciliation gate,
including configured names and absolute primary selectors. They retain the
runtime's live-walk fallback before the initial index is ready, so a blocked
writer or an unrelated refresh failure does not prevent querying readable
sources. Index-backed and editing tools still establish freshness, and explicit
linked-checkout queries retain their existing ingestion coordination.
When the primary index is ready, it still supplies query's candidate file list.
New source paths appear after a watcher update, an indexed read's refresh, or
an explicit update. Query-only traffic does not itself reconcile that list or
retry watcher ownership; the exemption preserves live source parsing and
availability rather than guaranteeing complete discovery of newly added files.

Linked checkout reconciliation uses the same durable marker protocol, so
compatible processes can reuse a successful scan within a positive interval.
The local coordinator retains the marker's original age instead of granting a
second full interval on cache insertion. A zero interval reconciles on every
request, and explicit `build` remains a forced operation.

Checkout coordination retains only that checkout's condition variable while
waiting, releasing the short registry lock first. MCP and HTTP use bounded
four-worker pools and queues of 16 pending requests. MCP responses are framed
through a single writer and retain their request IDs, so completion order can
differ from arrival order. EOF drains accepted requests; a transport failure
cancels queued work. On Unix a wakeup descriptor interrupts blocked stdin when
stdout fails. Other platforms retain blocking stdin cancellation behavior.
HTTP snapshots the runtime registry for reads and publishes configuration
changes before acknowledging them. A separate reader/writer admission lock
covers indexed tool calls and explicit rebuilds from before runtime capture
through completion. These calls share admission and can run concurrently;
catalog additions and removals require exclusive admission. An older refresh
therefore finishes before a mutation can publish and cannot subsequently prune
a newly acknowledged repository. The registry lock itself is released before
tool execution. Live syntax queries and metadata routes bypass admission, so
they do not wait on that lock. They still share the bounded worker pool, so
enough blocked requests can queue them as well. Indexed calls and rebuilds
wait for an active catalog mutation, whose exclusive section includes cloning.
This fence coordinates requests within one HTTP server. Existing host, origin,
size, and read-only tool guards remain in the HTTP dispatch path.

## MCP launch context and process ownership

`tsindex --root <path> serve --mcp` runs the MCP server inside its launching
process's environment. This preserves a harness's kernel sandbox when its MCP
command executes there. Shell sandboxes and MCP hosts can use different launch
contexts, so a host that starts MCP externally must wrap the executable in its
sandbox launcher. The README includes a Linux bubblewrap command that grants
one writable workspace and read-only runtime dependencies. Source data,
databases with cached snippets, and caches share the harness's access policy.

Trusted clients opt into shared supervision using `--mcp --via-daemon`. The
daemon authenticates clients by effective user identity. The proxy verifies the
private endpoint and server credentials. On macOS a manager-owned listener can
retain launchd's root creator credentials after handoff. Reaching the daemon
endpoint grants that user's daemon authority, including source modification. The
endpoint belongs outside the mount view of untrusted clients. Explicit transport
selection keeps filesystem authority visible in the host configuration.

The installed service uses a manager-owned Unix listening socket. systemd
provides its listening descriptor to `tsindex daemon run --socket-activated` on
Linux, and launchd provides the `Listeners` socket on macOS. The service manager
retains endpoint ownership through a daemon restart. Installation, service
start, and launchd bootstrap validate the reserved endpoint before binding. A
Linux socket-unit pre-bind hook repeats that validation on activation. Manual
foreground operation binds a private endpoint after obtaining its instance lease
and checking its filesystem entries. Sidecars use a namespace reserved for the
complete socket filename. Directory validation checks ownership and private
permissions before either side connects or creates runtime files.

The orchestrator starts without opening databases or registering watchers.
A trusted session receives an MCP worker and a lease on a shared index owner.
Startup has two stages. First, the cancellable session worker canonicalizes the
checkout and database, loads or projects its settings, and sends its runtime
key through its private startup pipe. The daemon supplies a matching owner's
channel through a bootstrap socket using `SCM_RIGHTS`. The worker then opens
its first lookup connection and waits for index initialization before sending
its readiness acknowledgement. Checkout filesystem I/O stays in a child that
can be inspected and stopped throughout startup.

The runtime key includes the canonical checkout, explicit database override
(which disables repo pruning), normalized language filters, refresh policy,
effective configuration digest, and executable image. Both the session worker
and index owner compute the digest from their own loaded settings. Linux uses
the mapped executable's device and inode from `/proc/self/exe`. macOS uses the
mapped executable UUID when available, falling back to executable-file metadata
for a build without a usable UUID. A generation mismatch fails initialization
and asks the client to reconnect.

Each index owner retains one `Runtimes` registry, including its primary
maintenance coordinator and lazy linked-checkout cache. The daemon passes the
worker a private Unix socket for this registry. Each session establishes up to
four reusable lookup connections by passing a private socket endpoint with
`SCM_RIGHTS`. An independent intake loop acknowledges descriptor receipt before
the lookup can block, then sends readiness after initialization. Each connection
serves lookup requests sequentially, while separate connections can overlap. The
owner bounds them to four per session and four times the effective session
maximum in aggregate. MCP request IDs stay in the session worker.

A timed-out accepted lookup drops its connection. The worker, index, and other
sessions remain usable, and its unfinished task stays charged to the session
until it returns. Failure to acknowledge an incoming connection marks the
owner unresponsive and retires that generation from future attachments.
Existing peers retain access while they have leases. Queries and edits execute
locally in the session worker against the prepared snapshot. Killing a worker
stops its edits. Background index maintenance can finish for other clients.
Linked-checkout reconciliation reloads the checkout's own configuration while
retaining its warm state between refreshes.

The last connection releases its lease. At most four idle owners survive for a
default 60 seconds. Owner loss ends its attached sessions, and the next
connection starts a new process. A new attachment that finds a stopped owner
requests its termination before creating a replacement. Old sessions end when
that owner exits. Current, retired, and exiting owners share a finite pool of
`max_sessions + 5` entries, including the idle allowance and one replacement.
A child awaiting kernel exit still counts until reaped. Pool shutdown requests
termination for all owners and bounds its final wait to two seconds.

A private lifetime pipe wakes an index owner when the daemon dies. Linux also
ties its parent-death signal to the long-lived pool thread. Live sessions stay
independent of the thread that accepted the first client.

The daemon reserves a session slot before launching children and rejects new
sessions during shutdown. Linux and manual foreground daemons give workers
separate process groups for per-session cleanup. Linux also applies
parent-death signaling to the direct worker. Service-level cleanup on Linux
uses `KillMode=mixed`, allowing the supervisor to receive the initial
termination signal before cgroup-wide escalation. Socket-activated macOS
workers remain in launchd's job process group for cleanup on daemon failure.
Their per-session stop targets the direct child PID. Current MCP workers run
operations in-process. Future helpers on every platform need explicit
containment because a helper can leave its assigned process group. Linux's
cgroup backstop applies to whole-service stops. Manual daemons depend on their
own process supervision.

Admission derives from the inherited `RLIMIT_NOFILE` soft limit and the
requested `TSINDEX_MAX_SESSIONS`. The daemon counts available descriptor
numbers after opening its own runtime handles. The per-session budget covers
worker pipe ends, exec-status handles, data sockets, startup/bootstrap pairs,
wake descriptors, one index owner, and an outcome watcher. Separate reserves
cover up to four idle owners, one replacement awaiting exit, and serialized
index-owner startup. The launcher's hard limit bounds its permitted
soft limit. Measured foreground Linux starts admitted 8 sessions at a soft limit
of 256 and 39 at 1024 with ordinary stdio and a private socket. Existing open
descriptors can change the result. Status exposes the computed maximum.

The resulting control budget admits at most 32 finite requests or connections
awaiting classification, retaining each permit through its response. Workers
in `Starting` and `Active` share the effective session limit. A separate pool
allows at most that many outcome watchers and retains each lease through its
terminal or error response, even after the worker leaves the registry. The
aggregate handler limit is the effective control budget plus twice the session
maximum. The accept loop checks shutdown between accepts.

Workers send a bounded internal readiness record after runtime initialization.
The proxy receives a session acknowledgement after that record, so invalid
configuration and worker startup failures reach stderr and exit status. It
registers a dedicated outcome connection before relaying MCP. The supervisor
sends the reaped worker's final status on that existing connection even during
whole-daemon shutdown, preserving exit status through stop and restart. Raw
newline-delimited MCP messages pass through 16 MiB relay buffers. Worker
startup has a 120-second deadline. A stop notification wakes supervision
independently of blocked pipes. Shutdown has a finite graceful phase followed
by TERM/KILL and child reaping, targeting the owned worker group or the direct
child according to the platform launch mode. Session summaries are paginated.
Per-session diagnostics retain up to 16 KiB and are fetched separately through
`daemon sessions --id`.

Direct MCP and HTTP/watch ownership use `SessionLifecycle`. Managed MCP shares
one orchestrator sweeper using `ProcessOwner`. Both inspect process identity,
including start time for PID reuse. Linux reads process metadata as bytes and
uses the thread count from the same stat record to distinguish an exited leader
with surviving threads from an entirely exited process. A connection can span
multiple conversations when its host retains the transport and owner process.
See [ADR 0004](adrs/0004-orchestrator-daemon.md).

## Filesystem updates

The elected MCP watcher owner uses the existing registration logic. Watch
startup validates the selected language filters for all configured repositories,
including absent clones. CLI overrides and repository/global fallback retain
their precedence;
unsupported IDs fail before the watcher initializes the index or allocates
watches, so an arriving clone cannot introduce a permanent retry error.
Incremental updates resolve
changed paths through the same ignore-aware walker used by full indexing.
Parsing and file I/O precede the database write transaction.

`src/watch.rs` coalesces raw notifications into distinct changed paths and
one pending wakeup. Read events are discarded before accumulation; writable
closes and overflow rescans remain actionable. A hash registry skips unchanged
registrations. Ancestor reference counts identify subtree invalidation even
when a moved ancestor has no watch of its own.

Linux walks eligible directories before allocating individual nonrecursive
source watches. Native platforms retain recursive workspace watches and use
the eligible-directory walk to discover ignore inputs only.

`src/ignore_inputs.rs` maps rule and indirection files to the directories they
govern. Nonrecursive parent probes observe file edits, replacement, and
removal, including inherited rules above workspace roots, external
configured/global files, and shared Git metadata.
Embedded Git repositories and directory-scoped rule files are discovered
during eligible walks; metadata events also discover newly created Git inputs.
Rule-file symlinks retain probes on their indirections and targets, including
directory links within the input path and missing targets, so replacement and
recreation remain observable. Source directory symlink trees remain pruned.
Cached rule owners are retired if their directory or a workspace-relative
ancestor becomes a symlink; explicit rule-file routes may still cross links.
Inputs keep both their original routes and canonical probe-path aliases so
macOS casing changes and Windows verbatim prefixes still match notifications.
Missing targets retain the canonical alias of their nearest existing ancestor.
Input resolution follows the pinned `ignore` dependency's semantics rather
than invoking Git with different configuration.
If a walk discovers new input parents, registration repeats the affected walk
after those probes exist. This reconciles eligibility read before a newly
discovered external rule could be observed, both at startup and during updates.
Index updates reuse that reconciled mapping and workspace snapshot rather than
resolving another set of rule paths for the same notification batch.

Indexing and watch refresh share target expansion, including nested workspaces
affected by inherited rules. Successful index updates precede removal of
obsolete watches, preserving a notification route if indexing fails.
The live-root snapshot advances only after successful indexing, so a failed
first update of an arriving clone retries its entire root on the next batch.
Removing an input parent also triggers retention when its governing source
directory has vanished and no live source registration pass is needed.
Optional probe failures are logged at startup and during discovery; source-root
registration failures and watch-limit exhaustion still stop startup.

## Platform and concurrency limits

Ancestor rule probes include absent rule paths through the filesystem root,
so creating an inherited rule remains observable. Linux and Windows implement
nonrecursive probes in their native backends. FSEvents uses recursive streams
and filters nonrecursive roots after delivery; broad ancestor probes can
therefore increase macOS event traffic. Native macOS measurements are needed
before changing this policy. Dropping ancestors or probing only existing rules
would lose creation notifications; a separate targeted polling backend would
require its own latency, replacement, and recovery contract.

Probe reconciliation waits for a stable input frontier. Continuous rule-link
or parent-directory churn can delay completion. A fixed pass cap must retain
unresolved targets and retry them, rather than report partial eligibility as a
successful registration. Static input topology settles without such a cap.
As with the underlying recursive watcher, creation between listing a source
directory and installing its watch can be missed; later subtree reconciliation
or a rebuild repairs that pre-existing gap.

## Validation

Unit tests cover target expansion, registry lifecycle, and event batching.
Linux integration tests run the actual MCP helper, inspect SQLite symbols and
inotify registrations, and verify later edits after input/lifecycle changes.
The pinned formatter, Clippy, tests, and startup checks are documented in
`CONTRIBUTING.md`.
