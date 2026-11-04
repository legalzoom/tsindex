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

Before a managed primary read, a configuration-matched successful full-refresh
marker can be reused for `worktree_refresh_ms` (default 2000 ms). Its generation
must still match the database. A local coordinator and a cross-process lease
coalesce scans; the marker is checked again after taking the lease. Only a
successful full build advances it. Partial path updates cannot conceal a gap
in notification coverage. Reconciliation runs even with a healthy watcher,
covering missed or incomplete registrations; failure returns a freshness
error instead of silently treating old indexed data as current.

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
changes before acknowledging them, without holding its configuration lock over
tool execution. Existing host, origin, size, and read-only tool guards remain
in the HTTP dispatch path.

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
