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

## Filesystem updates

The MCP server owns a watcher for its lifetime. Incremental updates resolve
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
