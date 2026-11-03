# Worktree index overlays

Status: implemented on the stack over
[PR #13](https://github.com/legalzoom/tsindex/pull/13), parent commit
`db9dc4d684521c0b9ae9e90f03be0cb331377599`.

The parent supplies checkout discovery, routing, configuration inheritance,
content checks, extraction, and tool behavior. This stack replaces fresh linked
checkout database copies with immutable shared extraction and private deltas.
It also removes request serialization across checkouts and makes notification
failure recoverable through on-demand maintenance. The
[prior-art research](../research/worktree-index-overlays-prior-art.md) records
related implementations and the constraints they expose.

## Behavior preserved from the parent

`tests/worktree_contract.rs` drives real MCP processes and CLI fixture builds.
It reads no index tables and asserts no storage layout. Its four contracts were
first committed and passed against the unchanged parent runtime, then retained
through this implementation:

- All five read tools select the same checkout by name or absolute root;
  omitted `repo` selects the primary. Bodies and edits use live selected source.
- Emptying, deleting, renaming, excluding, and restoring files do not resurrect
  old symbols or references after a server restart.
- Counts and one-result pages reflect effective checkout visibility before
  symbol, reference, and syntax-query filters and pagination.
- Two independent agent processes ingest separate worktrees without changing
  each other's results or the primary checkout.

Run these contracts in either checkout with:

```sh
cargo test --locked --test worktree_contract
```

Fixtures, logs, and test databases live under worktree-local `target/` paths.
Child processes are killed and reaped on success and failure.

## Data ownership

The primary keeps its materialized index. One consistent snapshot of a ready
main index becomes a fixed base under
`<main>/.tsindex/bases/extraction-v<schema>.db`. It is an additional full copy,
shared by all compatible fresh linked checkouts. Primary updates never mutate
that artifact. The completed base records a SHA-derived snapshot identity;
each overlay persists both the base path and that identity. Every attachment
verifies the identity before returning rows. Recreating a same-schema file at
the same path cannot silently rebase an existing overlay.

Each linked checkout owns a writable `.tsindex/index.db` containing its base
pin, per-path observations, whole-file replacement extraction, and hidden
markers. Unchanged files reuse base extraction only after local content and
language match. A changed file replaces all base extraction for its path even
when the new extraction is empty. Deleted or excluded paths disappear from the
logical index. Base IDs and private IDs occupy separate namespaces.

Transient command language filters do not restrict overlay publication: its
reconciliation includes every configured language so source validation checks
a complete logical generation. Query/result filters still apply, and unknown
repository or unsupported language selectors retain their existing errors.

The overlay domain is one workspace rooted at that checkout. Multi-repo
catalogs and outside source roots use materialized storage. Existing
materialized worktree caches stay materialized; this stack does not silently
convert them. A future content-addressed extraction cache, base rebase,
compaction, and garbage collection remain separate work.

## Request pseudotrace

```text
receive request(id, repo, arguments)
  schedule within the transport's bounded worker pool
  discover and resolve the checkout using the parent's naming rules
    missing repo -> primary catalog
    unknown checkout -> invalid-parameters error with known checkouts

  primary selection:
    if automatic maintenance is enabled
      ensure a watcher owner exists when this is an MCP server
      reuse a configuration-matched full-refresh marker within the interval
      otherwise coalesce and run a full on-demand reconciliation
      failed reconciliation -> explicit freshness error

  linked checkout selection:
    obtain its coordinator under the short registry lock, then release it
    if a refresh is due
      start or join that checkout's ingestion job
      wait on its condition variable, releasing its entry lock while waiting
      timeout after 30 seconds -> retryable busy result; ingestion may continue
    another checkout's coordinator remains independent

  open the selected index's SQLite read transaction
    read the committed base pin before attaching immutable base extraction
    require the attached base's persisted identity to match that pin
    expose effective files, symbols, and references through local views
    hide replaced/deleted/excluded base paths before filters and pagination

  dispatch the existing tool using the selected live source root
  frame response(id) through the transport's serialized output writer
  drop the read connection to release its snapshot
```

MCP and HTTP use four workers and queues of 16 pending requests. Responses may
complete out of arrival order and retain their request IDs. HTTP tool execution
uses a runtime snapshot without retaining the configuration lock. A bounded
pool can be occupied by several slow requests; independence is not an
unbounded-worker guarantee.

## First use pseudotrace

```text
prepare_checkout(checkout)
  preserve existing configuration or inherit the parent's effective rules
  acquire this database's cross-process writer lease
  recheck existence under that lease
    existing materialized index -> preserve its format
    unsupported configuration or no ready main source -> ordinary standalone

  find a compatible immutable base in the same Git repository
    none exists:
      copy the ready main database into a private scratch file using Backup
      persist its SHA-derived snapshot identity before publication
      close/checkpoint the copy and publish it with a create-only hard link
      reuse the completed winner if another creator published first

  initialize private overlay metadata and its persistent base pin
  run the normal complete checkout reconciliation
    walk local eligible source files
    hash to establish identity across different checkout roots
    reuse matching base extraction; parse changed or new content
    hide missing and excluded base paths
  publish the first ready generation
```

The hard link is to a completed immutable copy, never to a live mutable SQLite
database. Sharing removes repeated full extraction storage and parsing, but
first use still walks and checks local files. Dirty files in the main checkout
cannot substitute for the selected checkout's bytes.

If the main index is unready or unavailable on first use, the checkout builds
a standalone materialized index. It stays materialized when the main index
later becomes ready: availability does not depend on main-index preparation,
and there is no automatic later conversion of existing caches.

## Concurrent ingestion pseudotrace

```text
refresh(checkout_A)
  acquire A's database writer lease before reading or parsing
  copy only A's thin overlay into a private stage
  expose merged base/stage views to the reused extraction pipeline
  reconcile additions, changes, deletions, exclusions, and restores
    unchanged extraction stays in the immutable base
    replacements have whole-file authority, including empty extraction
  validate source timestamps, sizes, and hashes across the logical checkout
    source changed during preparation -> discard stage and report retry
    partial extraction failure -> retain prior published state
  unchanged effective state -> skip redundant publication
  otherwise publish the complete stage with SQLite online Backup
  release A's writer lease

refresh(checkout_B)
  perform the same cycle under B's independent lease
  parse and publish while A is still preparing or waiting
```

The lease covers the complete read-stage-parse-validate-publish cycle across
processes, so an older per-file parse cannot finish after a newer accepted
publication in the same checkout. Cooperative symbol edits also serialize
under the checkout's writer lease. External editors do not acquire that lease;
source validation and the existing stale-body checks still apply.

Publication uses a destination SQLite transaction in the stable live WAL
database. An existing reader keeps the old complete snapshot; a new reader sees
the new complete state. Dropping an incomplete backup rolls back the destination
transaction. Stages and their sidecars are cleaned up after use. No mutable
SQLite database is renamed over a live pathname.

A logical generation is an index snapshot, not an atomic filesystem snapshot.
A later source edit requires another observation. Bodies continue to verify
live content before returning indexed ranges.

Complete content-hash verification also runs for unchanged reconciliation;
only extraction parsing is limited to changed files. Any encountered source
read or extraction failure prevents freshness success, and an overlay does not
publish a partial replacement generation. Managed reads fail until a complete
refresh succeeds. A source changing during reconciliation returns a retryable
tool error with no overlay publication; continuous external editing can require
repeated retries. These availability and latency costs are deliberate, while
the primary's explicit `--no-refresh` opt-out remains supported.

## Notification resources and freshness

The parent was reproduced serving an old indexed symbol after watcher startup
failed with `EMFILE`, while a live syntax query returned the newly edited
symbol. The process remained responsive. Linux can report `EMFILE` when the
user's inotify-instance quota is exhausted even when that process has few open
file descriptors. See [Linux's inotify allocation errors][inotify-errors].

MCP servers with compatible database/configuration share watcher ownership
through a process-safe lease. Ownership releases on process exit. A failed
watcher retries, and another server may take over; reads can always use
on-demand reconciliation. HTTP uses that reconciliation without a watcher.
Explicit `--no-refresh` opts the primary out of automatic maintenance, while
linked-checkout selection retains its on-demand refresh behavior.

A successful full-refresh marker is configuration- and generation-matched,
checked again after acquiring the cross-process refresh coordinator lease.
Only a successful complete build advances it. Targeted path updates and
partial repo builds cannot postpone reconciliation of a missing notification
route. Even a healthy watcher does not bypass the bounded full-refresh check.
Failure to establish freshness is returned to the caller.

Linked checkout scans also coalesce across compatible processes through the
durable marker and coordinator lease. A reused marker retains its original
age in the local cache, so a second interval is not added to its freshness
window. Zero intervals reconcile every request; explicit builds remain forced.

Deterministic tests inject inotify allocation failure into a child process,
exercise watcher recovery and lease handover, and hold one checkout's writer
lease while proving another progresses. They do not exhaust the host's shared
watch budget or change system limits.

## Compatibility and lifecycle

- Published overlays have a distinct schema version and physical extraction
  table names. Parent indexed readers reject them; parent raw queries fall
  back to a complete live walk instead of treating the delta as a full index.
- Existing materialized worktree caches remain supported and retain their
  previous storage. A missing or incompatible base, or changed configuration
  outside the single-checkout domain, triggers a separately prepared standalone
  rebuild. Failed preparation leaves the old published bytes intact.
  Explicit changes to a configured workspace's name or root that no longer
  match the stored overlay workspace also use this cold-rebuild path.
- An absent or mismatched base identity refuses reads with rebuild guidance;
  ordinary reconciliation or an explicit build cold-recovers the checkout.
  Already pinned readers retain their original base. Older bases without an
  identity record stay untouched, and fresh checkouts use standalone storage.
- Bases are deliberately retained. Persistent checkout pins and active readers
  in other processes require a reclamation protocol beyond an in-process count.
  There is no automatic base deletion, rebase, or compaction in this stack.
- To downgrade, stop servers using affected caches and regenerate their derived
  indexes with the older binary, preserving configuration. Overlay formats are
  not backward-compatible materialized caches.

## Validation and evidence

The unchanged MCP contracts and original worktree tests preserve user-visible
behavior. Storage-interface tests cover thin extraction, snapshot retention,
whole-file authority, reuse on restore, missing-base recovery, incompatible
configurations, and legacy-reader fencing. Transport tests exercise deliberate
out-of-order completion, bounded queues, configuration publication, EOF, and
broken output; maintenance tests cover fallback, ownership, and coordination.

See [measurement notes](../benchmarks/worktree-overlays.md) for the reproducible
synthetic comparison. The test workflow runs on pull requests targeting feature
branches as well as `main`, allowing the stacked PR to receive ordinary CI.

[inotify-errors]: https://man7.org/linux/man-pages/man2/inotify_init.2.html
