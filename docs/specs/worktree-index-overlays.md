# Proposed worktree index overlays

Status: proposed execution trace, with baseline behavior tests implemented.
Parent: [PR #13](https://github.com/legalzoom/tsindex/pull/13), commit
`db9dc4d684521c0b9ae9e90f03be0cb331377599`.

The overlay implementation has not been added. The current branch retains the
parent PR's runtime unchanged. Its new tests capture behavior to preserve while
replacing copied worktree databases. See the
[prior-art research](../research/worktree-index-overlays-prior-art.md) for the
established mechanisms and their limitations.

## Existing behavior under test

`tests/worktree_contract.rs` drives real MCP processes and CLI fixture builds.
It reads no index tables and asserts no storage layout. Git worktree metadata
is constructed locally, as in the parent PR's tests. All fixtures and logs live
under this worktree's `target/contract-fixtures/`; child processes are killed
and reaped even when a request or assertion fails.

Run the contract against the parent implementation with:

```sh
cargo test --locked --test worktree_contract
```

The new baseline checks cover:

- All five read tools selecting the same checkout by name or absolute root,
  default-primary behavior, live source bodies, and worktree-local edits.
- Emptying, deleting, renaming, excluding, and restoring files, without old
  symbols or references resurfacing after a server restart.
- Filtering and one-result pages for symbols, references, and syntax queries
  after checkout changes remove matches from the primary's original files.
- Two independent agent processes ingesting edits in separate worktrees and
  leaving each other's results and the primary checkout unchanged.

The last check establishes concurrent process correctness. It does not prove
that a stalled refresh cannot delay another checkout within one server; that
is a new guarantee requiring a deterministic regression test during the
implementation.

## Data ownership

The primary checkout retains its materialized index. Its watcher implementation
can be reused, but per-server ownership and failure handling need changes. A
consistent snapshot of a ready index becomes an immutable, repository-scoped
base generation. Several worktrees may refer to the same generation.

Each linked worktree owns a writable SQLite overlay, its file metadata,
replacement extractions, deletion or exclusion markers, and its base pin.
Worktrees never share the mutable overlay database. An optional immutable
content cache may later reuse extraction across changed-file versions.

An unchanged file uses base extraction. A changed file replaces all base
extraction for that relative path, including when its new extraction is empty.
A deletion or exclusion hides the path completely. Base and overlay row IDs
remain separate namespaces.

## Request pseudotrace

```text
receive request(id, repo, arguments)
  resolve checkout using the parent's discovery and naming rules
    missing repo argument -> original primary catalog
    unknown checkout -> existing invalid-parameters error

  acquire registry lock
    find or create that checkout's coordinator handle
  release registry lock

  if selected checkout belongs to the primary catalog
    use its existing runtime and source root
    if its watcher is unavailable
      refresh on demand before serving an indexed read
      if refresh fails, return an explicit freshness error
  else
    open existing overlay, or prepare its first generation
    if refresh is due
      join or schedule that checkout's refresh job
      wait under the existing timeout/retry policy
      keep other checkout requests and ingestion runnable

    begin a read transaction on the overlay
      read its published generation and pinned base identifier
      retain the immutable base for this read's lifetime
      resolve effective files and extraction for this checkout

  dispatch the existing tool through that checkout's read view
    suppress replaced/deleted/excluded base paths first
    then apply predicates, counts, ordering, and pagination
    bodies and edits always resolve against the selected source root
  return response(id)
  release the read view and base lifetime pin
```

The MCP input loop must remain able to receive and schedule another request
while one request waits for ingestion. Responses retain request IDs. HTTP
dispatch must similarly release its current global runtime lock. Changing the
database layout alone does not remove either request-serialization point.

## First use pseudotrace

```text
prepare_checkout(checkout)
  preserve its existing configuration and effective language/ignore rules
  find a compatible immutable base from the same Git repository
    if none exists
      prepare a consistent base snapshot in a private temporary location
      close/checkpoint it and publish only a complete immutable artifact
      concurrent creators may produce equivalent bases safely

  create a private overlay with a persistent pin on that base
  enumerate this checkout's eligible source files
    stat/hash local files to establish identity across checkout roots
    reuse base extraction only when language, content, and extractor match
    parse only new or changed content
    record paths missing or excluded here as hidden
  publish the first complete checkout generation
```

This retains the parent's important content-identity check. Sharing a base
removes the per-worktree database copy, not the initial reconciliation cost.
If a base omits files eligible in this checkout, they become overlay additions.
Dirty files in the primary do not define another worktree's contents.

## Concurrent ingestion pseudotrace

```text
refresh(checkout_A)
  acquire coordinator ownership scoped to checkout A
    use cross-process coordination when multiple servers target A
    coalesce duplicate refresh requests

  capture the current published generation
  discover eligible additions, changes, removals, and exclusions
  parse distinct changed files in a bounded worker pool
    perform file reads and parsing outside SQLite write transactions
    associate each result with its observed source identity/version

  revalidate candidates and the generation they were prepared against
    discard/requeue stale observations
    never publish an old result over a newer accepted observation

  begin A's overlay write transaction
    apply replacement extraction, including empty extraction
    apply deletion/exclusion markers and local file metadata
    publish the new complete generation
  commit
  release A's coordinator ownership

refresh(checkout_B)
  execute the same flow against B's own coordinator and database
  proceed independently of A's parsing or publication
```

Readers see a committed logical index generation, not an atomic snapshot of
the live filesystem. A file changing during ingestion must schedule another
observation; existing body verification still handles subsequent source drift.
Primary index updates cannot mutate a base already pinned by a worktree.

## Resource exhaustion is part of the correctness contract

The unchanged parent implementation was reproduced serving an old symbol
after watcher startup failed with `EMFILE`, while a live syntax query returned
the newly edited symbol. The MCP process remained available. This is a failure
of freshness, not merely an inconvenient test environment. Linux can report
`EMFILE` when the user's inotify-instance quota is exhausted even when the
starting process has few open file descriptors. See
[Linux's inotify allocation errors][inotify-errors].

Sharing index storage does not by itself bound watcher allocation. Multiple
agent processes must coordinate watcher ownership or use a shared service;
resource use must not grow unchecked with the number of clients. On-demand
refresh must remain a correctness path when notification resources are
unavailable, with explicit failure if that refresh cannot complete. Logging
watcher failure only to stderr is insufficient for clients relying on results.

Required regressions include watcher startup failure before the initial
refresh, loss or incomplete registration after a successful refresh, edit
visibility through every indexed read, recovery when resources become
available, and cleanup after owner exit.
An exhausted watcher budget must not prevent independent worktree ingestion.
Use deterministic failure injection or a process-local resource limit in the
test, rather than exhausting the host's shared quota.

## Publication and lifecycle rules

- Store the base identifier and effective overlay changes coherently. A query
  must not combine a new base with an overlay prepared for an older one.
- Keep persistent worktree pins and active-reader lifetimes. An in-process
  reference count alone cannot protect bases used by another process.
- Prepare compaction or rebasing separately. Publish only if the checkout
  generation used to prepare it is still current, otherwise retry.
- Keep existing materialized worktree indexes usable during migration. Prepare
  replacement state separately and retain the old index if preparation fails.
- Do not add automatic base deletion or rebasing before their crash and reader
  lifetime tests pass. These are separate implementation slices.

## Implementation sequence after trace review

Each step adds one failing behavioral or storage-interface test, implements
that slice, and reruns the baseline contract. Storage-specific assertions will
belong at an explicit storage interface, rather than inspecting SQLite tables
from the MCP contract suite.

1. Introduce the checkout read-view seam, retaining the copied-index adapter.
2. Add immutable base publication and a private overlay for one checkout.
3. Implement full-file authority, deletions, exclusions, and effective paging.
4. Add cross-process ingestion coordination and stale-observation rejection.
5. Remove global request waits; prove B progresses while A is deliberately
   blocked. Test independent processes and multiple requests to one server.
6. Bound notification ownership across clients and add watcher-failure
   freshness and recovery tests before retaining automatic watching.
7. Add safe migration and lifecycle behavior, then measure storage, first-use
   latency, sustained ingestion, and query latency against the parent PR.

Checkout discovery, names and aliases, configuration inheritance, source
walking, language extraction, MCP tool schemas, timeout policy, and watcher
registration logic are candidates for reuse. The full database copy, direct
storage queries, global dispatch locks, and watcher ownership/failure policy
are the main replacement points.

Before opening the stacked PR, account for the test workflow's current
`pull_request.branches: [main]` filter: a PR targeting the parent feature branch
otherwise receives no runs from that workflow.

[inotify-errors]: https://man7.org/linux/man-pages/man2/inotify_init.2.html
