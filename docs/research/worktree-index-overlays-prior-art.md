# Worktree index overlays: prior art

Researched: 2026-11-02.

This note evaluates prior art for replacing the copied worktree databases in
[tsindex PR #13][tsindex-pr] with shared index data and independently writable
worktree deltas. Parallel agents must be able to process and ingest changes in
different worktrees concurrently. This is research and a proposed direction;
it does not describe an implemented tsindex architecture.

## Findings

There are direct precedents for the important parts of this design. Glean
implements stacked code-index databases with file ownership filtering. Clangd
implements live-over-static code indexes and versioned index publication.
RocksDB supports writable database clones sharing immutable storage files.
OverlayFS supplies a particularly clear statement of which layers may be
shared. These systems establish useful mechanisms, but none of the evidence
below establishes an off-the-shelf replacement for tsindex's complete storage,
query, and worktree lifecycle.

| Prior art | Borrow | Does not establish |
| --- | --- | --- |
| Glean | Persistent code-index stacks | Independent process writers |
| Clangd | File masking; pinned views | Durable worktree branches |
| RocksDB | Writable clones sharing files | SQLite compatibility |
| OverlayFS | Shared base; private changes | Index query behavior |
| Zoekt | Deltas; rebuild fallback | Immutable base metadata |
| Salsa | Parallel queries | Concurrent branch writes |

## Glean: the closest persistent code-index precedent

Glean's official incrementality description explicitly builds a new database
on an existing one while hiding selected ownership units, normally source
files. Older databases and multiple divergent successors can coexist. Its
database lifecycle distinguishes writable, incomplete databases from completed
databases. This is substantially closer to worktree indexes than a generic
filesystem overlay. [Incremental indexing][glean-blog],
[database lifecycle][glean-databases].

The implementation models ownership sets and slices of a base database. The
test suite completes a base, excludes units from a pruned view, and constructs
multiple incremental variants. This verifies persisted composition and
coexistence. It does not, by itself, verify that independent operating-system
processes can ingest each branch concurrently. [Ownership implementation
guide][glean-ownership], [incremental tests][glean-tests],
[stacked lookup implementation][glean-stacked].

**Application to tsindex:** model the effective index as a base plus new facts,
with base facts excluded by their owning file. Glean also shows why dependency
ownership must be explicit: changing one file can invalidate derived facts
owned elsewhere. Tsindex's current syntactic extraction is simpler than a
semantic graph, but any later cross-file analysis would need its own
invalidation policy. This is an architectural inference from the cited design.

## Clangd: file authority and publication under concurrent work

Clangd layers a dynamic `FileIndex` over background, static, or remote index
data through `MergedIndex`. Its background index uses a worker pool and stores
per-file shards. Its remote index is produced by a separate batch indexer and
queried by developer processes, with source-root translation on the client.
[Index design][clangd-design], [remote index design][clangd-remote].

The important behavior is stronger than combining results and removing
duplicates. When the dynamic index owns a file, a symbol absent from that
dynamic data is suppressed from the static index. References from an owned
file are taken from the dynamic layer, because old and new offsets cannot be
reliably deduplicated. Coverage therefore matters even when a freshly indexed
file contains zero symbols or references. [Merge implementation][clangd-merge].

Tests cover an emptied file hiding its previous symbol and removing every
dynamic reference while suppressing the corresponding static references.
However, another test deliberately preserves a static definition when the
symbol still exists dynamically: clangd merges semantic information in that
case. Tsindex should borrow the file-authority rule, not assume clangd's entire
symbol merge policy matches checkout isolation. [Removal and reference
tests][clangd-merge-tests].

Clangd supplies two useful concurrency mechanisms:

- `FileSymbols::buildIndex` collects shared pointers to per-file slabs and file
  coverage under its mutex, then constructs the search index outside that
  lock. Readers retain the immutable slabs they need.
  [File snapshot construction][clangd-file-index].
- `SwapIndex` takes a short lock to acquire or replace a shared index pointer.
  Queries pin that pointer for their lifetime; replacement does not destroy
  the old index under the lock. The background rebuilder constructs a new
  index separately and rejects an older build finishing after a newer one.
  [Snapshot publication][clangd-swap], [generation guard][clangd-rebuild].

There are also concrete limitations to avoid copying. Background indexing
explicitly documents that an older per-file parse can finish later and replace
a newer result. Whole-index build ordering does not solve that per-file race.
The disk cache is keyed by file path, with content digests used for freshness;
it is not a content-addressed cache shared across arbitrary checkouts.
[Background updates][clangd-background], [shard storage][clangd-storage].

**Application to tsindex:** publish complete views, pin them during queries,
and keep expensive parsing outside shared locks. Independently guard the
version of each file update. Clangd establishes these mechanisms within a
process, rather than proving the proposed multi-process overlay protocol.

## RocksDB: writable clones sharing immutable storage

RocksDB checkpoints create a consistent database snapshot. On the same
filesystem, SST files can be hard-linked instead of copied; mutable database
metadata is separate. The checkpoint is a standalone database that can be
opened for reads and writes. The implementation prepares a private directory,
disables source file deletion while linking or copying files, then publishes
the prepared directory by rename and syncs it.
[Checkpoint guide][rocksdb-checkpoints], [design explanation][rocksdb-blog],
[implementation][rocksdb-source].

Tests cover a checkpoint retaining an old value after the source changes and
surviving deletion of the source database, plus snapshot creation while a
writer runs. These verify stable snapshots during source activity, not
simultaneous ingestion into multiple writable clones. RocksDB documents one
process per database directory; multiple threads may share a database handle.
[Snapshot tests][rocksdb-tests], [parallel-writer test][rocksdb-parallel],
[process concurrency][rocksdb-concurrency].

**Application to tsindex:** this is evidence that sharing immutable storage and
retaining separately writable database instances is an established engine
design. Independent ingestion would use distinct writable clone directories;
it was not benchmarked here. Compaction can replace shared SSTs, reducing
storage sharing. Replacing SQLite would require substantial changes to
tsindex's relational queries. This is an alternative implementation strategy,
not a small SQLite configuration change.

## OverlayFS: explicit rules for shared and private state

The kernel documentation permits a lower layer to be shared across overlay
mounts, but forbids sharing an upper layer or work directory between them.
Whiteouts hide deleted lower-layer entries. Changing the underlying layers
while mounted has undefined behavior. These restrictions make the ownership
model unusually clear. [OverlayFS documentation][overlayfs].

**Application to tsindex:** immutable base generations can be shared; mutable
overrides and deletion markers belong to one worktree. An updated main index
must become a new base generation, rather than mutate an existing lower layer.
This is a transfer of the model, not a recommendation to mount OverlayFS or
share a mutable SQLite file through filesystem overlays.

## Zoekt: useful delta indexing with different sharing semantics

Zoekt's delta builder supplies a useful counterexample. It creates delta
artifacts, but updates file tombstones and commit metadata in existing shard
sidecars. Artifact publication uses individual renames. Thus, its delta path
does not establish that several independent writable worktree indexes may
safely share one unchanged base and its metadata.
[Delta builder implementation][zoekt-builder].

Its Git indexer rejects delta builds with submodules, missing base shards, or
too many existing shards, and falls back to a normal build. This provides
concrete precedent for defining when incremental reuse is unsafe or no longer
worthwhile. [Delta eligibility][zoekt-git-indexer],
[normal-build fallback][zoekt-fallback].

**Application to tsindex:** retain deletion state in the worktree overlay and
publish a coherent view explicitly. Delta indexing does not automatically
provide branching isolation. Define compatibility checks and a full-build
fallback before borrowing this implementation.

## Salsa: parallel queries are a separate guarantee

Rust-analyzer exposes transactional input changes through `AnalysisHost` and
immutable query snapshots through `Analysis`. Salsa's current runtime shares
memoized storage across cloned handles, but an input mutation cancels parallel
handles and waits for them to finish before acquiring mutable storage.
[Rust-analyzer architecture][rust-analyzer], [Salsa runtime][salsa-runtime].

This is useful prior art for incremental computation and cancellation. It does
not establish independently mutable worktree branches. A single shared Salsa
database would require additional separation before satisfying parallel
ingestion across worktrees.

## Proposed tsindex design and unresolved work

The following is a synthesis, not behavior verified in an existing tsindex
implementation:

1. Keep checkout discovery and explicit `repo` routing from PR #13.
2. Publish immutable base generations containing relative paths, extraction
   identity, symbols, and references. Pin a generation for each overlay.
3. Give each worktree a private writable database containing changed and new
   file extractions, deletion markers, eligibility changes, and its base pin.
   Other worktrees never write that database.
4. Resolve a query as overlay facts plus base facts whose paths are not
   overridden, deleted, or excluded by that checkout. Apply this resolution
   before ordering, counting, limiting, and pagination.
5. Parse in parallel outside transactions. Serialize publication only within a
   worktree, with cross-process coordination when multiple agents target that
   same checkout. Publish file observations monotonically so a slow old parse
   cannot overwrite a new one. Release shared routing locks before parsing,
   refreshing, querying, or waiting on a worktree's worker.
6. Pin one coherent base-and-overlay view for each query. Build replacement
   generations and rebased overlays separately; publish only after both agree.
   Retain old generations until readers and overlays release them.
7. Add an optional immutable extraction cache keyed by exact source content,
   language, and grammar/extractor revision. This can deduplicate changed-file
   versions across worktrees without requiring one shared mutable index writer.

Source bodies and edits must still use the selected live checkout's root,
with the existing content verification. Shared extraction does not make
another checkout's source file an acceptable substitute. Base and overlay
row identifiers must also remain distinct when composing query results.

The current `parse_file` already takes a language specification and source
bytes, while `upsert_file` associates extraction with a repository path. That
is a plausible seam for separating shared extraction from checkout membership.
The existing PR also already avoids reparsing identical content, so the first
gains would be reduced database copying and duplicated storage. Initial
change detection, full scans, and hashing are separate costs.
[PR extraction and storage code][tsindex-extraction].

Open questions include compatible generation identity, storage growth when
branches diverge, compaction scheduling, garbage collection across processes,
crash recovery, bounded parser concurrency, and preserving existing partial or
unreadable-file behavior. A Git commit alone is not a complete checkout
identity: committed divergence, dirty files, untracked files, and checkout
ignore policy all affect the effective index.

## Validation required before implementation is accepted

The prior art suggests adversarial tests rather than assuming ordinary update
tests cover the risks:

- Empty, delete, rename, and newly ignore a base file; none may resurrect its
  symbols or references. Restore the file and verify current extraction.
- Run separate operating-system processes ingesting different worktrees while
  both receive queries. Block A's publication and verify B still progresses.
- Race two observations of the same file so the older parse finishes last;
  verify it cannot replace the newer result, including after a process crash.
- Publish a new base while a reader holds the old one; retain that reader's
  complete view and protect the old generation from garbage collection.
- Rebase an overlay onto a new base, including overlapping changes and
  deletions. Inject failures at each publication step and restart.
- Query with small pages and restrictive filters while many base rows are
  hidden. Verify counts, order, and continuation offsets against a clean full
  rebuild of the selected checkout.
- Change language or extractor identity with identical bytes. Reject
  incompatible cache entries and recover through a full extraction.
- Compare storage, first-use latency, sustained ingestion, and query latency
  against the existing copied-database implementation across multiple active
  worktrees. No speedup or storage ratio is established by this research.

[tsindex-pr]: https://github.com/legalzoom/tsindex/pull/13
[tsindex-extraction]: https://github.com/legalzoom/tsindex/blob/db9dc4d684521c0b9ae9e90f03be0cb331377599/src/index.rs#L4159-L4304
[glean-blog]: https://glean.software/blog/incremental/
[glean-databases]: https://glean.software/docs/databases/
[glean-ownership]: https://glean.software/docs/implementation/incrementality/
[glean-tests]: https://github.com/facebookincubator/Glean/blob/ec2351fb007976c704a2a49957f04b69b92713fd/glean/test/tests/IncrementalTest.hs#L90-L191
[glean-stacked]: https://github.com/facebookincubator/Glean/blob/ec2351fb007976c704a2a49957f04b69b92713fd/glean/rts/stacked.h
[clangd-design]: https://clangd.llvm.org/design/indexing
[clangd-remote]: https://clangd.llvm.org/design/remote-index
[clangd-merge]: https://github.com/llvm/llvm-project/blob/9640b6a6cc9516095c37006f78438e6aa7fc7c05/clang-tools-extra/clangd/index/Merge.cpp#L54-L144
[clangd-merge-tests]: https://github.com/llvm/llvm-project/blob/9640b6a6cc9516095c37006f78438e6aa7fc7c05/clang-tools-extra/clangd/unittests/IndexTests.cpp#L294-L514
[clangd-file-index]: https://github.com/llvm/llvm-project/blob/9640b6a6cc9516095c37006f78438e6aa7fc7c05/clang-tools-extra/clangd/index/FileIndex.cpp#L272-L300
[clangd-swap]: https://github.com/llvm/llvm-project/blob/9640b6a6cc9516095c37006f78438e6aa7fc7c05/clang-tools-extra/clangd/index/Index.cpp#L16-L98
[clangd-rebuild]: https://github.com/llvm/llvm-project/blob/9640b6a6cc9516095c37006f78438e6aa7fc7c05/clang-tools-extra/clangd/index/BackgroundRebuild.cpp#L81-L107
[clangd-background]: https://github.com/llvm/llvm-project/blob/9640b6a6cc9516095c37006f78438e6aa7fc7c05/clang-tools-extra/clangd/index/Background.cpp#L185-L250
[clangd-storage]: https://github.com/llvm/llvm-project/blob/9640b6a6cc9516095c37006f78438e6aa7fc7c05/clang-tools-extra/clangd/index/BackgroundIndexStorage.cpp#L27-L82
[rocksdb-checkpoints]: https://github.com/meta/rocksdb/wiki/Checkpoints
[rocksdb-blog]: https://rocksdb.org/blog/2015/11/10/use-checkpoints-for-efficient-snapshots.html
[rocksdb-source]: https://github.com/meta/rocksdb/blob/49a8f7d762df26a4a9c47790883453339e2a2553/utilities/checkpoint/checkpoint_impl.cc#L435-L552
[rocksdb-tests]: https://github.com/meta/rocksdb/blob/49a8f7d762df26a4a9c47790883453339e2a2553/utilities/checkpoint/checkpoint_test.cc#L284-L325
[rocksdb-parallel]: https://github.com/meta/rocksdb/blob/49a8f7d762df26a4a9c47790883453339e2a2553/utilities/checkpoint/checkpoint_test.cc#L1230-L1240
[rocksdb-concurrency]: https://github.com/meta/rocksdb/wiki/Basic-Operations#concurrency
[overlayfs]: https://docs.kernel.org/filesystems/overlayfs.html
[zoekt-builder]: https://github.com/sourcegraph/zoekt/blob/e1fafb15177040d792bd7c8c41a24a0fb39aada4/index/builder.go#L690-L779
[zoekt-git-indexer]: https://github.com/sourcegraph/zoekt/blob/e1fafb15177040d792bd7c8c41a24a0fb39aada4/gitindex/index.go#L919-L945
[zoekt-fallback]: https://github.com/sourcegraph/zoekt/blob/e1fafb15177040d792bd7c8c41a24a0fb39aada4/gitindex/index.go#L593-L597
[rust-analyzer]: https://rust-analyzer.github.io/book/contributing/architecture.html
[salsa-runtime]: https://salsa-rs.github.io/salsa/plumbing/database_and_runtime.html
