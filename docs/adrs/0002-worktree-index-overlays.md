# Share immutable extraction with private worktree overlays

Parallel agents need independent checkout ingestion, but copying every symbol
and reference into every worktree scales storage with their mostly identical
content. We retain the primary materialized index, freeze one consistent base,
and give each fresh single-checkout workspace a private delta with whole-file
authority. This preserves the existing extraction and query pipeline while
letting different checkouts publish independently.

An overlay pins a persisted SHA-derived identity from the completed base, and
every attachment checks it before resolving rows. The path and extraction
schema are insufficient generation identities: deleting and recreating a base
at the same filename must not silently change older overlays' meaning. A
mismatch refuses reads with rebuild guidance and recovers through separately
prepared reconciliation rather than relabeling existing masks or row IDs.

The private overlay is staged under a per-database cross-process writer lease
and published through SQLite online backup into the stable WAL database.
Readers pin a SQLite transaction before reading base metadata. This trades
serial ingestion within one checkout and private-stage I/O for atomic complete
generations, crash rollback, and readers that can finish on the previous state.
Renaming a live database would split SQLite locks and sidecars across inodes;
a shared writable extraction database would introduce another contention and
reclamation protocol. Neither is needed for this experiment.

Bases stay immutable and retained. This costs one extra full snapshot beside
the primary and delays automatic rebasing, compaction, and deletion until their
cross-process lifetime rules are implemented. Existing materialized caches and
multi-repo catalogs retain their previous storage; unavailable bases recover
through a separately prepared materialized checkout. See the
[execution trace](../specs/worktree-index-overlays.md) and
[prior art](../research/worktree-index-overlays-prior-art.md).
