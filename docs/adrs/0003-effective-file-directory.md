# Index the effective file directory per checkout

Selective symbol and reference queries join merged extraction to files. SQLite
materializes the original `UNION ALL` file view before that join, walking all
base files even when the name index returns one symbol. Adding another index
to the base table does not remove this intermediate materialization.

Persist a resolved directory of file metadata in each private overlay, keyed
by signed logical file ID and relative path. The existing `files` view can
select this table, and existing tool queries can seek each matching file.
Extraction, parsers, masks, body verification, and tool SQL keep their existing
behavior. Directory maintenance joins the existing file write operations and
publishes with the same private-stage backup.

The directory is a derived cache tagged with its own version and the completed
extraction generation. An absent or mismatched tag uses the original merged
view. This permits existing overlays and older PR15 writers to remain usable:
an old writer advances extraction generation without stamping the directory,
and the next new writer rebuilds it from authoritative facts and masks. No
extraction-format migration or unconditional reparse is required.

This trades duplicated file metadata and additional staging bytes for faster
selective reads. Symbols and references still share immutable extraction, and
each checkout still ingests independently. Full source walking, content hashing,
validation, and private staging remain reconciliation costs. The
[benchmark](../benchmarks/worktree-file-directory.md) compares both costs with
the original overlay and materialized implementations.
