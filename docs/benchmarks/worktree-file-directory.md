# Effective file directory experiment

Persisting resolved file metadata removes the full file-view materialization
from selective symbol and reference joins. This experiment leaves tool SQL,
extraction, parsers, source hashing, and staging policy unchanged. The directory
has a primary-key index on signed logical file ID and a unique path index.

## Measurement

On November 4, 2026, three sequential rounds compared matching debug builds on
Linux using 2,000 synthetic Python files per checkout, four symbols per file,
one primary, and eight linked Git worktrees. The variants were PR13 at
`db9dc4d`, PR15 at `840d7d8`, and the directory candidate at `fcd7732`, including
the compatibility fixes. Variant order rotated between rounds. No compiler or
other task-owned benchmark ran
concurrently, though the shared development host was not otherwise idle.

| Measurement | PR13 copied | PR15 overlay | Directory |
| --- | ---: | ---: | ---: |
| Cached `get_symbol` | 1.34 ms | 8.68 ms | 2.20 ms |
| Cached `find_references` | 1.56 ms | 10.04 ms | 2.97 ms |
| First use per checkout | 138 ms | 306 ms | 330 ms |
| No-op refresh per checkout | 75 ms | 202 ms | 173 ms |
| One-file update per checkout | 74 ms | 221 ms | 174 ms |
| Total SQLite storage | 22,597,632 B | 6,860,800 B | 10,137,600 B |

Entries are medians of the three round medians. Each round measured 200 cached
requests per tool, cycling through checkouts and source names after warmup.
`get_symbol` omitted bodies; `find_references` included its usual verified
one-line snippet. These are complete MCP request timings, including dispatch,
connection setup, and serialization. A ten-minute refresh interval separated
cached reads from ingestion. Generation checks and build logs verified that
the measured cached requests performed no reconciliation.

Ingestion used a zero refresh interval and included a symbol query with its
verified source body. First use changed one source file per checkout; the
later update changed a second file. All variants parsed exactly one file per
changed checkout and zero on unchanged scans, with zero extraction failures.
Both overlay versions preserved their generation across unchanged refreshes.
The harness also verified that the primary's source bodies stayed unchanged.

Storage includes the primary, shared immutable base, all private indexes, and
WAL/SHM sidecars after the update. Source files, Git objects, and usage logs are
excluded. Every round produced the same storage counts. The directory adds
3,276,800 bytes across eight checkouts, about 400 KiB each. Total storage grows
47.8% versus PR15 but remains 55.1% below PR13. Shared symbol and reference
storage is retained; the extra copies contain file metadata.

## Interpretation

The directory reduced cached symbol latency by 74.7% (4.0 times faster) and
reference latency by 70.4% (3.4 times faster) versus PR15. All three rounds
showed this improvement: symbol medians ranged from 1.89 to 2.30 ms versus
8.25 to 9.81 ms; reference medians were 2.63 to 3.06 ms versus 9.88 to 11.27 ms.
The copied indexes remained faster for these reads.

The directory's first-use median rose 7.9%, no-op refresh fell 14.4%, and
one-file updates fell 21.2%. Refresh variability was substantial: first-use
medians were 323 to 382 ms versus 297 to 405 ms, and one-file updates were
173 to 182 ms versus 180 to 238 ms. No-op medians were 162 to 178 ms versus
193 to 212 ms. The copied indexes remained faster for ingestion as well.
The larger private database adds preparation and staging work; walking, full
source hashing, validation, and backup remain. Treat these local debug-build
timings as a focused experiment, not a production latency estimate.

The query-plan regression asserts that the existing selective tool query
shapes seek `overlay_file_directory` by integer primary key without
materializing `files`. It failed against PR15 before the implementation.
Compatibility tests cover missing, stale, and unknown-version directories,
failed publication, and pinned readers across private/base file-ID changes.
Mutation sequences compare the directory with extraction and masks directly.
An interoperability regression uses the actual PR15 binary to add a source
file: the new reader returns it through the generation-mismatch fallback,
and the next new writer rebuilds the directory. CI builds that original
writer and runs the regression explicitly; local runs can set
`TSINDEX_LEGACY_WRITER_BIN` and run the ignored test in `tests/overlays.rs`.

## Reproduce

Python 3.11 or newer and Git are required; no Python packages are needed.
Build each revision with `cargo build --locked`, using matching profiles.

```sh
python3 -B bench/worktree_file_directory.py \
  --materialized-bin /path/to/pr13/target/debug/tsindex \
  --overlay-bin /path/to/pr15/target/debug/tsindex \
  --candidate-bin target/debug/tsindex \
  --files 2000 --worktrees 8 --rounds 3 --samples 200 \
  --output target/file-directory-benchmark/pr13-pr15-directory-debug-final.json
```

Use `-n` to inspect the configuration without creating fixtures or processes.
The optional `--materialized-bin` adds PR13 to the overlay/candidate comparison.
The harness reuses `bench/worktree_overlays.py`, creates fixtures under its
worktree-local artifact directory, and cleans up Git registrations and child
processes. Primary automatic maintenance is disabled to avoid allocating
watchers. Linked worktrees retain ordinary on-demand ingestion. The JSON
records individual samples, binary SHA-256 digests, build counts, generations,
storage components, round order, and environment information.
