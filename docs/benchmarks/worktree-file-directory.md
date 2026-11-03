# Effective file directory experiment

Persisting resolved file metadata removes the full file-view materialization
from selective symbol and reference joins. This experiment leaves tool SQL,
extraction, parsers, source hashing, and staging policy unchanged. The directory
has a primary-key index on signed logical file ID and a unique path index.

## Measurement

On November 3, 2026, three sequential rounds compared matching debug builds on
Linux using 2,000 synthetic Python files per checkout, four symbols per file,
one primary, and eight linked Git worktrees. The variants were PR13 at
`db9dc4d`, PR15 at `840d7d8`, and the directory candidate based on PR15. Variant
order rotated between rounds. No compiler or other task-owned benchmark ran
concurrently, though the shared development host was not otherwise idle.

| Measurement | PR13 copied | PR15 overlay | Directory |
| --- | ---: | ---: | ---: |
| Cached `get_symbol` | 1.24 ms | 8.29 ms | 1.87 ms |
| Cached `find_references` | 1.44 ms | 8.92 ms | 2.52 ms |
| First use per checkout | 129 ms | 271 ms | 332 ms |
| No-op refresh per checkout | 77 ms | 177 ms | 157 ms |
| One-file update per checkout | 67 ms | 195 ms | 182 ms |
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

The directory reduced cached symbol latency by 77.4% (4.4 times faster) and
reference latency by 71.8% (3.5 times faster) versus PR15. All three rounds
showed this improvement: symbol medians ranged from 1.87 to 1.94 ms versus
7.74 to 8.53 ms; reference medians were 2.40 to 2.71 ms versus 8.75 to 9.03 ms.
The copied indexes remained faster for these reads.

Ingestion changes were modest and noisy. The directory's first-use median
rose 22.4%. No-op refresh fell 11.5% and one-file updates fell 6.4%, but round
ranges overlap: no-op medians were 145 to 175 ms versus 149 to 187 ms, and
one-file updates were 182 to 206 ms versus 183 to 199 ms. The larger private
database adds preparation and staging work; walking, full source hashing,
validation, and backup remain. Treat these local debug-build timings as a
focused experiment, not a production latency estimate.

The query-plan regression asserts that the existing selective tool query
shapes seek `overlay_file_directory` by integer primary key without
materializing `files`. It failed against PR15 before the implementation.
Compatibility tests cover missing and stale directories and pinned readers.
An additional local probe used the actual PR15 binary to add a source file:
the new reader returned it through the generation-mismatch fallback, and the
next new writer rebuilt the directory correctly.

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
