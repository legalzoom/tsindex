# Worktree overlay measurement

These results describe PR15 before the
[effective file directory experiment](worktree-file-directory.md).

`bench/worktree_overlays.py` compares the PR #13 binary with this stack on
fresh, real Git worktrees. It uses synthetic Python sources and actual MCP
requests, checking each checkout's initial body, a later one-file edit, and
the unchanged primary. Both variants run sequentially with primary automatic
maintenance disabled; linked worktrees retain ordinary on-demand ingestion.

The benchmark requires Python 3.11 or newer and Git. Build both revisions using
the same profile, then run from this checkout:

```sh
python3 bench/worktree_overlays.py \
  --baseline-bin /path/to/pr13/target/debug/tsindex \
  --candidate-bin target/debug/tsindex \
  --files 2000 --worktrees 8 \
  --output target/overlay-benchmark/pr13-vs-overlay-final-query-plan-fix.json
```

`--dry-run` shows fixture parameters without creating files or starting
processes. Fixtures and usage logs live under `target/overlay-benchmark/` and
are removed after the run; the JSON result remains there. The script closes
or terminates and reaps its MCP children on success and failure. It never
changes the developer repository's worktree registrations.

Each checkout changes one of 2000 files before first use and another file
before the ingestion measurement. The first-use total includes the candidate's
first base publication. `worktree_refresh_ms = 0` makes unchanged requests run
their normal reconciliation, rather than time only a cached coordinator hit.
Storage counts the primary index, the extra immutable base, every private
index, and WAL/SHM sidecars. Source trees, Git objects, and usage logs are
excluded. The JSON separately reports database files and sidecars.

Each reported comparison is a single synthetic run. Filesystem caches, system
load, debug versus release builds, file sizes, and extraction density affect
these timings. It cannot establish production throughput or the number of parse
operations from latency alone. Storage-interface tests separately verify that
unchanged file extraction stays shared.

Named symbol and reference queries still materialize the complete effective
file set once in the runtime's outer join. Direct base-file joins inside the
symbol/reference views avoid duplicating that materialization, but each lookup
retains work proportional to the base file count. The fixture times a symbol
request together with reconciliation; it does not isolate query latency or
establish selective-lookup scalability for larger repositories.

## Observed comparison

On 2026-11-02, one matching debug-build run on Linux/WSL2 used 2000 files per
checkout and eight linked worktrees. The candidate includes base identity pins
and direct base-file joins in the symbol/reference views. Each variant returned
every expected checkout body and edit; the primary remained unchanged. All build
summaries reported zero failed files.

| Measurement | PR #13 | Overlay stack |
| --- | ---: | ---: |
| Total SQLite storage, including sidecars | 22,597,632 B | 6,860,800 B |
| Shared base included in that total | 0 B | 2,482,176 B |
| Private worktrees, including sidecars | 20,086,784 B | 1,867,776 B |
| First use, total over eight worktrees | 1.179 s | 2.347 s |
| Unchanged reconciliation, total over eight | 0.552 s | 2.226 s |
| One-file ingestion, total over eight | 1.038 s | 1.597 s |

This run used 69.6% less SQLite storage. It did **not** improve ingestion time:
first use took about twice as long. The candidate adds stage copying, merged
views, and complete source validation; this run does not isolate their costs.
Content hashes across the entire logical checkout are revalidated even on a
no-op reconciliation. Sharing extraction does not remove that full-source I/O.
The parent already reuses unchanged extraction after copying its index, so both
variants reported one file indexed and 1999 touched on each first use, zero
indexed and 2000 skipped on unchanged reconciliation, and one indexed with
1999 skipped on each later edit. The storage design saves duplicated extraction;
this measurement supplies no claim of fewer parsed files than the parent.

The validation policy also affects availability: a source changing during
preparation discards the overlay stage and asks the caller to retry. Encountered
file-read or extraction failures prevent freshness success, so managed reads
fail until a complete refresh succeeds. This run used stable, readable sources;
it does not measure retry costs under continuous external editing. The primary
retains an explicit `--no-refresh` opt-out. Index generations describe logical
index state, not atomic snapshots of the live filesystem.

The parent advanced each worktree's generation from 2 to 3 on the no-op scan.
The overlay kept each generation at 1, confirming that these unchanged
reconciliations did not republish the staged database. This is a diagnostic
observation, separate from the MCP behavior checks.

The primary contributes 2,510,848 bytes in both totals. The overlay total also
includes the one-time 2,482,176-byte base and all eight thin indexes; excluding
the base would overstate savings. Results are warm-cache, sequential synthetic
observations, not cold-filesystem or production throughput measurements. Use
the script to repeat the comparison on representative extraction density and
checkout deltas before making a latency decision.
