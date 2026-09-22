#!/usr/bin/env python3
"""Measure tsindex MCP response payload size: old (main) vs new (HEAD).

This is a deterministic payload-size harness, not a cost or savings benchmark:
it reports bytes (and a rough chars/4 token approximation) only. It makes no
USD or realized-saving claim.

Reproduce with a separate index built by each binary so schema differences
do not invalidate the comparison:

    # 1. Build the "old" (main) binary in a worktree.
    git worktree add -f /tmp/tsindex-main main
    ( cd /tmp/tsindex-main && cargo build --release )

    # 2. Build the "new" (HEAD) binary. Do NOT rely on `cargo install` here:
    #    build it explicitly and point TSINDEX_BENCH_NEW at the artifact so the
    #    harness cannot silently measure a stale ~/.cargo/bin/tsindex.
    cargo build --release
    export TSINDEX_BENCH_OLD=/tmp/tsindex-main/target/release/tsindex
    export TSINDEX_BENCH_NEW=$PWD/target/release/tsindex

    # 3. Build matching old/new index DBs.
    export TSINDEX_BENCH_OLD_DB=/tmp/bench-index-old.db
    export TSINDEX_BENCH_NEW_DB=/tmp/bench-index-new.db
    "$TSINDEX_BENCH_OLD" --root <repo> --db "$TSINDEX_BENCH_OLD_DB" init
    "$TSINDEX_BENCH_OLD" --root <repo> --db "$TSINDEX_BENCH_OLD_DB" build
    "$TSINDEX_BENCH_NEW" --root <repo> --db "$TSINDEX_BENCH_NEW_DB" init
    "$TSINDEX_BENCH_NEW" --root <repo> --db "$TSINDEX_BENCH_NEW_DB" build

    # 4. Run the harness.
    python3 bench/payload_size_old_vs_new.py

The harness runs both binaries with --no-refresh so startup is identical. The
measured size is the full JSON-RPC `result` payload -- what the model receives as
input tokens. It validates inputs up front and fails fast (non-zero exit) on any
subprocess failure, malformed JSON, or missing response id so a broken run can
never print misleading numbers.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

REPO = os.environ.get("TSINDEX_BENCH_REPO", str(Path(__file__).resolve().parents[1]))
REPO_NAME = Path(REPO).resolve().name
OLD_DB = os.environ.get("TSINDEX_BENCH_OLD_DB", "/tmp/bench-index-old.db")
NEW_DB = os.environ.get("TSINDEX_BENCH_NEW_DB", "/tmp/bench-index-new.db")
OLD = os.environ.get("TSINDEX_BENCH_OLD", "/tmp/tsindex-main/target/release/tsindex")
NEW = os.environ.get("TSINDEX_BENCH_NEW", str(Path.home() / ".cargo/bin/tsindex"))

# Representative queries spanning the bench task shapes. Each tool call is sent
# with a distinct integer id; the harness requires every id to be answered.
QUERIES = [
    ("get_symbol: deep trace (1 symbol, body)",
     {"name": "get_symbol", "arguments": {"repo": REPO_NAME, "names": ["replace_symbol"], "include_body": True}}),
    ("find_references: medium fan-out (parse_file)",
     {"name": "find_references", "arguments": {"repo": REPO_NAME, "names": ["parse_file"]}}),
    ("find_references: heavy fan-out (LanguageSpec)",
     {"name": "find_references", "arguments": {"repo": REPO_NAME, "names": ["LanguageSpec"]}}),
    ("list_file_outline: src/model.rs",
     {"name": "list_file_outline", "arguments": {"repo": REPO_NAME, "file": "src/model.rs"}}),
    ("get_symbol: scan 3 names, no body",
     {"name": "get_symbol", "arguments": {"repo": REPO_NAME, "names": ["serve_mcp", "handle_request", "call_tool"], "include_body": False}}),
]

# id=0 is initialize; QUERIES get ids 1..N. The capped bonus run reuses id=1.
INIT_ID = 0


def die(msg):
    """Print a failure to stderr and exit non-zero so CI/bench scripts can detect it."""
    print(f"payload_size_old_vs_new: {msg}", file=sys.stderr)
    sys.exit(1)


def require_executable(path, label):
    if not path:
        die(f"{label} binary path is empty (set it via the TSINDEX_BENCH_* env var).")
    p = Path(path)
    if not p.is_file():
        die(f"{label} binary not found at {path}")
    if not os.access(path, os.X_OK):
        die(f"{label} binary not executable: {path}")


def require_db(path):
    if not Path(path).is_file():
        die(f"index DB not found at {path} -- build it first with "
            f"`tsindex --root <repo> --db {path} init && tsindex --root <repo> --db {path} build`.")


def run(binary, db, calls, require_ids):
    """Send initialize + one tools/call per query; return {id: result}.

    Fails fast on subprocess error, truncated stdout, malformed JSON lines, or
    any missing/expected id so a broken run cannot produce partial numbers.
    """
    lines = [json.dumps({"jsonrpc": "2.0", "id": INIT_ID, "method": "initialize", "params": {}})]
    for i, (_, params) in enumerate(calls, start=1):
        lines.append(json.dumps({"jsonrpc": "2.0", "id": i, "method": "tools/call", "params": params}))
    proc = subprocess.run(
        [binary, "--root", REPO, "--db", db, "--no-refresh", "serve", "--mcp"],
        input="\n".join(lines) + "\n", capture_output=True, text=True)
    if proc.returncode != 0:
        die(f"{binary} exited {proc.returncode}. stderr:\n{proc.stderr.strip()}")
    if not proc.stdout.strip():
        die(f"{binary} produced no stdout (process may have failed to start). stderr:\n{proc.stderr.strip()}")

    results = {}
    expected = set(require_ids)
    for lineno, line in enumerate(proc.stdout.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            die(f"{binary} emitted non-JSON on stdout line {lineno}: {exc}\nline: {line[:200]}")
        rid = obj.get("id")
        if rid is None:
            continue
        if obj.get("error"):
            die(f"{binary} returned a JSON-RPC error for id={rid}: {obj['error']}")
        results[rid] = obj.get("result")
        expected.discard(rid)

    if expected:
        die(f"{binary} did not answer required ids: {sorted(expected)} "
            f"(got {sorted(results.keys())}). The index may be empty or the binary may have crashed mid-run.")
    return results


def size(result):
    # Bytes the model effectively receives = the full `result` payload.
    return len(json.dumps(result, separators=(",", ":")))


def result_shape(result):
    """Return stable cardinality metadata used to reject incomparable runs."""
    if not isinstance(result, dict):
        return None
    text = result.get("content", [{}])[0].get("text")
    if not isinstance(text, str):
        return None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    for key in ("total", "returned"):
        if key in payload:
            return (key, payload[key])
    for key in ("matches", "symbols", "captures"):
        if isinstance(payload.get(key), list):
            return (key, len(payload[key]))
    return None


def main():
    # Validate all inputs before running anything so the error message names
    # the missing prerequisite, not a downstream KeyError.
    require_executable(OLD, "OLD")
    require_executable(NEW, "NEW")
    require_db(OLD_DB)
    require_db(NEW_DB)

    query_ids = list(range(1, len(QUERIES) + 1))

    old = run(OLD, OLD_DB, QUERIES, query_ids)
    new = run(NEW, NEW_DB, QUERIES, query_ids)

    for i, (label, _) in enumerate(QUERIES, start=1):
        old_shape = result_shape(old[i])
        new_shape = result_shape(new[i])
        if old_shape is not None and new_shape is not None and old_shape != new_shape:
            die(f"incomparable result sets for {label}: old={old_shape}, new={new_shape}")

    print(f"{'Query':<46} {'old B':>9} {'new B':>9} {'saved':>9} {'%':>6}")
    print("-" * 82)
    tot_o = tot_n = 0
    for i, (label, _) in enumerate(QUERIES, start=1):
        o, n = size(old[i]), size(new[i])
        tot_o += o
        tot_n += n
        pct = 100 * (o - n) / o if o else 0
        print(f"{label:<46} {o:>9,} {n:>9,} {o-n:>9,} {pct:>5.1f}%")
    print("-" * 82)
    pct = 100 * (tot_o - tot_n) / tot_o if tot_o else 0
    print(f"{'TOTAL (bytes)':<46} {tot_o:>9,} {tot_n:>9,} {tot_o-tot_n:>9,} {pct:>5.1f}%")
    print(f"{'TOTAL (~tokens, chars/4 heuristic)':<46} {tot_o//4:>9,} {tot_n//4:>9,} {(tot_o-tot_n)//4:>9,} {pct:>5.1f}%")

    # Bonus: new-only opt-in max_body_lines skim on the big symbol. Reuses id=1
    # so only that id is required.
    capped = run(NEW, NEW_DB, [("capped", {"name": "get_symbol", "arguments": {"repo": REPO_NAME, "names": ["replace_symbol"], "include_body": True, "max_body_lines": 5}})], [1])
    full_new = size(new[1])
    cap = size(capped[1])
    print(f"\nOpt-in skim (max_body_lines=5) on replace_symbol:")
    print(f"  full new body: {full_new:,} B   capped: {cap:,} B   saved {full_new-cap:,} B "
          f"({100*(full_new-cap)/full_new:.1f}%)" if full_new else
          f"  full new body: {full_new:,} B   capped: {cap:,} B")


if __name__ == "__main__":
    main()
