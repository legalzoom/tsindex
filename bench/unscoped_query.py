#!/usr/bin/env python3
"""Measure the agent-safe-filter spec's unscoped iq-flow query.

Build with `cargo build --release --locked`, then run:
    python3 bench/unscoped_query.py --root /path/to/iq-flow

Uses the existing index without refreshing or writing it. Measures full CLI
wall time (including startup), requires nonempty pages without timeouts, and fails
if any run exceeds the spec's 2000 ms target. --db can select an isolated index.
"""
import argparse
import json
from pathlib import Path
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--binary", type=Path, default=Path(__file__).resolve().parents[1] / "target/release/tsindex")
    parser.add_argument("--db", type=Path)
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be positive")
    command = [str(args.binary.resolve()), "--root", str(args.root.resolve()), "--json"]
    if args.db:
        command += ["--db", str(args.db.resolve())]
    command += ["query", '(call_expression function: (identifier) @fn (#eq? @fn "useAutoRouteSubmit"))', "--language", "tsx"]
    passed = True
    for run in range(args.runs):
        start = time.perf_counter()
        result = subprocess.run(command, capture_output=True, text=True, timeout=30)
        elapsed_ms = round((time.perf_counter() - start) * 1000, 1)
        if result.returncode:
            raise SystemExit(result.stderr)
        response = json.loads(result.stdout)
        complete = bool(response["captures"]) and not response.get("timed_out", False)
        passed &= complete and elapsed_ms < 2000
        print(json.dumps({"run": run + 1, "elapsed_ms": elapsed_ms,
                          "captures": len(response["captures"]), "no_timeout": complete,
                          "truncated": response["truncated"], "partial": response.get("partial", False),
                          "files_scanned": response["files_scanned"],
                          "files_parsed": response["files_parsed"]}))
    raise SystemExit(0 if passed else 1)


if __name__ == "__main__":
    main()
