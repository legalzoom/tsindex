#!/usr/bin/env python3
"""Compare ingestion, storage and cached reads for the file-directory trial.

Reuse the real Git/MCP fixture from worktree_overlays.py. Each round rotates
variant order; cached reads use a fresh client with a ten-minute refresh TTL
after ingestion has completed. Timings include MCP dispatch and SQLite setup.
All generated fixtures and child processes are cleaned up on exit.
"""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

# Importing the shared harness must not leave artifacts in the source tree.
sys.dont_write_bytecode = True
from worktree_overlays import (
    McpClient,
    build_stats,
    generations,
    measure,
    positive,
)


def lookup(client: McpClient, tool: str, checkout: Path, index: int) -> float:
    arguments: dict[str, Any] = {"repo": str(checkout)}
    if tool == "get_symbol":
        arguments.update(name=f"identify{index:05d}", include_body=False)
        rows_key = "matches"
    else:
        arguments.update(
            names=[f"helper{index:05d}"], snippet_lines=1, group_by_file=False
        )
        rows_key = "refs"
    start = time.perf_counter()
    response = client.request("tools/call", {"name": tool, "arguments": arguments})
    elapsed = time.perf_counter() - start
    result = response["result"]
    if result.get("isError"):
        raise RuntimeError(f"{tool} failed: {result}")
    payload = json.loads(result["content"][0]["text"])
    expected_file = f"src/module{index:05d}.py"
    if payload.get("total") != 1 or payload[rows_key][0]["file"] != expected_file:
        raise RuntimeError(f"{tool} returned the wrong file: {payload}")
    return elapsed


def cached_reads(
    binary: Path, root: Path, files: int, worktrees: int, samples: int, timeout: float
) -> dict[str, Any]:
    main = root / "main"
    linked = [root / f"linked-{index}" for index in range(worktrees)]
    config = main / ".tsindex" / "config.toml"
    config.write_text(
        config.read_text(encoding="utf-8").replace(
            "worktree_refresh_ms = 0", "worktree_refresh_ms = 600000"
        ),
        encoding="utf-8",
    )
    results: dict[str, Any] = {}
    with McpClient(binary, main, timeout) as client:
        for index, checkout in enumerate(linked):
            client.symbol(checkout, 0, f"checkout-{index}-first")
        before = generations(linked)
        priming_log = client.stderr_path.read_text(encoding="utf-8")
        for tool in ("get_symbol", "find_references"):
            for index in range(20):
                lookup(client, tool, linked[index % worktrees], index % files)
            seconds = [
                lookup(client, tool, linked[index % worktrees], index % files)
                for index in range(samples)
            ]
            results[tool] = {
                "median_ms": statistics.median(seconds) * 1000,
                "p95_ms": sorted(seconds)[int((samples - 1) * 0.95)] * 1000,
                "samples_seconds": seconds,
            }
        after = generations(linked)
        if after != before:
            raise RuntimeError("cached reads unexpectedly triggered ingestion")
        measured_log = client.stderr_path.read_text(encoding="utf-8")[
            len(priming_log) :
        ]
        if build_stats(measured_log):
            raise RuntimeError("cached reads unexpectedly ran a no-op reconciliation")
        results["generation_before"] = before
        results["generation_after"] = after
        results["measured_build_stats"] = build_stats(measured_log)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--overlay-bin", type=Path, required=True)
    parser.add_argument("--candidate-bin", type=Path, required=True)
    parser.add_argument("--materialized-bin", type=Path)
    parser.add_argument("--files", type=positive, default=2000)
    parser.add_argument("--worktrees", type=positive, default=8)
    parser.add_argument("--rounds", type=positive, default=3)
    parser.add_argument("--samples", type=positive, default=200)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument(
        "--artifact-root", type=Path, default=Path("target/file-directory-benchmark")
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("-n", "--dry-run", action="store_true")
    args = parser.parse_args()
    if args.files < 2 or args.timeout <= 0:
        parser.error("--files must be at least 2 and --timeout must be positive")
    binaries = {"overlay": args.overlay_bin, "directory": args.candidate_bin}
    if args.materialized_bin:
        binaries["materialized"] = args.materialized_bin
    configuration = {
        "files_per_checkout": args.files,
        "linked_worktrees": args.worktrees,
        "rounds": args.rounds,
        "samples_per_tool_per_round": args.samples,
        "cached_refresh_interval_ms": 600000,
        "ingestion_refresh_interval_ms": 0,
        "variants": {label: str(path) for label, path in binaries.items()},
        "build_profiles": "caller must supply matching profiles",
    }
    if args.dry_run:
        print(json.dumps(configuration, indent=2))
        return
    binaries = {label: path.resolve(strict=True) for label, path in binaries.items()}
    artifact_root = args.artifact_root.resolve()
    artifact_root.mkdir(parents=True, exist_ok=True)
    results: dict[str, Any] = {
        "configuration": configuration,
        "platform": platform.platform(),
        "python": platform.python_version(),
        "rounds": [],
    }
    output = args.output or artifact_root / "results.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    labels = list(binaries)
    for round_index in range(args.rounds):
        order = (
            labels[round_index % len(labels) :] + labels[: round_index % len(labels)]
        )
        round_result: dict[str, Any] = {"order": order, "variants": {}}
        with tempfile.TemporaryDirectory(prefix="run-", dir=artifact_root) as directory:
            for label in order:
                print(
                    f"Round {round_index + 1}/{args.rounds}: {label}",
                    file=sys.stderr,
                    flush=True,
                )
                root = Path(directory) / label
                result = measure(
                    binaries[label], root, args.files, args.worktrees, args.timeout
                )
                result["cached_reads"] = cached_reads(
                    binaries[label],
                    root,
                    args.files,
                    args.worktrees,
                    args.samples,
                    args.timeout,
                )
                round_result["variants"][label] = result
        results["rounds"].append(round_result)
        output.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    summary = {}
    for label in labels:
        runs = [item["variants"][label] for item in results["rounds"]]
        metrics = {
            "get_symbol_ms": [
                r["cached_reads"]["get_symbol"]["median_ms"] for r in runs
            ],
            "find_references_ms": [
                r["cached_reads"]["find_references"]["median_ms"] for r in runs
            ],
            "sqlite_bytes": [r["storage_after_edit"]["total_bytes"] for r in runs],
        }
        for metric in ("first_use", "unchanged_reconciliation", "one_file_ingestion"):
            metrics[metric + "_ms"] = [r[metric]["median_seconds"] * 1000 for r in runs]
        summary[label] = {
            metric: {"median": statistics.median(values), "rounds": values}
            for metric, values in metrics.items()
        }
    results["summary"] = summary
    output.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
