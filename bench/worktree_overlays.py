#!/usr/bin/env python3
"""Compare copied indexes and overlays using fresh, real Git worktrees.

Build each revision's binary first, then run from the repository root:

    python3 bench/worktree_overlays.py \
      --baseline-bin /path/to/pr13/target/debug/tsindex \
      --candidate-bin target/debug/tsindex --files 2000 --worktrees 8

The synthetic Python source contains four symbols per file. Every linked
checkout changes one file before first use, then one more before the edit
measurement. The first-use timer includes the request that establishes each
checkout's index and, for the candidate, the first immutable base publication.
Storage includes the primary index, shared bases, private indexes, and SQLite
WAL/SHM sidecars. Source files, Git objects, and usage-tally logs are excluded.

Both variants run sequentially with automatic primary maintenance disabled;
linked checkouts still use their normal on-demand ingestion. This isolates
storage and reconciliation costs from watcher allocation. Results describe
this synthetic fixture and build profile, not a general speed guarantee.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import queue
import re
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Self


def run(
    arguments: list[str], cwd: Path, timeout: float
) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    if arguments[0] == "git":
        # Keep fixture commits/worktree creation independent of developer
        # hooks, signing settings, or inherited repository override variables.
        environment = {
            key: value
            for key, value in environment.items()
            if not key.startswith("GIT_")
        }
        environment["GIT_CONFIG_NOSYSTEM"] = "1"
        environment["GIT_CONFIG_GLOBAL"] = os.devnull
    result = subprocess.run(
        arguments,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
        env=environment,
    )
    if result.returncode:
        raise RuntimeError(
            f"command {arguments[0]!r} failed ({result.returncode}): "
            f"{result.stderr.strip()}"
        )
    return result


def build_stats(log: str) -> list[dict[str, int]]:
    return [
        dict(
            zip(
                ("indexed", "touched", "skipped", "failed"),
                map(int, counts),
                strict=True,
            )
        )
        for counts in re.findall(
            r"build complete: (\d+) indexed, (\d+) touched, (\d+) skipped, (\d+) failed",
            log,
        )
    ]


def generations(checkouts: list[Path]) -> list[int]:
    observed = []
    for checkout in checkouts:
        path = checkout / ".tsindex" / "index.db"
        connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
        try:
            observed.append(
                connection.execute(
                    "SELECT generation FROM index_state WHERE id = 1"
                ).fetchone()[0]
            )
        finally:
            connection.close()
    return observed


def source(index: int, marker: str) -> str:
    return (
        f"class Item{index:05d}:\n"
        "    def render(self, value):\n"
        f"        return helper{index:05d}(value)\n\n"
        f"def helper{index:05d}(value):\n"
        f"    return value + {index}\n\n"
        f"def identify{index:05d}():\n"
        f"    return {marker!r}\n"
    )


def write_source(checkout: Path, index: int, marker: str) -> None:
    path = checkout / "src" / f"module{index:05d}.py"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source(index, marker), encoding="utf-8")


def make_fixture(
    root: Path, files: int, worktrees: int, timeout: float
) -> tuple[Path, list[Path]]:
    main = root / "main"
    main.mkdir(parents=True)
    (main / ".gitignore").write_text(".tsindex/\n", encoding="utf-8")
    for index in range(files):
        write_source(main, index, "primary")
    run(["git", "init", "--quiet"], main, timeout)
    run(["git", "add", "."], main, timeout)
    run(
        [
            "git",
            "-c",
            "user.name=tsindex benchmark",
            "-c",
            "user.email=benchmark@example.invalid",
            "commit",
            "--quiet",
            "--no-gpg-sign",
            "-m",
            "fixture",
        ],
        main,
        timeout,
    )
    linked = []
    for index in range(worktrees):
        checkout = root / f"linked-{index}"
        run(
            ["git", "worktree", "add", "--quiet", "--detach", str(checkout), "HEAD"],
            main,
            timeout,
        )
        write_source(checkout, 0, f"checkout-{index}-first")
        linked.append(checkout)
    config = main / ".tsindex" / "config.toml"
    config.parent.mkdir()
    config.write_text(
        '[languages]\ninclude = ["python"]\n\n[server]\nworktree_refresh_ms = 0\n',
        encoding="utf-8",
    )
    return main, linked


class McpClient:
    def __init__(self, binary: Path, root: Path, timeout: float):
        self.timeout = timeout
        self.next_id = 0
        self.lines: queue.Queue[str | None] = queue.Queue()
        self.stderr_path = root.parent / "mcp.stderr.log"
        self.stderr = self.stderr_path.open("w", encoding="utf-8")
        environment = os.environ.copy()
        environment["TSINDEX_HOME"] = str(root.parent / "usage-tally")
        try:
            self.child = subprocess.Popen(
                [str(binary), "--root", str(root), "--no-refresh", "serve", "--mcp"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=self.stderr,
                text=True,
                encoding="utf-8",
                env=environment,
            )
        except BaseException:
            self.stderr.close()
            raise
        self.reader = threading.Thread(target=self.read_lines, daemon=True)
        self.reader.start()

    def read_lines(self) -> None:
        assert self.child.stdout is not None
        try:
            for line in self.child.stdout:
                self.lines.put(line)
        finally:
            self.lines.put(None)

    def __enter__(self) -> Self:
        try:
            self.request(
                "initialize",
                {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "overlay-benchmark", "version": "1"},
                },
            )
            self.send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        except BaseException:
            self.close()
            raise
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        if self.child.stdin is not None:
            try:
                self.child.stdin.close()
            except BrokenPipeError:
                pass
        try:
            self.child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.child.terminate()
            try:
                self.child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.child.kill()
                self.child.wait()
        self.reader.join(timeout=5)
        if self.child.stdout is not None:
            self.child.stdout.close()
        self.stderr.close()

    def send(self, message: dict[str, Any]) -> None:
        assert self.child.stdin is not None
        self.child.stdin.write(json.dumps(message) + "\n")
        self.child.stdin.flush()

    def request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        self.next_id += 1
        self.send(
            {"jsonrpc": "2.0", "id": self.next_id, "method": method, "params": params}
        )
        try:
            line = self.lines.get(timeout=self.timeout)
        except queue.Empty as error:
            raise RuntimeError(
                f"MCP response timed out: {self.stderr_path.read_text()}"
            ) from error
        if line is None:
            raise RuntimeError(f"MCP exited: {self.stderr_path.read_text()}")
        response = json.loads(line)
        if response.get("id") != self.next_id or "error" in response:
            raise RuntimeError(f"unexpected MCP response: {response}")
        return response

    def symbol(self, checkout: Path, index: int, marker: str) -> float:
        start = time.perf_counter()
        response = self.request(
            "tools/call",
            {
                "name": "get_symbol",
                "arguments": {
                    "repo": str(checkout),
                    "name": f"identify{index:05d}",
                    "include_body": True,
                },
            },
        )
        elapsed = time.perf_counter() - start
        result = response["result"]
        if result.get("isError"):
            raise RuntimeError(f"tool failed: {result}")
        payload = json.loads(result["content"][0]["text"])
        if payload.get("total") != 1:
            raise RuntimeError(f"expected one symbol: {payload}")
        body = payload["matches"][0].get("body", "")
        if marker not in body:
            raise RuntimeError(f"wrong checkout body; expected {marker!r}: {payload}")
        return elapsed


def storage(main: Path, linked: list[Path]) -> dict[str, Any]:
    groups: dict[str, int] = {"primary": 0, "shared_base": 0, "worktrees": 0}
    database_bytes = 0
    sidecar_bytes = 0
    entries = []
    for checkout in [main, *linked]:
        for path in sorted((checkout / ".tsindex").rglob("*")):
            if not path.is_file():
                continue
            is_database = path.name.endswith(".db")
            is_sidecar = path.name.endswith((".db-wal", ".db-shm"))
            if not is_database and not is_sidecar:
                continue
            size = path.stat().st_size
            group = (
                "shared_base"
                if path.parent.name == "bases"
                else "primary"
                if checkout == main
                else "worktrees"
            )
            groups[group] += size
            database_bytes += size if is_database else 0
            sidecar_bytes += size if is_sidecar else 0
            entries.append({"path": str(path.relative_to(main.parent)), "bytes": size})
    return {
        "total_bytes": sum(groups.values()),
        "database_bytes": database_bytes,
        "sidecar_bytes": sidecar_bytes,
        **{f"{name}_bytes": size for name, size in groups.items()},
        "files": entries,
    }


def timing(seconds: list[float]) -> dict[str, Any]:
    return {
        "seconds": seconds,
        "total_seconds": sum(seconds),
        "median_seconds": statistics.median(seconds),
    }


def measure(
    binary: Path, root: Path, files: int, worktrees: int, timeout: float
) -> dict[str, Any]:
    main, linked = make_fixture(root, files, worktrees, timeout)
    start = time.perf_counter()
    primary_result = run([str(binary), "--root", str(main), "build"], main, timeout)
    primary_build = time.perf_counter() - start
    with McpClient(binary, main, timeout) as client:
        first_use = [
            client.symbol(checkout, 0, f"checkout-{index}-first")
            for index, checkout in enumerate(linked)
        ]
        first_storage = storage(main, linked)
        first_generations = generations(linked)
        first_log = client.stderr_path.read_text(encoding="utf-8")
        # This also reconciles: interval zero makes every request scan, rather
        # than measuring a cached coordinator lookup as ingestion throughput.
        unchanged = [
            client.symbol(checkout, 0, f"checkout-{index}-first")
            for index, checkout in enumerate(linked)
        ]
        unchanged_generations = generations(linked)
        unchanged_log = client.stderr_path.read_text(encoding="utf-8")
        for index, checkout in enumerate(linked):
            write_source(checkout, 1, f"checkout-{index}-edit")
        changed = [
            client.symbol(checkout, 1, f"checkout-{index}-edit")
            for index, checkout in enumerate(linked)
        ]
        changed_log = client.stderr_path.read_text(encoding="utf-8")
        client.symbol(main, 0, "primary")
        client.symbol(main, 1, "primary")
        final_storage = storage(main, linked)
    return {
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "primary_build_seconds": primary_build,
        "primary_build_stats": build_stats(primary_result.stderr),
        "first_use": timing(first_use),
        "unchanged_reconciliation": timing(unchanged),
        "one_file_ingestion": timing(changed),
        "first_use_build_stats": build_stats(first_log),
        "unchanged_build_stats": build_stats(unchanged_log[len(first_log) :]),
        "one_file_build_stats": build_stats(changed_log[len(unchanged_log) :]),
        "generation_after_first_use": first_generations,
        "generation_after_unchanged_reconciliation": unchanged_generations,
        "storage_after_first_use": first_storage,
        "storage_after_edit": final_storage,
        "verified": "every checkout first-use body, later edit body, and unchanged primary",
    }


def positive(value: str) -> int:
    result = int(value)
    if result < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-bin", type=Path, required=True)
    parser.add_argument("--candidate-bin", type=Path, required=True)
    parser.add_argument("--files", type=positive, default=2000)
    parser.add_argument("--worktrees", type=positive, default=8)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument(
        "--artifact-root", type=Path, default=Path("target/overlay-benchmark")
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("-n", "--dry-run", action="store_true")
    args = parser.parse_args()
    if args.files < 2:
        parser.error("--files must be at least 2 to measure a later file edit")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    configuration = {
        "files_per_checkout": args.files,
        "linked_worktrees": args.worktrees,
        "symbols_per_file": 4,
        "refresh_interval_ms": 0,
        "automatic_primary_maintenance": False,
    }
    if args.dry_run:
        print(json.dumps(configuration, indent=2))
        return
    binaries = {
        "baseline": args.baseline_bin.resolve(strict=True),
        "candidate": args.candidate_bin.resolve(strict=True),
    }
    artifact_root = args.artifact_root.resolve()
    artifact_root.mkdir(parents=True, exist_ok=True)
    results: dict[str, Any] = {
        "configuration": configuration,
        "platform": platform.platform(),
        "python": platform.python_version(),
        "measurement": "single sequential synthetic run; matching build profiles required",
    }
    # The common Git directories live inside the temporary fixture, so cleanup
    # cannot leave worktree registrations in the developer's source repository.
    with tempfile.TemporaryDirectory(prefix="run-", dir=artifact_root) as directory:
        fixture_root = Path(directory)
        for label, binary in binaries.items():
            print(f"Measuring {label}...", file=sys.stderr, flush=True)
            results[label] = measure(
                binary, fixture_root / label, args.files, args.worktrees, args.timeout
            )
    baseline_bytes = results["baseline"]["storage_after_first_use"]["total_bytes"]
    candidate_bytes = results["candidate"]["storage_after_first_use"]["total_bytes"]
    results["comparison"] = {
        "storage_ratio_candidate_to_baseline": candidate_bytes / baseline_bytes,
        "storage_reduction_fraction": 1 - candidate_bytes / baseline_bytes,
        "first_use_ratio_candidate_to_baseline": (
            results["candidate"]["first_use"]["total_seconds"]
            / results["baseline"]["first_use"]["total_seconds"]
        ),
    }
    output = args.output or artifact_root / "results.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
