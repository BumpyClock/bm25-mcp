#!/usr/bin/env python3
"""Exercise pressure-mode reads and refreshes in an isolated MCP fixture.

The output is JSONL metadata only. Indexed excerpts and source contents never
appear in the output; result paths and numeric scores are retained so the two
owners can be compared.
"""

import argparse
import hashlib
import math
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import time

from acceptance import Client, emit, settle


READ_QUERY = "sharedmarker"
EDIT_QUERY = "freshmarker"
OLD_QUERY = "oldmarker"
TARGET_PATH = "src/editable.rs"
MEMORY_MIB = 1


def run_git(root, env, *args):
    subprocess.run(
        ["git", "-C", str(root), *args],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        env=env,
    )


def make_fixture(root, env):
    (root / "src").mkdir(parents=True)
    for index in range(32):
        path = root / "src" / f"module{index:02d}.rs"
        lines = [
            "fn stable_fixture_function() {",
            "    let sharedmarker = \"fixture\";",
            f"    let module_number = {index};",
            "}",
        ]
        path.write_text("\n".join(lines * 24) + "\n", encoding="utf-8")
    (root / TARGET_PATH).write_text(
        "fn editable_fixture() {\n    sharedmarker oldmarker\n}\n",
        encoding="utf-8",
    )
    run_git(root, env, "init", "-q")
    run_git(root, env, "config", "user.name", "BM25 pressure check")
    run_git(root, env, "config", "user.email", "pressure-check@example.test")
    run_git(root, env, "add", ".")
    run_git(root, env, "commit", "-qm", "pressure fixture")


def isolated_env(base, label, memory_mib=None):
    home = base / f"{label}-home"
    xdg = base / f"{label}-xdg"
    tmp = base / f"{label}-tmp"
    providers = {
        "CODEX_HOME": base / f"{label}-codex",
        "CLAUDE_CONFIG_DIR": base / f"{label}-claude",
        "COPILOT_HOME": base / f"{label}-copilot",
    }
    for path in [home, xdg, tmp, *providers.values()]:
        path.mkdir(parents=True, exist_ok=True)
    (xdg / "git").mkdir(parents=True, exist_ok=True)
    (xdg / "git" / "ignore").write_text("", encoding="utf-8")
    env = os.environ.copy()
    env.update(
        {
            "HOME": str(home),
            "XDG_CONFIG_HOME": str(xdg),
            "TMPDIR": str(tmp),
            "GIT_CONFIG_NOSYSTEM": "1",
            "CODEX_HOME": str(providers["CODEX_HOME"]),
            "CLAUDE_CONFIG_DIR": str(providers["CLAUDE_CONFIG_DIR"]),
            "COPILOT_HOME": str(providers["COPILOT_HOME"]),
        }
    )
    if memory_mib is None:
        env.pop("BM25_MCP_MEMORY_MIB", None)
    else:
        env["BM25_MCP_MEMORY_MIB"] = str(memory_mib)
    return env


def project_arguments(query, limit=50):
    return {
        "query": query,
        "limit": limit,
        "max_response_bytes": 65536,
    }


def signature(result):
    """Keep only the public path/score pair used for cross-owner comparison."""
    return [
        (hit.get("relative_path"), float(hit.get("score", 0.0)))
        for hit in result.get("results", [])
    ]


def assert_same_signature(left, right, label):
    left_signature = signature(left)
    right_signature = signature(right)
    if [path for path, _ in left_signature] != [path for path, _ in right_signature]:
        raise AssertionError(f"{label}: result paths differ")
    if len(left_signature) != len(right_signature):
        raise AssertionError(f"{label}: result counts differ")
    for (_, left_score), (_, right_score) in zip(left_signature, right_signature):
        if not math.isclose(left_score, right_score, rel_tol=1e-6, abs_tol=1e-6):
            raise AssertionError(f"{label}: result scores differ")


def wait_pressure(client, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = client.tool("search_project", project_arguments(READ_QUERY, 10))
        memory = result.get("coverage", {}).get("memory") or {}
        coverage = result.get("coverage", {})
        if (
            memory.get("target_bytes") == MEMORY_MIB * 1024 * 1024
            and memory.get("pressure") is True
            and result.get("status") in ("ready", "degraded")
            and coverage.get("pending_changes") == 0
            and result.get("results")
        ):
            return result
        time.sleep(0.1)
    raise RuntimeError("memory pressure was not observed")


def wait_fresh(client, query, target_path, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = client.tool("search_project", project_arguments(query))
        coverage = result.get("coverage", {})
        paths = [hit.get("relative_path") for hit in result.get("results", [])]
        if (
            result.get("status") in ("ready", "degraded")
            and coverage.get("pending_changes") == 0
            and coverage.get("error_count") == 0
            and target_path in paths
        ):
            return result
        time.sleep(0.1)
    raise RuntimeError(f"fresh result did not converge for {query}")


def wait_for_reads(counts, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if all(count > 0 for count in counts):
            return
        time.sleep(0.02)
    raise RuntimeError("not all pressure clients completed a read")


def reader_loop(client, stop, edit_started, counts, during_edit, errors, index):
    while not stop.is_set():
        try:
            result = client.tool("search_project", project_arguments(READ_QUERY, 5))
            counts[index] += 1
            if edit_started.is_set():
                during_edit[index] += 1
        except Exception as error:  # pragma: no cover - exercised by process failures
            errors.append((index, type(error).__name__))
            return
        stop.wait(0.01)


def metadata_signature(result):
    return [
        {"path": path, "score": round(score, 8)}
        for path, score in signature(result)
    ]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=120.0)
    args = parser.parse_args()
    binary = args.binary.resolve()
    if not binary.is_file():
        raise RuntimeError(f"binary not found: {binary}")

    emit(
        {
            "phase": "environment",
            "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
            "memory_mib": MEMORY_MIB,
            "reader_clients": 4,
            "pressure_connections": 5,
        }
    )

    normal_client = None
    pressure_clients = []
    pressure_poller = None
    readers = []
    stop = threading.Event()
    edit_started = threading.Event()
    reader_counts = [0] * 4
    reads_during_edit = [0] * 4
    reader_errors = []
    passed = False
    with tempfile.TemporaryDirectory(prefix="bm25-pressure-") as scratch:
        base = Path(scratch)
        project = base / "project"
        project.mkdir()
        setup_env = isolated_env(base, "setup")
        make_fixture(project, setup_env)
        normal_env = isolated_env(base, "normal")
        pressure_env = isolated_env(base, "pressure", MEMORY_MIB)
        normal_cache = base / "normal-cache"
        pressure_cache = base / "pressure-cache"
        try:
            normal_client = Client(binary, project, normal_cache, normal_env)
            normal_initial, _ = settle(
                normal_client,
                "search_project",
                READ_QUERY,
                timeout=args.timeout,
            )
            normal_initial = normal_client.tool(
                "search_project", project_arguments(READ_QUERY)
            )

            for _ in range(4):
                pressure_clients.append(
                    Client(binary, project, pressure_cache, pressure_env)
                )
            pressure_initial, _ = settle(
                pressure_clients[0],
                "search_project",
                READ_QUERY,
                timeout=args.timeout,
            )
            pressure_initial = pressure_clients[0].tool(
                "search_project", project_arguments(READ_QUERY)
            )
            assert_same_signature(normal_initial, pressure_initial, "initial")
            emit(
                {
                    "phase": "baseline_compare",
                    "normal": metadata_signature(normal_initial),
                    "pressure_owner": metadata_signature(pressure_initial),
                    "paths_scores_equal": True,
                }
            )

            # Keep the four reader streams single-owner per Client. MCP
            # request/response streams are sequential, so freshness polling
            # uses a fifth connection rather than racing a reader's stream.
            pressure_poller = Client(binary, project, pressure_cache, pressure_env)
            pressure_probe = wait_pressure(
                pressure_poller, timeout=args.timeout
            )
            pressure_memory = pressure_probe["coverage"]["memory"]
            # Store deliberately disables its in-memory posting cache during
            # pressure; a successful ranking here exercises the disk-backed
            # SQLite accumulator through the public MCP protocol.
            if pressure_memory.get("pressure") is not True:
                raise AssertionError("pressure flag is false")
            emit(
                {
                    "phase": "pressure",
                    "pressure": True,
                    "target_bytes": pressure_memory.get("target_bytes"),
                    "observed_rss_bytes": pressure_memory.get(
                        "observed_rss_bytes"
                    ),
                    "disk_backed_search": bool(pressure_probe.get("results")),
                }
            )
            if not pressure_probe.get("results"):
                raise AssertionError("pressure search returned no disk-backed results")

            for index, client in enumerate(pressure_clients):
                thread = threading.Thread(
                    target=reader_loop,
                    args=(
                        client,
                        stop,
                        edit_started,
                        reader_counts,
                        reads_during_edit,
                        reader_errors,
                        index,
                    ),
                    daemon=True,
                )
                thread.start()
                readers.append(thread)
            wait_for_reads(reader_counts, timeout=10.0)

            edit_started.set()
            target = project / TARGET_PATH
            with target.open("w", encoding="utf-8") as file:
                file.write(
                    "fn editable_fixture() {\n"
                    "    sharedmarker freshmarker\n"
                    "}\n"
                )
                file.flush()
                os.fsync(file.fileno())

            pressure_fresh = wait_fresh(
                pressure_poller, EDIT_QUERY, TARGET_PATH, args.timeout
            )
            normal_fresh = wait_fresh(
                normal_client, EDIT_QUERY, TARGET_PATH, args.timeout
            )
            assert_same_signature(normal_fresh, pressure_fresh, "fresh edit")
            old_pressure = pressure_poller.tool(
                "search_project", project_arguments(OLD_QUERY)
            )
            old_normal = normal_client.tool("search_project", project_arguments(OLD_QUERY))
            if any(hit.get("relative_path") == TARGET_PATH for hit in old_pressure.get("results", [])):
                raise AssertionError("pressure owner retained stale edit result")
            if any(hit.get("relative_path") == TARGET_PATH for hit in old_normal.get("results", [])):
                raise AssertionError("normal owner retained stale edit result")

            # Leave the readers active briefly after convergence so every
            # client observes the ordinary edit window, not just the polling
            # client used for the freshness assertion.
            time.sleep(0.25)
            stop.set()
            for thread in readers:
                thread.join(timeout=5)
            if reader_errors:
                raise AssertionError("pressure reader failed")
            if any(thread.is_alive() for thread in readers):
                raise AssertionError("pressure reader did not stop")
            if not all(count > 0 for count in reads_during_edit):
                raise AssertionError("not every pressure client read during edit")
            fresh_memory = pressure_fresh.get("coverage", {}).get("memory") or {}
            if fresh_memory.get("pressure") is not True:
                raise AssertionError("pressure flag was lost during fresh read")
            emit(
                {
                    "phase": "ordinary_edit",
                    "target_path": TARGET_PATH,
                    "fresh_paths_scores": metadata_signature(pressure_fresh),
                    "normal_pressure_paths_scores_equal": True,
                    "read_counts": reader_counts,
                    "reads_during_edit": reads_during_edit,
                    "reader_errors": len(reader_errors),
                    "pressure": True,
                    "eventual_fresh": True,
                }
            )
            passed = True
        finally:
            stop.set()
            for thread in readers:
                thread.join(timeout=2)
            for client in pressure_clients:
                client.close()
            if pressure_poller is not None:
                pressure_poller.close()
            if normal_client is not None:
                normal_client.close()
            for thread in readers:
                thread.join(timeout=5)

    if not passed:
        raise RuntimeError("pressure acceptance failed")
    emit({"phase": "complete", "pass": True})


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        emit({"phase": "failure", "error": type(error).__name__})
        raise SystemExit(1)
