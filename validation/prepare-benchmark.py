#!/usr/bin/env python3
"""Prepare temporary repository text corpora without retaining paths or text in reports."""

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import tempfile
import time


MAX_FILE_BYTES = 2 * 1024 * 1024
MAX_CHUNK_BYTES = 64 * 1024
CHUNK_LINES = 60
CONTROL_BYTES = re.compile(rb"[\x00-\x08\x0b\x0e-\x1f\x7f]")


def git(repo, *args, input_data=None, allowed=(0,)):
    result = subprocess.run(
        ["git", "-C", str(repo), *args], input=input_data, capture_output=True
    )
    if result.returncode not in allowed:
        raise RuntimeError(f"git {args[0]} failed for {repo.name}: {result.returncode}")
    return result.stdout


def prepare(repo):
    started = time.perf_counter()
    candidates = sorted(set(filter(None, git(
        repo, "ls-files", "--cached", "--others", "--exclude-standard", "-z"
    ).split(b"\0"))))
    ignored = set(filter(None, git(
        repo, "check-ignore", "--no-index", "-z", "--stdin",
        input_data=b"\0".join(candidates) + (b"\0" if candidates else b""),
        allowed=(0, 1),
    ).split(b"\0")))
    enumeration_seconds = time.perf_counter() - started
    excluded = Counter({name: 0 for name in (
        "ignored", "git_metadata", "symlink_or_nonregular", "oversize_file",
        "read_error", "changed_during_read", "binary_control", "invalid_utf8",
    )})
    counts = Counter()
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", prefix=f"bm25-{repo.name}-",
        suffix=".jsonl", dir="/tmp", delete=False,
    ) as corpus:
        for file_id, raw_path in enumerate(candidates):
            relative = Path(os.fsdecode(raw_path))
            if ".git" in relative.parts:
                excluded["git_metadata"] += 1
                continue
            if raw_path in ignored:
                excluded["ignored"] += 1
                continue
            path = repo / relative
            try:
                before = path.lstat()
                if not stat.S_ISREG(before.st_mode) or any(
                    parent.is_symlink() for parent in path.parents if parent != repo.parent
                ):
                    excluded["symlink_or_nonregular"] += 1
                    continue
                if before.st_size > MAX_FILE_BYTES:
                    excluded["oversize_file"] += 1
                    continue
                with path.open("rb") as source:
                    data = source.read(MAX_FILE_BYTES + 1)
                after = path.lstat()
            except OSError:
                excluded["read_error"] += 1
                continue
            if (before.st_size, before.st_mtime_ns, before.st_ino) != (
                after.st_size, after.st_mtime_ns, after.st_ino
            ):
                excluded["changed_during_read"] += 1
                continue
            if len(data) > MAX_FILE_BYTES:
                excluded["oversize_file"] += 1
                continue
            if CONTROL_BYTES.search(data):
                excluded["binary_control"] += 1
                continue
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError:
                excluded["invalid_utf8"] += 1
                continue
            counts["included_files"] += 1
            counts["included_file_bytes"] += len(data)
            lines = text.splitlines(keepends=True)
            for start in range(0, len(lines), CHUNK_LINES):
                chunk = "".join(lines[start:start + CHUNK_LINES])
                if not chunk.strip():
                    counts["whitespace_chunks_skipped"] += 1
                    continue
                size = len(chunk.encode("utf-8"))
                if size > MAX_CHUNK_BYTES:
                    counts["oversize_chunks_skipped"] += 1
                    counts["oversize_chunk_bytes_skipped"] += size
                    continue
                corpus.write(json.dumps({"file_id": file_id, "text": chunk}) + "\n")
                counts["chunks"] += 1
                counts["chunk_text_bytes"] += size
        corpus_path = corpus.name
    assert counts["included_files"] + sum(excluded.values()) == len(candidates)
    return {
        "repo": repo.name,
        "commit": git(repo, "rev-parse", "HEAD").decode().strip(),
        "temporary_corpus": corpus_path,
        "candidate_files": len(candidates),
        "excluded_files": dict(excluded),
        **{key: counts[key] for key in (
            "included_files", "included_file_bytes", "chunks", "chunk_text_bytes",
            "whitespace_chunks_skipped", "oversize_chunks_skipped",
            "oversize_chunk_bytes_skipped",
        )},
        "corpus_jsonl_bytes": Path(corpus_path).stat().st_size,
        "enumeration_seconds": round(enumeration_seconds, 6),
        "preparation_seconds": round(time.perf_counter() - started, 6),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repos", nargs="*", type=Path)
    parser.add_argument("--manifest", type=Path, default=Path(__file__).with_name("corpus-manifest.json"))
    args = parser.parse_args()
    projects = Path.home() / "Projects"
    repos = args.repos or [projects / name for name in (
        "BM25-Turbo-Rust-Python-WASM-CLI", "neutron", "t3code"
    )]
    manifest = {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "limits": {"max_file_bytes": MAX_FILE_BYTES, "max_chunk_bytes": MAX_CHUNK_BYTES,
                   "chunk_lines": CHUNK_LINES},
        "selection": "All git-listed tracked and nonignored untracked files; tracked ignore matches also excluded; regular nonsymlink UTF-8 text only.",
        "binary_detection": "Reject any NUL, DEL, or C0 control byte other than tab, LF, form feed, and CR.",
        "notes": "Ignored untracked files never enter candidate counts. File and chunk size caps are benchmark bounds, not proposed product limits. Source trees are read-only; corpora contain private text and remain temporary.",
        "corpora": [prepare(repo.expanduser().absolute()) for repo in repos],
    }
    args.manifest.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
