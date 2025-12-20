#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import subprocess
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
LOCAL_BUFFER_ROOT = BASE_DIR / "local_buffer"
LOCAL_BUFFER_DIRS = ("hdfs_in", "hdfs_out", "monitor_logs", "logs")


def local_list_files() -> list[tuple[Path, int]]:
    files: list[tuple[Path, int]] = []
    if not LOCAL_BUFFER_ROOT.is_dir():
        return files
    for name in LOCAL_BUFFER_DIRS:
        d = LOCAL_BUFFER_ROOT / name
        if not d.is_dir():
            continue
        for path in d.rglob("*"):
            if path.is_file():
                files.append((path, path.stat().st_size))
    return files


def local_delete_files() -> None:
    for name in LOCAL_BUFFER_DIRS:
        d = LOCAL_BUFFER_ROOT / name
        if not d.is_dir():
            continue
        for path in d.rglob("*"):
            if path.is_file():
                path.unlink()
        for path in sorted(d.rglob("*"), reverse=True):
            if path.is_dir():
                path.rmdir()


def main() -> int:
    print("cleanbuf: start")
    print(f"Local root: {LOCAL_BUFFER_ROOT}")

    local_files = local_list_files()
    if local_files:
        total_files = len(local_files)
        total_bytes = sum(size for _path, size in local_files)
        print(f"Local files: count={total_files} total_bytes={total_bytes}")
        for path, size in local_files:
            print(f"  - {path} size={size}")
    else:
        print("Local files: count=0")

    local_delete_files()

    print("cleanbuf: done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
