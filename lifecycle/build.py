#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
构建 factor-mapreduce 的 Hadoop Jar（mvn clean package）。

特性：
- 直接删除旧 jar，再生成新 jar（jar 名字与 lifecycle/launch.py 同步）。
- 默认会运行测试；可用 --skip-tests 跳过测试加速。
"""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


DEFAULT_JAR_REL = Path("factor-mapreduce/target/factor-mapreduce-0.1.0-SNAPSHOT.jar")


def main() -> int:
    parser = argparse.ArgumentParser(description="mvn clean package 并生成 jar（删除旧 jar）。")
    parser.add_argument("--skip-tests", action="store_true", help="跳过测试（-DskipTests）")
    args = parser.parse_args()

    root = repo_root()
    pom = root / "factor-mapreduce" / "pom.xml"
    jar_path = root / DEFAULT_JAR_REL

    if jar_path.exists():
        jar_path.unlink()

    cmd = ["mvn", "-f", str(pom), "clean", "package"]
    if args.skip_tests:
        cmd.insert(1, "-DskipTests")

    subprocess.run(cmd, check=True)

    if not jar_path.is_file():
        raise SystemExit(f"Build finished but jar not found: {jar_path}")

    print(f"Jar ready: {jar_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

