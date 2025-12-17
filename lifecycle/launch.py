#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
启动 Hadoop MapReduce 作业，并在完成后将 HDFS 输出拷贝回本地缓冲目录。

默认参数（路径全部写死）：
- JAR: factor-mapreduce/target/factor-mapreduce-0.1.0-SNAPSHOT.jar
- MAIN_CLASS: factor.Driver
- HDFS_INPUT: 固定读取全部 day：/user/pogi/HD_INPUT_REPO/FINANCE_for_YP/*/*/snapshot.csv
- HDFS_OUTPUT: /user/pogi/HD_OUTPUT_REPO/FINANCE_for_YP/run_<timestamp>
- LOCAL_OUT_DIR: local_buffer/hdfs_out
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, TextIO


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def sep() -> str:
    return "=" * 72


DEFAULT_JAR_PATH = "factor-mapreduce/target/factor-mapreduce-0.1.0-SNAPSHOT.jar"
DEFAULT_MAIN_CLASS = "factor.Driver"
DEFAULT_HDFS_INPUT_ROOT = "/user/pogi/HD_INPUT_REPO/FINANCE_for_YP"
DEFAULT_HDFS_OUTPUT_ROOT = "/user/pogi/HD_OUTPUT_REPO/FINANCE_for_YP"
DEFAULT_LOCAL_OUT_DIR = "local_buffer/hdfs_out"


def run(
    cmd: list[str],
    *,
    check: bool = True,
    stdout: Optional[TextIO] = None,
    stderr: Optional[TextIO] = None,
    env: Optional[dict[str, str]] = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=check, text=True, stdout=stdout, stderr=stderr, env=env)


def capture(
    cmd: list[str],
    *,
    check: bool = True,
    stdout: int = subprocess.PIPE,
    stderr: int = subprocess.STDOUT,
    env: Optional[dict[str, str]] = None,
) -> str:
    cp = subprocess.run(cmd, check=check, text=True, stdout=stdout, stderr=stderr, env=env)
    return cp.stdout


@dataclass(frozen=True)
class JobConfig:
    jar_path: Path
    main_class: str
    hdfs_input: str
    hdfs_output: str
    local_out_dir: Path


def build_hdfs_input() -> str:
    # 固定跑全部 day：/ROOT/*/*/snapshot.csv
    return f"{DEFAULT_HDFS_INPUT_ROOT}/*/*/snapshot.csv"


def load_config() -> JobConfig:
    root = repo_root()
    jar_path = root / DEFAULT_JAR_PATH
    main_class = DEFAULT_MAIN_CLASS
    hdfs_input = build_hdfs_input()
    hdfs_output = f"{DEFAULT_HDFS_OUTPUT_ROOT}/run_{time.strftime('%Y%m%d_%H%M%S')}"
    local_out_dir = root / DEFAULT_LOCAL_OUT_DIR
    return JobConfig(
        jar_path=jar_path,
        main_class=main_class,
        hdfs_input=hdfs_input,
        hdfs_output=hdfs_output,
        local_out_dir=local_out_dir,
    )


def print_config(cfg: JobConfig) -> None:
    print(f"Jar       : {cfg.jar_path}")
    print(f"Main      : {cfg.main_class}")
    print(f"HDFS input: {cfg.hdfs_input}")
    print(f"HDFS out  : {cfg.hdfs_output}")
    print(f"Local out : {cfg.local_out_dir}")


def hdfs_ls(pattern: str, *, cfg: JobConfig) -> list[str]:
    out = capture(["hdfs", "dfs", "-ls", pattern], check=False, stderr=subprocess.STDOUT)
    paths: list[str] = []
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 8 and not line.startswith("Found "):
            paths.append(parts[-1])
    return paths


def hdfs_exists(path: str, *, cfg: JobConfig) -> bool:
    return subprocess.run(["hdfs", "dfs", "-test", "-e", path]).returncode == 0


def hdfs_rm(path: str, *, cfg: JobConfig) -> None:
    run(["hdfs", "dfs", "-rm", "-r", "-f", path], check=False)


def hdfs_get(src_dir: str, local_parent_dir: Path, *, cfg: JobConfig) -> Path:
    local_parent_dir.mkdir(parents=True, exist_ok=True)
    run(["hdfs", "dfs", "-get", src_dir, str(local_parent_dir)])
    return local_parent_dir / Path(src_dir).name


def preflight(cfg: JobConfig) -> None:
    print("Preflight: 检查 HDFS 输入文件...")
    files = hdfs_ls(cfg.hdfs_input, cfg=cfg)
    copying = [p for p in files if "_COPYING_" in p]
    if copying:
        print("检测到 _COPYING_（未完成上传）文件，建议先清理/重传后再跑：")
        for p in copying[:20]:
            print(p)
        raise SystemExit(2)
    if not files:
        print("未匹配到任何 snapshot.csv，请检查 HDFS_INPUT 是否指向文件/通配符。")
        raise SystemExit(2)
    print(f"Preflight: 匹配到 snapshot.csv 文件数：{len(files)}")


def run_job(cfg: JobConfig) -> None:
    if not cfg.jar_path.is_file():
        print(sep())
        print("作业配置如下：")
        print_config(cfg)
        print(sep())
        print("\n\n未找到 jar：", cfg.jar_path)
        print("请先构建：mvn -f factor-mapreduce/pom.xml clean package")
        raise SystemExit(1)

    print(sep())
    print("作业配置如下：")
    print_config(cfg)
    print(sep())
    print("\n\n")
    preflight(cfg)

    if hdfs_exists(cfg.hdfs_output, cfg=cfg):
        print("Output exists, removing...")
        hdfs_rm(cfg.hdfs_output, cfg=cfg)

    print(sep())
    print("\n\n作业启动中...\n######## LEAVE PYTHON DOMAIN ########")
    # 使用 hadoop jar 直接运行（不依赖 bash alias）
    jar_cmd = ["hadoop", "jar", str(cfg.jar_path), cfg.main_class, cfg.hdfs_input, cfg.hdfs_output]
    subprocess.run(jar_cmd, check=True)

    print("######## REENTER PYTHON DOMAIN ########")
    print(sep())
    print("\n\n作业已完成，作业配置回显：")
    print_config(cfg)
    print(sep())
    print("\n\n作业完成，开始拷贝 HDFS 输出到本地...")

    cfg.local_out_dir.mkdir(parents=True, exist_ok=True)
    dest_dir = hdfs_get(cfg.hdfs_output, cfg.local_out_dir, cfg=cfg)

    print("完成。")
    print("HDFS 输出：", cfg.hdfs_output)
    print("本地输出：", dest_dir)


def main() -> int:
    argparse.ArgumentParser(description="运行 Hadoop 作业并拷回本地（路径全部写死）。").parse_args()
    cfg = load_config()

    run_job(cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
