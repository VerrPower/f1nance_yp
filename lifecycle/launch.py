#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
启动 Hadoop MapReduce 作业，并在完成后将 HDFS 输出拷贝回本地缓冲目录。

默认参数（可通过环境变量覆盖）：
- JAR_PATH: factor-mapreduce/target/factor-mapreduce-0.1.0-SNAPSHOT.jar
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
    timing: bool
    log_mode: str


def build_hdfs_input() -> str:
    # 固定跑全部 day：/ROOT/*/*/snapshot.csv
    return f"{DEFAULT_HDFS_INPUT_ROOT}/*/*/snapshot.csv"


def load_config() -> JobConfig:
    root = repo_root()
    jar_path = Path(os.environ.get("JAR_PATH", str(root / DEFAULT_JAR_PATH)))
    main_class = os.environ.get("MAIN_CLASS", DEFAULT_MAIN_CLASS)
    hdfs_input = os.environ.get("HDFS_INPUT", build_hdfs_input())
    hdfs_output = os.environ.get(
        "HDFS_OUTPUT",
        f"{DEFAULT_HDFS_OUTPUT_ROOT}/run_{time.strftime('%Y%m%d_%H%M%S')}",
    )
    local_out_dir = Path(os.environ.get("LOCAL_OUT_DIR", str(root / DEFAULT_LOCAL_OUT_DIR)))
    return JobConfig(
        jar_path=jar_path,
        main_class=main_class,
        hdfs_input=hdfs_input,
        hdfs_output=hdfs_output,
        local_out_dir=local_out_dir,
        timing=True,
        log_mode="mute",
    )


def print_config(cfg: JobConfig) -> None:
    print(f"Jar       : {cfg.jar_path}")
    print(f"Main      : {cfg.main_class}")
    print(f"HDFS input: {cfg.hdfs_input}")
    print(f"HDFS out  : {cfg.hdfs_output}")
    print(f"Local out : {cfg.local_out_dir}")
    print(f"Timing    : {cfg.timing}")
    print(f"Log mode  : {cfg.log_mode}")


def _hadoop_env_for_mute() -> dict[str, str]:
    env = dict(os.environ)
    # Best-effort: different clusters may use log4j/log4j2, but this commonly reduces console noise.
    env.setdefault("HADOOP_ROOT_LOGGER", "ERROR,console")
    env.setdefault("HDFS_ROOT_LOGGER", "ERROR,console")
    env.setdefault("YARN_ROOT_LOGGER", "ERROR,console")
    return env


def hdfs_ls(pattern: str, *, cfg: JobConfig) -> list[str]:
    env = _hadoop_env_for_mute() if cfg.log_mode == "mute" else None
    stderr = subprocess.DEVNULL if cfg.log_mode == "mute" else subprocess.STDOUT
    out = capture(["hdfs", "dfs", "-ls", pattern], check=False, stderr=stderr, env=env)
    paths: list[str] = []
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 8 and not line.startswith("Found "):
            paths.append(parts[-1])
    return paths


def hdfs_exists(path: str, *, cfg: JobConfig) -> bool:
    env = _hadoop_env_for_mute() if cfg.log_mode == "mute" else None
    stdout = subprocess.DEVNULL if cfg.log_mode == "mute" else None
    stderr = subprocess.DEVNULL if cfg.log_mode == "mute" else None
    return subprocess.run(["hdfs", "dfs", "-test", "-e", path], stdout=stdout, stderr=stderr, env=env).returncode == 0


def hdfs_rm(path: str, *, cfg: JobConfig) -> None:
    env = _hadoop_env_for_mute() if cfg.log_mode == "mute" else None
    stdout = subprocess.DEVNULL if cfg.log_mode == "mute" else None
    stderr = subprocess.DEVNULL if cfg.log_mode == "mute" else None
    run(["hdfs", "dfs", "-rm", "-r", "-f", path], check=False, stdout=stdout, stderr=stderr, env=env)


def hdfs_get(src_dir: str, local_parent_dir: Path, *, cfg: JobConfig) -> Path:
    local_parent_dir.mkdir(parents=True, exist_ok=True)
    env = _hadoop_env_for_mute() if cfg.log_mode == "mute" else None
    stdout = subprocess.DEVNULL if cfg.log_mode == "mute" else None
    stderr = subprocess.DEVNULL if cfg.log_mode == "mute" else None
    run(["hdfs", "dfs", "-get", src_dir, str(local_parent_dir)], stdout=stdout, stderr=stderr, env=env)
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
    print("\n\n作业启动中...\n")
    # 使用 hadoop jar 直接运行（不依赖 bash alias）
    timing_flag = f"--timing={'True' if cfg.timing else 'False'}"
    jar_cmd = ["hadoop", "jar", str(cfg.jar_path), cfg.main_class, timing_flag, cfg.hdfs_input, cfg.hdfs_output]
    if cfg.log_mode == "normal":
        subprocess.run(jar_cmd, check=True)
    elif cfg.log_mode == "mute":
        env = _hadoop_env_for_mute()
        # 只禁掉 Hadoop/HDFS 的 console 日志（通常走 stderr），保留 stdout 以显示自定义输出（如 timing）。
        subprocess.run(jar_cmd, check=True, text=True, stdout=None, stderr=subprocess.DEVNULL, env=env)
    else:
        raise SystemExit(f"未知 log_mode：{cfg.log_mode}")

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
    parser = argparse.ArgumentParser(description="运行 Hadoop 作业并拷回本地（参数默认从环境变量读取）。")
    parser.add_argument(
        "--log",
        choices=["normal", "mute"],
        default="mute",
        help="控制输出：normal=不干预；mute=调高日志级别并尽量禁输出",
    )
    parser.add_argument(
        "--timing",
        choices=["True", "False", "true", "false"],
        default="True",
        help="是否让 jar 输出计时信息（默认 True，会传 --timing=True 给 jar）",
    )
    args = parser.parse_args()

    cfg = load_config()
    cfg = JobConfig(**{**cfg.__dict__, "timing": str(args.timing).lower() == "true"})
    cfg = JobConfig(**{**cfg.__dict__, "log_mode": str(args.log)})

    run_job(cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
