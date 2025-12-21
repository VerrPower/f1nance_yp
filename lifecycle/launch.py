#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
启动 Hadoop MapReduce 作业，并在完成后将 HDFS 输出拷贝回本地缓冲目录。

默认参数（路径全部写死）：
- JAR: POGI-ONE-RELEASE/target/POGI-ONE-RELEASE-0.1.0-SNAPSHOT.jar
- MAIN_CLASS: 固定为 pogi_one.Driver（脚本内写死）
- HDFS_INPUT: 数据根目录（day 根目录）：/user/pogi/HD_INPUT_REPO/FINANCE_for_YP
- HDFS_OUTPUT: /user/pogi/HD_OUTPUT_REPO/FINANCE_for_YP/run_<timestamp>
- LOCAL_OUT_DIR: local_buffer/hdfs_out
"""

from __future__ import annotations

import argparse
import subprocess
import time
from pathlib import Path
 

SEP = "=" * 72

JAR_PATH = "POGI-ONE-RELEASE/target/POGI-ONE-RELEASE-0.1.0-SNAPSHOT.jar"
MAIN_CLASS = "pogi_one.Driver"

HDFS_INPUT_ROOT = "/user/pogi/HD_INPUT_REPO/FINANCE_for_YP"
HDFS_OUTPUT_ROOT = "/user/pogi/HD_OUTPUT_REPO/FINANCE_for_YP"

LOCAL_OUT_DIR = "local_buffer/hdfs_out"


def main() -> int:
    argparse.ArgumentParser(description="运行 Hadoop 作业并拷回本地（路径全部写死）。").parse_args()

    root = Path(__file__).resolve().parent.parent
    jar_path = root / JAR_PATH
    hdfs_input = HDFS_INPUT_ROOT  # 只传根目录：Driver 固定按 <root>/*/*/snapshot.csv 读取
    hdfs_output = f"{HDFS_OUTPUT_ROOT}/run_{time.strftime('%Y%m%d_%H%M%S')}"
    local_out_dir = root / LOCAL_OUT_DIR

    print(SEP)
    print("作业配置如下：")
    print(f"Jar       : {jar_path}")
    print(f"HDFS input: {hdfs_input}")
    print(f"HDFS out  : {hdfs_output}")
    print(f"Local out : {local_out_dir}")
    print(SEP)
    print("\n\n")

    # 清理旧输出（不存在也不报错）
    #subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", hdfs_output], check=False)

    print("\n\n作业启动中...")
    _t0 = time.perf_counter_ns()
    subprocess.run(["hadoop", "jar", str(jar_path), MAIN_CLASS, hdfs_input, hdfs_output], check=True)
    _t1 = time.perf_counter_ns()
    print("\n\n######## REENTER PYTHON DOMAIN ########")
    elapsed_s = (_t1 - _t0) / 1_000_000_000
    print(f"@ launch.py: hadoop-jar elapsed_s={elapsed_s:.6f}")
    

    print("\n\n作业已完成，作业配置回显：")
    print(f"Jar       : {jar_path}")
    print(f"HDFS input: {hdfs_input}")
    print(f"HDFS out  : {hdfs_output}")
    print(f"Local out : {local_out_dir}")
    print(SEP)
    print("\n\n作业完成，开始拷贝 HDFS 输出到本地...")

    local_out_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(["hdfs", "dfs", "-get", hdfs_output, str(local_out_dir)], check=True)
    dest_dir = local_out_dir / Path(hdfs_output).name

    print("完成。")
    print("HDFS 输出：", hdfs_output)
    print("本地输出：", dest_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
