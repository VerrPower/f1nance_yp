#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
以老师提供的 pandas/numpy 版本为核心逻辑：
- 对每个因子 alpha_i，在每个交易日上计算：
    err_day = mean_t |std(t) - pred(t)| / |std(t) + eps|
  再对所有交易日取平均：
    err = average(err_day)
- err < 0.01 判定通过
"""

from __future__ import annotations

import argparse
import glob
import os
import re
import sys
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd


FACTOR_COUNT = 20
THRESHOLD = 0.01
FACTOR_COLS = [f"alpha_{i}" for i in range(1, FACTOR_COUNT + 1)]

BASE_DIR = "/home/pogi/FINANCE_for_YP"
TRUTH_DIR = os.path.join(BASE_DIR, "std_ref")
BUFFER_ROOT = os.path.join(BASE_DIR, "local_buffer", "hdfs_out")

DAY_FILE_RE = re.compile(r"^(?P<day>\d{4})\.csv(?:-[rm]-\d+)?$")


def bold(text: str) -> str:
    return f"\033[1m{text}\033[0m"


def fmt_metric(value: float) -> str:
    return "0" if value == 0.0 else f"{value:.2e}"


def list_days_in_dir(path: str) -> List[str]:
    days: List[str] = []
    for p in glob.glob(os.path.join(path, "*.csv*")):
        base = os.path.basename(p)
        m = DAY_FILE_RE.match(base)
        if not m:
            continue
        day = m.group("day")
        if day not in days:
            days.append(day)
    days.sort()
    return days


def find_latest_pred_dir_any() -> Optional[str]:
    if not os.path.isdir(BUFFER_ROOT):
        return None

    candidates: List[Tuple[float, str]] = []

    def consider_dir(d: str) -> None:
        newest = 0.0
        any_day = False
        for p in glob.glob(os.path.join(d, "*")):
            base = os.path.basename(p)
            if not DAY_FILE_RE.match(base):
                continue
            any_day = True
            try:
                newest = max(newest, os.path.getmtime(p))
            except OSError:
                continue
        if any_day and newest > 0.0:
            candidates.append((newest, d))

    consider_dir(BUFFER_ROOT)
    for name in os.listdir(BUFFER_ROOT):
        p = os.path.join(BUFFER_ROOT, name)
        if os.path.isdir(p):
            consider_dir(p)

    if not candidates:
        return None
    candidates.sort()
    return candidates[-1][1]


def _read_one_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, index_col=0)
    df.index = df.index.astype("str").str.zfill(6)
    df = df[(df.index >= "093000") & (df.index <= "145700")]
    return df


def get_data(path: str, day: str) -> pd.DataFrame:
    paths = []
    direct = os.path.join(path, f"{day}.csv")
    if os.path.isfile(direct):
        paths = [direct]
    else:
        paths = sorted(glob.glob(os.path.join(path, f"{day}.csv-r-*"))) + sorted(
            glob.glob(os.path.join(path, f"{day}.csv-m-*"))
        )
    if not paths:
        raise FileNotFoundError(f"缺失 day 输出：{path}/{day}.csv(-r-*|-m-*)")

    dfs = [_read_one_csv(p) for p in paths]
    df = pd.concat(dfs, axis=0)
    df = df[~df.index.duplicated(keep="first")]
    df = df.sort_index()
    return df


def score(*, days: List[str], std_path: str, eval_path: str, eps: float) -> Tuple[bool, List[float]]:
    all_ok = True
    err_days: List[np.ndarray] = []

    for day in days:
        standard_df = get_data(std_path, day)
        test_df = get_data(eval_path, day)

        missing_cols = [c for c in FACTOR_COLS if c not in standard_df.columns or c not in test_df.columns]
        if missing_cols:
            raise ValueError(f"{day}: 缺失列：{', '.join(missing_cols)}")

        standard_df = standard_df[FACTOR_COLS]
        test_df = test_df[FACTOR_COLS]

        # 老师新版逻辑：用 outer 对齐；若缺点/重复点导致 NaN，则误差会变成 NaN，最终 FAIL。
        standard_df, test_df = standard_df.align(test_df, join="outer", axis=0)
        std_mat = standard_df.to_numpy(dtype=np.float64, copy=False)
        pred_mat = test_df.to_numpy(dtype=np.float64, copy=False)
        denom = np.abs(std_mat + eps)
        # 老师版本：按时间点取平均，再对天取平均（而不是 sum）。
        err_day = np.mean(np.abs(std_mat - pred_mat) / denom, axis=0)
        err_days.append(err_day)

    if not err_days:
        raise ValueError("没有任何可评估的 day")

    avg_errs = np.mean(np.stack(err_days, axis=0), axis=0)
    for i, avg_err in enumerate(avg_errs.tolist(), start=1):
        ok = avg_err < THRESHOLD
        all_ok = all_ok and ok
        print(f"alpha_{i}: {'PASS' if ok else 'FAIL'} err={fmt_metric(avg_err)}")

    return all_ok, avg_errs.tolist()


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--eps", type=float, default=1e-7)
    args = parser.parse_args(argv)

    os.makedirs(BUFFER_ROOT, exist_ok=True)
    if not os.path.isdir(TRUTH_DIR):
        raise SystemExit(f"未找到标准答案目录：{TRUTH_DIR}")

    pred_dir = find_latest_pred_dir_any()
    if not pred_dir:
        raise SystemExit(f"未找到预测输出目录：{BUFFER_ROOT}")

    truth_days = list_days_in_dir(TRUTH_DIR)
    pred_days = list_days_in_dir(pred_dir)
    missing_days = sorted(set(truth_days) - set(pred_days))
    extra_days = sorted(set(pred_days) - set(truth_days))

    print("\n======================= VALIDATION ========================")
    print(f"标准答案目录：{TRUTH_DIR}")
    print(f"预测输出目录：{pred_dir}")
    print(f"预测输出目录名：{os.path.basename(pred_dir)}")
    print("模式：form")

    days = truth_days
    all_ok, _ = score(days=days, std_path=TRUTH_DIR, eval_path=pred_dir, eps=float(args.eps))
    if missing_days:
        print(f"缺失 day 输出：{', '.join(missing_days)}")
        all_ok = False
    if extra_days:
        print(f"预测多余 day：{', '.join(extra_days)}")
        all_ok = False

    banner = "=" * 36
    if all_ok:
        print(f"\n{banner}\n{bold('㊗️   CONGRATULATIONS! ALL PASS!  ㊗️')}\n{banner}")
        return 0
    print(f"\n{banner}\n{bold('    Ohhh IT FUCKED UP!!!    ')}\n{banner}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
