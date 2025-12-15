#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
对 MapReduce 输出的因子均值序列进行“准确性测试”校验（忽略 Pearson IC）。

评分规则：
对每个因子 i，在所有交易日的所有时刻 t 上计算
    e_t = |x_t - x'_t| / |x'_t|
并可选择汇总准则（由 --crit 控制）：
    - mean：e_i^{mean} = (1/|T|) * sum_{t∈T} e_t
    - max ：e_i^{max} = max_t e_t
若汇总误差 <= 0.01（1%）则该因子判定为正确。

本脚本约定：
- 标准答案目录优先使用仓库根目录下的 `std_red/`；若不存在则回退到 `std_ref/`
- 预测输出从本地缓冲目录自动发现：`local_buffer/hdfs_out/`

校验模式：
- 指定 `--day 0108`：只校验单天（0108），并自动定位包含 0108 输出文件的最新目录
- 不指定 `--day`：自动选择包含输出文件的最新目录，尝试校验 std_red 中全部交易日；
  对缺失的 day 输出会打印出来。

误差分布直方图：
- 通过 `--plot` 控制：不画/只画不通过因子/画全部因子
- 若指定 `--day`：输出到 `err_distr_out/plot_<day>/`（目录存在则先删除重建）
- 不指定 `--day`：输出到 `err_distr_out/plot_ALL/`（目录存在则先删除重建）
- 图片名为 `alpha_xx-因子名称.png`
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import re
import shutil
import sys
import warnings
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


FACTOR_COUNT = 20
HEADER = ["tradeTime"] + [f"alpha_{i}" for i in range(1, FACTOR_COUNT + 1)]
THRESHOLD = 0.01

FACTOR_NAMES: Dict[int, str] = {
    1: "最优价差",
    2: "相对价差",
    3: "中间价",
    4: "买一不平衡",
    5: "前5档多档不平衡",
    6: "前5档买方深度",
    7: "前5档卖方深度",
    8: "买卖深度差",
    9: "买卖深度比",
    10: "全市场买卖量平衡指数",
    11: "前5档买方加权价格",
    12: "前5档卖方加权价格",
    13: "综合加权中价",
    14: "买卖加权价差",
    15: "每档平均挂单量差",
    16: "买卖不对称度",
    17: "最优价变动",
    18: "中间价变动",
    19: "深度比变动",
    20: "价压指标",
}


def repo_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _pick_truth_dir() -> str:
    root = repo_root()
    cand = os.path.join(root, "std_red")
    if os.path.isdir(cand):
        return cand
    return os.path.join(root, "std_ref")


TRUTH_DIR = _pick_truth_dir()
BUFFER_ROOT = os.path.join(repo_root(), "local_buffer", "hdfs_out")
ERR_DISTR_OUT_ROOT = os.path.join(repo_root(), "err_distr_out")
FONT_PATH = os.path.join(repo_root(), "factor-mapreduce", "fonts", "NotoSansCJKsc-Regular.otf")

DAY_FILE_RE = re.compile(r"^(?P<day>\d{4})\.csv(?:-r-\d+)?$")


@dataclass(frozen=True)
class DayData:
    rows: Dict[str, List[float]]


def _is_header_row(row: List[str]) -> bool:
    return bool(row) and row[0].strip() == "tradeTime"


def read_day_csv(path: str) -> DayData:
    rows: Dict[str, List[float]] = {}
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        for raw in reader:
            if not raw or all(not c.strip() for c in raw):
                continue
            if _is_header_row(raw):
                continue
            if len(raw) != 1 + FACTOR_COUNT:
                raise ValueError(f"{path}: 列数不正确，期望 {1+FACTOR_COUNT}，实际 {len(raw)}")
            trade_time = raw[0].strip()
            values = [float(x) for x in raw[1:]]
            rows[trade_time] = values
    return DayData(rows=rows)


def read_truth_dir(truth_dir: str) -> Dict[str, DayData]:
    result: Dict[str, DayData] = {}
    for path in sorted(glob.glob(os.path.join(truth_dir, "*.csv"))):
        base = os.path.basename(path)
        m = re.match(r"^(\d{4})\.csv$", base)
        if not m:
            continue
        day = m.group(1)
        result[day] = read_day_csv(path)
    if not result:
        raise ValueError(f"未在标准答案目录找到 *.csv：{truth_dir}")
    return result


def list_days_in_pred_dir(pred_dir: str) -> Dict[str, List[str]]:
    grouped: Dict[str, List[str]] = {}
    for path in sorted(glob.glob(os.path.join(pred_dir, "*"))):
        base = os.path.basename(path)
        m = DAY_FILE_RE.match(base)
        if not m:
            continue
        day = m.group("day")
        grouped.setdefault(day, []).append(path)
    return grouped


def read_pred_dir(pred_dir: str) -> Dict[str, DayData]:
    grouped = list_days_in_pred_dir(pred_dir)
    if not grouped:
        raise ValueError(f"未在预测输出目录找到形如 0102.csv-r-00000 的文件：{pred_dir}")
    result: Dict[str, DayData] = {}
    for day, paths in grouped.items():
        merged: Dict[str, List[float]] = {}
        for p in paths:
            data = read_day_csv(p)
            for t, v in data.rows.items():
                merged.setdefault(t, v)
        merged_sorted = {k: merged[k] for k in sorted(merged.keys())}
        result[day] = DayData(rows=merged_sorted)
    return result


def find_latest_pred_dir_for_day(day: str) -> Optional[str]:
    if not os.path.isdir(BUFFER_ROOT):
        return None
    candidates: List[Tuple[float, str]] = []

    def consider_dir(d: str) -> None:
        grouped = list_days_in_pred_dir(d)
        if day not in grouped:
            return
        newest = 0.0
        for p in grouped[day]:
            try:
                newest = max(newest, os.path.getmtime(p))
            except OSError:
                continue
        if newest > 0:
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


def find_latest_pred_dir_any() -> Optional[str]:
    if not os.path.isdir(BUFFER_ROOT):
        return None
    candidates: List[Tuple[float, str]] = []

    def consider_dir(d: str) -> None:
        grouped = list_days_in_pred_dir(d)
        if not grouped:
            return
        newest = 0.0
        for paths in grouped.values():
            for p in paths:
                try:
                    newest = max(newest, os.path.getmtime(p))
                except OSError:
                    continue
        if newest > 0:
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


def normalize_day(day: str) -> str:
    d = day.strip()
    if not re.match(r"^\d{4}$", d):
        raise ValueError(f"day 格式应为 4 位数字（如 0108），实际：{day!r}")
    return d


@dataclass
class EvalResult:
    factor_mean_errors: List[float]
    factor_max_errors: List[float]
    factor_errors: List[List[float]]
    matched_points: int
    missing_pred_points: int
    missing_truth_points: int
    zero_denom_points: int


def evaluate(truth: Dict[str, DayData], pred: Dict[str, DayData], *, eps: float) -> EvalResult:
    max_errors = [0.0] * FACTOR_COUNT
    sum_errors = [0.0] * FACTOR_COUNT
    per_factor: List[List[float]] = [[] for _ in range(FACTOR_COUNT)]
    count = 0
    missing_pred = 0
    missing_truth = 0
    zero_denom = 0

    for day, truth_day in truth.items():
        pred_day = pred.get(day)
        if pred_day is None:
            missing_pred += len(truth_day.rows)
            continue
        for t, truth_vals in truth_day.rows.items():
            pred_vals = pred_day.rows.get(t)
            if pred_vals is None:
                missing_pred += 1
                continue
            count += 1
            for i in range(FACTOR_COUNT):
                denom = abs(truth_vals[i])
                if denom == 0.0:
                    denom = eps
                    zero_denom += 1
                e_t = abs(pred_vals[i] - truth_vals[i]) / denom
                if e_t > max_errors[i]:
                    max_errors[i] = e_t
                sum_errors[i] += e_t
                per_factor[i].append(e_t)

    for day, pred_day in pred.items():
        truth_day = truth.get(day)
        if truth_day is None:
            missing_truth += len(pred_day.rows)
            continue
        for t in pred_day.rows.keys():
            if t not in truth_day.rows:
                missing_truth += 1

    if count == 0:
        raise ValueError("没有任何可对齐的 (day,tradeTime) 点，请检查输出路径与文件内容。")

    mean_errors = [s / count for s in sum_errors]
    return EvalResult(
        factor_mean_errors=mean_errors,
        factor_max_errors=max_errors,
        factor_errors=per_factor,
        matched_points=count,
        missing_pred_points=missing_pred,
        missing_truth_points=missing_truth,
        zero_denom_points=zero_denom,
    )


def evaluate_day(truth_day: DayData, pred_day: DayData, *, eps: float) -> EvalResult:
    max_errors = [0.0] * FACTOR_COUNT
    sum_errors = [0.0] * FACTOR_COUNT
    per_factor: List[List[float]] = [[] for _ in range(FACTOR_COUNT)]
    count = 0
    missing_pred = 0
    missing_truth = 0
    zero_denom = 0

    for t, truth_vals in truth_day.rows.items():
        pred_vals = pred_day.rows.get(t)
        if pred_vals is None:
            missing_pred += 1
            continue
        count += 1
        for i in range(FACTOR_COUNT):
            denom = abs(truth_vals[i])
            if denom == 0.0:
                denom = eps
                zero_denom += 1
            e_t = abs(pred_vals[i] - truth_vals[i]) / denom
            if e_t > max_errors[i]:
                max_errors[i] = e_t
            sum_errors[i] += e_t
            per_factor[i].append(e_t)

    for t in pred_day.rows.keys():
        if t not in truth_day.rows:
            missing_truth += 1

    if count == 0:
        raise ValueError("没有任何可对齐的 tradeTime 点，请检查输出路径与文件内容。")

    mean_errors = [s / count for s in sum_errors]
    return EvalResult(
        factor_mean_errors=mean_errors,
        factor_max_errors=max_errors,
        factor_errors=per_factor,
        matched_points=count,
        missing_pred_points=missing_pred,
        missing_truth_points=missing_truth,
        zero_denom_points=zero_denom,
    )

def _metric_values(result: EvalResult, crit: str) -> List[float]:
    c = (crit or "mean").lower()
    if c == "mean":
        return result.factor_mean_errors
    if c == "max":
        return result.factor_max_errors
    raise ValueError(f"未知 --crit：{crit!r}（应为 mean/max）")


def _metric_label(crit: str) -> str:
    c = (crit or "mean").lower()
    if c == "mean":
        return "mean_error"
    if c == "max":
        return "max_error"
    return "error"


def maybe_plot_histograms(
    *,
    plot_dir: str,
    threshold: float,
    metric_values: List[float],
    per_factor_errors: List[List[float]],
    mode: str,
    crit: str,
) -> None:
    mode = (mode or "none").lower()
    if mode == "none":
        return

    try:
        import tempfile

        os.environ.setdefault("MPLCONFIGDIR", tempfile.mkdtemp(prefix="mplcfg_"))
        import matplotlib

        matplotlib.use("Agg")
        try:
            from matplotlib import font_manager as fm

            if os.path.isfile(FONT_PATH):
                fm.fontManager.addfont(FONT_PATH)
                font_name = fm.FontProperties(fname=FONT_PATH).get_name()
                matplotlib.rcParams["font.family"] = "sans-serif"
                matplotlib.rcParams["font.sans-serif"] = [font_name, "DejaVu Sans", "Liberation Sans"]
                matplotlib.rcParams["axes.unicode_minus"] = False
        except Exception:
            pass
        warnings.filterwarnings("ignore", message=r".*Glyph.*missing from font.*", category=UserWarning)
        import matplotlib.pyplot as plt
    except Exception as e:
        print(f"警告：无法绘制直方图（未安装/不可用 matplotlib）：{e}")
        return

    if os.path.isdir(plot_dir):
        shutil.rmtree(plot_dir)
    os.makedirs(plot_dir, exist_ok=True)

    only_reject = mode == "reject_only"

    def sci_latex(value: float) -> str:
        if value == 0.0:
            return r"$0$"
        import math

        exp = int(math.floor(math.log10(abs(value))))
        mant = value / (10**exp)
        return rf"${mant:.2f}\times10^{{{exp}}}$"

    for i in range(FACTOR_COUNT):
        factor_id = i + 1
        name = FACTOR_NAMES.get(factor_id, "")
        metric = metric_values[i]
        ok = metric <= threshold
        if only_reject and ok:
            continue

        errors = per_factor_errors[i]
        color = "C0" if ok else "C3"
        status = "PASS" if ok else "FAIL"
        title_base = f"alpha_{factor_id:02d}-{name}"
        legend_label = f"{title_base}-{status}  {_metric_label(crit)}={sci_latex(metric)}"

        out_path = os.path.join(plot_dir, f"{title_base}.png")

        max_abs = max((abs(x) for x in errors), default=0.0)
        is_all_zero = max_abs == 0.0

        if is_all_zero:
            from matplotlib.patches import FancyBboxPatch

            fig, ax = plt.subplots(figsize=(7.2, 3.6))
            ax.set_axis_off()
            rect = FancyBboxPatch(
                (0.1, 0.25),
                0.8,
                0.5,
                boxstyle="round,pad=0.02,rounding_size=0.04",
                transform=ax.transAxes,
                facecolor="C0",
                alpha=0.15,
                edgecolor="C0",
                linewidth=3.0,
            )
            ax.add_patch(rect)
            ax.text(
                0.5,
                0.83,
                f"[{title_base}]",
                transform=ax.transAxes,
                ha="center",
                va="center",
                color="black",
                fontsize=13,
                fontweight="bold",
            )
            ax.text(
                0.5,
                0.5,
                "PERFECT HIT",
                transform=ax.transAxes,
                ha="center",
                va="center",
                color="C0",
                fontsize=20,
                fontweight="bold",
            )
            fig.tight_layout()
            fig.savefig(out_path, dpi=160)
            plt.close(fig)
            continue

        fig, ax = plt.subplots(figsize=(8, 4.5))
        ax.hist(
            errors,
            bins=60,
            color=color,
            alpha=0.85,
            edgecolor="white",
            linewidth=0.5,
            label=legend_label,
        )
        ax.axvline(threshold, color="black", linestyle="--", linewidth=1.0, label=f"拒绝阈值={threshold:g}")

        min_v = min(errors)
        max_v = max(errors)
        if min_v == max_v:
            span = max(abs(min_v) * 0.1, threshold * 0.1, 1e-12)
            x_left = min_v - span
            x_right = max_v + span
        else:
            data_range = max_v - min_v
            pad = data_range * 0.125
            x_left = min_v - pad
            x_right = max_v + pad
        ax.set_xlim(x_left, x_right)

        ax.set_title(title_base)
        ax.set_xlabel(r"$e_t = |x_t - x'_t| \,/\, |x'_t|$")
        ax.set_ylabel("count")
        ax.legend(loc="upper right")
        ax.grid(True, axis="y", alpha=0.25)

        fig.tight_layout()
        fig.savefig(out_path, dpi=160)
        plt.close(fig)


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", default=None, help="校验单天（如 0108）；留空则校验标准答案目录下全部天")
    parser.add_argument(
        "--crit",
        default="mean",
        choices=["mean", "max"],
        help="误差汇总准则：mean=对所有时刻取平均；max=对所有时刻取最大值（默认 mean）",
    )
    parser.add_argument("--eps", type=float, default=1e-12, help="标准值为 0 时的分母替代值")
    # 新参数名：--plot（保留 --err-distr 作为兼容别名）
    parser.add_argument(
        "--plot",
        default="none",
        choices=["none", "reject_only", "both"],
        help="绘制误差分布直方图：none=不画；reject_only=只画不通过；both=全部因子",
    )
    parser.add_argument(
        "--err-distr",
        dest="plot",
        choices=["none", "reject_only", "both"],
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args(argv)

    # 本地缓冲目录：用于承接从 HDFS 拉回的输出（launch.py 会写入该目录）。
    # validate 也会从这里自动发现最新输出；若目录不存在则主动创建，避免后续扫描时报错。
    os.makedirs(BUFFER_ROOT, exist_ok=True)
    # 误差分布图输出根目录：若用户开启 --plot，需要保证根目录存在。
    os.makedirs(ERR_DISTR_OUT_ROOT, exist_ok=True)

    if not os.path.isdir(TRUTH_DIR):
        raise SystemExit(f"未找到标准答案目录：{TRUTH_DIR}")
    truth_all = read_truth_dir(TRUTH_DIR)

    day: Optional[str] = None
    if args.day is not None:
        day = normalize_day(args.day)

    if day is not None:
        pred_dir = find_latest_pred_dir_for_day(day)
        if pred_dir is None:
            raise SystemExit(f"未找到 day={day} 的预测输出（在 {BUFFER_ROOT} 下未发现 {day}.csv-r-*）")
        if day not in truth_all:
            raise SystemExit(f"标准答案中不存在 day={day}（请检查 {TRUTH_DIR}）")
        truth = {day: truth_all[day]}
        pred_all = read_pred_dir(pred_dir)
        if day not in pred_all:
            raise SystemExit(f"预测输出目录中未找到 day={day}（请检查 {pred_dir}）")
        pred = {day: pred_all[day]}
        missing_days: List[str] = []
        extra_days: List[str] = []
    else:
        pred_dir = find_latest_pred_dir_any()
        if pred_dir is None:
            raise SystemExit(f"未找到任何预测输出（在 {BUFFER_ROOT} 下未发现 ????\\.csv-r-*）")
        truth = truth_all
        pred = read_pred_dir(pred_dir)
        missing_days = sorted(set(truth.keys()) - set(pred.keys()))
        extra_days = sorted(set(pred.keys()) - set(truth.keys()))
        
    print("\n======================= VALIDATION ========================")

    print(f"标准答案目录：{TRUTH_DIR}")
    print(f"预测输出目录：{pred_dir}")
    print(f"误差准则：{args.crit}")
    if missing_days:
        print(f"缺失 day 输出：{', '.join(missing_days)}")
    if day is None and extra_days:
        print(f"预测多余 day（标准答案中不存在）：{', '.join(extra_days)}")

    def bold(text: str) -> str:
        return f"\033[1m{text}\033[0m"

    def fmt_mean_err(value: float) -> str:
        return "0" if value == 0.0 else f"{value:.2e}"

    def cell_tag(ok: bool) -> str:
        return "[P]" if ok else f"[{bold('F')}]"

    # 逐天评估：为表格/绘图提供输入。
    day_results: Dict[str, EvalResult] = {}
    for d in sorted(truth.keys()):
        pred_day = pred.get(d)
        if pred_day is None:
            continue
        day_results[d] = evaluate_day(truth[d], pred_day, eps=args.eps)

    # 输出：DataFrame（index=因子，columns=day）
    try:
        import pandas as pd
    except Exception as e:
        raise SystemExit(f"需要 pandas 才能输出 DataFrame：{e}")

    factor_index = [f"alpha_{i:02d}" for i in range(1, FACTOR_COUNT + 1)]
    columns: Dict[str, List[str]] = {}
    for d in sorted(day_results.keys()):
        r = day_results[d]
        metric_values = _metric_values(r, args.crit)
        day_has_fail = (
            any(err > THRESHOLD for err in metric_values)
            or r.missing_pred_points > 0
            or r.missing_truth_points > 0
        )
        col_label = f"{cell_tag(not day_has_fail)} {d}"
        col_values: List[str] = []
        for metric in metric_values:
            ok = metric <= THRESHOLD
            col_values.append(f"{cell_tag(ok)} {fmt_mean_err(metric)}")
        columns[col_label] = col_values

    df = pd.DataFrame(columns, index=factor_index)
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_colwidth", None)
    print("\n--------------------- FACTOR TABLE ---------------------")

    ansi_re = re.compile(r"\x1b\[[0-9;]*m")

    def visible_len(s: str) -> int:
        return len(ansi_re.sub("", s))

    def ljust_visible(s: str, width: int) -> str:
        pad = max(0, width - visible_len(s))
        return s + (" " * pad)

    index_width = max((visible_len(str(x)) for x in df.index), default=0)
    col_widths: Dict[str, int] = {}
    for col in df.columns:
        values = [str(v) for v in df[col].tolist()]
        col_widths[str(col)] = max([visible_len(str(col))] + [visible_len(v) for v in values])

    index_header = "fctr/day"
    header_cells = [ljust_visible(index_header, index_width)]
    for col in df.columns:
        header_cells.append(ljust_visible(str(col), col_widths[str(col)]))
    print("  ".join(header_cells).rstrip())

    for idx in df.index:
        row_cells = [ljust_visible(str(idx), index_width)]
        for col in df.columns:
            cell = str(df.at[idx, col])
            row_cells.append(ljust_visible(cell, col_widths[str(col)]))
        print("  ".join(row_cells).rstrip())

    all_days_ok = True
    for d in truth.keys():
        r = day_results.get(d)
        if r is None:
            all_days_ok = False
            continue
        if any(err > THRESHOLD for err in _metric_values(r, args.crit)):
            all_days_ok = False
        if r.missing_pred_points != 0 or r.missing_truth_points != 0:
            all_days_ok = False
    if missing_days or extra_days:
        all_days_ok = False

    if all_days_ok:
        banner = "=" * 36
        print(f"\n{banner}\n{bold('㊗️   CONGRATULATIONS! ALL PASS!  ㊗️')}\n{banner}")
    else:
        banner = "=" * 36
        print(f"\n{banner}\n{bold('    Ohhh IT FUCKED UP!!!    ')}\n{banner}")

    if args.plot != "none":
        print("\n--------------------- PLOTS ---------------------")
        print("开始打印误差分布直方图...")
        plot_root = os.path.join(ERR_DISTR_OUT_ROOT, f"plot_{day}" if day is not None else "plot_ALL")
        if os.path.isdir(plot_root):
            shutil.rmtree(plot_root)
        os.makedirs(plot_root, exist_ok=True)

        if day is not None:
            day_result = day_results[day]
            maybe_plot_histograms(
                plot_dir=plot_root,
                threshold=THRESHOLD,
                metric_values=_metric_values(day_result, args.crit),
                per_factor_errors=day_result.factor_errors,
                mode=args.plot,
                crit=args.crit,
            )
        else:
            for d, day_result in day_results.items():
                maybe_plot_histograms(
                    plot_dir=os.path.join(plot_root, d),
                    threshold=THRESHOLD,
                    metric_values=_metric_values(day_result, args.crit),
                    per_factor_errors=day_result.factor_errors,
                    mode=args.plot,
                    crit=args.crit,
                )
        print(f"直方图已输出到：{plot_root}")
    all_ok = all_days_ok
    return 0 if all_ok else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
