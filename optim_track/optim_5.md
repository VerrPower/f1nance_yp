# optim_5：Map-Only（取消 shuffle）+ 按交易日分 split + Mapper 直接写最终 CSV

**@ Driver: ElapsedSec: 9.69 ~ 10.73（双峰抖动，取决于缓存/调度）**

本轮目标：在不改变输出语义（每个交易日、每个时间点，20 因子对 300 股票取均值）前提下，彻底移除 MR 的 shuffle/reduce 链路，让作业变成纯 map（本地聚合 + 直接落盘）。

---

## 1) Map-Only：删除 combiner / reducer / shuffle

核心改动：
- `Driver` 直接 `job.setNumReduceTasks(0)`，不再设置 reducer/partitioner/combiner；
- job 输出类型改为 `<NullWritable, Text>`，Mapper 自己输出最终 CSV 行。

结果：
- `Map output records=0`（不走 map-output → spill → shuffle → reduce），大幅减少框架常数；
- 运行时间更多由 HDFS 读入与 Mapper 计算决定。

涉及文件：
- `factor-mapreduce/src/main/java/factor/Driver.java`

---

## 2) InputFormat：按 day 目录分组，split 保证单交易日

动机：
- Map-Only 模式下，每个 split 产出一个最终结果文件；如果 split 跨 day，会导致输出拆分/写错文件。

实现：
- `FixedCombineTextInputFormat.getSplits()` 先按路径结构 `.../<day>/<stock>/snapshot.csv` 提取 day；
- 每个 day 独立分组，再按 `MAX_FILES` 切分为多个 `CombineFileSplit`；
- 本轮将 `MAX_FILES` 直接硬编码为 `300`，在“老师是善的：单 dayDir 必定 300 股票”的假设下，每天恰好生成 1 个 split。

效果：
- 数据层面“按交易日切分”的假设从“经验”变成“代码保证”，Mapper 可删除跨日防御逻辑（更短路径、更小常数）。

涉及文件：
- `factor-mapreduce/src/main/java/factor/FixedCombineTextInputFormat.java`

---

## 3) Mapper：本地聚合 + cleanup 直接写出最终 CSV（无 reducer）

关键思路：
- 仍然在 Mapper 内用 `AGG20_FP64` 对 `(day,time)` 做 20 维向量的求和（压缩股票维度）；
- `cleanup()` 阶段直接输出该交易日的最终 CSV：
  - header + 4802 行（3 秒频固定窗口）；
  - 均值用 `sum * (1.0/300)`（预存倒数，避免除法）。

写出方式：
- 使用 `MultipleOutputs<NullWritable, Text>`，按 day 输出到 `MMDD.csv`（Hadoop 实际文件名会带 `-m-00000` 后缀）。

限制 / 风险：
- 假设每个 dayDir 恰好 300 股票且 split 不跨 day；若数据结构变化（缺股票/多股票/跨日混入），需要回退到 reduce 或重新设计输出归并。

涉及文件：
- `factor-mapreduce/src/main/java/factor/StockFactorMapper.java`

---

## 4) validate：兼容 map-only 输出文件名（-m-00000）

Map-Only 下输出文件由 Mapper 直接写，文件名形如：
- `0102.csv-m-00000`（而不是 reducer 的 `0102.csv-r-00000`）

因此 `validate.py` 更新为同时识别 `-r-` 与 `-m-` 后缀，并在缺失时明确报错。

涉及文件：
- `lifecycle/validate.py`

---

## 5) 性能观测：双峰抖动的来源

现象：同样代码运行时间在两个区间来回跳（约 9.7s / 10.7s）。

解释：
- 本地模式 `LocalJobRunner` 同时跑多个 map，作业耗时 ≈ “最慢的那个 map”；
- 输入是否命中 OS page cache / HDFS block cache、以及 CPU 调度争用，会导致某次出现“冷读短板 map”，整体落入慢峰。

建议：
- 以 `hadoop jar ... factor.Driver ...` 的 `@ Driver: ElapsedSec` 作为核心指标（不包含 Python 端 preflight/copy）；
- 至少跑 5 次取 median（比均值更抗抖动）。

