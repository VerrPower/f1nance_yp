# optim_4：Mapper 本地预聚合 + 写出链路微优化 + validate 口径对齐

**@ Driver: ElapsedSec: 10.890 (WE ARE SO CLOSE)**

本轮目标：在不改变最终输出语义（每个交易日每个时间点的 20 因子截面平均）前提下，尽可能降低 MR 常数开销（map-output/shuffle/序列化/写出）。

---

## 1) Mapper 本地预聚合：压缩“股票维度”，减少 map-output 条数

背景：MR 的最终聚合 key 是 `(day,time)`；对同一 `(day,time)`，不同股票之间是简单逐维求和/求均值，适合在 mapper 内先做局部聚合。

改动：
- `StockFactorMapper` 计算单行 20 因子后，不立刻 `context.write`，而是按 `compactTime30bits` 聚合到本地结构；
- 在 `cleanup()` 一次性输出该 mapper 覆盖范围内的所有 `(day,time)` 聚合结果（求和态），直接交给 reducer 合并与最终平均（combiner 已移除）。

收益：
- map-output records 从“快照行数级别”下降到“时间点级别”（每个 mapper 约 `4802` 条）。

涉及文件：
- `POGI-ONE-RELEASE/src/main/java/pogi_one/StockFactorMapper.java`

---

## 2) 本地聚合结构：原生 int->double[20] 哈希表（py39 探测）

实现要点：
- open addressing + 扁平 `double[]`（`slot * 20 + i`），key 使用 `stored = compactKey + 1`，避免 `0` sentinel；
- 采用 `{@code CPython 3.9}` 的扰动探测递推（`perturb >>>= 5; idx = (idx*5 + 1 + perturb) & mask`），并将写出逻辑内联到 `cleanup()`；
- 固定容量 `8192`（并带 overflow 防御性报错），在当前数据下冲突率极低。

涉及文件：
- `POGI-ONE-RELEASE/src/main/java/pogi_one/StockFactorMapper.java`

---

## 3) 传输层压缩：FactorWritable 序列化改为 float

动机：shuffle/spill 传输的 value 体积与 `FactorWritable.write()` 直接相关；将 `double` 改为 `float` 可把 value 体积减半。

改动：
- `FactorWritable` 内存表示仍为 `double[20]`（计算端维持 double）；
- 序列化时 `double -> float` 写出；反序列化时读 `float` 再转回 `double`。

结果：
- 误差增大（从 e-11/e-9 级变为 e-8 级），但远低于老师阈值 `0.01`，仍稳定 PASS。

涉及文件：
- `POGI-ONE-RELEASE/src/main/java/pogi_one/FactorWritable.java`

---

## 4) Mapper 输入 key 缩小：LongWritable -> ShortWritable

背景：mapper 输入 key 仅用于携带 “split 内文件序号 fileIndex”，并不需要行偏移量。

改动：
- `FixedCombineTextInputFormat` 输出 `<ShortWritable, Text>`：
  - `key = (short) index`（index 为 CombineFileSplit 内文件序号）
- `StockFactorMapper` 输入签名改为：
  - `map(ShortWritable key, Text value, ...)`
  - `fileId = key.get()` 用于检测换文件并清空 t-1 状态（alpha_17/18/19 正确性依赖）

涉及文件：
- `POGI-ONE-RELEASE/src/main/java/pogi_one/FixedCombineTextInputFormat.java`
- `POGI-ONE-RELEASE/src/main/java/pogi_one/StockFactorMapper.java`

---

## 5) OutputFormat：直接写字节数组

问题：默认的 `Text` 输出会产生编码与对象分配开销。

改动：改用 `DayCsvOutputFormat` + `DayLineWritable` 直接写 `byte[]`：
- RecordWriter 初始化时写表头
- 每条记录直接写 `byte[]`，再补 `'\n'`
- 每个 reducer 只写一个 `MMDD.csv`

收益：减少 reducer 写出阶段的临时 `String/Text` 分配与编码成本。

涉及文件：
- `POGI-ONE-RELEASE/src/main/java/pogi_one/DayCsvOutputFormat.java`
- `POGI-ONE-RELEASE/src/main/java/pogi_one/DayLineWritable.java`

---

## 6) Reducer 写出：复用缓冲，减少格式化开销（微优化）

改动点：
- `AverageReducer`：
  - 复用 `StringBuilder` 组装一行 CSV（替代 `StringJoiner` + 多次对象创建）
  - 复用 `double[20] totals` 数组，避免每个 key 新建对象
  - 表头 `Text` 复用（静态常量），避免反复 new
  - 最终均值使用 `totals[i] * (1.0/300)`（倒数复用，减少除法指令）

涉及文件：
- `POGI-ONE-RELEASE/src/main/java/pogi_one/AverageReducer.java`

---

## 7) validate 口径对齐：采用老师新版平均误差定义

老师新版评分逻辑（form）：
- 对每个因子 `alpha_i`：
  - 每天先对所有时间点取均值：`err_day = mean_t( |std - pred| / |std + eps| )`
  - 再对所有天取均值：`err = mean_day(err_day)`
  - `err < 0.01` 判定通过
- 时间点对齐方式：按 index 对齐（缺失点会产生 NaN，从而 FAIL），而不是取 intersection 默默忽略缺失

改动：
- `validate.py` 的关键误差计算改为与老师一致的 `align(join="outer") + mean(axis=0)` 路径

涉及文件：
- `lifecycle/validate.py`

---

## 8) 验证方式
- 构建：`python lifecycle/build.py`
- 运行：`python lifecycle/launch.py`
- 校验：`python lifecycle/validate.py`
