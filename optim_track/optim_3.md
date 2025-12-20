# optim_3：Reduce Mapper 数量 + Key 32-bit 压缩（Combine 多 CSV + Unsigned 排序）

@ Driver: ElapsedSec: 25.678

目标：不再纠结单行解析常数，开始触及 MP 框架本体 —— **减少 mapper 数量** 和 **降低 shuffle/sort 的 key 体积与比较成本**。

本轮主要两个优化：
1) 一个 mapper 处理多个 CSV（CombineFileInputFormat）
2) DayTimeKey 由 2×long 压到 1×int，并用 `Integer.compareUnsigned` 排序

---

## 1) OPTIM 点 1：一个 mapper 处理多个 CSV（降低 mapper 数量）

### 1.1 背景
原先每个 `snapshot.csv` 一个 split，导致 mapper 数量过多（300 股票 × 多天），任务调度/启动/框架开销很大，CPU 反而被 MR 框架吃掉。

### 1.2 方案
- 引入 `FixedCombineTextInputFormat`：继承 `CombineFileInputFormat`，把多个小 CSV **按文件数打包为一个 split**。
- `isSplitable=false`：单个 CSV 不切分，仍然保证一行就是一条记录（LineRecordReader）。
- 当前 `maxFiles` 已在 `FixedCombineTextInputFormat.DEFAULT_MAX_FILES` 内硬编码（调大后 mapper 数显著下降，ElapsedSec 降到 ~26s）。

### 1.3 关键正确性：t-1 因子（alpha_17/18/19）跨文件污染问题
Combine 后一个 mapper 会顺序读多个文件，如果不处理，`alpha_17/18/19` 的 t-1 状态可能跨股票文件串味，直接炸 validate。

解决方式：
- `FixedCombineTextInputFormat.CFLineRecordReader` 把 `fileIndex` 编码进输入 `LongWritable key` 高位（Mapper 可识别“来自哪个文件”）。
- `StockFactorMapper` 在检测到 `fileId` 变化时清空 t-1 状态，并确保 **非输出窗口行** 也更新 `prevTradeTime/secOfDay`。

涉及文件：
- `POGI-ONE-RELEASE/src/main/java/pogi_one/FixedCombineTextInputFormat.java`
- `POGI-ONE-RELEASE/src/main/java/pogi_one/StockFactorMapper.java`
- `POGI-ONE-RELEASE/src/main/java/pogi_one/Driver.java`

---

## 2) OPTIM 点 2：Key 编码压缩（2×long → 1×int，Unsigned 排序）

### 2.1 背景
`DayTimeKey` 以前是 `(tradingDay,long)+(tradeTime,long)`：
- 序列化体积更大（16 bytes）
- sort/compare 成本更高（两次 long compare）
- shuffle/sort 的 key 负担偏重

### 2.2 方案：CompactTime（32-bit）
将 `(YYYYMMDD, secOfDay)` 打包为单 `int compactTime`，并用无符号比较保证排序正确：

- `yearOffset = year - 1990`：6 bit（1990..2053）
- `month`：4 bit（1..12）
- `day`：5 bit（1..31）
- `secOfDay`：17 bit（0..86399）

组合：
`compactTime = (y<<26) | (m<<22) | (d<<17) | secOfDay`

排序：
- 因为 2025 等年份会让最高位为 1，默认有符号 int 会乱序
- 所以 `DayTimeKey.compareTo()` 使用 `Integer.compareUnsigned(a,b)`

同时：
- `DayPartitioner` 改为使用 `dayCode = compactTime>>>17` 做分区，避免解码 long day 参与 hash。
- `StockFactorMapper` 解析 `HHMMSS` 时直接转 `secOfDay`，并用秒窗口判断是否 emit。
- `AverageReducer` 输出仍维持原格式：tradeTime 写回 6 位 `HHMMSS`，day 仍输出为 `MMDD.csv` 前缀（通过 `DayTimeKey` 解码）。

涉及文件：
- `POGI-ONE-RELEASE/src/main/java/pogi_one/DayTimeKey.java`
- `POGI-ONE-RELEASE/src/main/java/pogi_one/DayPartitioner.java`
- `POGI-ONE-RELEASE/src/main/java/pogi_one/StockFactorMapper.java`
- `POGI-ONE-RELEASE/src/main/java/pogi_one/AverageReducer.java`（行为保持一致，依赖 key 解码）

---

## 3) 其他同步变更（属于清理/稳定性）
- `Driver` 不再解析 `--timing`，计时永久开启，且计时起点移动到 `main()` 最开始。
- `launch.py` 删除 `--log`/mute 逻辑：日志永久开启（不再静音）。
- `validate.py` 输出改为 `PASS/FAIL`，并打印当前使用的预测输出目录名；误差计算保持 numpy 向量化实现。

---

## 4) 验证方式
- 构建：`mvn -f POGI-ONE-RELEASE/pom.xml package -DskipTests`
- 跑 MR：`python lifecycle/launch.py`
- 校验：`python lifecycle/validate.py --eps 1e-7`

---

## 5) 收益与后续方向
- **收益 1：Mapper 数显著减少**（Combine），框架调度开销下降是本轮主要提速来源。
- **收益 2：Key 更小更快**（1×int + unsigned compare），降低 sort/shuffle 负担，尤其对 reducer 端排序聚合更友好。

后续主要瓶颈更可能转移到：
- Reducer 单点写出（`MultipleOutputs` + 单 reducer）与 HDFS 写入吞吐
- Combiner 命中率（是否能进一步减少 shuffle）
