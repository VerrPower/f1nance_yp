# POGI-ONE-R1：从 Map-Only 到 “1 day / 1 reducer”（支持单日/多日 + day 内均匀切分）

基线版本（上一版）：`ecf4ae2 OPTIM_5: [POGI-ONE beta1] map-only`。当前版本（R1）：working tree（未 commit）。

这次不是“抠常数”的微优化，而是一次**为并行性与兼容性服务的架构重构**：把原来“每个交易日只跑 1 个 map task，并在 mapper 里直接落盘”的 Map-Only 方案，改造成“日内多 mapper 并行 + reduce 端按天汇总输出”的标准 MR 链路。核心动机来自老师的新要求：需要**同时兼容单个 day 与多个 day**，并且希望在 LocalJobRunner 下尽可能利用容器 CPU（即日内切分，让单日也能并行跑起来）。

---

## 1. 背景：Map-Only 的优势与瓶颈

OPTIM_5 的 Map-Only 版本把 shuffle/reduce 完全移除：`FixedCombineTextInputFormat` 保证 split 不跨 day，`StockFactorMapper` 在 mapper 内对 `(day,time)` 做本地聚合，然后 `cleanup()` 直接写出当天最终 CSV（固定 4802 行、均值按 `sum*(1/300)` 计算）。这条链路的优势很直接：框架常数小、map-output 为 0、没有 spill/shuffle/merge 的额外开销。

但 Map-Only 的并行性存在结构性上限：当输入只包含**单个交易日**时，按照“1 day → 1 split → 1 mapper”的设计，作业最多只有一个 map task 在做计算；即使容器 `cpuset=4`，也很难把 4 核吃满。此外，Map-Only 还隐含了“每个 day 一定是 300 支股票且不缺点”的强假设，一旦真实数据出现 `N≠300` 或缺失时间点，固定除以 300 会引入系统性偏差。

---

## 2. 设计目标与硬约束（需求对齐）

本轮重构围绕以下目标与约束展开：

1) **并行性**：day 之间天然独立（embarrassingly parallel），同时 day 内也要能切出多个 mapper 并行；并行度写死为 `P = min(8, availableProcessors)`，不通过 `-D` 传参控制。  
2) **兼容性**：不再依赖“300 股票”的常量，均值一律由 `count` 计算；`count==0` 不做防御性处理（发生则听天由命）。  
3) **隔离性/确定性**：保证 **1 day / 1 reducer**，同一天的数据不会跨 reducer 混算；day→reducer 的映射需要可复现（确定性）。  
4) **输出策略**：reducer 只输出自己收到的 key；不强制循环 4802 做补零/补点（数据链路正确且数据完整时，自然会对齐到 4802 行）。

---

## 3. R1 总体方案（Pipeline）

R1 把日内并行拆到 map 侧，把“跨 split 的合并与最终均值”交给 reduce 侧完成：

`InputFormat（日内切分）` → `Mapper（本地哈希聚合 sum+count）` → `Partitioner（按 day 固定路由）` → `Reducer（按 (day,time) 汇总并求均值输出 CSV）`

其中 key 采用单个 `int` 打包 `(dayId,time)`，dayId 使用 `MMDD`（`tradingDay % 10000`），避免引入 year/month/dayCode 的额外语义与成本。

---

## 4. 关键实现（按模块说明）

### 4.1 InputFormat：按 day 分组，day 内均匀切分到 P 份

实现位于 `POGI-ONE-RELEASE/src/main/java/pogi_one/FixedCombineTextInputFormat.java`。逻辑保持 “split 不跨 day”，但把原来的 `MAX_FILES=300`（每 day 1 split）改成 day 内均匀切分：先按路径 `.../<day>/<stock>/snapshot.csv` 做 day 分组，然后对每个 day 的 `N` 个股票文件计算 `P=min(8,availableProcessors)`，实际切分份数为 `splitsForDay=min(P,N)`，每份 chunk 大小 `chunkSize=ceil(N/splitsForDay)`，最终生成若干 `CombineFileSplit`。

你要求的分配形式可以直接由这个 `ceil` 切分得到：例如 `N=299,P=4` 时为 `75/75/75/74`；`N=603,P=4` 时为 `151/151/151/150`。这样即使只跑单日输入，也会产生多个 map task，在 LocalJobRunner 下具备可观的并行度。

### 4.2 Mapper：由“直接写最终 CSV”改为“输出聚合态 KV”

实现位于 `POGI-ONE-RELEASE/src/main/java/pogi_one/StockFactorMapper.java`。因子计算与解析策略基本延续基线：仍然是 byte-level ASCII 扫描解析，仍然保留 `t-1` 状态以计算 `alpha_17/18/19`（并在 fileId 变化或时间回跳时清空状态）。

关键变化是输出语义：mapper 不再写最终 CSV，而是在 mapper 内对 `(dayId,time)` 做本地哈希聚合，并在 `cleanup()` 统一输出聚合后的 KV。聚合结构按你的要求实现为 `double[CAPACITY * 21]` 的扁平数组（`AGG21_FP64`）：`0..19` 累加 20 个因子求和，`20` 累加 count（每条有效记录 `+1`），不额外维护 count 数组。

为便于分区，key 使用单个 `int` 打包：

- `dayId = tradingDay % 10000`（MMDD）  
- `timeCode = (secOfDay - 06:00:00) & 0x7fff`  
- `packedKey = (dayId << 15) | timeCode`

mapper 在 `cleanup()` 遍历哈希表非空槽位并 `context.write(packedKey, FactorWritable(sum+count))`，因此 reducer 端只会看到“真实出现过的数据点”；不再做固定 4802 的补点循环。

### 4.3 Shuffle Value：FactorWritable 扩展为 21 维（最后一维 count）

实现位于 `POGI-ONE-RELEASE/src/main/java/pogi_one/FactorWritable.java`。从 `double[20]` 扩展为 `double[21]`，最后一维作为 count 传递到 reducer。传输层仍保留之前的压缩策略：序列化按 `float` 写出、反序列化读回 `float` 转 `double`，以降低 shuffle/spill 体积；count 的数值范围很小，float 表示不会引入额外风险。

### 4.4 Partitioner：确定性 day→reducer 映射，保证 1 day / 1 reducer

实现位于 `POGI-ONE-RELEASE/src/main/java/pogi_one/DayIdPartitioner.java`。Driver 在 job 提交前会自动扫描输入路径并构建 day 列表（字典序），写入 conf：`finyp.dayIds=0102,0103,...`。Partitioner 读取该配置并建立 `dayId -> partitionIndex` 的查找表，分区时从 `packedKey>>>15` 取出 dayId 并路由到固定 reducer。这样可以确保同一天的数据不会跨 reducer 混算，同时 day→reducer 的映射在同一输入集上是可复现的。

### 4.5 Reducer：按 count 求均值，按天输出 CSV

实现位于 `POGI-ONE-RELEASE/src/main/java/pogi_one/DayAverageReducer.java`。Reducer 对同一 `(dayId,time)` 的多个 `FactorWritable(sum+count)` 做逐维累加，最终按 `mean[i]=sum[i]/count` 输出 20 个均值；按你的要求不对 `count==0` 做分支判断。输出使用 `MultipleOutputs`，按 day 写入 `MMDD.csv-r-xxxxx`，并复用 `ValueOnlyTextOutputFormat` 保证输出行是纯 CSV（不带 `key<TAB>`）。

补充说明：MR 默认输出通道的 `part-r-0000x` 在本实现中不会被写入，因此可能出现 0 字节文件，这是正常副产物；`validate.py` 会按 `MMDD.csv-*` 规则读取，不受影响。

### 4.6 Driver：自动发现 day + 输入路径补全 + LocalJobRunner 并行上限

实现位于 `POGI-ONE-RELEASE/src/main/java/pogi_one/Driver.java`。Driver 从简单 `main()` 入口升级为 `ToolRunner/Configured` 的标准写法，同时在提交 job 前完成三件关键工作：  
（1）设置 localrunner 并行上限：`mapreduce.local.map.tasks.maximum` 与 `mapreduce.local.reduce.tasks.maximum` 均设为 `availableProcessors()`；  
（2）对输入路径做“文件级通配符补全”，保证传入 root 目录时能实际匹配到 `*/*/snapshot.csv`；  
（3）扫描输入并自动发现 day 目录，写入 `finyp.dayIds`，并把 `numReduceTasks` 设置为 day 的数量，最终形成 “1 day / 1 reducer” 的执行形态。

---

## 5. 运行与验证

R1 在本地 `file://` 数据集 `data_fix/` 上验证通过：既支持传入 `file:///.../data_fix/*/*/snapshot.csv`（文件级通配符），也支持直接传入 `file:///.../data_fix`（root 目录）。实测在 `data_fix`（5 天、共 1500 文件）上，日内 `P=4` 时 split 数为 20，`validate.py` 全部 PASS（误差量级维持在 `1e-8` 左右，远低于阈值 `0.01`）。

输出文件形态与 baseline 的区别在于后缀：Map-Only 写 `MMDD.csv-m-xxxxx`，R1 写 `MMDD.csv-r-xxxxx`。

---

## 6. 用 git 回看 baseline / 对比 R1

- 看 baseline 的 Map-Only Driver：`git show ecf4ae2:POGI-ONE-RELEASE/src/main/java/pogi_one/Driver.java`  
- 看 baseline 的 Map-Only Mapper：`git show ecf4ae2:POGI-ONE-RELEASE/src/main/java/pogi_one/StockFactorMapper.java`  
- 看 baseline 的 InputFormat：`git show ecf4ae2:POGI-ONE-RELEASE/src/main/java/pogi_one/FixedCombineTextInputFormat.java`  
- 查看当前改动概览：`git diff --stat`  
- 对单文件做差分：`git diff -- POGI-ONE-RELEASE/src/main/java/pogi_one/Driver.java`
