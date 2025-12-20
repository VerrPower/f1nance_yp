# optim_0：基线版本性能瓶颈与优化方向（当前实现）

@ Driver: ElapsedSec: 80.913

本文档用于记录“性能优化过程”的第 0 个阶段：在不改动算法正确性与输出格式前提下，对当前 MapReduce 实现做一次性能画像，明确主要瓶颈与可落地的优化方向，为后续 `optim_1/optim_2/...` 提供对照基线。

## 0. 当前实现（Baseline）概览

### 0.1 数据形态（来自 `data_fix/` 的抽样结论）
- 输入目录结构：`data_fix/<day>/<stock>/snapshot.csv`
- 文件数：5 天 × 300 股 = 1500 个 CSV（典型“海量小文件”）
- `tradeTime`：在 `data_fix` 里观察到均为 6 位 `HHmmss`（如 `092500`），未发现 `hmmssffffffff` 的长时间戳形式
- 单文件为按时间顺序的快照流；包含 `09:30` 之前的记录（例如 `09:25`），因此 `t-1` 相关因子需要在 mapper 内维护状态，并允许读取窗口外上一条以服务 `09:30` 的 `t-1`

### 0.2 MapReduce 流水线（当前工程）
```
HDFS 输入（1500 个 snapshot.csv 小文件）
  -> NonSplittableTextInputFormat（每文件一个 split，保证 mapper 内 lastSnapshot 不跨 split 丢失）
  -> StockFactorMapper（逐行解析 CSV，计算 20 因子；输出 key=(day,time)，value=(sum[20],count=1)）
  -> FactorCombiner（对同 key 做 sum+count，本地预聚合）
  -> Shuffle/Sort（按 DayPartitioner 分区、DayTimeKey 排序）
  -> DayAverageReducer（对同 key 汇总 300 股并求平均）
  -> DayCsvOutputFormat（每 reducer 输出一个 MMDD.csv，直接写字节数组）
```

## 1. 主要瓶颈（按影响优先级排序）

### 1.1 瓶颈 A：海量小文件导致 mapper 数量爆炸（高优先级）
- 现状：InputFormat 不可切分 + 每股一个文件 => 大量 map task（1500 个）
- 影响：
  - YARN 调度/容器启动开销显著（对“速度排名”尤其敏感）
  - HDFS 打开/关闭文件频繁、NameNode 元数据压力增大
  - 即便每个 mapper 计算很轻，总 wall time 仍会被“任务管理开销”抬高

### 1.2 瓶颈 B：Combiner 在当前 key 设计下几乎无效（中高优先级）
- 根因：key=(day,time)，而单个股票文件内每个 `time` 基本只出现一次
- 结果：
  - Combiner 输入 records ≈ 输出 records（几乎不减少）
  - 额外引入一次序列化/反序列化与 combiner reduce 调用开销
- 结论：在“每 mapper 仅处理一个股票文件”的前提下，Combiner 很难产生收益

### 1.3 瓶颈 C：Reducer 并行度不足/汇聚风险（中优先级）
- 现状：默认 reducer 数为 1（为了避免同日多输出文件/重复表头等问题）
- 影响：
  - 多天一起跑时，reduce 阶段容易成为尾部瓶颈（所有 day 的 key 都在少量 reducer 上聚合）
- 注意：如果简单提高 reducer 数，需要确保“同一天只由一个 reducer 写出”，否则会出现同一日多个 `0102.csv-r-xxxxx` 文件及可能的重复表头/下游合并问题

### 1.4 瓶颈 D：CSV 解析与对象分配（中优先级）
- 现状：每行 `String.split(",")` 解析全部字段，随后只用到其中一部分列
- 影响：
  - 产生大量短命对象（token 数组、子串等），CPU 与 GC 有额外负担
  - 在 map 数量很大时，这类常数开销会被放大

## 2. 优化方向（不改变正确性/输出格式）

### 2.1 方向 1：减少 mapper 数量（最高优先级）
目标：将“1500 个文件 -> 1500 个 map task”压缩到更少的 map task，减少调度/启动/打开文件成本。

可选方案：
- **CombineFileInputFormat / 自定义 Combine 输入**：让单个 mapper 处理多个股票文件（仍保持“文件内不切分”，但允许“一 task 多文件”）
- **HDFS 侧预合并**：将同一天/同一批股票的 `snapshot.csv` 合并成更少的大文件（保持文件内记录顺序），再跑 MapReduce
- 预期收益：通常是最显著的 wall time 降低来源

副作用/风险：
- 需要谨慎处理 mapper 内的 `lastSnapshot`：在“切换到下一个文件/股票”时必须显式 reset，避免跨股票串线
- 需要确认输入文件边界的处理方式（如在 RecordReader 层面携带当前文件路径以便识别换文件）

### 2.2 方向 2：在“可产生重复 key”的前提下重新启用聚合（中高优先级）
如果采用了方向 1（单 mapper 多文件），同一个 mapper 内会出现大量重复 `(day,time)` key，此时：
- **Combiner 才会真正减少 records/shuffle bytes**
- 或者直接做 **in-mapper aggregation**（HashMap<(day,time),sum+count>），在 mapper 结束时输出聚合结果

风险：
- in-mapper aggregation 需要控制内存（`(day,time)` 的 key 数量约为当日有效时间点数，量级可控，但仍需评估）

### 2.3 方向 3：提升 reducer 并行但保持“同日单文件”（中优先级）
目标：多天并行 reduce，但同一天仍由固定一个 reducer 负责写出，避免同日多分片导致后处理复杂。

可选方案：
- reducer 数设置为“天数”或更多，但 partition 必须保证 `tradingDay -> partition` 为单射/稳定映射
- 或把 job 拆成“按天跑多个 job”（调度层面并行），每个 job reducer=1

### 2.4 方向 4：优化 CSV 解析（中优先级）
目标：降低 map 端常数开销与对象分配。

可选方案：
- 手写按需列解析：只扫描逗号分隔，抽取 `0,1,12,13,17..(前5档*4)` 这些列
- 使用更高性能的 CSV 解析方式（注意引入依赖与运行环境兼容性）

## 3. 基线指标建议（后续每个 optim_x 都要记录）

建议每次优化阶段至少记录：
- 总运行时间（wall time）
- Map task 数量、平均 map 时间、尾部 map（straggler）情况
- Shuffle bytes、Shuffle records
- Reduce task 数量、平均 reduce 时间
- 输出正确性校验结果（`lifecycle/validate.py`）

## 4. 下一阶段（optim_1）建议目标
- 优先尝试“减少 mapper 数量”的方案（Combine 小文件 / 一 task 多文件）
- 在该前提下再讨论是否保留/增强 combiner 或改成 in-mapper aggregation
