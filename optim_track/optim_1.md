# optim_1：CSV 按需解析（降低 Map 端常数开销）

@ Driver: ElapsedSec: 66.804

本阶段目标：在不改变算法逻辑与输出格式的前提下，减少 Map 端 CSV 解析带来的 CPU/GC 开销（避免 `String.split` 与大量临时对象）。

本项目的 Map 端热路径基本是：
`Text(line)` → `Snapshot.parse(line)` → `Snapshot.computeInto(cur,last,factors)` → `context.write(key,value)`。

因此本阶段的优化聚焦在两件事：
1) 让 `parse` 变成“只解析必要字段的线性扫描”；2) 让因子计算尽量走“少分支/少循环/少除法”的路径。

## 1) 改动点（代码细读）

### 1.1 `Snapshot.parse` 改为“硬编码 int32 顺序解析”
文件：`POGI-ONE-RELEASE/src/main/java/pogi_one/Snapshot.java`

原实现：
- `csvLine.split(",")` 全列切分（约 57 列）
- 再按索引读取 `0/1/12/13/17..36` 的字段

现实现：
- 顺序推进指针 `pos`，用多段 `while` 直接解析“用得到的字段”
- 对用不到的字段（包含字符串列 MIC/code、以及 openInterest 等）仅扫描到下一个逗号直接跳过
- 只解析算法所需列：
  - `0`：`tradingDay`（int32）
  - `1`：`tradeTime`（int32）
  - `12`：`tBidVol`（int32，最终转 double）
  - `13`：`tAskVol`（int32，最终转 double）
  - `17..36`：前 5 档 `bp/bv/ap/av`（int32，最终转 double）

另外还有两个“更激进”的假设/优化点（需要写在文档里，避免踩坑）：
- `tradingDay` 与 `tradeTime` 直接用 `parseFixed8Digits/parseFixed6Digits` 定长解析（避免循环/分支），要求行首严格是：
  `YYYYMMDD,HHMMSS,`（`data_fix` 数据满足；若换成含微秒的 `hmmssffffffff` 则会直接解析错位）。
- 当前解析器只支持“纯数字的非负整数”字段（本项目数据满足）；如果未来出现负号/小数/科学计数法/引号包裹，会需要单独的慢路径。

### 1.2 `Snapshot.computeInto` 的“少循环/少除法”实现
文件：`POGI-ONE-RELEASE/src/main/java/pogi_one/Snapshot.java`

`computeInto` 本身也是热路径（每行都会算一次），代码里做了几类典型的常数优化：
- 前 5 档相关的求和/加权求和都“手动展开”（unroll），避免 `for` 循环的边界检查与数组寻址开销。
- 把常用的除法改为“预先算倒数再乘”（例如 `_invSumAsk = 1/(sumAsk+EPS)`），减少重复除法。
- 用统一的 `EPSILON = 1e-7` 在分母上做偏移，满足题面“分母为 0 加 1e-7”的要求，并避免 `if(denom==0)` 分支。
- `alpha_17/18/19` 依赖 `t-1`：当 `previous == null`（通常是每个股票文件第一行，或换日后重置）时将这三项置 0（`Arrays.fill(factors, 16, 19, 0.0d)`）。

### 1.3 `Snapshot` 对象/数组的分配现状（以及为什么这仍是瓶颈）
当前 `Snapshot.parse` 每行都会分配 4 个 `double[5]`（`bp/bv/ap/av`）并构造一个新的 `Snapshot` 对象；构造器本身“不拷贝数组”，只是保存引用。

这意味着：与 `split` 相比，解析阶段的“字符串临时对象”已经大幅减少，但 JVM 仍然要为每行支付：
- `Snapshot` 对象 1 个
- `double[]` 4 个（每个 5 长度）

如果后续还要继续提速，下一阶段最有潜力的方向就是“把 per-line 分配继续压下去”（见第 5 节）。

### 1.4 `FactorWritable` 的设计与 Shuffle 影响
文件：`POGI-ONE-RELEASE/src/main/java/pogi_one/FactorWritable.java`

`FactorWritable` 是 Map/Shuffle/Reduce 的核心载荷（payload）：
- 结构：`double[20] factors`（20 个因子“求和态”），不带 `count`
- `add`：逐维累加（可交换/可结合），因此 `FactorCombiner` 可以安全复用同一套求和逻辑
- 序列化：`write/readFields` 逐个 `writeDouble/readDouble`，每条记录固定 20*8 = 160B（不含 Hadoop 框架开销）

这里的关键假设是：Reducer 端直接按 `EXPECTED_STOCKS = 300` 做平均（见 `AverageReducer`），所以 `FactorWritable` 不需要携带 `count`。
这在 `data_fix` 的“每个 (day,time) 都齐 300 只股票”前提下是最快的；但一旦真实数据出现缺失/停牌/数据质量问题，固定除以 300 会引入系统性偏差，此时必须把 `count` 放回 `FactorWritable`（或单独的计数 Writable）。

## 2) 预期收益（为什么能更快）

- 避免 `split` 带来的：
  - `String[]` 分配
  - 大量 token 字符串分配（及其底层 char[] 复制）
  - 额外的遍历与边界检查
- 在 `data_fix` 这类“高频、整数为主、行数巨大”的数据上，Map 阶段通常会有可观收益（CPU 与 GC 压力同时下降）
- 因子计算阶段减少循环与除法，也能降低 Map 端 CPU 常数（尤其在启用 Combiner、Mapper 吞吐拉满时更明显）

## 3) 正确性与风险

- 正确性：输出值不变（本阶段只改变解析方式，不改变列索引与因子公式）
- 风险点：
  - 行首格式强依赖 `YYYYMMDD,HHMMSS,`：若输入的 `tradeTime` 变成 `hmmssffffffff`（题面字段说明里确实可能如此），当前定长解析会错位，需要升级解析器
  - 解析器假设“无引号/无转义/无空字段”的简单 CSV（当前数据集符合）
  - `FactorWritable` 不含 `count`：依赖 Reducer 端固定除以 300；如出现缺失股票会导致均值偏差

## 4) 验证方式（本阶段建议记录的对照）

- 编译与单测：`mvn -f POGI-ONE-RELEASE/pom.xml test`
- 运行对照：
  - 同一输入（单天/全量）跑两次：对比 wall time
  - 对比 Hadoop 计数器：Map time、GC time（如有）、Shuffle bytes
  - 用 `lifecycle/validate.py` 校验输出正确性（确保 20 因子 PASS）

## 5) 下一阶段（如果还要更快）：继续压低 per-line 分配

在已经去掉 `split` 之后，Map 端的下一大块开销通常来自“每行 new 对象/数组”：
- 方向 A：把 `Snapshot.parse` 改为“写入可复用的容器”，例如 `parseInto(SnapshotMutable dst)`，并用两份容器轮换保存 `current/previous`，避免每行 new（注意：不能复用同一个对象当 `lastSnapshot`，否则 `t-1` 会被覆盖）。
- 方向 B：进一步把 4 个 `double[5]` 拆成 20 个标量字段（或 4 个 `long`/`int` 数组）并在计算中直接使用，减少数组对象与数组寻址。
- 方向 C：如果精度允许（通常不建议在评分题里冒险），将 Shuffle 载荷从 `double` 降为 `float` 或做定点缩放压缩，换取更小网络传输量（但会影响误差门限与可解释性）。
