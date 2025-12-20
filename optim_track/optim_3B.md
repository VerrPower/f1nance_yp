# optim_3.5：Key 进一步压缩到 30-bit + 彻底去封装（IntWritable 直通）

本轮是在 OPTIM_3（Combine 多 CSV + 32-bit key）基础上继续“压榨 MapReduce 框架开销”的一轮：
- **把时间从日内秒改为“06:00 截断秒”**，把 key 的时间段压到 15 bit
- 从而使整体 key 压到 **30 bit**，最高位恒为 0，**不再需要 unsigned compare / uint32_t 语义**
- **最重要：彻底取消 key 类封装**，全链路使用 Hadoop 原生 `IntWritable` 作为 map/reduce key

---

## 1) 动机：消除 unsigned 比较风险 + 降低 key 成本

OPTIM_3 的 `compactTime:int` 因为年份偏移会触发 bit31=1，排序需要 `Integer.compareUnsigned`。
这虽然能 work，但存在两点“框架层面的不优雅”：
- Hadoop 默认比较器是有符号语义，必须依赖自定义 `WritableComparable` 才能保证正确
- key 仍然是自定义类（序列化/比较/封装开销）

因此本轮目标是：让 key **天然是非负 int**，直接用 Hadoop 自带 `IntWritable` 和默认排序/分组。

---

## 2) 30-bit CompactTime 设计（避免 unsigned compare）

### 2.1 时间 15 bit：06:00 截断秒
交易时段最晚 15:00:00，对应 `secOfDay=54000`。设基准 `BASE=06:00:00=21600`：
- `timeCode = secOfDay - BASE`
- 取值范围：`0..32400`，可用 **15 bit** 覆盖（`2^15=32768`）

### 2.2 日期 15 bit：year/month/day 打包
沿用之前的“狭窄但足够”的日期编码：
- `yearOffset = year - 1990`：6 bit（1990..2053）
- `month`：4 bit（1..12）
- `day`：5 bit（1..31）

得到：
`dayCode = (yearOffset<<9) | (month<<5) | day`（15 bit）

### 2.3 合成 30-bit key
`compact30 = (dayCode<<15) | timeCode`

因为最高只到 `2^30-1`，**始终非负**：
- 不再存在 uint32_t / unsigned compare 的必要
- Hadoop 默认的有符号 int 排序 == 我们期望的按 (day,time) 排序

---

## 3) 去封装：DayTimeKey 删除，IntWritable 直通

最关键改动：删除自定义 key（DayTimeKey），改用 Hadoop 的 `IntWritable`：
- Mapper 输出 key：`IntWritable(compact30)`
- Combiner/Reducer key：同为 `IntWritable`
- Partitioner：从 `compact30>>>15` 取 dayCode 分区
- Reducer 输出：从 `compact30` 解码回 `secOfDay` 再格式化成 `HHMMSS`；从 `dayCode` 解码 `MMDD` 生成 `0102.csv` 等输出文件名

---

## 4) 变更点（文件级）
- `POGI-ONE-RELEASE/src/main/java/pogi_one/StockFactorMapper.java`
  - 输出 key 改为 `IntWritable`
  - 新增 `packCompactTime30(tradingDay, secOfDay)`（06:00 截断 + 30-bit 打包）
- `POGI-ONE-RELEASE/src/main/java/pogi_one/FactorCombiner.java`
  - key 泛型从 `DayTimeKey` 改为 `IntWritable`
- `POGI-ONE-RELEASE/src/main/java/pogi_one/DayPartitioner.java`
  - key 泛型改为 `IntWritable`，分区使用 `compact>>>15`
- `POGI-ONE-RELEASE/src/main/java/pogi_one/AverageReducer.java`
  - key 泛型改为 `IntWritable`
  - 解码输出 `tradeTime(HHMMSS)` 与 `MMDD.csv` 输出路径
- `POGI-ONE-RELEASE/src/main/java/pogi_one/Driver.java`
  - `job.setMapOutputKeyClass(IntWritable.class)`（key 直通）
- `POGI-ONE-RELEASE/src/main/java/pogi_one/DayTimeKey.java`
  - 已删除（去封装完成）

---

## 5) 验证
- 运行：`python lifecycle/launch.py`
- 校验：`python lifecycle/validate.py --eps 1e-7`
- 结果：20 个因子全 `PASS`；计时稳定在 ~18s 左右（示例：`@ Driver: ElapsedSec: 18.685`）

---

## 6) 风险与边界（明确写死的假设）
- 日期编码只覆盖 `1990..2053`；超范围将产生溢出风险（会导致聚合/排序错误）。
- 时间编码假设数据不会早于 06:00:00；若出现更早时刻，`timeCode` 会为负，需要升级编码策略（或改基准）。
