# POGI-ONE-RELEASE: Optimization Notes (Since Last Commit)

以下内容仅覆盖相对上一次提交后的优化改动，按性能收益优先级排序。

## 1) 输出数据链缩短（最大收益）

**目标**：减少 reducer -> output 的对象与格式化开销，缩短写出链路。  
**核心变更**：
- 使用自定义输出行对象 `FactorLineWritable`，避免 Text 中转。
- `DayCsvOutputFormat` 只输出 value，并在 RecordWriter 内格式化为 CSV 字节，避免默认 `key<TAB>value` 破坏 CSV。
- 引入 `schubfach` 工具类，提供无 String 中转的 float -> ASCII 格式化路径。

**数据链路（重构后）**：
1. **Reducer 内**：  
   - 计算 `sum/count` 得到 `float[20]`，直接写入 `outLine.factors[i]`。  
   - 设置 `outLine.secOfDay`，调用 `context.write(NullWritable, outLine)`。
2. **RecordWriter**（DayCsvRecordWriter）：  
   - 复用 `byte[] buf` 与 `byte[] digits`；  
   - 先写 `tradeTime`（6 位），再逐个 `alpha_i` 格式化为 ASCII；  
   - 全行 `out.write(buf, 0, pos)`，追加 `'\n'`。
3. **输出流**：  
   - `BufferedOutputStream` 直接落盘（HDFS/本地模拟），无额外缓冲层链路。

**对比重构前**：
- **旧链路**：`float -> StringBuilder -> String -> byte[] -> Text -> RecordWriter -> out`  
- **新链路**：`float -> ASCII(byte[]) -> out`

**相关文件**：
- `POGI-ONE-RELEASE/src/main/java/pogi_one/DayAverageReducer.java`
- `POGI-ONE-RELEASE/src/main/java/pogi_one/DayCsvOutputFormat.java`
- `POGI-ONE-RELEASE/src/main/java/pogi_one/FactorLineWritable.java`
- `POGI-ONE-RELEASE/src/main/java/schubfach/SchubfachFloat.java`
- `POGI-ONE-RELEASE/src/main/java/schubfach/SchubfachBench.java`

## 2) 任务分配阶段对象分配优化

**目标**：降低 split 构造阶段的装箱和临时对象，减少 GC。  
**核心变更**：
- `FixedCombineTextInputFormat` 使用 `DayGroup[10000] + dayOrder[]` 取代 `LinkedHashMap<String,DayGroup>`。
- 去掉 `List<Long>` 装箱，改为 `long[]`。
- 避免 `subList` 与二次 `ArrayList` 拷贝，改为数组片段复制。

**相关文件**：
- `POGI-ONE-RELEASE/src/main/java/pogi_one/FixedCombineTextInputFormat.java`

## 3) Mapper/Reducer 热路径微调

**目标**：减少热路径内的中间缓冲与额外输出。  
**核心变更**：
- Reducer 直接写入 `outLine.factors`，避免 `lineFactors` 中转与拷贝。
- 删除 reducer 输出计时与累计字段。
- `DayCsvOutputFormat` 解析 dayId 不再 `split()`，改为线性扫描取第 N 段。

**相关文件**：
- `POGI-ONE-RELEASE/src/main/java/pogi_one/DayAverageReducer.java`
- `POGI-ONE-RELEASE/src/main/java/pogi_one/DayCsvOutputFormat.java`

## 4) 工具脚本与自动化统计

**目标**：日志更精简、清理更彻底、统计更一致。  
**核心变更**：
- `cleanbuf.py` 统一清理 `local_buffer/hdfs_in|hdfs_out|monitor_logs|logs`。
- `ananlyze_time_distr.ipynb` 自动运行只写单行 `@ Driver: ElapsedSec: <timecost.4f>` 到日志。

**相关文件**：
- `lifecycle/cleanbuf.py`
- `ananlyze_time_distr.ipynb`

## 5) 性能验证与基准

**目标**：为 Schubfach 实现提供可复现的 correctness/bench/full 测试入口。  
**核心变更**：
- `SchubfachBench` 支持 `sample|bench|full` 三种模式。
- `full` 模式支持大样本日志节拍输出。

**相关文件**：
- `POGI-ONE-RELEASE/src/main/java/schubfach/SchubfachBench.java`

## 6) 仍未动或保留项

- `Mapper` 内哈希表结构未再做侵入式改动（保持 python3.9 风格实现）。
- 输出格式仍维持 CSV，评分逻辑按相对误差校验。
