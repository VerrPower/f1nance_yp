package pogi_one;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer：对来自不同 mapper 的 {@link FactorWritable}（sum[20] + count）做累加汇总，并计算 20 个因子的最终均值。
 *
 * <p><b>
 * 1) 归并计算均值
 * </b></p>
 * <p>
 *      本 reducer 的输入 value（{@link FactorWritable}）已经在 mapper 端做过 mapper 内部聚合，
 *      reducer 侧只需把同一 (dayId, secOfDay) 对应的多份 <code>sum[i]</code> 与 <code>count</code> 相加，得到
 *      <code>inv_count = 1 / count</code>，并计算 <code>mean[i] = (float)(sum[i] * inv_count)</code>。
 * </p>
 *
 * <p><b>
 * 2) 均值(fp32) 到写入 CSV 的完整数据链
 * </b></p>
 * <p>
 *      我们为了尽可能消灭短命对象、把输出链路的性能榨到极致，选择手动重写 IEEE754 浮点数的格式化过程：
 *      不再走 Float.toString() 这类会产生中间 String/char[] 的路径，
 *      而是把 fp32 的十进制表示直接写入 RecordWriter 内部的 byte[] 行缓存，再由 BufferedOutputStream 批量下沉到文件系统，
 *      从而把“格式化 + 拼行”阶段几乎完全对象零分配化。
 *      当然也有两点比较遗憾：第一，本项目的最终输出量其实不大，因此这条“极致优化的数据链路”在总耗时上的优势不算特别显著；
 *      第二，Hadoop 框架把底层输出流的私有缓冲区封装得比较严，无法直接拿到并复用其内部 byte buffer 的引用，
 *      否则我们理论上还能进一步把链路缩短成更理想的形态：fp32 → out 的 bytebuffer → HDD（减少一层中间拷贝/拼装边界）
 * </p>
 * <pre>
 *      Data flow diagram (mean(fp32) -> CSV bytes -> persisted)
 *
 *      ┌──────────────────────────────START-FP32──────────────────────────────┐
 *      │ DayAverageReducer.reduce()                                           │
 *      │  - sums[0..19] += mapper partial sums                                │
 *      │  - count      += mapper partial count                                │
 *      │  - mean[i] = (float)(sums[i] / count)                                │
 *      └──────────────────────────────────────────────────────────────────────┘
 *                               │
 *                               │  (Layer 1: in-memory floats)
 *                               ▼
 *      ┌─────────────────────────FactorLineWritable───────────────────────────┐
 *      │ FactorLineWritable outLine                                           │
 *      │  - outLine.secOfDay                                                  │
 *      │  - outLine.factors[20]  (float[20] means)                            │
 *      └──────────────────────────────────────────────────────────────────────┘
 *                               │
 *                               │  context.write(NullWritable, outLine)
 *                               ▼
 *      ┌────────────────────────────RecordWriter──────────────────────────────┐
 *      │ DayCsvOutputFormat.DayCsvRecordWriter.write(...)                     │
 *      │  - Schubfach: float -> ASCII -- free of tmp obj produced by toString()  
 *      │  - pack one CSV row into byte[] buf                                  │
 *      │    (Layer 2: per-record row buffer)                                  │
 *      └──────────────────────────────────────────────────────────────────────┘
 *                               │
 *                               │  out.write(buf, 0, pos)
 *                               ▼
 *      ┌─────────────────────────────[NOT EXPOSED]────────────────────────────┐
 *      │ BufferedOutputStream (out)                                           │
 *      │  - private byte[] buffer (1<<20)                                     │
 *      │    (Layer 3: large stream buffer; flush/close triggers bulk write)   │
 *      └──────────────────────────────────────────────────────────────────────┘
 *                               │
 *                               │  writes to FSDataOutputStream
 *                               ▼
 *      ┌─────────────────────────────[NOT EXPOSED]────────────────────────────┐
 *      │ FSDataOutputStream -> FileSystem                                     │
 *      │  - task workPath temporary output                                    │
 *      │  - FileOutputCommitter commits (rename)                              │
 *      └──────────────────────────────────────────────────────────────────────┘
 *                               │
 *                               │  OS page cache / disk / (or HDFS DataNode)
 *                               ▼
 *      ┌──────────────────────────────────────────────────────────────────────┐
 *      │ Persistent storage (HDD/SSD)                                         │
 *      └──────────────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <p>
 * - 第一层：本类在 reduce() 内把均值写入 <code>outLine.factors[i]</code>（{@link FactorLineWritable} 的 float[20]），
 *   并通过 <code>context.write(NullWritable, outLine)</code> 交给 Hadoop 输出链。
 * </p>
 *
 * <p>
 * - 第二层：输出侧的 {@link DayCsvOutputFormat.DayCsvRecordWriter#write(NullWritable, FactorLineWritable)}
 *   读取 <code>value.factors</code>，将时间与 20 个 float 用 Schubfach 转成 ASCII，拼装进它自己的行缓冲
 *   <code>byte[] buf</code>，然后调用 <code>out.write(buf, 0, pos)</code>。
 * </p>
 *
 * <p>
 * - 第三层：这里的 <code>out</code> 是 {@link java.io.BufferedOutputStream}，内部维护一个更大的私有 byte buffer
 *   （本项目在 {@link DayCsvOutputFormat#getRecordWriter(TaskAttemptContext)} 中构造为 1&lt;&lt;20）。多数行写入会先落在
 *   这个 buffer 里，达到阈值或 flush/close 时再批量写入底层 {@link org.apache.hadoop.fs.FSDataOutputStream}。
 * </p>
 *
 * <p>
 * - 最终落盘：<code>FSDataOutputStream</code> 将数据写入任务的输出工作目录（commit 前的 workPath），由 Hadoop 的输出
 *   commit 机制完成文件重命名与提交操作。在单机/本地运行时，数据最终落至本地文件系统；
 * 在集群环境下，则写入 HDFS 的 DataNode，由底层操作系统及磁盘完成持久化存储。
 * </p>
 *
 * <p>注意：按当前实现假设输入保证 count &gt; 0，因此不对 count==0 做防御性检查。</p>
 */
public final class DayAverageReducer extends Reducer<IntWritable, FactorWritable, NullWritable, FactorLineWritable> {
    private static final int BASE_SEC_6AM = 21600;
    private static final int MASK_TIME15 = (1 << 15) - 1;
    private static final int FACTOR_COUNT = 20;
    private static final int VALUE_SIZE = FACTOR_COUNT + 1; // last is count

    private final FactorLineWritable outLine = new FactorLineWritable();
    private final double[] sums = new double[VALUE_SIZE];

    @Override
    protected void reduce(IntWritable key, Iterable<FactorWritable> values, Context context)
            throws IOException, InterruptedException {
        int packed = key.get();
        int timeCode = packed & MASK_TIME15;
        int secOfDay = BASE_SEC_6AM + timeCode;

        for (int i = 0; i < VALUE_SIZE; i++) sums[i] = 0.0d;
        for (FactorWritable fWtb : values) {
            double[] f = fWtb.factors;
            for (int i = 0; i < VALUE_SIZE; i++) sums[i] += f[i];
        }

        double inv_count = 1.0d / sums[FACTOR_COUNT];
        outLine.secOfDay = secOfDay;
        for (int i = 0; i < FACTOR_COUNT; i++) {
            outLine.factors[i] = (float) (sums[i] * inv_count);
        }
        context.write(NullWritable.get(), outLine);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // no-op
    }

}
