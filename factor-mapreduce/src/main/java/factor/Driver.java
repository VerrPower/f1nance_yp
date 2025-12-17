package factor;

import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MapReduce 作业入口（Driver）。
 *
 * <p><b>项目目标：</b> 对 CSI300（300只股票）的 Level-10 行情快照（3秒频）计算 20 个订单簿因子，
 * 并在每个时间点对 300 只股票做截面平均，输出每天从 9:30 到 15:00 的平均因子序列。</p>
 *
 * <p><b>整体流水线架构：</b></p>
 * <ol>
 *   <li><b>InputFormat：</b>{@link FixedCombineTextInputFormat} 组合多个 CSV 为一个 split（单文件不切分），
 *   以减少 mapper 数量。</li>
 *   <li><b>Mapper：</b>{@link StockFactorMapper} 逐行解析 CSV（直接在 {@code byte[]} 上做 ASCII 扫描），计算 20 个因子，
 *   输出键为 30-bit {@link IntWritable}（day+time 压缩后的 CompactTime），值为 {@link FactorWritable}(20 维因子向量“求和态”。)</li>
 *   <li><b>Combiner：</b>{@link FactorCombiner} 对同一个 (day,time) 的 value 做本地预聚合
 *   （逐维求和），减少 shuffle 传输量。</li>
 *   <li><b>Partitioner：</b>{@link DayPartitioner} 仅按 tradingDay 分区，避免不同日期混到同一个 reduce 分区。</li>
 *   <li><b>Reducer：</b>{@link AverageReducer} 计算每个 (day,time) 的 300股截面平均，
 *   并用 {@code MultipleOutputs} 按天输出 CSV（含表头）。</li>
 *   <li><b>OutputFormat：</b>{@link ValueOnlyTextOutputFormat} 仅输出 reducer 生成的 CSV 行文本，
 *   避免 Hadoop 默认的 {@code key<TAB>value} 破坏 CSV 格式。</li>
 * </ol>
 *
 * <p><b>流水线本身示意图</b></p>
 * <pre>
 * (HDFS 输入目录，包含多个股票文件。结构：/day=YYYYMMDD/stock=XXXXXX/snapshot.csv)
 *                 |
 *                 v
 *   +------------------------------+
 *   | InputFormat 读取文本行（CSV） |
 *   | Combine：多个文件合成一个 split |
 *   +------------------------------+
 *                 |
 *                 v
 *   +------------------------------+
 *   | Mapper（逐行）                |
 *   | 1) 跳过表头/空行              |
 *   | 2) 解析 CSV（取前5档）        |
 *   | 3) 计算 20 因子               |
 *   |    - alpha_17/18/19 依赖 t-1  |
 *   | 4) 输出：                     |
 *   |    key   = (tradingDay,tradeTime)
 *   |    value = sum[20]           |
 *   +------------------------------+
 *                 |
 *                 v
 *   +------------------------------+
 *   | Combiner（可选，本地预聚合）  |
 *   | 对同 key：sum += sum          |
 *   +------------------------------+
 *                 |
 *                 v
 *   +-----------------------------------------------+
 *   | Shuffle/Sort                                   |
 *   | - 按 DayPartitioner：以 tradingDay 分区        |
 *   | - 分区内按 CompactTime 排序：(day,time) 升序     |
 *   +-----------------------------------------------+
 *                 |
 *                 v
 *   +----------------------------------------------+
 *   | Reducer（按 key 聚合所有股票）                |
 *   | 1) 汇总：total[20] = Σ sum[20], totalCount=Σcount
 *   | 2) 平均：avg[i] = total[i] / totalCount       |
 *   | 3) 生成 CSV 行：tradeTime,alpha_1..alpha_20    |
 *   | 4) MultipleOutputs：按 tradingDay 分目录输出   |
 *   +----------------------------------------------+
 *                 |
 *                 v
 * (HDFS 输出目录)
 *   output/
 *     tradingDay=20240102/part-r-00000   (首行表头 + 多行 tradeTime 均值)
 *     tradingDay=20240103/part-r-00000
 *     ...
 * </pre>
 *
 * <p>Mapper 在“股票维度”计算单股因子；Reducer 在“时间维度”对 CSI300 做截面平均。
 * 这保证输出直接满足作业要求的“每个时刻 300 只股票平均因子值序列”。</p>
 *
 * <p><b>运行方式：</b>{@code hadoop jar ... factor.Driver <input> <output>}</p>
 */
public class Driver {

    public static void main(String[] args) throws Exception {
        System.out.println("@ Driver: Start timing");
        final long startNs = System.nanoTime();

        if (args.length != 2) {
            System.err.println("Usage: Driver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CSI300 Factor Calculation");
        
        job.setJarByClass(Driver.class);

        // Combine Input：多个 CSV 合为一个 split（单文件不切分），减少 mapper 数。
        job.setInputFormatClass(FixedCombineTextInputFormat.class);

        // 固定 double（float 精度不满足 validate 门限，且提速不明显）。
        job.setMapperClass(StockFactorMapper.class);
        job.setCombinerClass(FactorCombiner.class);
        job.setReducerClass(AverageReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(FactorWritable.class);
        
        // 关键点：使用数值型复合 key，按 (tradingDay, tradeTime) 聚合与排序，并按天分区输出。
        job.setPartitionerClass(DayPartitioner.class);

        // 默认使用单 reducer：保证每个日期文件只写一次表头，且避免同一天被多个 reducer 写入导致重复表头。
        // 如需性能调优，可通过 -Dfactor.reducers=N 覆盖；若 N>1，请确保分区策略与下游合并逻辑能处理多输出文件。
        job.setNumReduceTasks(conf.getInt("factor.reducers", 1));

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        // 关键点：输出只写 value（CSV 行），不写 key<TAB>value。
        job.setOutputFormatClass(ValueOnlyTextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean ok;
        try {
            ok = job.waitForCompletion(true);
        } finally {
            double sec = (System.nanoTime() - startNs) / 1_000_000_000.0;
            System.out.printf(Locale.ROOT, "\n\n@ Driver: ElapsedSec: %.3f%n", sec);
        }
        System.exit(ok ? 0 : 1);
    }
}
