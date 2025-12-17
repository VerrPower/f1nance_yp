package factor;

import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
 *   在 mapper 内对同一 (day,time) 做本地聚合与平均，并在 {@code cleanup()} 一次性写出当天 CSV。</li>
 *   <li><b>OutputFormat：</b>{@link ValueOnlyTextOutputFormat} 仅输出 mapper 生成的 CSV 行文本，
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
 * (HDFS 输出目录)
 *   output/
 *     0102.csv-m-00000   (首行表头 + 多行 tradeTime 均值)
 *     0103.csv-m-00000
 *     ...
 * </pre>
 *
 * <p>当前实现为 map-only：每个 mapper 负责一天 300 只股票的 CSV，直接输出当天每个时刻的 20 因子截面平均序列。</p>
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

        // map-only：mapper 直接输出最终 CSV。
        job.setMapperClass(StockFactorMapper.class);
        job.setNumReduceTasks(0);

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
