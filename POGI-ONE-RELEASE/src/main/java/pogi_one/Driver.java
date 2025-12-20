package pogi_one;

import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * MapReduce 作业入口（Driver）。
 *
 * <p><b>项目目标：</b> 对 CSI300（300只股票）的 Level-10 行情快照（3秒频）计算 20 个订单簿因子，
 * 并在每个时间点对 300 只股票做截面平均，输出每天从 9:30 到 15:00 的平均因子序列。</p>
 *
 * <p><b>整体流水线架构：</b></p>
 * <ol>
 *   <li><b>InputFormat：</b>{@link FixedCombineTextInputFormat} 组合多个 CSV 为一个 split（单文件不切分），
 *   以减少 mapper 数量，并保证 split 不跨 day。</li>
 *   <li><b>Mapper：</b>{@link StockFactorMapper} 逐行解析 CSV（直接在 {@code byte[]} 上做 ASCII 扫描），计算 20 个因子，
 *   在 mapper 内对同一 (day,time) 做本地聚合，输出 {@code (dayId,time)->sum[20]+count}。</li>
 *   <li><b>Partitioner：</b>{@link DayIdPartitioner} 保证 1 day / 1 reducer。</li>
 *   <li><b>Reducer：</b>{@link DayAverageReducer} 汇总各 split 的 sum+count，并计算截面均值输出当天 CSV。</li>
 *   <li><b>OutputFormat：</b>{@link DayCsvOutputFormat} 直接输出 reducer 生成的 CSV 行字节，
 *   只保留 value，避免 Hadoop 默认的 {@code key<TAB>value} 破坏 CSV 格式。</li>
 * </ol>
 *
 * <p><b>流水线本身示意图</b></p>
 * <pre>
 * (HDFS 输入目录，包含多个股票文件。结构：/<MMDD>/<stock>/snapshot.csv)
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
 *   |    key   = (dayId,time)      |
 *   |    value = sum[20] + count   |
 *   +------------------------------+
 *                 |
 * (HDFS 输出目录)
 *   output/
 *     0102.csv   (首行表头 + 多行 tradeTime 均值)
 *     0103.csv
 *     ...
 * </pre>
 *
 * <p><b>运行方式：</b>{@code hadoop jar ... pogi_one.Driver <input> <output>}</p>
 */
public class Driver extends Configured implements Tool {
    
    private static final String CONF_DAY_IDS = "finyp.dayIds";
    private static final String JOB_NAME = "CSI300 Factor Calculation - POGI-ONE FINAL RELEASE";
    
    @Override
    public int run(String[] args) throws Exception {
        System.out.println("@ Driver: Start timing");
        final long startNs = System.nanoTime();

        if (args.length != 2) {
            System.err.println("Usage: Driver <input path> <output path>");
            return 2;
        }

        Configuration conf = getConf();
        if (conf == null) {
            conf = new Configuration();
            setConf(conf);
        }

        int procs = Runtime.getRuntime().availableProcessors();
        conf.setInt("mapreduce.local.map.tasks.maximum", procs);
        conf.setInt("mapreduce.local.reduce.tasks.maximum", procs);

        // args[0] 必定是数据根目录，结构固定为：
        // <root>/<MMDD>/<stock>/snapshot.csv
        Path inputRoot = new Path(args[0]);
        Path inputFiles = new Path(inputRoot, "*/*/snapshot.csv");

        // 自动发现 day：仅扫描根目录的一级子目录名（MMDD）。
        String[] dayIds = discoverDayIdsFromRoot(conf, inputRoot);
        StringBuilder dayCsv = new StringBuilder(dayIds.length * 5);
        for (int i = 0; i < dayIds.length; i++) {
            if (i > 0) dayCsv.append(',');
            dayCsv.append(dayIds[i]);
        }
        conf.set(CONF_DAY_IDS, dayCsv.toString());

        Job job = Job.getInstance(conf, JOB_NAME);

        job.setJarByClass(Driver.class);

        // Combine Input：多个 CSV 合为一个 split（单文件不切分），减少 mapper 数。
        job.setInputFormatClass(FixedCombineTextInputFormat.class);

        job.setMapperClass(StockFactorMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(FactorWritable.class);

        job.setPartitionerClass(DayIdPartitioner.class);
        job.setReducerClass(DayAverageReducer.class);
        job.setNumReduceTasks(dayIds.length);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FactorLineWritable.class);
        // 关键点：输出只写 value（CSV 行），不写 key<TAB>value。
        job.setOutputFormatClass(DayCsvOutputFormat.class);

        FileInputFormat.addInputPath(job, inputFiles);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean ok;
        ok = job.waitForCompletion(true);
        double sec = (System.nanoTime() - startNs) / 1_000_000_000.0;
        System.out.printf(Locale.ROOT, "\n\n@ Driver: ElapsedSec: %.4f%n", sec);
        return ok ? 0 : 1;
    }

    private static String[] discoverDayIdsFromRoot(Configuration conf, Path inputRoot) throws Exception {
        FileSystem fs = inputRoot.getFileSystem(conf);
        RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(inputRoot);
        String[] days = new String[16];
        int count = 0;
        while (iter.hasNext()) {
            LocatedFileStatus st = iter.next();
            if (st == null || !st.isDirectory()) continue;
            String day = st.getPath().getName();
            if (day != null) {
                if (count == days.length) {
                    String[] next = new String[days.length << 1];
                    System.arraycopy(days, 0, next, 0, days.length);
                    days = next;
                }
                days[count++] = day;
            }
        }
        if (count == days.length) return days;
        String[] trimmed = new String[count];
        System.arraycopy(days, 0, trimmed, 0, count);
        return trimmed;
    }

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new Driver(), args);
        System.exit(rc);
    }
}
