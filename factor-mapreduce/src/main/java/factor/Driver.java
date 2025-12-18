package factor;

import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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
 * <p><b>运行方式：</b>{@code hadoop jar ... factor.Driver <input> <output>}</p>
 */
public class Driver extends Configured implements Tool {
    private static final String CONF_DAY_IDS = "finyp.dayIds";

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

        Path input = resolveInput(conf, new Path(args[0]));

        // 自动发现 day（兼容单日/多日输入）。
        Set<String> dayIds = discoverDayIds(conf, input);
        if (dayIds.isEmpty()) {
            System.err.println("No day directories discovered from input: " + args[0]);
            return 2;
        }
        StringBuilder dayCsv = new StringBuilder(dayIds.size() * 5);
        boolean first = true;
        for (String d : dayIds) {
            if (!first) dayCsv.append(',');
            first = false;
            dayCsv.append(d);
        }
        conf.set(CONF_DAY_IDS, dayCsv.toString());

        Job job = Job.getInstance(conf, "CSI300 Factor Calculation");

        job.setJarByClass(Driver.class);

        // Combine Input：多个 CSV 合为一个 split（单文件不切分），减少 mapper 数。
        job.setInputFormatClass(FixedCombineTextInputFormat.class);

        job.setMapperClass(StockFactorMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(FactorWritable.class);

        job.setPartitionerClass(DayIdPartitioner.class);
        job.setReducerClass(DayAverageReducer.class);
        job.setNumReduceTasks(dayIds.size());

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        // 关键点：输出只写 value（CSV 行），不写 key<TAB>value。
        job.setOutputFormatClass(ValueOnlyTextOutputFormat.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean ok;
        try {
            ok = job.waitForCompletion(true);
        } finally {
            double sec = (System.nanoTime() - startNs) / 1_000_000_000.0;
            System.out.printf(Locale.ROOT, "\n\n@ Driver: ElapsedSec: %.3f%n", sec);
        }
        return ok ? 0 : 1;
    }

    private static Path resolveInput(Configuration conf, Path input) throws Exception {
        FileSystem fs = input.getFileSystem(conf);

        FileStatus[] direct = fs.globStatus(input);
        if (direct == null || direct.length == 0) return input;

        boolean anyFile = false;
        boolean anyDir = false;
        for (FileStatus st : direct) {
            if (st == null) continue;
            if (st.isFile()) {
                anyFile = true;
                break;
            }
            if (st.isDirectory()) anyDir = true;
        }
        if (anyFile || !anyDir) return input;

        // 输入是目录/目录通配符：尝试补全到 snapshot.csv 文件级通配符（不依赖递归遍历）。
        Path p1 = new Path(input, "*/*/snapshot.csv");
        FileStatus[] s1 = fs.globStatus(p1);
        if (s1 != null && s1.length > 0) return p1;

        Path p2 = new Path(input, "*/snapshot.csv");
        FileStatus[] s2 = fs.globStatus(p2);
        if (s2 != null && s2.length > 0) return p2;

        return input;
    }

    private static Set<String> discoverDayIds(Configuration conf, Path input) throws Exception {
        FileSystem fs = input.getFileSystem(conf);

        Set<String> days = new TreeSet<>();

        FileStatus[] direct = fs.globStatus(input);
        if (direct != null) {
            for (FileStatus st : direct) {
                if (st == null) continue;
                if (st.isFile()) {
                    addDayFromSnapshotPath(days, st.getPath());
                } else if (st.isDirectory()) {
                    FileStatus[] files = fs.globStatus(new Path(st.getPath(), "*/*/snapshot.csv"));
                    if (files == null || files.length == 0) {
                        files = fs.globStatus(new Path(st.getPath(), "*/snapshot.csv"));
                    }
                    if (files != null) {
                        for (FileStatus f : files) {
                            if (f != null && f.isFile()) addDayFromSnapshotPath(days, f.getPath());
                        }
                    }
                }
            }
        }

        if (!days.isEmpty()) return days;

        // fallback：如果 input 不是 glob 且 globStatus 返回空，尝试按根目录推断结构
        try {
            FileStatus st = fs.getFileStatus(input);
            if (st.isDirectory()) {
                FileStatus[] files = fs.globStatus(new Path(input, "*/*/snapshot.csv"));
                if (files != null) {
                    for (FileStatus f : files) {
                        if (f != null && f.isFile()) addDayFromSnapshotPath(days, f.getPath());
                    }
                }
            } else if (st.isFile()) {
                addDayFromSnapshotPath(days, input);
            }
        } catch (Exception ignored) {
            // keep empty
        }

        return days;
    }

    private static void addDayFromSnapshotPath(Set<String> days, Path snapshotPath) {
        // Expect: .../<day>/<stock>/snapshot.csv
        Path p = snapshotPath;
        if (p == null) return;
        p = p.getParent(); // stock
        if (p == null) return;
        p = p.getParent(); // day
        if (p == null) return;
        String day = p.getName();
        if (day == null) return;
        if (day.length() == 4) {
            // keep lexicographic order for deterministic mapping
            days.add(day);
        } else {
            // fallback: still add, but zero-pad if it's numeric
            try {
                int id = Integer.parseInt(day);
                days.add(String.format(Locale.ROOT, "%04d", id));
            } catch (NumberFormatException ignored) {
                days.add(day);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new Driver(), args);
        System.exit(rc);
    }
}
