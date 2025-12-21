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
 * <p><b>Driver 职责：</b> 解析输入/输出路径，构建 Job 配置并提交作业。</p>
 *
 * <p><b>整体流水线架构：</b></p>
 * <p>
 * InputFormat：{@link FixedCombineTextInputFormat} 实现 day 间任务隔离，day 内组合多个股票 csv 为一个 split，单文件不切分。
 * </p><p>
 * Mapper：{@link StockFactorMapper} 逐行解析 CSV，在 {@code byte[]} 上做 ASCII 扫描，计算 20 个因子，
 * 在 mapper 内对同一 (day,time) 做本地聚合，{@code cleanup()} 输出 {@code (dayId,time)->sum[20]+count}。
 * </p><p>
 * Partitioner：{@link DayIdPartitioner} 保证 1 day / 1 reducer。
 * </p><p>
 * Reducer：{@link DayAverageReducer} 一个 reducer 负责一个交易日，汇总各 split 的 sum+count，压缩股票维度（求均值）并写入 csv。
 * </p><p>
 * OutputFormat：{@link DayCsvOutputFormat} 输出 reducer 生成的 CSV 行字节，只保留 value。
 * </p>
 *
 * <p><b>Driver配置：</b></p>
 * <p>
 * 本地并行度：读取 {@code Runtime.getRuntime().availableProcessors()}，写入
 * {@code mapreduce.local.map.tasks.maximum} 与 {@code mapreduce.local.reduce.tasks.maximum}。
 * </p><p>
 * 输入路径：传入数据的根目录 {@code <root>} ，股票的快照文件路径为 {@code <root>/* /* /snapshot.csv}。
 * </p><p>
 * dayIds 探测：扫描根目录一级子目录（MMDD），按出现顺序拼成 {@code finyp.dayIds=0102,0103,...} 写入 conf。
 * </p><p>
 * Reducer 数：与 dayIds 数量一致，确保 1 day / 1 reducer。
 * </p><p>
 * 输出格式：设置为 {@link DayCsvOutputFormat}，仅输出 CSV 行内容。
 * </p>
 */
public class Driver extends Configured implements Tool {
    
    private static final String CONF_DAY_IDS = "finyp.dayIds";
    private static final String JOB_NAME = "CSI300 Factor Calculation - POGI-ONE FINAL RELEASE";
    

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new Driver(), args);
        System.exit(rc);
    }


    @Override
    public int run(String[] args) throws Exception {
        final long startNs = System.nanoTime();

        Configuration conf = new Configuration();
        setConf(conf);

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

    
    /**
     * 扫描输入根目录的一级子目录名作为 dayId（MMDD），按发现顺序返回。
     *
     * <p>无排序 无格式校验 无不去重。</p>
     */
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

}
