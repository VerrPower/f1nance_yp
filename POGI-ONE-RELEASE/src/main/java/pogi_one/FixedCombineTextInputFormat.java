package pogi_one;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * Combine InputFormat：按交易日分组切分；日间互不跨 split，日内按股票 CSV 聚合到若干 split。
 *
 * <p><b>切分粒度：</b> 单个股票 CSV 是最小粒度（不可再切），单个交易日是最大边界（split 不跨 day）。</p>
 *
 * <p><b>日内切分数：</b> 设 {@code P = min(8, availableProcessors)}，每个交易日的 split 数为
 * {@code min(P, N)}，其中 {@code N} 为该交易日的 CSV 文件数；再按均匀块大小分组。</p>
 *
 * <p><b>上限为 8 的原因：</b> 本项目运行在本地模式（单机多线程），非真实集群；容器资源为
 * 2 物理核 / 4 逻辑核。为避免测试环境变动，核数提高导致切分过细，设置日内切分上限为 8。</p>
 */
public final class FixedCombineTextInputFormat extends CombineFileInputFormat<ShortWritable, Text> {
    public static final int MAX_THREADS = 8;
    static {
        System.out.printf("@ FixedCombineTextInputFormat: maxThreads=%d%n", MAX_THREADS);
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        // 单个 CSV 不可切分。
        return false;
    }

    @Override
    public RecordReader<ShortWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext ctx)
            throws IOException {
        return new CombineFileRecordReader<>((CombineFileSplit) split, ctx, CombineFileLineRR.class);
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<FileStatus> files = listStatus(job);
        List<InputSplit> splits = new ArrayList<>();

        // 【1】扫描所有输入文件并按交易日分组：
        // byDay: 以 dayId 为索引的 DayGroup 数组；每个 dayId 对应一个 DayGroup。
        // dayOrder: 记录发现的 dayId 顺序，用于后续按该顺序生成 split。
        // 对每个文件：解析 dayId -> 取得/创建 DayGroup -> 记录 dayId 顺序 -> 保存路径与文件大小。
        // 保证 split 不跨 day（同一天的文件只会进入同一个 DayGroup）。
        // 路径结构期望：.../<day>/<stock>/snapshot.csv
        DayGroup[] byDay = new DayGroup[1232];
        int[] dayOrder = new int[16];
        int dayCount = 0;
        for (FileStatus stat : files) {
            if (stat.isDirectory()) continue;
            Path path = stat.getPath();
            int dayId = dayIdFromSnapshotPath(path);
            DayGroup g = byDay[dayId];
            if (g == null) {
                g = new DayGroup();
                byDay[dayId] = g;
                if (dayCount == dayOrder.length) {
                    int[] next = new int[dayOrder.length << 1];
                    System.arraycopy(dayOrder, 0, next, 0, dayOrder.length);
                    dayOrder = next;
                }
                dayOrder[dayCount++] = dayId;
            }
            g.add(path, stat.getLen());
        }

        // 【2】按交易日切分生成 CombineFileSplit：
        // p: 日内切分数上限（min(MAX_THREADS, availableProcessors)）。
        // splitsForDay: 该 day 实际 split 数（min(p, n)）。
        // chunkSize: 每个 split 承载的文件数上限（ceil(n / splitsForDay)）。
        // 逐块复制路径与长度，构造 CombineFileSplit 并加入 splits。
        int procs = Runtime.getRuntime().availableProcessors();
        int p = Math.min(MAX_THREADS, Math.max(1, procs));
        for (int di = 0; di < dayCount; di++) {
            DayGroup g = byDay[dayOrder[di]];
            int n = g.size;
            int splitsForDay = 1;
            int chunkSize = (n + splitsForDay - 1) / splitsForDay;

            for (int i = 0; i < n; i += chunkSize) {
                int j = Math.min(i + chunkSize, n);
                Path[] groupPaths = g.copyPaths(i, j);
                long[] groupLengths = g.copyLengths(i, j);
                splits.add(makeSplit(groupPaths, groupLengths));
            }
        }

        return splits;
    }

    /**
     * DayGroup：每个交易日一个实例，保存该 day 目录下的所有股票 CSV 信息（路径与文件大小）。
     * 为保证通用性，内部数组支持自动扩容；初始容量设为 512，避免在常见 300 文件规模下频繁扩容。
     */
    private static final class DayGroup {
        private static final int INIT_CAP = 512;
        private static final double GROWTH_FACTOR = 1.56d;

        private Path[] paths = new Path[INIT_CAP];
        private long[] lengths = new long[INIT_CAP];
        private int size = 0;

        void add(Path path, long len) {
            if (size == paths.length) {
                int nextSize = (int) Math.ceil(size * GROWTH_FACTOR);
                Path[] pNext = new Path[nextSize];
                long[] lNext = new long[nextSize];
                System.arraycopy(paths, 0, pNext, 0, paths.length);
                System.arraycopy(lengths, 0, lNext, 0, lengths.length);
                paths = pNext;
                lengths = lNext;
            }
            paths[size] = path;
            lengths[size] = len;
            size++;
        }

        Path[] copyPaths(int from, int to) {
            int n = to - from;
            Path[] out = new Path[n];
            System.arraycopy(paths, from, out, 0, n);
            return out;
        }

        long[] copyLengths(int from, int to) {
            int n = to - from;
            long[] out = new long[n];
            System.arraycopy(lengths, from, out, 0, n);
            return out;
        }
    }

    /**
     * 从 snapshot.csv 的路径中解析 dayId（MMDD 数值）。
     * 路径结构假设：.../<day>/<stock>/snapshot.csv，其中 day 为四位数字字符串。
     */
    private static int dayIdFromSnapshotPath(Path snapshotPath) {
        // Expect: .../<day>/<stock>/snapshot.csv
        Path p = snapshotPath.getParent().getParent(); // 上两级目录：day
        String day = p.getName(); // day 目录名，格式 MMDD
        // 将 "MMDD" 转为 int（例如 "0102" -> 102）
        int d0 = day.charAt(0) - '0';
        int d1 = day.charAt(1) - '0';
        int d2 = day.charAt(2) - '0';
        int d3 = day.charAt(3) - '0';
        return d0 * 1000 + d1 * 100 + d2 * 10 + d3;
    }

    private static InputSplit makeSplit(Path[] paths, long[] lenArr) {
        long[] start = new long[paths.length];
        return new CombineFileSplit(paths, start, lenArr, new String[0]);
    }

    /**
     * 针对 CombineFileSplit 的行读取器：内部用 LineRecordReader。
     */
    public static class CombineFileLineRR extends RecordReader<ShortWritable, Text> {
        private final int index;
        private final LineRecordReader lineReader = new LineRecordReader();
        private final ShortWritable key = new ShortWritable();

        public CombineFileLineRR(CombineFileSplit split, TaskAttemptContext attemptContext, Integer index) 
            throws IOException 
        {
            this.index = index;
            lineReader.initialize(
                    new FileSplit(
                        split.getPath(index), 
                        split.getOffset(index), 
                        split.getLength(index), 
                        split.getLocations()
                    ),
                    attemptContext
                );
        }

        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext attemptContext)
                throws IOException {
            // no-op: 已在构造函数里初始化。
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            if (!lineReader.nextKeyValue()) {
                return false;
            }
            key.set((short) index);
            return true;
        }

        @Override
        public ShortWritable getCurrentKey() {
            return key;
        }

        @Override
        public Text getCurrentValue() {
            return lineReader.getCurrentValue();
        }

        @Override
        public float getProgress() throws IOException {
            return lineReader.getProgress();
        }

        @Override
        public void close() throws IOException {
            lineReader.close();
        }
    }
}
