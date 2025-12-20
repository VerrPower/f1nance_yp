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
 * Combine InputFormat：将多个 CSV 合并为一个 split（按文件数控制），每个文件保持不切分。
 *
 * 关键目标：
 * <ul>
 *   <li><b>split 不跨交易日</b>：day 之间任务完全独立。</li>
 *   <li><b>day 内均匀切分</b>：若该 day 有 N 个股票 csv，则按 P 份均匀切分，P 写死为
 *       {@code min(8, Runtime.getRuntime().availableProcessors())}。</li>
 * </ul>
 */
public final class FixedCombineTextInputFormat extends CombineFileInputFormat<ShortWritable, Text> {
    public static final int MAX_THREADS = 8;
    private static final boolean PRINT_SPLIT_PLAN = false;

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

        // 关键保证：split 不跨交易日（day 之间任务完全独立；且 mapper 的 dayId 取自首行）。
        // 路径结构期望：.../<day>/<stock>/snapshot.csv
        DayGroup[] byDay = new DayGroup[10_000];
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

        int splitIndex = 0;
        for (int di = 0; di < dayCount; di++) {
            DayGroup g = byDay[dayOrder[di]];
            int n = g.size;
            int procs = Runtime.getRuntime().availableProcessors();
            int p = Math.min(MAX_THREADS, Math.max(1, procs));
            int splitsForDay = Math.min(p, n);
            int chunkSize = (n + splitsForDay - 1) / splitsForDay;

            for (int i = 0; i < n; i += chunkSize) {
                int j = Math.min(i + chunkSize, n);
                Path[] groupPaths = g.copyPaths(i, j);
                long[] groupLengths = g.copyLengths(i, j);
                if (PRINT_SPLIT_PLAN) {
                    debugPrintGroup(splitIndex, groupPaths);
                }
                splits.add(makeSplit(groupPaths, groupLengths));
                splitIndex++;
            }
        }

        return splits;
    }

    private static final class DayGroup {
        private Path[] paths = new Path[16];
        private long[] lengths = new long[16];
        private int size = 0;

        void add(Path path, long len) {
            if (size == paths.length) {
                int next = paths.length << 1;
                Path[] pNext = new Path[next];
                long[] lNext = new long[next];
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

    private static void debugPrintGroup(int splitIndex, Path[] groupPaths) {
        if (groupPaths.length == 0) {
            return;
        }
        String firstDay = dayFromSnapshotPath(groupPaths[0]);
        String lastDay = dayFromSnapshotPath(groupPaths[groupPaths.length - 1]);
        boolean singleDay = true;
        for (int i = 1; i < groupPaths.length; i++) {
            if (!firstDay.equals(dayFromSnapshotPath(groupPaths[i]))) {
                singleDay = false;
                break;
            }
        }
        System.out.printf(
                "@ SplitPlan #%d: files=%d singleDay=%s firstDay=%s lastDay=%s first=%s last=%s%n",
                splitIndex,
                groupPaths.length,
                singleDay ? "Y" : "N",
                firstDay,
                lastDay,
                groupPaths[0],
                groupPaths[groupPaths.length - 1]);
    }

    private static String dayFromSnapshotPath(Path snapshotPath) {
        // Expect: .../<day>/<stock>/snapshot.csv
        Path p = snapshotPath;
        if (p == null) return "?";
        p = p.getParent(); // stock
        if (p == null) return "?";
        p = p.getParent(); // day
        if (p == null) return "?";
        return p.getName();
    }

    private static int dayIdFromSnapshotPath(Path snapshotPath) {
        String day = dayFromSnapshotPath(snapshotPath);
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

        public CombineFileLineRR(CombineFileSplit split, TaskAttemptContext attemptContext, Integer index) throws IOException {
            this.index = index;
            lineReader.initialize(
                    new FileSplit(split.getPath(index), split.getOffset(index), split.getLength(index), split.getLocations()),
                    attemptContext);
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
