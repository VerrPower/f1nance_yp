package factor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
        Map<String, DayGroup> byDay = new LinkedHashMap<>();
        for (FileStatus stat : files) {
            if (stat.isDirectory()) continue;
            Path path = stat.getPath();
            String day = dayFromSnapshotPath(path);
            DayGroup g = byDay.get(day);
            if (g == null) {
                g = new DayGroup();
                byDay.put(day, g);
            }
            g.paths.add(path);
            g.lengths.add(stat.getLen());
        }

        int splitIndex = 0;
        for (Map.Entry<String, DayGroup> e : byDay.entrySet()) {
            DayGroup g = e.getValue();
            int n = g.paths.size();
            int procs = Runtime.getRuntime().availableProcessors();
            int p = Math.min(MAX_THREADS, Math.max(1, procs));
            int splitsForDay = Math.min(p, n);
            int chunkSize = (n + splitsForDay - 1) / splitsForDay;

            for (int i = 0; i < n; i += chunkSize) {
                int j = Math.min(i + chunkSize, n);
                List<Path> groupPaths = new ArrayList<>(g.paths.subList(i, j));
                List<Long> groupLengths = new ArrayList<>(g.lengths.subList(i, j));
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
        final List<Path> paths = new ArrayList<>();
        final List<Long> lengths = new ArrayList<>();
    }

    private static void debugPrintGroup(int splitIndex, List<Path> groupPaths) {
        if (groupPaths.isEmpty()) {
            return;
        }
        String firstDay = dayFromSnapshotPath(groupPaths.get(0));
        String lastDay = dayFromSnapshotPath(groupPaths.get(groupPaths.size() - 1));
        boolean singleDay = true;
        for (int i = 1; i < groupPaths.size(); i++) {
            if (!firstDay.equals(dayFromSnapshotPath(groupPaths.get(i)))) {
                singleDay = false;
                break;
            }
        }
        System.out.printf(
                "@ SplitPlan #%d: files=%d singleDay=%s firstDay=%s lastDay=%s first=%s last=%s%n",
                splitIndex,
                groupPaths.size(),
                singleDay ? "Y" : "N",
                firstDay,
                lastDay,
                groupPaths.get(0),
                groupPaths.get(groupPaths.size() - 1));
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

    private static InputSplit makeSplit(List<Path> paths, List<Long> lengths) {
        Path[] pArr = paths.toArray(new Path[0]);
        long[] start = new long[pArr.length];
        long[] lenArr = new long[pArr.length];
        for (int i = 0; i < pArr.length; i++) {
            start[i] = 0L;
            lenArr[i] = lengths.get(i);
        }
        return new CombineFileSplit(pArr, start, lenArr, new String[0]);
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
