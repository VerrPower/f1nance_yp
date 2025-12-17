package factor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
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
 * maxFiles 在类内硬编码为 {@link #MAX_FILES}：每个 mapper 处理的 csv 文件数上限。
 */
public final class FixedCombineTextInputFormat extends CombineFileInputFormat<LongWritable, Text> {
    public static final int MAX_FILES = 300;

    static {
        System.out.printf("@ FixedCombineTextInputFormat: maxFiles=%d%n", MAX_FILES);
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        // 单个 CSV 不可切分。
        return false;
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(org.apache.hadoop.mapreduce.InputSplit split, TaskAttemptContext ctx)
            throws IOException {
        return new CombineFileRecordReader<>((CombineFileSplit) split, ctx, CFLineRecordReader.class);
    }

    @Override
    public List<org.apache.hadoop.mapreduce.InputSplit> getSplits(JobContext job) throws IOException {
        List<FileStatus> files = listStatus(job);
        List<org.apache.hadoop.mapreduce.InputSplit> splits = new ArrayList<>();

        List<Path> groupPaths = new ArrayList<>();
        List<Long> groupLengths = new ArrayList<>();

        for (FileStatus stat : files) {
            if (stat.isDirectory()) {
                continue;
            }
            groupPaths.add(stat.getPath());
            groupLengths.add(stat.getLen());
            if (groupPaths.size() >= MAX_FILES) {
                splits.add(makeSplit(groupPaths, groupLengths));
                groupPaths.clear();
                groupLengths.clear();
            }
        }

        if (!groupPaths.isEmpty()) {
            splits.add(makeSplit(groupPaths, groupLengths));
        }

        return splits;
    }

    private static org.apache.hadoop.mapreduce.InputSplit makeSplit(List<Path> paths, List<Long> lengths) {
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
    public static class CFLineRecordReader extends RecordReader<LongWritable, Text> {
        private final CombineFileSplit split;
        private final int index;
        private final LineRecordReader lineReader = new LineRecordReader();
        private final LongWritable key = new LongWritable();

        // 将 fileIndex 编码进高位，供 Mapper 区分“当前记录来自哪个文件”（用于 t-1 状态隔离）。
        // fileIndex 最大值通常远小于 2^16；低 48 位留给行偏移。
        private static final int FILE_INDEX_SHIFT = 48;
        private static final long OFFSET_MASK = (1L << FILE_INDEX_SHIFT) - 1L;

        public CFLineRecordReader(CombineFileSplit split, TaskAttemptContext ctx, Integer index) throws IOException {
            this.split = split;
            this.index = index;
            initialize(new FileSplit(split.getPath(index), split.getOffset(index), split.getLength(index), split.getLocations()), ctx);
        }

        @Override
        public void initialize(org.apache.hadoop.mapreduce.InputSplit genericSplit, TaskAttemptContext context)
                throws IOException {
            // no-op: 已在构造函数里初始化。
        }

        private void initialize(FileSplit fileSplit, TaskAttemptContext context) throws IOException {
            lineReader.initialize(fileSplit, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            if (!lineReader.nextKeyValue()) {
                return false;
            }
            long offset = lineReader.getCurrentKey().get() & OFFSET_MASK;
            key.set((((long) index) << FILE_INDEX_SHIFT) | offset);
            return true;
        }

        @Override
        public LongWritable getCurrentKey() {
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
