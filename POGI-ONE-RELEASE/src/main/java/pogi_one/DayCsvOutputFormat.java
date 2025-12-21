package pogi_one;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import schubfach.SchubfachFloat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 输出格式：将 {@link DayAverageReducer} 产生的 {@link FactorLineWritable} 按天写成 CSV 文件（仅写 value，不写 key）。
 *
 * <p>
 * 流水线上游是 mapper 输出 (dayId,timeCode)->sum[20]+count（{@link FactorWritable}），
 * reducer 计算均值后产出 {@link FactorLineWritable}，本类负责把它序列化成 CSV 行字节。
 * </p>
 *
 * <p>
 * 文件名由 reducerId 在 {@code finyp.dayIds=0102,0103,...} 中定位得到，对应输出 {@code <dayId>.csv}。
 * 实际写入发生在 {@link FileOutputCommitter#getWorkPath()}，任务成功后再由 commit 流程提交。
 * </p>
 *
 * <p>
 * RecordWriter 内部复用 {@code byte[] buf} 组装整行，浮点数通过
 * {@link schubfach.SchubfachFloat#writeShortest(float, byte[], int, byte[])} 直接写入 buf，
 * 避免 {@code Float.toString()/StringBuilder} 的临时对象，再写入 {@link BufferedOutputStream} 的大缓冲。
 * </p>
 */
public final class DayCsvOutputFormat extends FileOutputFormat<NullWritable, FactorLineWritable> {

    private static final String CONF_DAY_IDS = "finyp.dayIds";
    private static final int FACTOR_COUNT = 20;
    private static final int LINE_BUF_SIZE = 1024;
    private static final byte[] HEADER_BYTES = buildHeaderBytes();

    private static byte[] buildHeaderBytes() {
        StringBuilder sb = new StringBuilder(256);
        sb.append("tradeTime");
        for (int i = 1; i <= FACTOR_COUNT; i++) sb.append(",alpha_").append(i);
        sb.append('\n');
        return sb.toString().getBytes(StandardCharsets.US_ASCII);
    }

    @Override
    public RecordWriter<NullWritable, FactorLineWritable> getRecordWriter(TaskAttemptContext context)
            throws IOException {
        Configuration conf = context.getConfiguration();
        int reducerId = context.getTaskAttemptID().getTaskID().getId();
        String dayId = dayIdForReducer(conf, reducerId);

        Path outputPath = getOutputPath(context);
        FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
        Path workPath = committer.getWorkPath();
        FileSystem fs = workPath.getFileSystem(conf);
        Path outputFile = new Path(workPath, dayId + ".csv");
        FSDataOutputStream fileOut = fs.create(outputFile, true);
        BufferedOutputStream out = new BufferedOutputStream(fileOut, 1 << 20);
        out.write(HEADER_BYTES);
        return new DayCsvRecordWriter(out);
    }

    /**
     * 从 conf 的 {@code finyp.dayIds} 中按顺序取出第 reducerId 个 dayId。
     *
     * <p>依赖 Driver 写入的顺序：dayId 发现顺序与 reducerId 一一对应。</p>
     */
    private static String dayIdForReducer(Configuration conf, int reducerId) {
        String csv = conf.get(CONF_DAY_IDS, "");
        int start = 0;
        int part = 0;
        int len = csv.length();
        for (int i = 0; i <= len; i++) {
            if (i == len || csv.charAt(i) == ',') {
                if (part == reducerId) 
                    return csv.substring(start, i);
                part++;
                start = i + 1;
            }
        }
        return "";
    }

    /**
     * Reducer 输出的逐行写入器：把一条 {@link FactorLineWritable} 转成 CSV 行并写到输出流。
     *
     * <p>内部复用行缓冲与数字缓冲，尽量减少对象分配。</p>
     */
    private static final class DayCsvRecordWriter extends RecordWriter<NullWritable, FactorLineWritable> {
        private final BufferedOutputStream out;
        private final byte[] buf = new byte[LINE_BUF_SIZE];
        private final byte[] digits = new byte[16];

        private DayCsvRecordWriter(BufferedOutputStream out) {
            this.out = out;
        }

        @Override
        public void write(NullWritable key, FactorLineWritable value) throws IOException {
            if (value == null) return;
            int pos = 0;
            pos = writeTime(buf, pos, value.secOfDay);
            float[] f = value.factors;
            for (int i = 0; i < FACTOR_COUNT; i++) {
                buf[pos++] = (byte) ',';
                pos = SchubfachFloat.writeShortest(f[i], buf, pos, digits);
            }
            out.write(buf, 0, pos);
            out.write('\n');
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException {
            out.flush();
            out.close();
        }
    }

    private static int writeTime(byte[] buf, int pos, int secOfDay) {
        int hh = secOfDay / 3600;
        int t = secOfDay - hh * 3600;
        int mm = t / 60;
        int ss = t - mm * 60;
        pos = append2Digits(buf, pos, hh);
        pos = append2Digits(buf, pos, mm);
        return append2Digits(buf, pos, ss);
    }

    private static int append2Digits(byte[] buf, int pos, int v) {
        int tens = v / 10;
        buf[pos++] = (byte) ('0' + tens);
        buf[pos++] = (byte) ('0' + (v - tens * 10));
        return pos;
    }

}
