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
                pos = writeFloatAsciiShortest(buf, pos, f[i], digits);
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

    private static int writeFloatAsciiShortest(byte[] buf, int pos, float v) {
        return writeFloatAsciiShortest(buf, pos, v, null);
    }

    private static int writeFloatAsciiShortest(byte[] buf, int pos, float v, byte[] digits) {
        return SchubfachFloat.writeShortest(v, buf, pos, digits);
    }

}
