package factor;

import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Reducer：对 mapper 输出的 sum[20]+count 做汇总并求均值，输出按天的 CSV。
 *
 * <p>注意：按用户要求，count==0 不做防御性检查。</p>
 */
public final class DayAverageReducer extends Reducer<IntWritable, FactorWritable, NullWritable, Text> {
    private static final int BASE_SEC_6AM = 21600;
    private static final int MASK_TIME15 = (1 << 15) - 1;
    private static final int FACTOR_COUNT = 20;
    private static final int VALUE_SIZE = FACTOR_COUNT + 1; // last is count

    private static final Text HEADER = new Text(csvHeaderLine());

    private MultipleOutputs<NullWritable, Text> multipleOutputs;
    private final Text outLine = new Text();
    private final StringBuilder sb = new StringBuilder(256);
    private final double[] sums = new double[VALUE_SIZE];

    private boolean headerWritten = false;
    private int currentDayId = -1;
    private String basePath = null;

    @Override
    protected void setup(Context context) {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(IntWritable key, Iterable<FactorWritable> values, Context context)
            throws IOException, InterruptedException {
        int packed = key.get();
        int dayId = packed >>> 15;
        int timeCode = packed & MASK_TIME15;
        int secOfDay = BASE_SEC_6AM + timeCode;

        if (basePath == null || dayId != currentDayId) {
            currentDayId = dayId;
            basePath = String.format(Locale.ROOT, "%04d.csv", dayId);
            headerWritten = false;
        }

        if (!headerWritten) {
            multipleOutputs.write(NullWritable.get(), HEADER, basePath);
            headerWritten = true;
        }

        for (int i = 0; i < VALUE_SIZE; i++) sums[i] = 0.0d;
        for (FactorWritable v : values) {
            double[] f = v.factors;
            for (int i = 0; i < VALUE_SIZE; i++) sums[i] += f[i];
        }

        double count = sums[FACTOR_COUNT];

        sb.setLength(0);
        appendTradeTimeFromSecOfDay(sb, secOfDay);
        for (int i = 0; i < FACTOR_COUNT; i++) {
            sb.append(',');
            sb.append(Double.toString(sums[i] / count));
        }
        outLine.set(sb.toString());
        multipleOutputs.write(NullWritable.get(), outLine, basePath);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (multipleOutputs != null) multipleOutputs.close();
    }

    private static String csvHeaderLine() {
        StringBuilder sb = new StringBuilder(256);
        sb.append("tradeTime");
        for (int i = 1; i <= FACTOR_COUNT; i++) sb.append(",alpha_").append(i);
        return sb.toString();
    }

    private static void appendTradeTimeFromSecOfDay(StringBuilder sb, int secOfDay) {
        int hh = secOfDay / 3600;
        int t = secOfDay - hh * 3600;
        int mm = t / 60;
        int ss = t - mm * 60;
        append2(sb, hh);
        append2(sb, mm);
        append2(sb, ss);
    }

    private static void append2(StringBuilder sb, int v) {
        int tens = v / 10;
        sb.append((char) ('0' + tens));
        sb.append((char) ('0' + (v - tens * 10)));
    }
}

