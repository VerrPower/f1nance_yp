package pogi_one;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer：对 mapper 输出的 sum[20]+count 做汇总并求均值，输出按天的 CSV。
 *
 * <p>注意：按用户要求，count==0 不做防御性检查。</p>
 */
public final class DayAverageReducer extends Reducer<IntWritable, FactorWritable, NullWritable, FactorLineWritable> {
    private static final int BASE_SEC_6AM = 21600;
    private static final int MASK_TIME15 = (1 << 15) - 1;
    private static final int FACTOR_COUNT = 20;
    private static final int VALUE_SIZE = FACTOR_COUNT + 1; // last is count

    private final FactorLineWritable outLine = new FactorLineWritable();
    private final double[] sums = new double[VALUE_SIZE];

    @Override
    protected void reduce(IntWritable key, Iterable<FactorWritable> values, Context context)
            throws IOException, InterruptedException {
        int packed = key.get();
        int timeCode = packed & MASK_TIME15;
        int secOfDay = BASE_SEC_6AM + timeCode;

        for (int i = 0; i < VALUE_SIZE; i++) sums[i] = 0.0d;
        for (FactorWritable fWtb : values) {
            double[] f = fWtb.factors;
            for (int i = 0; i < VALUE_SIZE; i++) sums[i] += f[i];
        }

        double inv_count = 1.0d / sums[FACTOR_COUNT];
        outLine.secOfDay = secOfDay;
        for (int i = 0; i < FACTOR_COUNT; i++) {
            outLine.factors[i] = (float) (sums[i] * inv_count);
        }
        context.write(NullWritable.get(), outLine);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // no-op
    }

}
