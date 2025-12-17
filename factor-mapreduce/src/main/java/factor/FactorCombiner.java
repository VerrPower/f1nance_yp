package factor;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;

/**
 * Combiner that pre-aggregates factor sums and counts per (day,time) key to
 * reduce shuffle size.
 */
public class FactorCombiner extends Reducer<IntWritable, FactorWritable, IntWritable, FactorWritable> {
    private final FactorWritable out = new FactorWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<FactorWritable> values, Context context) throws IOException, InterruptedException {
        Arrays.fill(out.factors, 0.0d);
        for (FactorWritable val : values) {
            double[] f = val.factors;
            for (int i = 0; i < 20; i++) {
                out.factors[i] += f[i];
            }
        }
        // 注意：Combiner 仅做“可交换可结合”的求和，不做最终平均。
        context.write(key, out);
    }
}
