package factor;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Combiner that pre-aggregates factor sums and counts per (day,time) key to
 * reduce shuffle size.
 */
public class FactorCombiner extends Reducer<DayTimeKey, FactorWritable, DayTimeKey, FactorWritable> {

    @Override
    protected void reduce(DayTimeKey key, Iterable<FactorWritable> values, Context context) throws IOException, InterruptedException {
        FactorWritable sum = new FactorWritable();
        for (FactorWritable val : values) {
            sum.add(val);
        }
        // 注意：Combiner 仅做“可交换可结合”的求和，不做最终平均。
        context.write(key, sum);
    }
}
