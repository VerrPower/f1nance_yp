package factor;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区器：只按 tradingDay 分区，保证不同日期不会在 reducer 端混合。
 *
 * <p>注意：是否“每一天只落到一个 reducer”，取决于 reducer 数量与 partition 规则。
 * 这里的目标是“不同日不混算”；同一天可以根据 reducer 数拆分到不同分区（如果你希望每一天严格单 reducer，
 * 可将 reducer 数设置为天数，或固定 numPartitions=1）。</p>
 */
public final class DayPartitioner extends Partitioner<IntWritable, FactorWritable> {
    @Override
    public int getPartition(IntWritable key, FactorWritable value, int numPartitions) {
        if (numPartitions <= 1) {
            return 0;
        }
        int compact = key == null ? 0 : key.get();
        int dayCode = compact >>> 15;
        return Math.floorMod(dayCode, numPartitions);
    }
}
