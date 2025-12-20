package pogi_one;

import java.util.Arrays;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 将同一交易日的所有 key 路由到同一个 reducer（1 day / 1 reducer）。
 *
 * <p>Driver 会把 dayId 列表（发现顺序，未排序）写入 conf：finyp.dayIds=0102,0103,...</p>
 *
 * <p>Mapper 的 key 打包规则：key = (dayId &lt;&lt; 15) | timeCode，其中 dayId 是 MMDD（int）。</p>
 */
public final class DayIdPartitioner extends Partitioner<IntWritable, FactorWritable> implements Configurable {
    private static final String CONF_DAY_IDS = "finyp.dayIds";

    private Configuration conf;
    private int[] dayIdToPartition; // index by dayId (0..9999), value is partition or -1
    private int cachedNumPartitions = -1;

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        this.dayIdToPartition = null;
        this.cachedNumPartitions = -1;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public int getPartition(IntWritable key, FactorWritable value, int numPartitions) {
        ensureInitialized(numPartitions);
        int packed = key.get();
        int dayId = packed >>> 15;
        if (dayId >= 0 && dayId < dayIdToPartition.length) {
            int p = dayIdToPartition[dayId];
            if (p >= 0) return p;
        }
        return (dayId & Integer.MAX_VALUE) % numPartitions;
    }

    private void ensureInitialized(int numPartitions) {
        if (dayIdToPartition != null && cachedNumPartitions == numPartitions) return;

        int[] map = new int[10_000];
        Arrays.fill(map, -1);

        String csv = (conf == null) ? null : conf.get(CONF_DAY_IDS);
        if (csv != null && !csv.trim().isEmpty()) {
            String[] parts = csv.split(",");
            int n = Math.min(parts.length, numPartitions);
            for (int i = 0; i < n; i++) {
                String s = parts[i].trim();
                if (s.isEmpty()) continue;
                int dayId = Integer.parseInt(s);
                if (dayId >= 0 && dayId < map.length) map[dayId] = i;
            }
        }

        this.dayIdToPartition = map;
        this.cachedNumPartitions = numPartitions;
    }
}
