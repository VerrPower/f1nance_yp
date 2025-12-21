package pogi_one;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 将同一交易日的所有 key 路由到同一个 reducer（1 day / 1 reducer）。
 *
 * <p>Driver 会把 dayId 列表按照发现顺序写入 conf：finyp.dayIds=0102,0103,...</p>
 *
 * <p>Mapper 的 key 打包规则：key = (dayId &lt;&lt; 15) | timeCode，其中 dayId 是 MMDD（int）。</p>
 */
public final class DayIdPartitioner extends Partitioner<IntWritable, FactorWritable> implements Configurable {
    private static final String CONF_DAY_IDS = "finyp.dayIds";

    private Configuration conf;
    private int[] dayIdToPartition; // index by dayId (0..1231)
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
        int dayId = key.get() >>> 15;
        return dayIdToPartition[dayId];
    }
    // dayId按照发现顺序领取属于自己的reducerId reducerId从0开始递增 
    private void ensureInitialized(int numPartitions) {
        if (dayIdToPartition != null && cachedNumPartitions == numPartitions) return;

        int[] map = new int[1232];

        // finyp.dayIds 由 Driver 以“纯数字+逗号”顺序拼接（如 0102,0103,...），
        // 直接手写扫描并映射 dayId->reducerId，避免 split/parseInt 的临时对象。
        String confDayIds = conf.get(CONF_DAY_IDS);
        int reducerId = 0;
        int dayId = 0;
        for (int i = 0, len = confDayIds.length(); i <= len && reducerId < numPartitions; i++) {
            char c = (i == len) ? ',' : confDayIds.charAt(i);
            if (c == ',') {
                map[dayId] = reducerId;
                reducerId++;
                dayId = 0;
            } else dayId = dayId * 10 + (c - '0');
        }
        this.dayIdToPartition = map;
        this.cachedNumPartitions = numPartitions;
    }
}
