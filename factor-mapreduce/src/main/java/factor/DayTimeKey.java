package factor;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * MapReduce 复合 Key：按 (tradingDay, tradeTime) 做数值排序与聚合。
 *
 * <p><b>为什么不用 Text 直接拼字符串？</b></p>
 * <ul>
 *   <li>如果 tradeTime 存在前导零（例如 092500），字符串比较可能出现隐式格式差异带来的排序问题。</li>
 *   <li>数值型 compareTo 更稳定，也便于按天分区、按时间排序输出。</li>
 * </ul>
 */
public final class DayTimeKey implements WritableComparable<DayTimeKey> {
    private long tradingDay;
    private long tradeTime;

    public DayTimeKey() {
        // Hadoop requires a public no-arg constructor.
    }

    public DayTimeKey(long tradingDay, long tradeTime) {
        this.tradingDay = tradingDay;
        this.tradeTime = tradeTime;
    }

    public long getTradingDay() {
        return tradingDay;
    }

    public long getTradeTime() {
        return tradeTime;
    }

    public void set(long tradingDay, long tradeTime) {
        this.tradingDay = tradingDay;
        this.tradeTime = tradeTime;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(tradingDay);
        out.writeLong(tradeTime);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.tradingDay = in.readLong();
        this.tradeTime = in.readLong();
    }

    @Override
    public int compareTo(DayTimeKey other) {
        if (other == null) {
            return 1;
        }
        // 先按交易日排序，再按交易时间排序，确保 reducer 端的遍历顺序与“按天按时序列”一致。
        int dayCmp = Long.compare(this.tradingDay, other.tradingDay);
        if (dayCmp != 0) {
            return dayCmp;
        }
        return Long.compare(this.tradeTime, other.tradeTime);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DayTimeKey)) {
            return false;
        }
        DayTimeKey that = (DayTimeKey) o;
        return tradingDay == that.tradingDay && tradeTime == that.tradeTime;
    }

    @Override
    public int hashCode() {
        int result = (int) (tradingDay ^ (tradingDay >>> 32));
        result = 31 * result + (int) (tradeTime ^ (tradeTime >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return tradingDay + "," + tradeTime;
    }
}
