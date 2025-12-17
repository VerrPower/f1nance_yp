package factor;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * MapReduce 复合 Key：把 (tradingDay, secOfDay) 压缩成单个 32-bit int，并用无符号比较做排序与聚合。
 *
 * <p><b>为什么不用 Text 直接拼字符串？</b></p>
 * <ul>
 *   <li>如果 tradeTime 存在前导零（例如 092500），字符串比较可能出现隐式格式差异带来的排序问题。</li>
 *   <li>数值型 compareTo 更稳定，也便于按天分区、按时间排序输出。</li>
 * </ul>
 */
public final class DayTimeKey implements WritableComparable<DayTimeKey> {
    /**
     * CompactTime（32-bit）布局（高位 -> 低位）：
     * <ul>
     *   <li>yearOffset = year - 1990: 6 bit（0..63 -> 1990..2053）</li>
     *   <li>month: 4 bit（1..12）</li>
     *   <li>day: 5 bit（1..31）</li>
     *   <li>secOfDay: 17 bit（0..86399）</li>
     * </ul>
     *
     * <p>由于 yearOffset 在当前数据上通常 >= 32，最高位可能为 1，因此排序必须使用无符号比较。</p>
     */
    private int compactTime;

    private static final int YEAR_BASE = 1990;
    private static final int SHIFT_YEAR = 26;
    private static final int SHIFT_MONTH = 22;
    private static final int SHIFT_DAY = 17;
    private static final int MASK_SEC = (1 << 17) - 1;

    public DayTimeKey() {
        // Hadoop requires a public no-arg constructor.
    }

    public DayTimeKey(int tradingDay, int secOfDay) {
        set(tradingDay, secOfDay);
    }

    public long getTradingDay() {
        int y = (compactTime >>> SHIFT_YEAR) & 0x3F;
        int year = YEAR_BASE + y;
        int month = (compactTime >>> SHIFT_MONTH) & 0x0F;
        int day = (compactTime >>> SHIFT_DAY) & 0x1F;
        return (long) year * 10_000L + (long) month * 100L + (long) day;
    }

    public long getTradeTime() {
        int sec = compactTime & MASK_SEC;
        int hh = sec / 3600;
        int mm = (sec - hh * 3600) / 60;
        int ss = sec - hh * 3600 - mm * 60;
        return (long) (hh * 10_000 + mm * 100 + ss);
    }

    public int getDayCode() {
        // 用于分区：dayCode = (yearOffset, month, day) 打包后的 15-bit 值。
        return compactTime >>> SHIFT_DAY;
    }

    public int getSecOfDay() {
        return compactTime & MASK_SEC;
    }

    public void set(int tradingDay, int secOfDay) {
        int year = tradingDay / 10_000;
        int month = (tradingDay / 100) % 100;
        int day = tradingDay % 100;
        int yearOffset = year - YEAR_BASE;
        this.compactTime = (yearOffset << SHIFT_YEAR) | (month << SHIFT_MONTH) | (day << SHIFT_DAY) | secOfDay;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(compactTime);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.compactTime = in.readInt();
    }

    @Override
    public int compareTo(DayTimeKey other) {
        if (other == null) {
            return 1;
        }
        return Integer.compareUnsigned(this.compactTime, other.compactTime);
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
        return compactTime == that.compactTime;
    }

    @Override
    public int hashCode() {
        return compactTime;
    }

    @Override
    public String toString() {
        return Long.toString(getTradingDay()) + "," + getTradeTime();
    }
}
