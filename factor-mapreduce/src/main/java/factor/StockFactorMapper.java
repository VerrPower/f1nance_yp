package factor;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper：将“单股票单日”快照 CSV 行映射为 (tradingDay, tradeTime) -> 因子向量。
 *
 * <p>关键点：</p>
 * <ul>
 *   <li>维护 {@code lastSnapshot} 用于 t-1 相关因子（alpha_17/18/19）。</li>
 *   <li>遇到换日则重置 lastSnapshot，避免跨日错误引用上一条。</li>
 * </ul>
 */
public class StockFactorMapper extends Mapper<LongWritable, Text, DayTimeKey, FactorWritable> {

    private Snapshot lastSnapshot = null;
    private final DayTimeKey outKey = new DayTimeKey();
    private final FactorWritable outValue = new FactorWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        
        // Skip header or empty lines
        if (line.startsWith("tradingDay") || line.trim().isEmpty()) {
            return;
        } 

        Snapshot currentSnapshot = Snapshot.parse(line);

        // 换日：t-1 只能在同一交易日内定义，必须清空。
        if (lastSnapshot != null && lastSnapshot.tradingDay != currentSnapshot.tradingDay) {
            lastSnapshot = null;
        }

        // 计算 20 个因子：上一条快照用于 alpha_17/18/19。
        Snapshot.computeInto(currentSnapshot, lastSnapshot, outValue.factors);

        // 注意：即使不输出该条记录，也要维护 lastSnapshot，
        // 因为 09:30:00 的 t-1 可能来自 09:29:57（在输出窗口之外）。
        if (shouldEmit(currentSnapshot.tradeTime)) {
            outKey.set(currentSnapshot.tradingDay, currentSnapshot.tradeTime);
            context.write(outKey, outValue);
        }

        // 更新 t-1
        lastSnapshot = currentSnapshot;
    }

    private static boolean shouldEmit(long tradeTime) {
        // 标准答案输出时间窗口：9:30:00~11:30:00 + 13:00:00~15:00:00（含端点）。
        // tradeTime 在输入 CSV 中通常是 6 位 HHmmss（例如 093000），parse 成 long 后会变为 93000（前导零丢失）。
        return (tradeTime >= 93_000L && tradeTime <= 113_000L)
                || (tradeTime >= 130_000L && tradeTime <= 150_000L);
    }
}
