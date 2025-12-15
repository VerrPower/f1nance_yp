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

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        
        // Skip header or empty lines
        if (line.startsWith("tradingDay") || line.trim().isEmpty()) {
            return;
        }

        try {
            Snapshot currentSnapshot = Snapshot.parse(line);
            
            // Ensure we are processing the same stock sequence. 
            // Since input splits are typically per-file and files are per-stock, 
            // we assume sequential processing. 
            // However, if we switch stocks (unlikely in one file), we should reset.
            // But here we just implement the logic as if the stream is continuous for one stock.
            // If the file contains multiple stocks mixed, this logic would be flawed, 
            // but the problem description implies per-stock files.
            
            // Check for day change or stock change if necessary? 
            // The prompt says "For each day...". 
            // If a file spans multiple days, we might need to reset on day change.
            if (lastSnapshot != null && lastSnapshot.tradingDay != currentSnapshot.tradingDay) {
                // 换日：t-1 只能在同一交易日内定义，必须清空。
                lastSnapshot = null;
            }

            // 计算 20 个因子：上一条快照用于 alpha_17/18/19。
            double[] factors = Snapshot.compute(currentSnapshot, lastSnapshot);

            // 注意：即使不输出该条记录，也要维护 lastSnapshot，
            // 因为 09:30:00 的 t-1 可能来自 09:29:57（在输出窗口之外）。
            if (shouldEmit(currentSnapshot.tradeTime)) {
                outKey.set(currentSnapshot.tradingDay, currentSnapshot.tradeTime);
                context.write(outKey, new FactorWritable(factors));
            }

            // 更新 t-1
            lastSnapshot = currentSnapshot;

        } catch (Exception e) {
            // Log error or ignore bad records
            System.err.println("Error parsing line: " + line + " - " + e.getMessage());
        }
    }

    private static boolean shouldEmit(long tradeTime) {
        // 标准答案输出时间窗口：9:30:00~11:30:00 + 13:00:00~15:00:00（含端点）。
        // 数据里的 tradeTime 通常是 6 位 HHmmss（例如 093000），parse 成 long 后会变为 93000。
        if (tradeTime < 0 || tradeTime >= 1_000_000L) {
            // 若出现包含小数秒的更长时间戳（HHmmssffffffff），这里无法按整数区间比较；
            // 为避免误删，默认输出。
            return true;
        }
        return (tradeTime >= 93_000L && tradeTime <= 113_000L)
                || (tradeTime >= 130_000L && tradeTime <= 150_000L);
    }
}
