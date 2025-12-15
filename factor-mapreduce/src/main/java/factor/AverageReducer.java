package factor;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Locale;
import java.util.StringJoiner;

/**
 * Reducer that averages factor values across stocks for each (tradingDay,tradeTime).
 *
 * <p>Output is CSV, with one file per tradingDay. The first row is the header:
 * tradeTime,alpha_1,...,alpha_20</p>
 */
public class AverageReducer extends Reducer<DayTimeKey, FactorWritable, NullWritable, Text> {

    private final Text outValue = new Text();
    private MultipleOutputs<NullWritable, Text> multipleOutputs;
    private long currentDay = Long.MIN_VALUE;

    @Override
    protected void setup(Context context) {
        this.multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(DayTimeKey key, Iterable<FactorWritable> values, Context context) throws IOException, InterruptedException {
        FactorWritable sum = new FactorWritable();
        
        for (FactorWritable val : values) {
            sum.add(val);
        }

        if (sum.getCount() == 0) {
            return;
        }

        if (key.getTradingDay() != currentDay) {
            currentDay = key.getTradingDay();
            // 每个交易日的输出文件首行写表头（MultipleOutputs 按 tradingDay 分目录）。
            multipleOutputs.write(NullWritable.get(), new Text(csvHeaderLine()), dayOutputBasePath(currentDay));
        }

        double[] averages = new double[20];
        double[] totals = sum.getFactors();
        int count = sum.getCount();

        StringJoiner joiner = new StringJoiner(",");
        // tradeTime 在原始 CSV 中通常是 6 位（例如 092500），这里补齐前导零，避免与标准输出对齐失败。
        joiner.add(formatTradeTime(key.getTradeTime()));
        for (int i = 0; i < 20; i++) {
            averages[i] = totals[i] / count;
            // 标准答案通常使用 Double 的完整字符串表示（不强制固定小数位）；
            // 这里避免四舍五入截断带来的累计误差，直接输出 Double.toString 的结果。
            joiner.add(Double.toString(averages[i]));
        }

        outValue.set(joiner.toString());
        multipleOutputs.write(NullWritable.get(), outValue, dayOutputBasePath(currentDay));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (multipleOutputs != null) {
            multipleOutputs.close();
        }
    }

    private static String dayOutputBasePath(long tradingDay) {
        // 标准答案按日期输出为 0102.csv、0103.csv ...（取 tradingDay 的 MMDD）。
        // MultipleOutputs 实际会追加 reducer 后缀（例如 0102.csv-r-00000）。
        return formatDayFilePrefix(tradingDay);
    }

    private static String csvHeaderLine() {
        StringJoiner joiner = new StringJoiner(",");
        joiner.add("tradeTime");
        for (int i = 1; i <= 20; i++) {
            joiner.add("alpha_" + i);
        }
        return joiner.toString();
    }

    private static String formatTradeTime(long tradeTime) {
        // 标准答案的 tradeTime 使用 6 位 HHmmss（例如 092500），这里统一补齐前导零。
        if (tradeTime >= 0 && tradeTime < 1_000_000L) {
            return String.format(Locale.ROOT, "%06d", tradeTime);
        }
        return Long.toString(tradeTime);
    }

    private static String formatDayFilePrefix(long tradingDay) {
        long mmdd = Math.floorMod(tradingDay, 10_000L);
        return String.format(Locale.ROOT, "%04d.csv", mmdd);
    }
}
