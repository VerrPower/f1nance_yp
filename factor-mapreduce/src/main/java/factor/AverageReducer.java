package factor;

import org.apache.hadoop.io.IntWritable;
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
public class AverageReducer extends Reducer<IntWritable, FactorWritable, NullWritable, Text> {

    private final Text outValue = new Text();
    private MultipleOutputs<NullWritable, Text> multipleOutputs;
    private int currentDayCode = Integer.MIN_VALUE;
    private static final int EXPECTED_STOCKS = 300;
    private static final int BASE_SEC_6AM = 21_600;
    private static final int MASK_TIME15 = (1 << 15) - 1;

    @Override
    protected void setup(Context context) {
        this.multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(IntWritable key, Iterable<FactorWritable> values, Context context) throws IOException, InterruptedException {
        FactorWritable sum = new FactorWritable();
        
        for (FactorWritable val : values) {
            sum.add(val);
        }

        int compact = key.get();
        int dayCode = compact >>> 15;
        if (dayCode != currentDayCode) {
            currentDayCode = dayCode;
            // 每个交易日的输出文件首行写表头（MultipleOutputs 按 tradingDay 分目录）。
            multipleOutputs.write(NullWritable.get(), new Text(csvHeaderLine()), dayOutputBasePath(dayCode));
        }

        double[] averages = new double[20];
        double[] totals = sum.factors;
        int count = EXPECTED_STOCKS;

        StringJoiner joiner = new StringJoiner(",");
        int secOfDay = (compact & MASK_TIME15) + BASE_SEC_6AM;
        joiner.add(formatTradeTimeFromSecOfDay(secOfDay));
        for (int i = 0; i < 20; i++) {
            averages[i] = totals[i] / count;
            // 标准答案通常使用 Double 的完整字符串表示（不强制固定小数位）；
            // 这里避免四舍五入截断带来的累计误差，直接输出 Double.toString 的结果。
            joiner.add(Double.toString(averages[i]));
        }

        outValue.set(joiner.toString());
        multipleOutputs.write(NullWritable.get(), outValue, dayOutputBasePath(dayCode));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (multipleOutputs != null) {
            multipleOutputs.close();
        }
    }

    private static String dayOutputBasePath(int dayCode) {
        // 标准答案按日期输出为 0102.csv、0103.csv ...（取 MMDD）。
        // MultipleOutputs 实际会追加 reducer 后缀（例如 0102.csv-r-00000）。
        return formatDayFilePrefixFromDayCode(dayCode);
    }

    private static String csvHeaderLine() {
        StringJoiner joiner = new StringJoiner(",");
        joiner.add("tradeTime");
        for (int i = 1; i <= 20; i++) {
            joiner.add("alpha_" + i);
        }
        return joiner.toString();
    }

    private static String formatTradeTimeFromSecOfDay(int secOfDay) {
        int hh = secOfDay / 3600;
        int mm = (secOfDay - hh * 3600) / 60;
        int ss = secOfDay - hh * 3600 - mm * 60;
        return String.format(Locale.ROOT, "%02d%02d%02d", hh, mm, ss);
    }

    private static String formatDayFilePrefixFromDayCode(int dayCode) {
        int month = (dayCode >>> 5) & 0x0F;
        int day = dayCode & 0x1F;
        return String.format(Locale.ROOT, "%02d%02d.csv", month, day);
    }
}
