package factor;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Locale;
import java.util.Arrays;

/**
 * Reducer that averages factor values across stocks for each (tradingDay,tradeTime).
 *
 * <p>Output is CSV, with one file per tradingDay. The first row is the header:
 * tradeTime,alpha_1,...,alpha_20</p>
 */
public class AverageReducer extends Reducer<IntWritable, FactorWritable, NullWritable, Text> {

    private final Text outValue = new Text();
    private final StringBuilder sb = new StringBuilder(512);
    private final double[] totals = new double[20];
    private MultipleOutputs<NullWritable, Text> multipleOutputs;
    private int currentDayCode = Integer.MIN_VALUE;
    private static final int EXPECTED_STOCKS = 300;
    private static final double INV_EXPECTED_STOCKS = 1.0d / (double) EXPECTED_STOCKS;
    private static final int BASE_SEC_6AM = 21_600;
    private static final int MASK_TIME15 = (1 << 15) - 1;
    private static final Text HEADER = new Text(csvHeaderLine());

    @Override
    protected void setup(Context context) {
        this.multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(IntWritable key, Iterable<FactorWritable> values, Context context) throws IOException, InterruptedException {
        Arrays.fill(totals, 0.0d);
        for (FactorWritable val : values) {
            double[] f = val.factors;
            for (int i = 0; i < 20; i++) {
                totals[i] += f[i];
            }
        }

        int compact = key.get();
        int dayCode = compact >>> 15;
        if (dayCode != currentDayCode) {
            currentDayCode = dayCode;
            // 每个交易日的输出文件首行写表头（MultipleOutputs 按 tradingDay 分目录）。
            multipleOutputs.write(NullWritable.get(), HEADER, dayOutputBasePath(dayCode));
        }

        int secOfDay = (compact & MASK_TIME15) + BASE_SEC_6AM;
        sb.setLength(0);
        appendTradeTimeFromSecOfDay(sb, secOfDay);
        for (int i = 0; i < 20; i++) {
            sb.append(',');
            // 标准答案通常使用 Double 的完整字符串表示（不强制固定小数位）；
            // 这里避免四舍五入截断带来的累计误差，直接输出 Double.toString 的结果。
            sb.append(Double.toString(totals[i] * INV_EXPECTED_STOCKS));
        }
        outValue.set(sb.toString());
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
        StringBuilder sb = new StringBuilder(256);
        sb.append("tradeTime");
        for (int i = 1; i <= 20; i++) {
            sb.append(",alpha_").append(i);
        }
        return sb.toString();
    }

    private static void appendTradeTimeFromSecOfDay(StringBuilder sb, int secOfDay) {
        int hh = secOfDay / 3600;
        int t = secOfDay - hh * 3600;
        int mm = t / 60;
        int ss = t - mm * 60;
        append2(sb, hh);
        append2(sb, mm);
        append2(sb, ss);
    }

    private static void append2(StringBuilder sb, int v) {
        int tens = v / 10;
        sb.append((char) ('0' + tens));
        sb.append((char) ('0' + (v - tens * 10)));
    }

    private static String formatDayFilePrefixFromDayCode(int dayCode) {
        int month = (dayCode >>> 5) & 0x0F;
        int day = dayCode & 0x1F;
        return String.format(Locale.ROOT, "%02d%02d.csv", month, day);
    }
}
