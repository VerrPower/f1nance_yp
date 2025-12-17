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
 *   <li><b>大跃进：</b> 取消 {@code Snapshot} 封装，CSV 解析 + 20 因子计算全部内联在 Mapper 热路径中。</li>
 *   <li><b>大跃进：</b> 避免 {@code Text -> String} 分配，直接在 {@code byte[]} 上做 ASCII 扫描解析，并只在行尾处理 CRLF。</li>
 *   <li><b>大跃进：</b> 字段解析/累加手动循环展开（level1~5），减少函数调用、对象分配与分支树。</li>
 *   <li>仅维护上一条快照的最小状态（ap1/bp1/前5档深度）用于 t-1 因子（alpha_17/18/19）；遇到换日则重置。</li>
 * </ul>
 */
public class StockFactorMapper extends Mapper<LongWritable, Text, DayTimeKey, FactorWritable> {

    private final DayTimeKey outKey = new DayTimeKey();
    private final FactorWritable outValue = new FactorWritable();

    private static final double EPSILON = 1.0e-7;
    private static final byte COMMA = (byte) ',';
    private static final byte CR = (byte) '\r';

    // t-1 相关状态（只保存计算 alpha_17/18/19 所需的最小信息）。
    private boolean hasPrev = false;
    private long prevTradingDay = Long.MIN_VALUE;
    private double prevAp1 = 0.0;
    private double prevBp1 = 0.0;
    private double prevSumBidVolumes = 0.0;
    private double prevSumAskVolumes = 0.0;
    private long prevTradeTime = Long.MIN_VALUE;
    private long prevFileId = Long.MIN_VALUE;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // CombineFileInputFormat 下一个 mapper 会处理多个文件；alpha_17/18/19 的 t-1 必须在“同一股票文件”内定义。
        // FixedCombineTextInputFormat 将 fileIndex 编码到输入 key 的高位：这里据此在文件切换时清空 t-1 状态。
        final long fileId = (key.get() >>> 48);
        if (prevFileId != fileId) {
            prevFileId = fileId;
            hasPrev = false;
            prevTradingDay = Long.MIN_VALUE;
            prevTradeTime = Long.MIN_VALUE;
        }

        // 进一步避免 Text->String 分配：直接在 byte[] 上做 ASCII 解析。
        final int n = value.getLength();
        if (n <= 0) {
            return;
        }
        final byte[] s = value.getBytes();
        // Hadoop LineRecordReader 在 CRLF 文件中通常会保留行尾 '\r'（但不包含 '\n'）。
        // 为了让每个字段的解析循环不必重复判断 '\r'，这里仅在行尾做一次裁剪。
        int end = n;
        if (s[end - 1] == CR) {
            end--;
        }
        // 数据行以 YYYYMMDD 开头；表头以 't' 开头。这里用首字符快速过滤。
        final byte c0 = s[0];
        if (c0 < '0' || c0 > '9') {
            return;
        }

        // -------------------- 1) 解析必要字段（只读前 5 档） --------------------
        // field 0/1：data_fix 行首为 YYYYMMDD,HHMMSS,
        final int tradingDay = parseFixed8Digits(s, 0);
        final int secOfDay = parseFixed6DigitsToSecOfDay(s, 9);
        int pos = 16; // 8 + ',' + 6 + ','

        // 换日：t-1 只能在同一交易日内定义，必须清空。
        if (hasPrev && prevTradingDay != tradingDay) {
            hasPrev = false;
        }
        // Combine 可能让一个 mapper 处理多个文件，时间戳会“跳回早期”；遇到 tradeTime 逆序时清空 t-1。
        if (hasPrev && secOfDay < prevTradeTime) {
            hasPrev = false;
        }

        final boolean emit = shouldEmit(secOfDay);

        // skip fields 2..11（10 个字段）
        for (int k = 0; k < 10; k++) {
            while (pos < end && s[pos] != COMMA) {
                pos++;
            }
            pos++; // skip comma
        }

        // field 12/13: tBidVol / tAskVol
        int tBidVol = 0;
        int tAskVol = 0;
        if (emit) {
            // tBidVol
            while (pos < end) {
                byte c = s[pos];
                if (c == COMMA) break;
                tBidVol = tBidVol * 10 + (c - '0');
                pos++;
            }
            pos++; // skip comma
            // tAskVol
            while (pos < end) {
                byte c = s[pos];
                if (c == COMMA) break;
                tAskVol = tAskVol * 10 + (c - '0');
                pos++;
            }
            pos++; // skip comma
        } else {
            // 非输出窗口行：alpha_10 不需要，跳过解析以降低常数开销。
            while (pos < end && s[pos] != COMMA) pos++;
            pos++;
            while (pos < end && s[pos] != COMMA) pos++;
            pos++;
        }

        // skip fields 14..16（3 个字段）
        while (pos < end && s[pos] != COMMA) pos++;
        pos++;
        while (pos < end && s[pos] != COMMA) pos++;
        pos++;
        while (pos < end && s[pos] != COMMA) pos++;
        pos++;

        // 解析前 5 档 bp/bv/ap/av，同时在线累计所需统计量（避免数组分配）。
        double ap1 = 0.0, bp1 = 0.0, av1 = 0.0, bv1 = 0.0;
        double sumBidVolumes = 0.0, sumAskVolumes = 0.0;
        double sumBidWeightedPrice = 0.0, sumAskWeightedPrice = 0.0;
        double weightedBidDepth = 0.0, weightedAskDepth = 0.0; // Σ (v_i / i)

        // level 1 (i=1)
        int bp1i = 0, bv1i = 0, ap1i = 0, av1i = 0;
        while (pos < end) { byte c = s[pos]; if (c == COMMA) break; bp1i = bp1i * 10 + (c - '0'); pos++; }
        pos++;
        while (pos < end) { byte c = s[pos]; if (c == COMMA) break; bv1i = bv1i * 10 + (c - '0'); pos++; }
        pos++;
        while (pos < end) { byte c = s[pos]; if (c == COMMA) break; ap1i = ap1i * 10 + (c - '0'); pos++; }
        pos++;
        while (pos < end) { byte c = s[pos]; if (c == COMMA) break; av1i = av1i * 10 + (c - '0'); pos++; }
        if (pos < end && s[pos] == COMMA) pos++;
        bp1 = (double) bp1i;
        bv1 = (double) bv1i;
        ap1 = (double) ap1i;
        av1 = (double) av1i;

        sumBidVolumes += bv1; sumAskVolumes += av1;
        sumBidWeightedPrice += bp1 * bv1; sumAskWeightedPrice += ap1 * av1;
        weightedBidDepth += bv1; weightedAskDepth += av1;

        // levels 2..5: 手动展开，避免 level 分支树；并把字段解析 while 压成单行。
        // level 2 (i=2)
        int bp2 = 0, bv2i = 0, ap2i = 0, av2i = 0;
        while (pos < end) { byte c = s[pos]; if (c == COMMA) break; bp2 = bp2 * 10 + (c - '0'); pos++; }
        pos++;
        while (pos < end) { byte c = s[pos]; if (c == COMMA) break; bv2i = bv2i * 10 + (c - '0'); pos++; }
        pos++;
        while (pos < end) { byte c = s[pos]; if (c == COMMA) break; ap2i = ap2i * 10 + (c - '0'); pos++; }
        pos++;
        while (pos < end) { byte c = s[pos]; if (c == COMMA) break; av2i = av2i * 10 + (c - '0'); pos++; }
        if (pos < end && s[pos] == COMMA) pos++;
        final double bv2 = (double) bv2i, av2 = (double) av2i;
        sumBidVolumes += bv2; sumAskVolumes += av2;
        sumBidWeightedPrice += ((double) bp2) * bv2; sumAskWeightedPrice += ((double) ap2i) * av2;
        weightedBidDepth += bv2 * 0.5d; weightedAskDepth += av2 * 0.5d;

        // level 3 (i=3)
        int bp3 = 0, bv3i = 0, ap3i = 0, av3i = 0;
        while (pos < end) { byte c = s[pos]; if (c == COMMA) break; bp3 = bp3 * 10 + (c - '0'); pos++; }
        pos++;
        while (pos < end) { byte c = s[pos]; if (c == COMMA) break; bv3i = bv3i * 10 + (c - '0'); pos++; }
        pos++;
        while (pos < end) { byte c = s[pos]; if (c == COMMA) break; ap3i = ap3i * 10 + (c - '0'); pos++; }
        pos++;
        while (pos < end) { byte c = s[pos]; if (c == COMMA) break; av3i = av3i * 10 + (c - '0'); pos++; }
        if (pos < end && s[pos] == COMMA) pos++;
        final double bv3 = (double) bv3i, av3 = (double) av3i;
        sumBidVolumes += bv3; sumAskVolumes += av3;
        sumBidWeightedPrice += ((double) bp3) * bv3; sumAskWeightedPrice += ((double) ap3i) * av3;
        weightedBidDepth += bv3 * 0.33333333d; weightedAskDepth += av3 * 0.33333333d;

        // level 4 (i=4)
        int bp4 = 0, bv4i = 0, ap4i = 0, av4i = 0;
        while (pos < end) { byte c = s[pos]; if (c == COMMA) break; bp4 = bp4 * 10 + (c - '0'); pos++; }
        pos++;
        while (pos < end) { byte c = s[pos]; if (c == COMMA) break; bv4i = bv4i * 10 + (c - '0'); pos++; }
        pos++;
        while (pos < end) { byte c = s[pos]; if (c == COMMA) break; ap4i = ap4i * 10 + (c - '0'); pos++; }
        pos++;
        while (pos < end) { byte c = s[pos]; if (c == COMMA) break; av4i = av4i * 10 + (c - '0'); pos++; }
        if (pos < end && s[pos] == COMMA) pos++;
        final double bv4 = (double) bv4i, av4 = (double) av4i;
        sumBidVolumes += bv4; sumAskVolumes += av4;
        sumBidWeightedPrice += ((double) bp4) * bv4; sumAskWeightedPrice += ((double) ap4i) * av4;
        weightedBidDepth += bv4 * 0.25d; weightedAskDepth += av4 * 0.25d;

        // level 5 (i=5)
        int bp5 = 0, bv5i = 0, ap5i = 0, av5i = 0;
        while (pos < end) { byte c = s[pos]; if (c == COMMA) break; bp5 = bp5 * 10 + (c - '0'); pos++; }
        pos++;
        while (pos < end) { byte c = s[pos]; if (c == COMMA) break; bv5i = bv5i * 10 + (c - '0'); pos++; }
        pos++;
        while (pos < end) { byte c = s[pos]; if (c == COMMA) break; ap5i = ap5i * 10 + (c - '0'); pos++; }
        pos++;
        while (pos < end) { byte c = s[pos]; if (c == COMMA) break; av5i = av5i * 10 + (c - '0'); pos++; }
        if (pos < end && s[pos] == COMMA) pos++;
        final double bv5 = (double) bv5i, av5 = (double) av5i;
        sumBidVolumes += bv5; sumAskVolumes += av5;
        sumBidWeightedPrice += ((double) bp5) * bv5; sumAskWeightedPrice += ((double) ap5i) * av5;
        weightedBidDepth += bv5 * 0.2d; weightedAskDepth += av5 * 0.2d;

        // -------------------- 2) 计算 20 因子（写入 outValue.factors） --------------------
        // 非输出窗口：不需要计算 20 因子，只更新 t-1 状态即可。
        if (!emit) {
            hasPrev = true;
            prevTradingDay = tradingDay;
            prevAp1 = ap1;
            prevBp1 = bp1;
            prevSumBidVolumes = sumBidVolumes;
            prevSumAskVolumes = sumAskVolumes;
            prevTradeTime = secOfDay;
            return;
        }

        final double[] factors = outValue.factors;

        final double spread = ap1 - bp1;
        final double midPrice = 0.5d * (ap1 + bp1);
        final double depthDiff = sumBidVolumes - sumAskVolumes;

        final double invMid = 1.0d / (midPrice + EPSILON);
        final double invBvAv = 1.0d / ((bv1 + av1) + EPSILON);
        final double invDepthSum = 1.0d / ((sumBidVolumes + sumAskVolumes) + EPSILON);
        final double invSumAsk = 1.0d / (sumAskVolumes + EPSILON);
        final double invSumBid = 1.0d / (sumBidVolumes + EPSILON);
        final double invTotalVol = 1.0d / (((double) tBidVol + (double) tAskVol) + EPSILON);
        final double invWeightedDepthSum = 1.0d / ((weightedBidDepth + weightedAskDepth) + EPSILON);

        // alpha_01..alpha_03
        factors[0] = spread;
        factors[1] = spread * invMid;
        factors[2] = midPrice;
        // alpha_04
        factors[3] = (bv1 - av1) * invBvAv;
        // alpha_05..alpha_09
        factors[4] = depthDiff * invDepthSum;
        factors[5] = sumBidVolumes;
        factors[6] = sumAskVolumes;
        factors[7] = depthDiff;
        factors[8] = sumBidVolumes * invSumAsk;
        // alpha_10
        factors[9] = (((double) tBidVol) - ((double) tAskVol)) * invTotalVol;
        // alpha_11..alpha_13
        factors[10] = sumBidWeightedPrice * invSumBid;
        factors[11] = sumAskWeightedPrice * invSumAsk;
        factors[12] = (sumBidWeightedPrice + sumAskWeightedPrice) * invDepthSum;
        // alpha_14/15
        factors[13] = factors[11] - factors[10];
        factors[14] = depthDiff / 5.0d;
        // alpha_16
        factors[15] = (weightedBidDepth - weightedAskDepth) * invWeightedDepthSum;
        // alpha_17/18/19（t-1）
        if (hasPrev) {
            factors[16] = ap1 - prevAp1;
            factors[17] = 0.5d * ((ap1 + bp1) - (prevAp1 + prevBp1));
            final double prevInvSumAsk = 1.0d / (prevSumAskVolumes + EPSILON);
            final double currentDepthRatio = sumBidVolumes * invSumAsk;
            final double prevDepthRatio = prevSumBidVolumes * prevInvSumAsk;
            factors[18] = currentDepthRatio - prevDepthRatio;
        } else {
            factors[16] = 0.0d;
            factors[17] = 0.0d;
            factors[18] = 0.0d;
        }
        // alpha_20
        factors[19] = spread * invDepthSum;

        // -------------------- 3) 输出与更新 t-1 状态 --------------------
        // 注意：即使不输出该条记录，也要维护 t-1 状态，
        // 因为 09:30:00 的 t-1 可能来自 09:29:57（在输出窗口之外）。
        outKey.set(tradingDay, secOfDay);
        context.write(outKey, outValue);

        // 更新 t-1（仅保留必要统计量）
        hasPrev = true;
        prevTradingDay = tradingDay;
        prevAp1 = ap1;
        prevBp1 = bp1;
        prevSumBidVolumes = sumBidVolumes;
        prevSumAskVolumes = sumAskVolumes;
        prevTradeTime = secOfDay;
    }

    private static boolean shouldEmit(int secOfDay) {
        // 标准答案输出时间窗口：9:30:00~11:30:00 + 13:00:00~15:00:00（含端点）。
        return (secOfDay >= 34_200 && secOfDay <= 41_400) || (secOfDay >= 46_800 && secOfDay <= 54_000);
    }

    private static int parseFixed8Digits(byte[] s, int pos) {
        int v = s[pos] - '0';
        v = v * 10 + (s[pos + 1] - '0');
        v = v * 10 + (s[pos + 2] - '0');
        v = v * 10 + (s[pos + 3] - '0');
        v = v * 10 + (s[pos + 4] - '0');
        v = v * 10 + (s[pos + 5] - '0');
        v = v * 10 + (s[pos + 6] - '0');
        v = v * 10 + (s[pos + 7] - '0');
        return v;
    }

    private static int parseFixed6DigitsToSecOfDay(byte[] s, int pos) {
        int hh = (s[pos] - '0') * 10 + (s[pos + 1] - '0');
        int mm = (s[pos + 2] - '0') * 10 + (s[pos + 3] - '0');
        int ss = (s[pos + 4] - '0') * 10 + (s[pos + 5] - '0');
        return hh * 3600 + mm * 60 + ss;
    }
}
