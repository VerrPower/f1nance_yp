package factor;

import java.util.Arrays;
import java.util.Objects;

/**
 * 行情快照（订单簿）对象：只保留前 5 档价量，并提供 20 因子计算与 CSV 解析。
 *
 * <p>输入：单行快照 CSV（见数据集中 {@code snapshot.csv}），字段包含 10 档 bp/bv/ap/av。
 * 本项目因子定义只用前 5 档（参数 n=5），因此内部只复制前 5 档到数组中。</p>
 *
 * <p>输出：{@link #compute(Snapshot, Snapshot)} 返回长度为 20 的因子数组，顺序与说明书 alpha_1..alpha_20 对齐。</p>
 */
public final class Snapshot {
    public static final int LEVELS = 5;
    private static final double EPSILON = 1.0e-7;
    private static final int FACTOR_COUNT = 20;

    public final long tradingDay;
    public final long tradeTime;
    public final double totalBidVolume;
    public final double totalAskVolume;
    public final double[] bidPrices;
    public final double[] bidVolumes;
    public final double[] askPrices;
    public final double[] askVolumes;

    public Snapshot(
            long tradingDay,
            long tradeTime,
            double totalBidVolume,
            double totalAskVolume,
            double[] bidPrices,
            double[] bidVolumes,
            double[] askPrices,
            double[] askVolumes) {
        this.tradingDay = tradingDay;
        this.tradeTime = tradeTime;
        this.totalBidVolume = totalBidVolume;
        this.totalAskVolume = totalAskVolume;
        Objects.requireNonNull(bidPrices, "bidPrices");
        Objects.requireNonNull(bidVolumes, "bidVolumes");
        Objects.requireNonNull(askPrices, "askPrices");
        Objects.requireNonNull(askVolumes, "askVolumes");
        
        // 极限性能版本：不做任何数组拷贝，直接复用调用方提供的数组（仅使用前 LEVELS 档）。
        this.bidPrices = bidPrices;
        this.bidVolumes = bidVolumes;
        this.askPrices = askPrices;
        this.askVolumes = askVolumes;
    }

    public double sumBidVolumes() {
        double[] v = bidVolumes;
        return v[0] + v[1] + v[2] + v[3] + v[4];
    }

    public double sumAskVolumes() {
        double[] v = askVolumes;
        return v[0] + v[1] + v[2] + v[3] + v[4];
    }

    /**
     * Computes the 20 required LOB factors for one snapshot.
     *
     * <p>因子数组下标与说明书（alpha_n）一一对应：</p>
     * <ul>
     *   <li>alpha_1  : ap1_t - bp1_t</li>
     *   <li>alpha_2  : (ap1_t - bp1_t) / ((ap1_t + bp1_t)/2)</li>
     *   <li>alpha_3  : (ap1_t + bp1_t)/2</li>
     *   <li>alpha_4  : (bv1_t - av1_t) / (bv1_t + av1_t)</li>
     *   <li>alpha_5  : (Σ_{i=1..5}bv(i)_t - Σ_{i=1..5}av(i)_t) / (Σ_{i=1..5}bv(i)_t + Σ_{i=1..5}av(i)_t)</li>
     *   <li>alpha_6  : Σ_{i=1..5}bv(i)_t</li>
     *   <li>alpha_7  : Σ_{i=1..5}av(i)_t</li>
     *   <li>alpha_8  : Σ_{i=1..5}bv(i)_t - Σ_{i=1..5}av(i)_t</li>
     *   <li>alpha_9  : (Σ_{i=1..5}bv(i)_t) / (Σ_{i=1..5}av(i)_t)</li>
     *   <li>alpha_10 : (tBidVol_t - tAskVol_t) / (tBidVol_t + tAskVol_t)</li>
     *   <li>alpha_11 : (Σ_{i=1..5}bp(i)_t*bv(i)_t) / (Σ_{i=1..5}bv(i)_t)</li>
     *   <li>alpha_12 : (Σ_{i=1..5}ap(i)_t*av(i)_t) / (Σ_{i=1..5}av(i)_t)</li>
     *   <li>alpha_13 : (Σ bp(i)_t*bv(i)_t + Σ ap(i)_t*av(i)_t) / (Σ bv(i)_t + Σ av(i)_t)</li>
     *   <li>alpha_14 : VWAPAsk_t(5) - VWAPBid_t(5)</li>
     *   <li>alpha_15 : (1/5)Σ bv(i)_t - (1/5)Σ av(i)_t</li>
     *   <li>alpha_16 : (Σ (bv(i)_t/i) - Σ (av(i)_t/i)) / (Σ (bv(i)_t/i) + Σ (av(i)_t/i))</li>
     *   <li>alpha_17 : ap1_t - ap1_{t-1}</li>
     *   <li>alpha_18 : 0.5 * [(ap1_t + bp1_t) - (ap1_{t-1} + bp1_{t-1})]</li>
     *   <li>alpha_19 : (Σ bv(i)_t/Σ av(i)_t) - (Σ bv(i)_{t-1}/Σ av(i)_{t-1})</li>
     *   <li>alpha_20 : (ap1_t - bp1_t) / (Σ_{i=1..5}bv(i)_t + Σ_{i=1..5}av(i)_t)</li>
     * </ul>
     *
     * <p>说明书要求：分母为 0 时在分母上添加极小值 1e-7，这里统一用 safeDiv 实现。</p>
     */
    public static double[] compute(Snapshot current, Snapshot previous) {
        double[] factors = new double[FACTOR_COUNT];
        computeInto(current, previous, factors);
        return factors;
    }

    public static void computeInto(Snapshot current, Snapshot previous, double[] factors) {
        double ap1 = current.askPrices[0];
        double bp1 = current.bidPrices[0];
        double av1 = current.askVolumes[0];
        double bv1 = current.bidVolumes[0];

        double sumBidVolumes = current.sumBidVolumes();
        double sumAskVolumes = current.sumAskVolumes();
        // 内联计算：Σ price_i * volume_i（前 5 档）
        double[] _bp = current.bidPrices;
        double[] _bvPrice = current.bidVolumes;
        double[] _ap = current.askPrices;
        double[] _avPrice = current.askVolumes;
        double sumBidWeightedPrice =
                (_bp[0] * _bvPrice[0])
                        + (_bp[1] * _bvPrice[1])
                        + (_bp[2] * _bvPrice[2])
                        + (_bp[3] * _bvPrice[3])
                        + (_bp[4] * _bvPrice[4]);
        double sumAskWeightedPrice =
                (_ap[0] * _avPrice[0])
                        + (_ap[1] * _avPrice[1])
                        + (_ap[2] * _avPrice[2])
                        + (_ap[3] * _avPrice[3])
                        + (_ap[4] * _avPrice[4]);
        double spread = ap1 - bp1;
        double depthDiff = sumBidVolumes - sumAskVolumes;

        // alpha_1: 最优价差
        factors[0] = spread;

        // alpha_2/alpha_3: 相对价差 / 中间价
        double midPrice = 0.5d * (ap1 + bp1);
        double _invMid = 1.0d / (midPrice + EPSILON);
        factors[1] = spread * _invMid;
        factors[2] = midPrice;
        // alpha_4: 买一不平衡
        double _invBvAv = 1.0d / ((bv1 + av1) + EPSILON);
        factors[3] = (bv1 - av1) * _invBvAv;
        // alpha_5 ~ alpha_9: 前5档深度相关
        double _invDepthSum = 1.0d / ((sumBidVolumes + sumAskVolumes) + EPSILON);
        double _invSumAsk = 1.0d / (sumAskVolumes + EPSILON);
        double _invSumBid = 1.0d / (sumBidVolumes + EPSILON);
        factors[4] = depthDiff * _invDepthSum;
        factors[5] = sumBidVolumes;
        factors[6] = sumAskVolumes;
        factors[7] = depthDiff;
        factors[8] = sumBidVolumes * _invSumAsk;
        // alpha_10: 全市场买卖量平衡指数
        double _invTotalVol = 1.0d / ((current.totalBidVolume + current.totalAskVolume) + EPSILON);
        factors[9] = (current.totalBidVolume - current.totalAskVolume) * _invTotalVol;
        // alpha_11 ~ alpha_13: 价格加权
        factors[10] = sumBidWeightedPrice * _invSumBid;
        factors[11] = sumAskWeightedPrice * _invSumAsk;
        factors[12] = (sumBidWeightedPrice + sumAskWeightedPrice) * _invDepthSum;
        // alpha_14/alpha_15: 买卖加权价差 / 每档平均挂单量差
        factors[13] = factors[11] - factors[10];
        factors[14] = (sumBidVolumes - sumAskVolumes) / LEVELS;

        // alpha_16: 买卖不对称度（按档位衰减加权）
        // 内联计算：Σ_{i=1..5} (v_i / i)
        double[] _bv = current.bidVolumes;
        double[] _av = current.askVolumes;
        double weightedBidDepth =
                _bv[0]
                        + (_bv[1] * 0.5d)
                        + (_bv[2] * (0.33333333d))
                        + (_bv[3] * 0.25d)
                        + (_bv[4] * 0.2d);
        double weightedAskDepth =
                _av[0]
                        + (_av[1] * 0.5d)
                        + (_av[2] * (0.33333333d))
                        + (_av[3] * 0.25d)
                        + (_av[4] * 0.2d);
        double _invWeightedDepthSum = 1.0d / ((weightedBidDepth + weightedAskDepth) + EPSILON);
        factors[15] = (weightedBidDepth - weightedAskDepth) * _invWeightedDepthSum;

        if (previous != null) {
            double prevAp1 = previous.askPrices[0];
            double prevBp1 = previous.bidPrices[0];
            double prevSumBidVolumes = previous.sumBidVolumes();
            double prevSumAskVolumes = previous.sumAskVolumes();

            // alpha_17/alpha_18/alpha_19: t-1 相关因子
            factors[16] = ap1 - prevAp1;
            factors[17] = 0.5d * ((ap1 + bp1) - (prevAp1 + prevBp1));
            double currentDepthRatio = sumBidVolumes * _invSumAsk;
            double _invPrevSumAsk = 1.0d / (prevSumAskVolumes + EPSILON);
            double previousDepthRatio = prevSumBidVolumes * _invPrevSumAsk;
            factors[18] = currentDepthRatio - previousDepthRatio;
        } else {
            // 对于每个股票文件的第一条记录（没有 t-1），按 0 处理。
            Arrays.fill(factors, 16, 19, 0.0d);
        }

        // alpha_20: 价压指标
        factors[19] = spread * _invDepthSum;
    }

    public static Snapshot parse(String csvLine) {
        // 特化解析（面向本项目数据）：除 MIC/code 字符串列外，其余数值列均为整数；我们仅依赖少量字段。
        // 为降低 Map 端常数开销：仅对用到的字段做 int32 解析，其他字段直接跳过。
        //
        // 需要的字段：
        // 0 tradingDay, 1 tradeTime, 12 tBidVol, 13 tAskVol, 17..36 (bp/bv/ap/av 前5档)

        final String s = csvLine;
        final int n = s.length();
        int pos = 0;

        // field 0/1：tradingDay 固定 8 位，tradeTime 固定 6 位（data_fix 数据集中均为该格式）
        // 为避免分支与循环开销，这里按定长展开解析。
        int tradingDay = parseFixed8Digits(s, 0);
        int tradeTime = parseFixed6Digits(s, 9);
        pos = 16; // 8 + ',' + 6 + ','

        // skip fields 2..11（recvTime, MIC, code, cumCnt, cumVol, turnover, last, open, high, low）
        for (int k = 0; k < 10; k++) {
            while (pos < n && s.charAt(pos) != ',') pos++;
            pos++; // skip comma
        }

        // field 12: tBidVol
        int tBidVol = 0;
        while (pos < n) {
            char c = s.charAt(pos);
            if (c == ',') break;
            tBidVol = tBidVol * 10 + (c - '0');
            pos++;
        }
        pos++; // skip comma

        // field 13: tAskVol
        int tAskVol = 0;
        while (pos < n) {
            char c = s.charAt(pos);
            if (c == ',') break;
            tAskVol = tAskVol * 10 + (c - '0');
            pos++;
        }
        pos++; // skip comma

        // skip fields 14..16（wBidPrc, wAskPrc, openInterest）
        for (int k = 0; k < 3; k++) {
            while (pos < n && s.charAt(pos) != ',') pos++;
            pos++; // skip comma
        }

        double[] bidPrices = new double[LEVELS];
        double[] bidVolumes = new double[LEVELS];
        double[] askPrices = new double[LEVELS];
        double[] askVolumes = new double[LEVELS];

        for (int level = 0; level < LEVELS; level++) {
            int v;

            // bp
            v = 0;
            while (pos < n) {
                char c = s.charAt(pos);
                if (c == ',') break;
                v = v * 10 + (c - '0');
                pos++;
            }
            pos++;
            bidPrices[level] = (double) v;

            // bv
            v = 0;
            while (pos < n) {
                char c = s.charAt(pos);
                if (c == ',') break;
                v = v * 10 + (c - '0');
                pos++;
            }
            pos++;
            bidVolumes[level] = (double) v;

            // ap
            v = 0;
            while (pos < n) {
                char c = s.charAt(pos);
                if (c == ',') break;
                v = v * 10 + (c - '0');
                pos++;
            }
            pos++;
            askPrices[level] = (double) v;

            // av
            v = 0;
            while (pos < n) {
                char c = s.charAt(pos);
                if (c == ',') break;
                v = v * 10 + (c - '0');
                pos++;
            }
            if (pos < n && s.charAt(pos) == ',') pos++;
            askVolumes[level] = (double) v;
        }

        return new Snapshot(
                (long) tradingDay,
                (long) tradeTime,
                (double) tBidVol,
                (double) tAskVol,
                bidPrices,
                bidVolumes,
                askPrices,
                askVolumes);
    }

    private static int parseFixed8Digits(String s, int pos) {
        int v = s.charAt(pos) - '0';
        v = v * 10 + (s.charAt(pos + 1) - '0');
        v = v * 10 + (s.charAt(pos + 2) - '0');
        v = v * 10 + (s.charAt(pos + 3) - '0');
        v = v * 10 + (s.charAt(pos + 4) - '0');
        v = v * 10 + (s.charAt(pos + 5) - '0');
        v = v * 10 + (s.charAt(pos + 6) - '0');
        v = v * 10 + (s.charAt(pos + 7) - '0');
        return v;
    }

    private static int parseFixed6Digits(String s, int pos) {
        int v = s.charAt(pos) - '0';
        v = v * 10 + (s.charAt(pos + 1) - '0');
        v = v * 10 + (s.charAt(pos + 2) - '0');
        v = v * 10 + (s.charAt(pos + 3) - '0');
        v = v * 10 + (s.charAt(pos + 4) - '0');
        v = v * 10 + (s.charAt(pos + 5) - '0');
        return v;
    }

}
