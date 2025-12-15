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
        this.bidPrices = copyTopLevels(bidPrices);
        this.bidVolumes = copyTopLevels(bidVolumes);
        this.askPrices = copyTopLevels(askPrices);
        this.askVolumes = copyTopLevels(askVolumes);
    }

    public double sumBidVolumes() {
        return sum(bidVolumes);
    }

    public double sumAskVolumes() {
        return sum(askVolumes);
    }

    private static double[] copyTopLevels(double[] values) {
        Objects.requireNonNull(values, "values");
        if (values.length < LEVELS) {
            throw new IllegalArgumentException("Expected at least " + LEVELS + " levels");
        }
        return Arrays.copyOf(values, LEVELS);
    }

    private static double sum(double[] values) {
        double acc = 0.0d;
        for (double value : values) {
            acc += value;
        }
        return acc;
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
        if (current == null) {
            throw new IllegalArgumentException("current snapshot is required");
        }

        double[] factors = new double[FACTOR_COUNT];
        double ap1 = current.askPrices[0];
        double bp1 = current.bidPrices[0];
        double av1 = current.askVolumes[0];
        double bv1 = current.bidVolumes[0];

        double sumBidVolumes = current.sumBidVolumes();
        double sumAskVolumes = current.sumAskVolumes();
        double sumBidWeightedPrice = weightedSum(current, true);
        double sumAskWeightedPrice = weightedSum(current, false);

        // alpha_1: 最优价差
        factors[0] = ap1 - bp1;

        // alpha_2/alpha_3: 相对价差 / 中间价
        double midPrice = 0.5d * (ap1 + bp1);
        factors[1] = safeDiv(ap1 - bp1, midPrice);
        factors[2] = midPrice;
        // alpha_4: 买一不平衡
        factors[3] = safeDiv(bv1 - av1, bv1 + av1);
        // alpha_5 ~ alpha_9: 前5档深度相关
        factors[4] = safeDiv(sumBidVolumes - sumAskVolumes, sumBidVolumes + sumAskVolumes);
        factors[5] = sumBidVolumes;
        factors[6] = sumAskVolumes;
        factors[7] = sumBidVolumes - sumAskVolumes;
        factors[8] = safeDiv(sumBidVolumes, sumAskVolumes);
        // alpha_10: 全市场买卖量平衡指数
        factors[9] = safeDiv(current.totalBidVolume - current.totalAskVolume,
                current.totalBidVolume + current.totalAskVolume);
        // alpha_11 ~ alpha_13: 价格加权
        factors[10] = safeDiv(sumBidWeightedPrice, sumBidVolumes);
        factors[11] = safeDiv(sumAskWeightedPrice, sumAskVolumes);
        factors[12] = safeDiv(sumBidWeightedPrice + sumAskWeightedPrice, sumBidVolumes + sumAskVolumes);
        // alpha_14/alpha_15: 买卖加权价差 / 每档平均挂单量差
        factors[13] = factors[11] - factors[10];
        factors[14] = (sumBidVolumes / LEVELS) - (sumAskVolumes / LEVELS);

        // alpha_16: 买卖不对称度（按档位衰减加权）
        double weightedBidDepth = decayedVolume(current, true);
        double weightedAskDepth = decayedVolume(current, false);
        factors[15] = safeDiv(weightedBidDepth - weightedAskDepth, weightedBidDepth + weightedAskDepth);

        if (previous != null) {
            double prevAp1 = previous.askPrices[0];
            double prevBp1 = previous.bidPrices[0];
            double prevSumBidVolumes = previous.sumBidVolumes();
            double prevSumAskVolumes = previous.sumAskVolumes();

            // alpha_17/alpha_18/alpha_19: t-1 相关因子
            factors[16] = ap1 - prevAp1;
            factors[17] = 0.5d * ((ap1 + bp1) - (prevAp1 + prevBp1));
            double currentDepthRatio = safeDiv(sumBidVolumes, sumAskVolumes);
            double previousDepthRatio = safeDiv(prevSumBidVolumes, prevSumAskVolumes);
            factors[18] = currentDepthRatio - previousDepthRatio;
        } else {
            // 对于每个股票文件的第一条记录（没有 t-1），按 0 处理。
            Arrays.fill(factors, 16, 19, 0.0d);
        }

        // alpha_20: 价压指标
        factors[19] = safeDiv(ap1 - bp1, sumBidVolumes + sumAskVolumes);
        return factors;
    }

    private static double weightedSum(Snapshot snapshot, boolean bidSide) {
        double sum = 0.0d;
        for (int i = 0; i < LEVELS; i++) {
            double price = bidSide ? snapshot.bidPrices[i] : snapshot.askPrices[i];
            double volume = bidSide ? snapshot.bidVolumes[i] : snapshot.askVolumes[i];
            sum += price * volume;
        }
        return sum;
    }

    private static double decayedVolume(Snapshot snapshot, boolean bidSide) {
        double acc = 0.0d;
        for (int i = 0; i < LEVELS; i++) {
            double weight = 1.0d / (i + 1);
            double volume = bidSide ? snapshot.bidVolumes[i] : snapshot.askVolumes[i];
            acc += weight * volume;
        }
        return acc;
    }

    private static double safeDiv(double numerator, double denominator) {
        return numerator / (denominator + EPSILON);
    }

    public static Snapshot parse(String csvLine) {
        // Simple CSV splitting, assuming no quoted commas in numeric fields
        String[] tokens = csvLine.split(",");
        
        // Basic validation
        if (tokens.length < 57) { // 17 + 10*4 = 57 columns minimum based on header
             throw new IllegalArgumentException("Insufficient columns: " + tokens.length);
        }

        // 字段位置对齐说明书：tradingDay=0, tradeTime=1, tBidVol=12, tAskVol=13, bp1 从 17 起。
        long tradingDay = Long.parseLong(tokens[0]);
        long tradeTime = Long.parseLong(tokens[1]);
        double totalBidVolume = Double.parseDouble(tokens[12]);
        double totalAskVolume = Double.parseDouble(tokens[13]);

        double[] bidPrices = new double[LEVELS];
        double[] bidVolumes = new double[LEVELS];
        double[] askPrices = new double[LEVELS];
        double[] askVolumes = new double[LEVELS];

        // bp1 index = 17
        // Stride = 4: bp, bv, ap, av
        int baseIndex = 17;
        for (int i = 0; i < LEVELS; i++) {
            int offset = i * 4;
            bidPrices[i] = Double.parseDouble(tokens[baseIndex + offset]);
            bidVolumes[i] = Double.parseDouble(tokens[baseIndex + offset + 1]);
            askPrices[i] = Double.parseDouble(tokens[baseIndex + offset + 2]);
            askVolumes[i] = Double.parseDouble(tokens[baseIndex + offset + 3]);
        }

        return new Snapshot(
            tradingDay,
            tradeTime,
            totalBidVolume,
            totalAskVolume,
            bidPrices,
            bidVolumes,
            askPrices,
            askVolumes
        );
    }
}
