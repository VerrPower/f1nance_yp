package pogi_one;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <b>StockFactorMapper</b>：逐行读取快照 CSV，计算 {@code alpha_1..alpha_20}，并在 Mapper 内对同一时间点做预聚合。
 * <p><b>业务范围</b>：</p>
 * <ul>
 *   <li><b>输入</b> : 
 *          {@code <ShortWritable, Text>}：key 为文件索引，用于检测换股票csv；value 为单行 CSV</li>
 *   <li><b>解析</b> : 
 *          维护单个指针 {@code pos} 在 {@code byte[]} 上进行 ASCII 扫描解析，避免 {@code Text->String} 的转换开销；
 *          依赖逗号作为字段分隔符，仅解析有价值数据，冗余信息直接跳过；在无需输出的情况下提前返回。
 *          该方案严格依赖输入记录的完整性假设。</li>
 *   <li><b>计算20个因子</b> : 
 *          解析前 5 档盘口与必要字段，计算 20 个因子；其中 {@code alpha_17/18/19} 依赖 t-1 状态。
 *          当检测到 fileId 变化或时间倒退时清空。</li>
 *   <li><b>输出</b> :
 *          mapper 输出 {@code <IntWritable, FactorWritable>}：
 *          key 为打包后的 32-bit int，value 为 {@code sum[20]+count}（供 reducer 求均值）。
 *      <pre>
 *             Bit diagram for packed 32-bit key
 *      ┌──────────HIGH────────┬───────────MID──────────┬───────LOW───────┐
 *      │ unused (6 bits)      │ dayId (MMDD)           │ timeCode (15)   │
 *      │ bits: [31..26]       │ bits: [25..15]         │ bits: [14..0]   │
 *      │                      │ range 0..1231          │ range 0..32767  │
 *      └──────────────────────┴────────────────────────┴─────────────────┘
 *      </pre>
 *      其中：{@code packed = (dayId << 15) | timeCode}，dayId=MMDD，timeCode=secOfDay-06:00:00。
 *    
 *      <p><b>计算示例：</b></p>
 *      <p>
 *      1. 对于 <b>3月12日 09:30:05</b> 的数据：<br>
 *      &nbsp;&nbsp; - dayId = 3 * 100 + 12 = 312<br>
 *      &nbsp;&nbsp; - secOfDay = 9*3600 + 30*60 + 5 = 34205<br>
 *      &nbsp;&nbsp; - timeCode = 34205 - 21600 = 12605<br>
 *      &nbsp;&nbsp; - packed = (312 &lt;&lt; 15) | 12605 = 10236221
 *      </p>
 *      <p>
 *      2. 对于 <b>11月30日 14:45:30</b> 的数据：<br>
 *      &nbsp;&nbsp; - dayId = 11 * 100 + 30 = 1130<br>
 *      &nbsp;&nbsp; - secOfDay = 14*3600 + 45*60 + 30 = 53130<br>
 *      &nbsp;&nbsp; - timeCode = 53130 - 21600 = 31530<br>
 *      &nbsp;&nbsp; - packed = (1130 &lt;&lt; 15) | 31530 = 37059370
 *      </p>
 *   <li><b>Mapper 内聚合</b> : 
 *          在mapper中提前执行不同股票间因子值累加，对输入“张量”的“股票”维度进行压缩。
 *          把 20 维因子向量按照时间戳为key，累加到本地的 {@code AGG21_FP64} 哈希表（额外维护 count）；
 *          并在最后的 {@code cleanup()} 中批量输出聚合后的 KV。由于额外维护了 count 属性，reducer 便可以根据 count 
 *          动态计算特定时间戳特定因子的均值，无需依赖记录完整性假设，哪怕有时间戳记录缺失也能正确处理。
 *   </li>
 * </ul>
 */
public class StockFactorMapper extends Mapper<ShortWritable, Text, IntWritable, FactorWritable> {

    // 常数列表
    private static final double EPSILON = 1.0e-7;
    private static final byte COMMA = (byte) ',';
    private static final int BASE_SEC_6AM = 21600;
    private static final int MASK_TIME15 = (1 << 15) - 1;
    private static final int FACTOR_COUNT = 20;
    private static final int VALUE_SIZE = FACTOR_COUNT + 1; // last element is count

    // 可复用对象
    private final IntWritable outKey = new IntWritable();
    private final FactorWritable outValue = new FactorWritable();
    private final double[] tmpFactors = new double[FACTOR_COUNT];
    private final AGG21_FP64 localAggHashTable = new AGG21_FP64();
    private boolean dayInited = false;
    private int dayId = 0; // MMDD as int (e.g., 0102 -> 102)

    // alpha_17/18/19 在 t-1 时刻的相关状态
    private boolean hasPrev = false;
    private double prevAp1 = 0.0;
    private double prevBp1 = 0.0;
    private double prevSumBidVolumes = 0.0;
    private double prevSumAskVolumes = 0.0;
    private int prevTradeTime = Integer.MIN_VALUE;
    private int prevFileId = Integer.MIN_VALUE;




// @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
// @#                                              ☆ Map 主函数 ☆                                            #@
// @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    /**
     * <p><b>职责：解析快照 CSV 单行并计算 20 个因子并写入本地聚合表</b></p>
     *         
     * <p><b>高性能单指针解析与算子融合</b></p>
     * <p>
     *      本方法直接操作底层字节数组 {@code s}，通过移动索引 {@code pos} 定位字段边界，避免使用 {@code String.split()} 等会产生中间对象的方法，实现零对象分配。
     *      针对前 5 档盘口数据，采用手动循环展开优化，结合 {@code val = val * 10 + (c - '0')} 递推公式将 ASCII 数字序列原位转换为整数值。
     *      解析过程中同步执行算子融合：在读取买卖盘价格与量（bp/bv/ap/av）的同时，在寄存器中实时累加 {@code sumVolumes}、{@code sumWeightedPrice} 及 {@code weightedDepth} 等统计量。
     *      通过将字段解析与初步统计合并为单一流程，有效减少了内存重复访问开销，达到O(1)的内存占用。
     * </p>
     * <p><b>输入字段（含索引与是否使用标记）</b></p>
     * <p>
     * <b>tradingDay[0|Y]</b>, <b>tradeTime[1|Y]</b>, 
     * </p><p>
     * recvTime[2|×], MIC[3|×], code[4|×],
     * </p><p>
     * cumCnt[5|×], cumVol[6|×], 
     * </p><p>
     * turnover[7|×], last[8|×], open[9|×], 
     * </p><p>
     * high[10|×], low[11|×],
     * </p><p>
     * <b>tBidVol[12|Y]</b>, <b>tAskVol[13|Y]</b>, 
     * </p><p>
     * wBidPrc[14|×], wAskPrc[15|×], openInterest[16|×],
     * </p><p>
     * <b>bp1[17|Y]</b>, <b>bv1[18|Y]</b>, <b>ap1[19|Y]</b>, <b>av1[20|Y]</b>,
     * </p><p>
     * <b>bp2[21|Y]</b>, <b>bv2[22|Y]</b>, <b>ap2[23|Y]</b>, <b>av2[24|Y]</b>,
     * </p><p>
     * <b>bp3[25|Y]</b>, <b>bv3[26|Y]</b>, <b>ap3[27|Y]</b>, <b>av3[28|Y]</b>,
     * </p><p>
     * <b>bp4[29|Y]</b>, <b>bv4[30|Y]</b>, <b>ap4[31|Y]</b>, <b>av4[32|Y]</b>,
     * </p><p>
     * <b>bp5[33|Y]</b>, <b>bv5[34|Y]</b>, <b>ap5[35|Y]</b>, <b>av5[36|Y]</b>, （解析完最后一个有价值信息）
     * </p><p>
     * bp6[37|×], bv6[38|×], ap6[39|×], av6[40|×],
     * </p><p>
     * bp7[41|×], bv7[42|×], ap7[43|×], av7[44|×],
     * </p><p>
     * bp8[45|×], bv8[46|×], ap8[47|×], av8[48|×],
     * </p><p>
     * bp9[49|×], bv9[50|×], ap9[51|×], av9[52|×],
     * </p><p>
     * bp10[53|×], bv10[54|×], ap10[55|×], av10[56|×]
     * </p>
     * 
     */
    @Override
    protected void map(ShortWritable p_key, Text p_value, Context p_context) 
        throws IOException, InterruptedException 
    {
        // CombineFileInputFormat：一个 mapper 会顺序处理多个股票文件；alpha_17/18/19 的 t-1 必须在同一股票文件内
        // FixedCombineTextInputFormat 的输入 key 是 fileIndex（ShortWritable），检测到 fileId 变化时清空 t-1 状态
        final int fileId = p_key.get();
        if (prevFileId != fileId) {
            prevFileId = fileId;
            hasPrev = false;
            prevTradeTime = Integer.MIN_VALUE;
        }

        // 在 byte[] 上做 ASCII 解析，避免 Text->String 分配
        final int n = p_value.getLength();
        if (n <= 0) return;  // 防御一下
        final byte[] s = p_value.getBytes();
        final byte c0 = s[0];
        if (c0 < '0' || c0 > '9') return; // 过滤表头。

        // @================================ 基于原始字节数组的单指针字段解析 ================================@
        // field 0/1：行首固定为 YYYYMMDD,HHMMSS
        // 直接跳过 year 的 4 位，只需要 MMDD
        if (!dayInited) {
            int month = (s[4] - '0') * 10 + (s[5] - '0');
            int day = (s[6] - '0') * 10 + (s[7] - '0');
            dayId = month * 100 + day; // MMDD（int）
            dayInited = true;
        }
        int hh = (s[9] - '0') * 10 + (s[10] - '0');
        int mm = (s[11] - '0') * 10 + (s[12] - '0');
        int ss = (s[13] - '0') * 10 + (s[14] - '0');
        final int secOfDay = hh * 3600 + mm * 60 + ss;
        int pos = 16; // 8 + ',' + 6 + ','

        // mapper函数将会处理多个股票csv文件 切换csv时又从早上九点开始遍历 检测到时间回拨 即为跨越csv
        if (hasPrev && secOfDay < prevTradeTime) hasPrev = false;
        
        // skip fields 2..11（10 个字段）
        for (int k = 0; k < 10; k++) {
            while (s[pos] != COMMA) pos++;
            pos++; // skip comma
        }

        // field 12/13: tBidVol / tAskVol
        int tBidVol = 0;
        int tAskVol = 0;

        final boolean shouldEmit = (secOfDay >= 34_200 && secOfDay <= 41_400) 
            || (secOfDay >= 46_800 && secOfDay <= 54_000);

        if (shouldEmit) {
            // tBidVol
            while (true) {byte c = s[pos]; if (c == COMMA) break; tBidVol = tBidVol * 10 + (c - '0'); pos++; }
            pos++; // skip comma

            // tAskVol
            while (true) {byte c = s[pos]; if (c == COMMA) break; tAskVol = tAskVol * 10 + (c - '0'); pos++; }
            pos++; // skip comma
        } else {
            // 非输出窗口行 alpha_10 不需要 跳过
            while (s[pos] != COMMA) pos++;
            pos++;
            while (s[pos] != COMMA) pos++;
            pos++;
        }

        // skip fields 14..16（3 个字段）
        while (s[pos] != COMMA) pos++;
        pos++;
        while (s[pos] != COMMA) pos++;
        pos++;
        while (s[pos] != COMMA) pos++;
        pos++;

        // 手动循环展开解析前 5 档 bp/bv/ap/av，同时累计sum统计量
        double ap1 = 0.0, bp1 = 0.0, av1 = 0.0, bv1 = 0.0;
        double sumBidVolumes = 0.0, sumAskVolumes = 0.0;
        double sumBidWeightedPrice = 0.0, sumAskWeightedPrice = 0.0;
        double weightedBidDepth = 0.0, weightedAskDepth = 0.0; // Σ (v_i / i)

        // level 1 (i=1)
        int bp1i = 0, bv1i = 0, ap1i = 0, av1i = 0;
        while (true) { byte c = s[pos]; if (c == COMMA) break; bp1i = bp1i * 10 + (c - '0'); pos++; }
        pos++;
        while (true) { byte c = s[pos]; if (c == COMMA) break; bv1i = bv1i * 10 + (c - '0'); pos++; }
        pos++;
        while (true) { byte c = s[pos]; if (c == COMMA) break; ap1i = ap1i * 10 + (c - '0'); pos++; }
        pos++;
        while (true) { byte c = s[pos]; if (c == COMMA) break; av1i = av1i * 10 + (c - '0'); pos++; }
        if (s[pos] == COMMA) pos++;
        bp1 = (double) bp1i;
        bv1 = (double) bv1i;
        ap1 = (double) ap1i;
        av1 = (double) av1i;

        sumBidVolumes += bv1; sumAskVolumes += av1;
        sumBidWeightedPrice += bp1 * bv1; sumAskWeightedPrice += ap1 * av1;
        weightedBidDepth += bv1; weightedAskDepth += av1;

        // levels 2,3,4,5 
        // level 2 (i=2)
        int bp2 = 0, bv2i = 0, ap2i = 0, av2i = 0;
        while (true) { byte c = s[pos]; if (c == COMMA) break; bp2 = bp2 * 10 + (c - '0'); pos++; }
        pos++;
        while (true) { byte c = s[pos]; if (c == COMMA) break; bv2i = bv2i * 10 + (c - '0'); pos++; }
        pos++;
        while (true) { byte c = s[pos]; if (c == COMMA) break; ap2i = ap2i * 10 + (c - '0'); pos++; }
        pos++;
        while (true) { byte c = s[pos]; if (c == COMMA) break; av2i = av2i * 10 + (c - '0'); pos++; }
        if (s[pos] == COMMA) pos++;
        final double bv2 = (double) bv2i, av2 = (double) av2i;
        sumBidVolumes += bv2; sumAskVolumes += av2;
        sumBidWeightedPrice += ((double) bp2) * bv2; sumAskWeightedPrice += ((double) ap2i) * av2;
        weightedBidDepth += bv2 * 0.5d; weightedAskDepth += av2 * 0.5d;

        // level 3 (i=3)
        int bp3 = 0, bv3i = 0, ap3i = 0, av3i = 0;
        while (true) { byte c = s[pos]; if (c == COMMA) break; bp3 = bp3 * 10 + (c - '0'); pos++; }
        pos++;
        while (true) { byte c = s[pos]; if (c == COMMA) break; bv3i = bv3i * 10 + (c - '0'); pos++; }
        pos++;
        while (true) { byte c = s[pos]; if (c == COMMA) break; ap3i = ap3i * 10 + (c - '0'); pos++; }
        pos++;
        while (true) { byte c = s[pos]; if (c == COMMA) break; av3i = av3i * 10 + (c - '0'); pos++; }
        if (s[pos] == COMMA) pos++;
        final double bv3 = (double) bv3i, av3 = (double) av3i;
        sumBidVolumes += bv3; sumAskVolumes += av3;
        sumBidWeightedPrice += ((double) bp3) * bv3; sumAskWeightedPrice += ((double) ap3i) * av3;
        weightedBidDepth += bv3 * 0.33333333d; weightedAskDepth += av3 * 0.33333333d;

        // level 4 (i=4)
        int bp4 = 0, bv4i = 0, ap4i = 0, av4i = 0;
        while (true) { byte c = s[pos]; if (c == COMMA) break; bp4 = bp4 * 10 + (c - '0'); pos++; }
        pos++;
        while (true) { byte c = s[pos]; if (c == COMMA) break; bv4i = bv4i * 10 + (c - '0'); pos++; }
        pos++;
        while (true) { byte c = s[pos]; if (c == COMMA) break; ap4i = ap4i * 10 + (c - '0'); pos++; }
        pos++;
        while (true) { byte c = s[pos]; if (c == COMMA) break; av4i = av4i * 10 + (c - '0'); pos++; }
        if (s[pos] == COMMA) pos++;
        final double bv4 = (double) bv4i, av4 = (double) av4i;
        sumBidVolumes += bv4; sumAskVolumes += av4;
        sumBidWeightedPrice += ((double) bp4) * bv4; sumAskWeightedPrice += ((double) ap4i) * av4;
        weightedBidDepth += bv4 * 0.25d; weightedAskDepth += av4 * 0.25d;

        // level 5 (i=5)
        int bp5 = 0, bv5i = 0, ap5i = 0, av5i = 0;
        while (true) { byte c = s[pos]; if (c == COMMA) break; bp5 = bp5 * 10 + (c - '0'); pos++; }
        pos++;
        while (true) { byte c = s[pos]; if (c == COMMA) break; bv5i = bv5i * 10 + (c - '0'); pos++; }
        pos++;
        while (true) { byte c = s[pos]; if (c == COMMA) break; ap5i = ap5i * 10 + (c - '0'); pos++; }
        pos++;
        while (true) { byte c = s[pos]; if (c == COMMA) break; av5i = av5i * 10 + (c - '0'); pos++; }
        if (s[pos] == COMMA) pos++;
        final double bv5 = (double) bv5i, av5 = (double) av5i;
        sumBidVolumes += bv5; sumAskVolumes += av5;
        sumBidWeightedPrice += ((double) bp5) * bv5; sumAskWeightedPrice += ((double) ap5i) * av5;
        weightedBidDepth += bv5 * 0.2d; weightedAskDepth += av5 * 0.2d;


        // @=============================== 计算20个因子，写入可复用缓存数组 ================================@
        // 非输出窗口：不需要计算 20 因子，只更新 t-1 状态即可
        if (!shouldEmit) {
            hasPrev = true;
            prevAp1 = ap1;
            prevBp1 = bp1;
            prevSumBidVolumes = sumBidVolumes;
            prevSumAskVolumes = sumAskVolumes;
            prevTradeTime = secOfDay;
            return;
        }

        final double[] factors = tmpFactors;

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

        // -------------------- 3) Mapper 内聚合与更新 t-1 状态 --------------------
        // 注意：即使不输出该条记录，也要维护 t-1 状态，
        // 因为 09:30:00 的 t-1 可能来自 09:29:57（在输出窗口之外）。
        // Key 编码（int32）：
        // ┌───────────────────── FROM HIGH --to--> LOW ────────────────────────┐
        // │ unused(6b,=0)    │ dayId(MMDD,0..1231)   │ timeCode(15b)           │
        // │ bits:[31..26]    │ bits:[25..15]         │ bits:[14..0]            │
        // │                  │                       │ secOfDay-06:00:00       │
        // └────────────────────────────────────────────────────────────────────┘
        // - dayId：month*100+day（跳过 year），放在高位
        // - timeCode：secOfDay - 06:00:00，限定在 0..32767（低 15 位）
        int timeCode = secOfDay - BASE_SEC_6AM;
        int packedKey = (dayId << 15) | (timeCode & MASK_TIME15);
        localAggHashTable.add_by_python3p9(packedKey, factors);

        // 更新 t-1
        hasPrev = true;
        prevAp1 = ap1;
        prevBp1 = bp1;
        prevSumBidVolumes = sumBidVolumes;
        prevSumAskVolumes = sumAskVolumes;
        prevTradeTime = secOfDay;
    }





// @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
// @#                                         ☆ Mapper 内聚合哈希表 ☆                                         #@
// @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@    

   /**
     * Mapper 清理与最终输出阶段
     * 
     * <p>在所有 {@code map()} 调用结束后执行，负责将本地内存中缓存的聚合结果写出到 HDFS。</p>
     * 
     * <p>mapper对象全流程：</p>
     * <p>1. Mapper 端预聚合：利用 {@code localAggHashTable} 按时间戳（packedKey）对因子进行累加和计数。</p>
     * <p>2. 批量输出：遍历哈希表，将每个时间点的 {@code sum[20] + count} 封装为 {@code FactorWritable} 并写入上下文，
     *    从而大幅减少 Shuffle 过程中的网络传输量。
     * </p>
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // mapper 输出：对 (dayId, tradeTime) 的本地聚合 sum[20] + count。
        localAggHashTable.emitTo(context, outKey, outValue);
    }




    

// @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
// @#                                         ☆ Mapper 内聚合哈希表 ☆                                         #@
// @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

    /**
     * <b>原生 int-&gt;double[21] 哈希表</b>：
     * <p>
     * 采用开放定址法与扁平 double 数组存储（容量 * 21）。
     * key 为 {@code ((dayId &lt;&lt; 15) | timeCode) + 1}，零是保留值，作为哨兵（nil）。
     * </p>
     *
     * <b>性能优化设计：</b>
     * <p>
     * 1. <b>初始容量16384 + 自动扩容</b>：
     * </p><p>
     *      以单交易日最多 {@code 4802} 个时间点为基准设置初始容量，
     *      并在负载超过阈值时自动扩容，避免溢出。
     *      如未来时间范围扩大或频率提升，仍能通过扩容维持正确性。
     * </p><p>
     * 2. <b>容量取 2 的幂</b>：
     * </p><p>
     *      用 {@code ptr = hash & MASK} 替代取模，经典的取余优化，不赘述。
     * </p><p>
     * 3. <b>先进的探测算法</b>：
     * </p><p>
     *      根据基准测试结果，在增删操作方面，{@code CPython 3.9} 内置字典的性能显著优于 C++ 标准库的 
     *      {@code std::map}（基于红黑树）和 {@code std::unordered_map}（基于哈希表，具体实现未深入分析）。
     *      经查阅 {@code CPython 3.9} 源码发现，其字典实现采用了基于<b>伪随机序列</b>和<b>扰动探测递推</b>的探测机制，
     *      具体递推公式为：{@code perturb >>>= 5; ptr = (5*ptr + 1 + perturb) & MASK;}（其中 {@code MASK = CAPACITY - 1}）。
     *      该算法要求容量必须为 2 的幂，否则其寻址与探测性质将失效。
     *      除探测机制外，{@code CPython 3.9} 还为不同类型的键对象实现了针对性优化，此处不再展开。
     *      本哈希表的设计复现了 {@code CPython 3.9} 字典实现中的核心探测逻辑。
     * </p><p>
     * 4. <b>简化操作</b>：
     * </p><p>
     *      仅支持插入与累加，不提供删除功能，
     *      匹配 Mapper 单次构建、cleanup 阶段输出的使用模式。
     * </p><p>
     * <b>REMARK BY AUTHOR</b>
     * <p>
     *      测试表明，当前键值在 16384 个槽位中呈现出零冲突的完美哈希分布。虽然曾尝试设计针对该特定分布的静态完美哈希，
     *      但实际性能相比 {@code CPython 3.9} 字典方案提升不足 0.1 秒。考虑到数据分布可能随跨日或时间编码调整而变化，
     *      最终决定采用 {@code CPython 3.9} 通用的伪随机探测逻辑，在确保通用性的同时，在当前数据集上实现了最优性能。
     * </p><p>  
     *      进一步分析表明，时间戳的 30 位压缩优化效果远超过哈希表本身的优化，基准测试显示其相比类封装方案可提升超过 10 秒。
     *      而哈希表内部的各类优化（线性探测、完美哈希、以及当前仿 {@code CPython 3.9} 的实现）带来的性能增益均不显著，
     *      这是因为程序的主要性能瓶颈在磁盘 I/O 与内存之间，而非哈希碰撞的微调。
     * </p><p>  
     *      因此可以得出结论：在此应用场景下，哈希表并非关键瓶颈，进一步的优化难以带来明显的整体提升。
     *      当前采用 {@code CPython 3.9} 的探测策略，更多是基于其算法本身的简洁性与通用性考量。
     * </p>
     */
    private static final class AGG21_FP64 {
        private static final int NUM_FACTORS = VALUE_SIZE;
        private static final int INITIAL_CAPACITY = 16384;
        private static final double MAX_LOAD = 0.555d;

        int capacity;
        int mask;
        int size;
        int[] keys;      // storedKey = compactKey + 1; 0 means empty
        double[] vals;   // flat: slot * 21 + i

        AGG21_FP64() {
            capacity = INITIAL_CAPACITY;
            mask = capacity - 1;
            size = 0;
            keys = new int[capacity];
            vals = new double[capacity * NUM_FACTORS];
        }

        /**
         * add_by_python3p9：复刻 CPython 3.9 dict 的部分逻辑
         *
         * <p>CPython 3.9 的 Objects/dictobject.c / dictnotes.txt 所实现/描述的核心递推是：</p>
         * <pre>
         * ======================================
         * mask = (1 << p) - 1
         * i = h &amp; mask
         * perturb = h
         * while True:
         *     perturb >>= 5
         *     i = (i * 5 + perturb + 1) &amp; mask
         * ======================================
         * </pre>
         *
         * <b>设计说明：</b>
         * <p>
         * <b>1. (5i+1)mod(2^p) 伪随机探测序列</b> ：
         *      当表长为 2 的幂时，递推式 (5*i + 1) mod 2^k 会生成一个遍历 0 到 2^k-1 所有值的满周期序列。
         *      这保证了从任意位置出发的探测都能覆盖整个表，且步长分布较分散，有效缓解了线性探测（i+1）带来的主聚簇问题。
         * </p><p>
         * <b>2. perturb 扰动因子</b> ：
         *      将原始哈希值的高位信息逐步引入探测计算。每轮探测将 perturb 逻辑右移 5 位（Java 必须使用 `>>>` 实现无符号移位），
         *      并将其加至递推结果中。这使得探测序列不仅依赖于当前索引 i，还受到哈希值所有比特的渐进影响，进一步减少了不同哈希值映射到相同探测路径的可能性。
         * </p>
         */
        void add_by_python3p9(int packedKey, double[] factors20) {
            int stored = packedKey + 1;
            if ((size + 1) > (int) (capacity * MAX_LOAD)) {
                resize(capacity << 1);
            }
            int hash = stored;
            int ptr = hash & mask;
            int perturb = hash;
            int probes = 0;

            while (true) {
                int k = keys[ptr];
                if (k == 0) {
                    keys[ptr] = stored;
                    int base = ptr * NUM_FACTORS;
                    for (int i = 0; i < FACTOR_COUNT; i++) vals[base + i] = factors20[i];
                    vals[base + FACTOR_COUNT] = 1.0d;
                    size++;
                    return;
                }
                if (k == stored) {
                    int base = ptr * NUM_FACTORS;
                    for (int i = 0; i < FACTOR_COUNT; i++) vals[base + i] += factors20[i];
                    vals[base + FACTOR_COUNT] += 1.0d;
                    return;
                }

                probes++;
                if (probes > mask)
                    throw new IllegalStateException("AGG21_FP64 overflow: CAPACITY too small for observed keys");
                // CPython: perturb >>= PERTURB_SHIFT (5)
                perturb >>>= 5;
                ptr = (5 * ptr + 1 + perturb) & mask;
            }
        }

        private void resize(int newCapacity) {
            if (newCapacity <= 0) {
                throw new IllegalStateException("AGG21_FP64 resize overflow");
            }
            int[] oldKeys = keys;
            double[] oldVals = vals;
            int oldCapacity = capacity;
            int newMask = newCapacity - 1;
            int[] newKeys = new int[newCapacity];
            double[] newVals = new double[newCapacity * NUM_FACTORS];

            for (int ptr = 0; ptr < oldCapacity; ptr++) {
                int stored = oldKeys[ptr];
                if (stored == 0) continue;
                int hash = stored;
                int i = hash & newMask;
                int perturb = hash;
                while (true) {
                    if (newKeys[i] == 0) {
                        newKeys[i] = stored;
                        int oldBase = ptr * NUM_FACTORS;
                        int newBase = i * NUM_FACTORS;
                        for (int j = 0; j < NUM_FACTORS; j++) newVals[newBase + j] = oldVals[oldBase + j];
                        break;
                    }
                    perturb >>>= 5;
                    i = (5 * i + 1 + perturb) & newMask;
                }
            }

            keys = newKeys;
            vals = newVals;
            capacity = newCapacity;
            mask = newMask;
        }

        /**
         * 将哈希表中已聚合的 sum+count 逐条写出到 Mapper Context。
         * <p>仅做遍历与拷贝，不再做任何求和计算；key 为打包后的 (dayId,timeCode)。</p>
         */
        void emitTo(Context context, IntWritable outKey, FactorWritable outValue)
                throws IOException, InterruptedException {
            final int[] keys = this.keys;
            final double[] vals = this.vals;
            final double[] out = outValue.factors;
            for (int ptr = 0; ptr < capacity; ptr++) {
                int stored = keys[ptr];
                if (stored == 0) continue;
                int packedKey = stored - 1;
                outKey.set(packedKey);
                int base = ptr * NUM_FACTORS;
                for (int i = 0; i < NUM_FACTORS; i++) out[i] = vals[base + i];
                context.write(outKey, outValue);
            }
        }


    }


}
