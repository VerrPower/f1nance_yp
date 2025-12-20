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
 *          {@code <ShortWritable, Text>}：key 为文件索引（用于检测“换股票文件”）；
 *          value 为单行 CSV（CRLF 时可能含行尾 {@code '\r'}）。</li>
 *   <li><b>解析</b> : 
 *          直接在 {@code byte[]} 上进行 ASCII 扫描解析，避免 {@code Text->String} 分配；
 *          只在行尾裁剪 {@code '\r'}。</li>
 *   <li><b>计算20个因子</b> : 
 *          解析前 5 档盘口与必要字段，计算 20 个因子；其中 {@code alpha_17/18/19} 依赖 t-1 状态。
 *          当检测到 fileId 变化或时间倒退时清空。</li>
 *   <li><b>输出</b> : 
 *          mapper 输出 {@code <IntWritable, FactorWritable>}：
 *          key 为打包的 {@code (dayId,time)}，value 为 {@code sum[20]+count}（供 reducer 求均值）。</li>
 *   <li><b>Mapper 内聚合</b> : 
 *          在mapper中提前执行不同股票间因子值累加，对输入“张量”的“股票”维度进行压缩。
 *          把 20 维因子向量按照时间戳为key，累加到本地的 {@code AGG21_FP64} 哈希表（额外维护 count）；
 *          并在最后的 {@code cleanup()} 中批量输出聚合后的 KV。
 *   </li>
 * </ul>
 */
public class StockFactorMapper extends Mapper<ShortWritable, Text, IntWritable, FactorWritable> {

    // 常数列表
    private static final double EPSILON = 1.0e-7;
    private static final byte COMMA = (byte) ',';
    private static final byte CR = (byte) '\r';
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

    
    // t-1 相关状态（只保存计算 alpha_17/18/19 所需的最少信息）。
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
    @Override
    protected void map(ShortWritable p_key, Text p_value, Context p_context) 
        throws IOException, InterruptedException 
    {
        // CombineFileInputFormat：一个 mapper 会顺序处理多个股票文件；alpha_17/18/19 的 t-1 必须在“同一股票文件”内定义。
        // FixedCombineTextInputFormat 的输入 key 直接是 fileIndex（ShortWritable）：检测 fileId 变化时清空 t-1 状态。
        final int fileId = p_key.get();
        if (prevFileId != fileId) {
            prevFileId = fileId;
            hasPrev = false;
            prevTradeTime = Integer.MIN_VALUE;
        }

        // 进一步避免 Text->String 分配：直接在 byte[] 上做 ASCII 解析。
        final int n = p_value.getLength();
        if (n <= 0) return;
        final byte[] s = p_value.getBytes();
        // Hadoop LineRecordReader 在 CRLF 文件中通常会保留行尾 '\r'（但不包含 '\n'）。
        // 为了让每个字段的解析循环不必重复判断 '\r'，这里仅在行尾做一次裁剪。
        int end = n;
        if (s[end - 1] == CR) end--;
        // 数据行以 YYYYMMDD 开头；表头以 't' 开头。这里用首字符快速过滤。
        final byte c0 = s[0];
        if (c0 < '0' || c0 > '9') return;

        // @===================================== 基于原始字节数组的单指针字段解析 ====================================@
        // field 0/1：行首固定为 YYYYMMDD,HHMMSS,
        // 只需要 MMDD：直接跳过 year 的 4 位。
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

        // Combine 可能让一个 mapper 处理多个文件，时间戳会“跳回早期”；遇到 tradeTime 逆序时清空 t-1。
        if (hasPrev && secOfDay < prevTradeTime) hasPrev = false;
        

        // skip fields 2..11（10 个字段）
        for (int k = 0; k < 10; k++) {
            while (pos < end && s[pos] != COMMA) pos++;
            pos++; // skip comma
        }

        // field 12/13: tBidVol / tAskVol
        int tBidVol = 0;
        int tAskVol = 0;

        final boolean shouldEmit = (secOfDay >= 34_200 && secOfDay <= 41_400) || (secOfDay >= 46_800 && secOfDay <= 54_000);

        if (shouldEmit) {
            // tBidVol
            while (pos < end) {byte c = s[pos]; if (c == COMMA) break; tBidVol = tBidVol * 10 + (c - '0'); pos++; }
            pos++; // skip comma

            // tAskVol
            while (pos < end) {byte c = s[pos]; if (c == COMMA) break; tAskVol = tAskVol * 10 + (c - '0'); pos++; }
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


        // @==================================== 计算20个因子，写入可复用缓存数组 ====================================@
        // 非输出窗口：不需要计算 20 因子，只更新 t-1 状态即可。
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
        // ┌────────────── FROM HIGH --to--> LOW ──────────────┐
        // │ dayId = MMDD（数值） │ timeCode(15 bits)           │
        // └───────────────────────────────────────────────────┘
        // - dayId：month*100+day（跳过 year），放在高位
        // - timeCode：secOfDay - 06:00:00，限定在 0..32767（低 15 位）
        int timeCode = secOfDay - BASE_SEC_6AM;
        int packedKey = (dayId << 15) | (timeCode & MASK_TIME15);
        localAggHashTable.add_by_python3p9(packedKey, factors);

        // 更新 t-1（仅保留必要统计量）
        hasPrev = true;
        prevAp1 = ap1;
        prevBp1 = bp1;
        prevSumBidVolumes = sumBidVolumes;
        prevSumAskVolumes = sumAskVolumes;
        prevTradeTime = secOfDay;
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // mapper 输出：对 (dayId, tradeTime) 的本地聚合 sum[20] + count。
        localAggHashTable.emitTo(context, outKey, outValue);
    }


    // murmur...
    // key 编码与 tradingDay/tradeTime 的解析均已内联到 map() 热路径（year 不再参与 key 编码）。


    

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
     * 1. <b>固定容量（16384）</b>：
     * </p><p>
     *      基于当前数据处理范围（单交易日最多 {@code 4802} 个时间点，
     *      对应 9:30-11:30 + 13:00-15:00 窗口的 3 秒频数据），预分配足够容量以避免动态扩容。
     *      若未来数据处理范围扩大（跨日、窗口延长或频率提高），可能触发溢出保护
     *      （防御性编程而已，实际不大可能遇到）。
     * </p><p>
     * 2. <b>容量取 2 的幂</b>：
     * </p><p>
     *      用 {@code ptr = hash & MASK} 替代取模，经典的取余优化，不赘述。
     * </p><p>
     * 3. <b>先进的探测算法</b>：
     * </p><p>
     *      benchmark实测显示， {@code CPython 3.9} 的字典的增删
     *      反而显著快于 <b>c++</b> 标准库{@code std::map}（底层红黑树）和{@code std::unordered_map}（底层哈希表，具体实现没研究）。
     *      研究 {@code CPython 3.9} 源码发现，其采用了先进的<b>伪随机序列</b> 和<b>扰动探测递推</b> ：
     *      {@code perturb >>>= 5; ptr = (5*ptr + 1 + perturb) & MASK;} （{@code MASK=CAPACITY-1}）。
     *      其中容量必须为 2 的幂，否则寻址与探测性质失效。
     *      除此之外，{@code CPython 3.9} 还针对不同键对象做了单独优化，这里不展开。
     *      这个哈希表复刻了 {@code CPython 3.9} 的 dict 的关键部分。
     * </p><p>
     * 4. <b>简化操作语义</b>：
     * </p><p>
     *      仅支持插入与累加，不提供删除功能，
     *      匹配 Mapper 单次构建、cleanup 阶段输出的使用模式。
     * </p><p>
     * <b>[锐评一下]</b>
     * </p><p>
     *      测试发现，当前 key 分布在 16384 个槽位的地址空间里恰好构成完美哈希，<b>冲突率为 0</b>。
     *      我曾尝试过基于该特化分布设计静态完美哈希，但实际测试显示相对于 {@code CPython 3.9} 方案提升不足 <b>0.1</b> 秒。
     *      考虑到数据分布可能变化（跨日或时间编码调整），最终采用 {@code CPython 3.9} 中通用的伪随机探测逻辑，
     *      在保证通用性的同时意外获得了当前数据集的最佳性能表现。
     * </p><p>
     *      实际上，上面的对时间戳的30bit位运算压缩的优化效果比优化哈希表大得多，bench测试显示比类封装版本的key快了10多秒？
     *      而哈希表优化（先是实现的线性探测，后面改为完美哈希，最后又复刻python3.9字典）带来的提升只能说“统计不显著”，
     *      因为整个程序的热点函数一定在HDD<->RAM的IO上，而不是那寥寥几个哈希冲突。
     * </p><p>
     *      最后，我们只能无奈地总结道，在这个场景之下，哈希表并不是瓶颈，对他的优化注定不会带来显著的提升。
     *      我们最终选择 {@code CPython 3.9} 的版本更多的是出于对其算法本身的欣赏
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
