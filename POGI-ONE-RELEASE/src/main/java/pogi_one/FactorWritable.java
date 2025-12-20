package pogi_one;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Hadoop Writable：用于在 Map/Shuffle/Reduce 阶段传递“20 维因子向量（求和态）+ count”。
 *
 * <p>为兼容不同 day 的股票数不一致/缺点，最后一维 {@code factors[20]} 用作 {@code count}（累计条数）。</p>
 *
 * <p><b>传输层压缩：</b> 为降低 shuffle/spill 体积，本 Writable 在序列化时按 {@code float} 写出，
 * 反序列化时读回 {@code float} 并转换为 {@code double} 存放到 {@link #factors}。
 * 计算与累加仍使用 {@code double}（精度主要损失来自 double-&gt;float-&gt;double）。</p>
 */
public class FactorWritable implements Writable {
    private static final int SIZE = 21;
    public final double[] factors = new double[SIZE];

    public FactorWritable() {
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for (double f : factors) out.writeFloat((float) f);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        for (int i = 0; i < SIZE; i++) this.factors[i] = (double) in.readFloat();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(64);
        sb.append("Factors: [");
        for (int i = 0; i < SIZE; i++) {
            if (i != 0) sb.append(", ");
            sb.append(factors[i]);
        }
        sb.append(']');
        return sb.toString();
    }
}
