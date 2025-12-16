package factor;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Hadoop Writable：用于在 Map/Shuffle/Reduce 阶段传递“20 维因子向量（求和态）”。
 *
 * <p>当前 data_fix 数据集中，每个 (day,time) 都有 300 只股票且无缺失，因此 reducer 端可直接除以 300。</p>
 */
public class FactorWritable implements Writable {
    private static final int SIZE = 20;
    public final double[] factors = new double[SIZE];

    public FactorWritable() {
    }

    public void add(FactorWritable other) {
        // 逐维累加因子和。
        for (int i = 0; i < SIZE; i++) {
            this.factors[i] += other.factors[i];
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for (double f : factors) {
            out.writeDouble(f);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        for (int i = 0; i < SIZE; i++) {
            this.factors[i] = in.readDouble();
        }
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
