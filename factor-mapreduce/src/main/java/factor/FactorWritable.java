package factor;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * Hadoop Writable：用于在 Map/Shuffle/Reduce 阶段传递“因子和 + 样本数”。
 *
 * <p>设计要点：</p>
 * <ul>
 *   <li>Mapper 输出时：count=1，factors 为单只股票在该 (day,time) 的 20 维因子值。</li>
 *   <li>Combiner/Reducer 聚合时：逐维累加 factors，同时累加 count。</li>
 *   <li>Reducer 最终平均：avg[i] = sumFactors[i] / count。</li>
 * </ul>
 */
public class FactorWritable implements Writable {
    private static final int SIZE = 20;
    private double[] factors;
    private int count;

    public FactorWritable() {
        this.factors = new double[SIZE];
        this.count = 0;
    }

    public FactorWritable(double[] factors) {
        if (factors.length != SIZE) {
            throw new IllegalArgumentException("Expected " + SIZE + " factors");
        }
        this.factors = Arrays.copyOf(factors, SIZE);
        this.count = 1;
    }

    public double[] getFactors() {
        return factors;
    }

    public int getCount() {
        return count;
    }

    public void add(FactorWritable other) {
        // 逐维累加因子和，并累加样本数量。
        for (int i = 0; i < SIZE; i++) {
            this.factors[i] += other.factors[i];
        }
        this.count += other.count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(count);
        for (double f : factors) {
            out.writeDouble(f);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.count = in.readInt();
        for (int i = 0; i < SIZE; i++) {
            this.factors[i] = in.readDouble();
        }
    }

    @Override
    public String toString() {
        return "Count: " + count + ", Factors: " + Arrays.toString(factors);
    }
}
