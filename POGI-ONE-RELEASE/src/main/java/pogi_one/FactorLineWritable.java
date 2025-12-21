package pogi_one;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * Reducer 输出行容器：保存 secOfDay 与 20 维因子均值（float[20]）。
 *
 * <p>用于输出链路最短化：Reducer 直接写入本对象，RecordWriter 读取并格式化为 CSV。</p>
 */
public final class FactorLineWritable implements Writable {
    public static final int FACTOR_COUNT = 20;

    public int secOfDay;
    public final float[] factors = new float[FACTOR_COUNT];

    public FactorLineWritable() {
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(secOfDay);
        for (int i = 0; i < FACTOR_COUNT; i++) out.writeFloat(factors[i]);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        secOfDay = in.readInt();
        for (int i = 0; i < FACTOR_COUNT; i++) factors[i] = in.readFloat();
    }
}
