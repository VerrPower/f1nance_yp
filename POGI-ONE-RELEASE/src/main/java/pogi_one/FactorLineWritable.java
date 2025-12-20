package pogi_one;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

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
