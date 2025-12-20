package pogi_one;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

public class Agg21Fp64Test {
    private static final int FACTOR_COUNT = 20;
    private static final int VALUE_SIZE = FACTOR_COUNT + 1;

    @Test
    void resizeTriggersAtLoadFactor() throws Exception {
        Class<?> clazz = Class.forName("pogi_one.StockFactorMapper$AGG21_FP64");
        Constructor<?> ctor = clazz.getDeclaredConstructor();
        ctor.setAccessible(true);
        Object agg = ctor.newInstance();

        Field capacityField = clazz.getDeclaredField("capacity");
        capacityField.setAccessible(true);
        int cap0 = (int) capacityField.get(agg);

        Field maxLoadField = clazz.getDeclaredField("MAX_LOAD");
        maxLoadField.setAccessible(true);
        double maxLoad = (double) maxLoadField.get(null);

        Method add = clazz.getDeclaredMethod("add_by_python3p9", int.class, double[].class);
        add.setAccessible(true);

        int threshold = (int) (cap0 * maxLoad);
        double[] factors = new double[FACTOR_COUNT];
        for (int i = 0; i < FACTOR_COUNT; i++) factors[i] = 1.0d;

        for (int i = 0; i < threshold; i++) {
            add.invoke(agg, i, factors);
        }
        assertEquals(cap0, (int) capacityField.get(agg));

        add.invoke(agg, threshold, factors);
        int cap1 = (int) capacityField.get(agg);
        assertEquals(cap0 * 2, cap1);
    }

    @Test
    void insertAccumulatesFactorsAndCount() throws Exception {
        Class<?> clazz = Class.forName("pogi_one.StockFactorMapper$AGG21_FP64");
        Constructor<?> ctor = clazz.getDeclaredConstructor();
        ctor.setAccessible(true);
        Object agg = ctor.newInstance();

        Method add = clazz.getDeclaredMethod("add_by_python3p9", int.class, double[].class);
        add.setAccessible(true);

        int key = 12345;
        double[] f1 = new double[FACTOR_COUNT];
        double[] f2 = new double[FACTOR_COUNT];
        for (int i = 0; i < FACTOR_COUNT; i++) {
            f1[i] = 1.0d;
            f2[i] = 2.0d;
        }

        add.invoke(agg, key, f1);
        add.invoke(agg, key, f2);

        Field keysField = clazz.getDeclaredField("keys");
        Field valsField = clazz.getDeclaredField("vals");
        keysField.setAccessible(true);
        valsField.setAccessible(true);

        int[] keys = (int[]) keysField.get(agg);
        double[] vals = (double[]) valsField.get(agg);

        int stored = key + 1;
        int slot = findSlot(keys, stored);
        assertNotEquals(-1, slot);

        int base = slot * VALUE_SIZE;
        for (int i = 0; i < FACTOR_COUNT; i++) {
            assertEquals(3.0d, vals[base + i], 1e-12);
        }
        assertEquals(2.0d, vals[base + FACTOR_COUNT], 1e-12);
    }

    private static int findSlot(int[] keys, int stored) {
        for (int i = 0; i < keys.length; i++) {
            if (keys[i] == stored) return i;
        }
        return -1;
    }
}
