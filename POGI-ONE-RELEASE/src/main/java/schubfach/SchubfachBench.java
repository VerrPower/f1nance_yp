package schubfach;

import java.nio.charset.StandardCharsets;
import java.util.Random;

/**
 * SchubfachFloat 的小型自测与性能基准入口。
 *
 * <p>包含抽样正确性检查、百万级吞吐量基准、以及全量枚举测试（可调打印间隔）。</p>
 */
public final class SchubfachBench {
    private static final int SAMPLE_COUNT = 1_000_000;
    private static final int LOG_EVERY = 25_000_000;

    public static void main(String[] args) {
        String mode = args.length > 0 ? args[0] : "sample";
        if ("sample".equalsIgnoreCase(mode)) {
            runSampleCorrectness();
        } else if ("bench".equalsIgnoreCase(mode)) {
            runSampleBenchmark();
        } else if ("full".equalsIgnoreCase(mode)) {
            runFullTest();
        } else {
            System.out.println("Usage: SchubfachBench [sample|bench|full]");
        }
    }

    private static void runSampleCorrectness() {
        Random rng = new Random(42);
        byte[] buf = new byte[64];
        byte[] digits = new byte[16];
        int errors = 0;
        long start = System.nanoTime();

        for (int i = 1; i <= SAMPLE_COUNT; i++) {
            int bits = rng.nextInt();
            float v = Float.intBitsToFloat(bits);
            int len = SchubfachFloat.writeShortest(v, buf, 0, digits);
            String s = new String(buf, 0, len, StandardCharsets.US_ASCII);
            if (s.indexOf('E') >= 0 || s.indexOf('e') >= 0) {
                errors++;
            } else if (!Float.isFinite(v) || v == 0.0f) {
                if (!"0".equals(s)) {
                    errors++;
                }
            } else {
                float parsed = Float.parseFloat(s);
                if (Float.floatToIntBits(parsed) != Float.floatToIntBits(v)) {
                    errors++;
                }
            }
        }

        double sec = (System.nanoTime() - start) / 1_000_000_000.0;
        System.out.printf("sample_correctness: samples=%d errors=%d elapsed=%.3fs%n", SAMPLE_COUNT, errors, sec);
    }

    private static void runSampleBenchmark() {
        Random rng = new Random(42);
        byte[] buf = new byte[64];
        byte[] digits = new byte[16];
        long start = System.nanoTime();

        for (int i = 0; i < SAMPLE_COUNT; i++) {
            int bits = rng.nextInt();
            float v = Float.intBitsToFloat(bits);
            SchubfachFloat.writeShortest(v, buf, 0, digits);
        }

        double sec = (System.nanoTime() - start) / 1_000_000_000.0;
        double rate = SAMPLE_COUNT / sec;
        System.out.printf("bench: samples=%d elapsed=%.3fs rate=%.1f sample/sec%n", SAMPLE_COUNT, sec, rate);
    }

    private static void runFullTest() {
        byte[] buf = new byte[64];
        byte[] digits = new byte[16];
        long limit = 1L << 31;
        int errors = 0;
        long start = System.nanoTime();
        long lastLog = start;

        for (long i = 0; i < limit; i++) {
            float v = Float.intBitsToFloat((int) i);
            int len = SchubfachFloat.writeShortest(v, buf, 0, digits);
            String s = new String(buf, 0, len, StandardCharsets.US_ASCII);
            if (s.indexOf('E') >= 0 || s.indexOf('e') >= 0) {
                errors++;
            } else if (!Float.isFinite(v) || v == 0.0f) {
                if (!"0".equals(s)) {
                    errors++;
                }
            } else {
                float parsed = Float.parseFloat(s);
                if (Float.floatToIntBits(parsed) != Float.floatToIntBits(v)) {
                    errors++;
                }
            }

            if ((i + 1) % LOG_EVERY == 0) {
                long now = System.nanoTime();
                double sec = (now - start) / 1_000_000_000.0;
                double rate = (i + 1) / sec;
                double spanSec = (now - lastLog) / 1_000_000_000.0;
                System.out.printf("full: done=%d elapsed=%.1fs rate=%.1f sample/sec last_span=%.2fs errors=%d%n",
                        i + 1, sec, rate, spanSec, errors);
                lastLog = now;
            }
        }

        double sec = (System.nanoTime() - start) / 1_000_000_000.0;
        double rate = limit / sec;
        System.out.printf("full_done: samples=%d elapsed=%.1fs rate=%.1f sample/sec errors=%d%n",
                limit, sec, rate, errors);
    }
}
