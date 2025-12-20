package schubfach;

public final class SchubfachFloat {
    private static final int FLOAT_MANTISSA_BITS = 23;
    private static final int FLOAT_MANTISSA_MASK = (1 << FLOAT_MANTISSA_BITS) - 1;
    private static final int FLOAT_EXPONENT_BITS = 8;
    private static final int FLOAT_EXPONENT_MASK = (1 << FLOAT_EXPONENT_BITS) - 1;
    private static final int FLOAT_EXPONENT_BIAS = (1 << (FLOAT_EXPONENT_BITS - 1)) - 1;

    private static final long LOG10_2_DENOMINATOR = 10000000L;
    private static final long LOG10_2_NUMERATOR = 3010299L;
    private static final long LOG10_5_DENOMINATOR = 10000000L;
    private static final long LOG10_5_NUMERATOR = 6989700L;
    private static final long LOG2_5_DENOMINATOR = 10000000L;
    private static final long LOG2_5_NUMERATOR = 23219280L;

    private static final int POW5_BITCOUNT = 61;
    private static final int POW5_HALF_BITCOUNT = 31;
    private static final int POW5_INV_BITCOUNT = 59;
    private static final int POW5_INV_HALF_BITCOUNT = 31;

    private static final int[] POW5_SPLIT_HI = {
        536870912, 671088640, 838860800, 1048576000, 655360000, 819200000, 1024000000, 640000000, 800000000,
        1000000000, 625000000, 781250000, 976562500, 610351562, 762939453, 953674316, 596046447, 745058059,
        931322574, 582076609, 727595761, 909494701, 568434188, 710542735, 888178419, 555111512, 693889390,
        867361737, 542101086, 677626357, 847032947, 1058791184, 661744490, 827180612, 1033975765, 646234853,
        807793566, 1009741958, 631088724, 788860905, 986076131, 616297582, 770371977, 962964972, 601853107,
        752316384, 940395480
    };

    private static final int[] POW5_SPLIT_LO = {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1073741824, 268435456, 872415232, 1619001344, 1486880768,
        1321730048, 289210368, 898383872, 1659850752, 1305842176, 1632302720, 1503507488, 671256724, 839070905,
        2122580455, 521306416, 1725374844, 546105819, 145761362, 91100851, 1187617888, 1484522360, 1196261931,
        2032198326, 1466506084, 379695390, 474619238, 1130144959, 437905143, 1621123253, 415791331, 1333611405,
        1130143345, 1412679181
    };

    private static final int[] POW5_INV_SPLIT_HI = {
        268435456, 214748364, 171798691, 137438953, 219902325, 175921860, 140737488, 225179981, 180143985,
        144115188, 230584300, 184467440, 147573952, 236118324, 188894659, 151115727, 241785163, 193428131,
        154742504, 247588007, 198070406, 158456325, 253530120, 202824096, 162259276, 259614842, 207691874,
        166153499, 265845599, 212676479, 170141183
    };

    private static final int[] POW5_INV_SPLIT_LO = {
        1, 1717986919, 1803886265, 1013612282, 1192282922, 953826338, 763061070, 791400982, 203624056, 162899245,
        1978625710, 1582900568, 1266320455, 308125809, 675997377, 970294631, 1981968139, 297084323, 1955654377,
        1840556814, 613451992, 61264864, 98023782, 78419026, 1780722139, 1990161963, 733136111, 1016005619,
        337118801, 699191770, 988850146
    };

    private SchubfachFloat() {
    }

    static void convert_zero_string_Schubfach(float x, byte[] dst, int startIndex) {
        int end = writeShortest(x, dst, startIndex, null);
        dst[end] = 0;
    }

    public static int writeShortest(float value, byte[] buf, int pos, byte[] digits) {
        if (!Float.isFinite(value) || value == 0.0f) {
            buf[pos++] = (byte) '0';
            return pos;
        }

        int bits = Float.floatToRawIntBits(value);
        boolean sign = bits < 0;
        bits &= 0x7fffffff;

        int ieeeExponent = (bits >> FLOAT_MANTISSA_BITS) & FLOAT_EXPONENT_MASK;
        int ieeeMantissa = bits & FLOAT_MANTISSA_MASK;
        int e2;
        int m2;
        if (ieeeExponent == 0) {
            e2 = 1 - FLOAT_EXPONENT_BIAS - FLOAT_MANTISSA_BITS;
            m2 = ieeeMantissa;
        } else {
            e2 = ieeeExponent - FLOAT_EXPONENT_BIAS - FLOAT_MANTISSA_BITS;
            m2 = ieeeMantissa | (1 << FLOAT_MANTISSA_BITS);
        }

        boolean even = (m2 & 1) == 0;
        int mv = 4 * m2;
        int mp = mv + 2;
        int mm = mv - ((m2 != (1 << FLOAT_MANTISSA_BITS)) || (ieeeExponent <= 1) ? 2 : 1);
        e2 -= 2;

        int dp;
        int dv;
        int dm;
        int e10;
        boolean dpIsTrailingZeros;
        boolean dvIsTrailingZeros;
        boolean dmIsTrailingZeros;
        int lastRemovedDigit = 0;
        if (e2 >= 0) {
            int q = (int) ((e2 * LOG10_2_NUMERATOR) / LOG10_2_DENOMINATOR);
            int k = POW5_INV_BITCOUNT + pow5bits(q) - 1;
            int i = -e2 + q + k;
            dv = (int) mulPow5InvDivPow2(mv, q, i);
            dp = (int) mulPow5InvDivPow2(mp, q, i);
            dm = (int) mulPow5InvDivPow2(mm, q, i);
            if (q != 0 && ((dp - 1) / 10 <= dm / 10)) {
                int l = POW5_INV_BITCOUNT + pow5bits(q - 1) - 1;
                lastRemovedDigit = (int) (mulPow5InvDivPow2(mv, q - 1, -e2 + q - 1 + l) % 10);
            }
            e10 = q;
            dpIsTrailingZeros = pow5Factor(mp) >= q;
            dvIsTrailingZeros = pow5Factor(mv) >= q;
            dmIsTrailingZeros = pow5Factor(mm) >= q;
        } else {
            int q = (int) ((-e2 * LOG10_5_NUMERATOR) / LOG10_5_DENOMINATOR);
            int i = -e2 - q;
            int k = pow5bits(i) - POW5_BITCOUNT;
            int j = q - k;
            dv = (int) mulPow5divPow2(mv, i, j);
            dp = (int) mulPow5divPow2(mp, i, j);
            dm = (int) mulPow5divPow2(mm, i, j);
            if (q != 0 && ((dp - 1) / 10 <= dm / 10)) {
                j = q - 1 - (pow5bits(i + 1) - POW5_BITCOUNT);
                lastRemovedDigit = (int) (mulPow5divPow2(mv, i + 1, j) % 10);
            }
            e10 = q + e2;
            dpIsTrailingZeros = 1 >= q;
            dvIsTrailingZeros = (q < FLOAT_MANTISSA_BITS) && (mv & ((1 << (q - 1)) - 1)) == 0;
            dmIsTrailingZeros = (mm % 2 == 1 ? 0 : 1) >= q;
        }

        int dplength = decimalLength(dp);
        int exp = e10 + dplength - 1;

        int removed = 0;
        if (dpIsTrailingZeros && !even) {
            dp--;
        }
        while (dp / 10 > dm / 10) {
            dmIsTrailingZeros &= dm % 10 == 0;
            dp /= 10;
            lastRemovedDigit = dv % 10;
            dv /= 10;
            dm /= 10;
            removed++;
        }
        if (dmIsTrailingZeros && even) {
            while (dm % 10 == 0) {
                dp /= 10;
                lastRemovedDigit = dv % 10;
                dv /= 10;
                dm /= 10;
                removed++;
            }
        }

        if (dvIsTrailingZeros && (lastRemovedDigit == 5) && ((dv & 1) == 0)) {
            lastRemovedDigit = 4;
        }
        int output = dv + ((dv == dm && !(dmIsTrailingZeros && even)) || (lastRemovedDigit >= 5) ? 1 : 0);
        int olength = dplength - removed;

        if (sign) {
            buf[pos++] = (byte) '-';
        }
        if (digits == null) {
            digits = new byte[16];
        }
        int tmp = output;
        for (int i = olength - 1; i >= 0; i--) {
            int c = tmp % 10;
            tmp /= 10;
            digits[i] = (byte) ('0' + c);
        }
        return writePlain(buf, pos, digits, olength, exp);
    }

    private static int writePlain(byte[] buf, int pos, byte[] digits, int len, int exp) {
        if (exp >= 0) {
            int intLen = exp + 1;
            if (len > intLen) {
                int fracLen = len - intLen;
                System.arraycopy(digits, 0, buf, pos, intLen);
                pos += intLen;
                buf[pos++] = (byte) '.';
                System.arraycopy(digits, intLen, buf, pos, fracLen);
                pos += fracLen;
                return pos;
            }
            System.arraycopy(digits, 0, buf, pos, len);
            pos += len;
            int zeros = intLen - len;
            for (int i = 0; i < zeros; i++) {
                buf[pos++] = (byte) '0';
            }
            return pos;
        }
        buf[pos++] = (byte) '0';
        buf[pos++] = (byte) '.';
        int zeros = -exp - 1;
        for (int i = 0; i < zeros; i++) {
            buf[pos++] = (byte) '0';
        }
        System.arraycopy(digits, 0, buf, pos, len);
        return pos + len;
    }

    private static int pow5bits(int e) {
        return e == 0 ? 1 : (int) ((e * LOG2_5_NUMERATOR + LOG2_5_DENOMINATOR - 1) / LOG2_5_DENOMINATOR);
    }

    private static int pow5Factor(int value) {
        int count = 0;
        while (value > 0) {
            if (value % 5 != 0) return count;
            value /= 5;
            count++;
        }
        return count;
    }

    private static long mulPow5divPow2(int m, int i, int j) {
        long bits0 = m * (long) POW5_SPLIT_HI[i];
        long bits1 = m * (long) POW5_SPLIT_LO[i];
        return (bits0 + (bits1 >> POW5_HALF_BITCOUNT)) >> (j - POW5_HALF_BITCOUNT);
    }

    private static long mulPow5InvDivPow2(int m, int q, int j) {
        long bits0 = m * (long) POW5_INV_SPLIT_HI[q];
        long bits1 = m * (long) POW5_INV_SPLIT_LO[q];
        return (bits0 + (bits1 >> POW5_INV_HALF_BITCOUNT)) >> (j - POW5_INV_HALF_BITCOUNT);
    }

    private static int decimalLength(int v) {
        int length = 10;
        int factor = 1000000000;
        for (; length > 0; length--) {
            if (v >= factor) break;
            factor /= 10;
        }
        return length;
    }
}
