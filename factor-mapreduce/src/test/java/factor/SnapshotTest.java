package factor;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class SnapshotTest {

    @Test
    public void testComputeWithSampleData() {
        // Data from user request
        // Row: 20240104,092500,...,1957500,5143750,...,254100,200,254200,12700,...
        long tradingDay = 20240104L;
        long tradeTime = 92500L; // 092500
        double totalBidVolume = 1957500.0;
        double totalAskVolume = 5143750.0;

        // Top 5 levels extracted from the row
        // bp1, bv1, ap1, av1
        // bp2, bv2, ap2, av2
        // ...
        double[] bidPrices = {254100, 254000, 253900, 253800, 253600};
        double[] bidVolumes = {200, 51500, 1000, 1100, 15500};
        double[] askPrices = {254200, 254300, 254400, 254500, 254600};
        double[] askVolumes = {12700, 8300, 15600, 40300, 40200};

        Snapshot snapshot = new Snapshot(
            tradingDay,
            tradeTime,
            totalBidVolume,
            totalAskVolume,
            bidPrices,
            bidVolumes,
            askPrices,
            askVolumes
        );

        // Compute factors (no previous snapshot)
        double[] factors = Snapshot.compute(snapshot, null);

        // Manual verifications
        // Factor 1: ap1 - bp1
        assertEquals(100.0, factors[0], 1e-6, "Factor 1 (Spread) mismatch");

        // Factor 3: Mid Price = (ap1 + bp1) / 2
        assertEquals(254150.0, factors[2], 1e-6, "Factor 3 (MidPrice) mismatch");

        // Factor 6: Sum Bid Volumes (Top 5)
        double expectedSumBid = 200 + 51500 + 1000 + 1100 + 15500; // 69300
        assertEquals(expectedSumBid, factors[5], 1e-6, "Factor 6 (SumBidVol) mismatch");

        // Factor 7: Sum Ask Volumes (Top 5)
        double expectedSumAsk = 12700 + 8300 + 15600 + 40300 + 40200; // 117100
        assertEquals(expectedSumAsk, factors[6], 1e-6, "Factor 7 (SumAskVol) mismatch");
    }
}
