package org.hestiastore.index.senku;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.LongStream;

import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.Test;

class SenkuLongKeySummaryTest {

    @Test
    void validatesAndDefensivelyCopiesWeightedArrays() {
        final long[] keys = { -4, 7 };
        final long[] weights = { 2, 9 };
        final SenkuLongKeySummary summary = SenkuLongKeySummary.of(11, keys,
                weights);
        keys[0] = 99;
        weights[1] = 3;
        summary.keys()[0] = 100;
        summary.weights()[0] = 100;
        assertEquals(11, summary.recordCount());
        assertArrayEquals(new long[] { -4, 7 }, summary.keys());
        assertArrayEquals(new long[] { 2, 9 }, summary.weights());
        assertThrows(IllegalArgumentException.class, () -> SenkuLongKeySummary
                .of(1, new long[] { 1 }, new long[] { 2 }));
        assertThrows(IllegalArgumentException.class, () -> SenkuLongKeySummary
                .of(1, new long[] { 1 }, new long[] { 0 }));
        assertThrows(IllegalArgumentException.class, () -> SenkuLongKeySummary
                .of(2, new long[] { 1, 1 }, new long[] { 1, 1 }));
        assertThrows(IllegalArgumentException.class,
                () -> SenkuLongKeySummary.of(1, new long[] { 1 }, new long[0]));
        assertThrows(IllegalArgumentException.class,
                () -> SenkuLongKeySummary.of(-1, new long[0], new long[0]));
        assertThrows(IllegalArgumentException.class,
                () -> SenkuLongKeySummary.of(0,
                        new long[SenkuLongKeySummary.MAX_SAMPLES + 1],
                        new long[SenkuLongKeySummary.MAX_SAMPLES + 1]));
        assertThrows(IndexException.class,
                () -> SenkuLongKeySummary.of(Long.MAX_VALUE,
                        new long[] { 1, 2 }, new long[] { Long.MAX_VALUE, 1 }));
    }

    @Test
    void mergesUnequalShardWeightsAndCoincidentRepresentatives() {
        final SenkuLongKeySummary large = SenkuLongKeySummary.of(1000,
                new long[] { 1, 9 }, new long[] { 900, 100 });
        final SenkuLongKeySummary small = SenkuLongKeySummary.of(2,
                new long[] { 1, 5 }, new long[] { 1, 1 });
        final SenkuLongKeySummary merged = SenkuLongKeySummary
                .merge(List.of(large, small));
        assertEquals(1002, merged.recordCount());
        assertArrayEquals(new long[] { 1, 5, 9 }, merged.keys());
        assertArrayEquals(new long[] { 901, 1, 100 }, merged.weights());
        assertEquals(0, SenkuLongKeySummary.merge(List.of()).keys().length);
    }

    @Test
    void boundsCombinedRepresentativesWithoutChangingExactMass() {
        final List<SenkuLongKeySummary> inputs = new ArrayList<>();
        for (int shard = 0; shard < 32; shard++) {
            final long[] keys = LongStream
                    .range(shard * 256L, (shard + 1) * 256L).toArray();
            final long[] weights = new long[keys.length];
            Arrays.fill(weights, 3L);
            inputs.add(SenkuLongKeySummary.of(768, keys, weights));
        }
        final SenkuLongKeySummary merged = SenkuLongKeySummary.merge(inputs);
        assertEquals(24_576, merged.recordCount());
        assertEquals(24_576, Arrays.stream(merged.weights()).sum());
        assertTrue(merged.keys().length <= SenkuLongKeySummary.MAX_SAMPLES);
        assertEquals(4096, merged.keys().length);
        final SenkuLongKeySummary maximum = SenkuLongKeySummary.of(
                Long.MAX_VALUE, new long[] { 1 },
                new long[] { Long.MAX_VALUE });
        assertThrows(IndexException.class,
                () -> SenkuLongKeySummary.merge(List.of(maximum, maximum)));
    }
}
