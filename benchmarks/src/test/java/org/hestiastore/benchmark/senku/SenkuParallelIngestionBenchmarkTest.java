package org.hestiastore.benchmark.senku;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

class SenkuParallelIngestionBenchmarkTest {

    @Test
    void biasedKeysAreUniqueAcrossCyclesAndCoverOldStripes() {
        final long first = SenkuParallelIngestionBenchmark.biasedBoardKey(0L);
        final long second = SenkuParallelIngestionBenchmark
                .biasedBoardKey(1L << 27);

        assertNotEquals(first, second);
        assertEquals(0, oldStripe(first));
        assertEquals(0, oldStripe(second));
        for (long sequence = 0L; sequence < 32L; sequence++) {
            assertEquals((int) sequence, oldStripe(
                    SenkuParallelIngestionBenchmark.biasedBoardKey(sequence)));
        }
    }

    @Test
    void filesystemStateCompletesRotationAndFinalization() {
        final SenkuParallelIngestionBenchmark.IngestionState state =
                new SenkuParallelIngestionBenchmark.IngestionState();
        state.rotationThreshold = 64;

        assertDoesNotThrow(() -> {
            state.setup();
            try {
                for (long sequence = 0L; sequence < 96L; sequence++) {
                    state.put(SenkuParallelIngestionBenchmark
                            .biasedBoardKey(sequence));
                }
            } finally {
                state.tearDown();
            }
        });
    }

    private static int oldStripe(final long key) {
        final int hash = Long.hashCode(key);
        return (hash ^ hash >>> 16) & 31;
    }
}
