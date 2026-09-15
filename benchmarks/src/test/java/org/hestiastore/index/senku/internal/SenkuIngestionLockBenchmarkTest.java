package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.datatype.NullValue.NULL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SenkuIngestionLockBenchmarkTest {

    private static final int ENTRY_COUNT = 128;

    private SenkuIngestionLockBenchmark benchmark;
    private SenkuIngestionLockBenchmark.LockTimingCounters counters;
    private SenkuIngestionMapBenchmark.KeyCursor cursor;

    @BeforeEach
    void setUp() {
        benchmark = new SenkuIngestionLockBenchmark();
        counters = new SenkuIngestionLockBenchmark.LockTimingCounters();
        cursor = new SenkuIngestionMapBenchmark.KeyCursor();
        cursor.reset(7);
    }

    @ParameterizedTest
    @ValueSource(strings = { "hash-map", "mixed-open-addressed" })
    void measuredUpdatesCountEveryOperationWithoutGrowingMaps(
            final String implementation) {
        final SenkuIngestionMapBenchmark.ExistingEntryState state =
                new SenkuIngestionMapBenchmark.ExistingEntryState();
        state.distribution = "collision-heavy";
        state.implementation = implementation;
        state.stripeCount = 64;
        state.entryCount = ENTRY_COUNT;
        state.setup();

        for (int operation = 0; operation < 2 * ENTRY_COUNT; operation++) {
            assertSame(NULL,
                    benchmark.updateExistingEntry(state, cursor, counters));
        }

        assertEquals(ENTRY_COUNT, state.size());
        assertEquals(2 * ENTRY_COUNT, counters.operationCount());
        assertTrue(counters.lockWaitNanoseconds >= 0L);
        assertTrue(counters.lockHoldNanoseconds >= 0L);
    }

    @Test
    void iterationResetClearsAllTotalsAndSupportsTheNextIteration() {
        counters.record(5L, 7L);
        counters.record(11L, 13L);
        assertEquals(16L, counters.lockWaitNanoseconds);
        assertEquals(20L, counters.lockHoldNanoseconds);
        assertEquals(2L, counters.operationCount());

        counters.reset();

        assertEquals(0L, counters.lockWaitNanoseconds);
        assertEquals(0L, counters.lockHoldNanoseconds);
        assertEquals(0L, counters.operationCount());
        counters.record(2L, 3L);
        assertEquals(2L, counters.lockWaitNanoseconds);
        assertEquals(3L, counters.lockHoldNanoseconds);
        assertEquals(1L, counters.operationCount());
    }
}
