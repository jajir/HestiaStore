package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.datatype.NullValue.NULL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import java.util.Map;

import org.hestiastore.index.datatype.NullValue;
import org.junit.jupiter.api.Test;

class SenkuIngestionMapBenchmarkTest {

    private static final int TEST_ENTRY_COUNT = 4_096;

    private final SenkuIngestionMapBenchmark benchmark =
            new SenkuIngestionMapBenchmark();

    @Test
    void uniqueBuildRetainsAllEntriesForEveryDistribution() {
        for (final String implementation : implementations()) {
            for (final int stripeCount : stripeCounts()) {
                assertUniqueBuild("collision-heavy", implementation,
                        stripeCount);
                assertUniqueBuild("bit-board", implementation, stripeCount);
                assertUniqueBuild("randomized", implementation, stripeCount);
            }
        }
    }

    @Test
    void existingUpdatesRemainBoundedForEveryDistribution() {
        for (final String implementation : implementations()) {
            for (final int stripeCount : stripeCounts()) {
                assertExistingUpdates("collision-heavy", implementation,
                        stripeCount);
                assertExistingUpdates("bit-board", implementation,
                        stripeCount);
                assertExistingUpdates("randomized", implementation,
                        stripeCount);
            }
        }
    }

    @Test
    void lockTimingCountsEveryBoundedUpdate() {
        final SenkuIngestionMapBenchmark.ExistingEntryState state =
                existingState("collision-heavy", "mixed-open-addressed", 128);
        final SenkuIngestionMapBenchmark.KeyCursor cursor = cursor();
        final SenkuIngestionLockBenchmark.LockTimingCounters counters =
                new SenkuIngestionLockBenchmark.LockTimingCounters();
        counters.reset();

        for (int index = 0; index < TEST_ENTRY_COUNT; index++) {
            assertSame(NULL, state.updateExistingWithTiming(
                    cursor.nextIndex(state.entryMask()), counters));
        }

        assertEquals(TEST_ENTRY_COUNT, state.size());
        assertEquals(TEST_ENTRY_COUNT, counters.operationCount());
        assertEquals(TEST_ENTRY_COUNT, state.size());
    }

    private void assertUniqueBuild(final String distribution,
            final String implementation, final int stripeCount) {
        final SenkuIngestionMapBenchmark.UniqueBuildState state =
                new SenkuIngestionMapBenchmark.UniqueBuildState();
        state.distribution = distribution;
        state.implementation = implementation;
        state.stripeCount = stripeCount;
        state.entryCount = TEST_ENTRY_COUNT;
        state.setup();

        final List<Map<Long, NullValue>> maps = benchmark
                .buildUniqueEntries(state);

        assertEquals(TEST_ENTRY_COUNT,
                maps.stream().mapToInt(Map::size).sum());
    }

    private void assertExistingUpdates(final String distribution,
            final String implementation, final int stripeCount) {
        final SenkuIngestionMapBenchmark.ExistingEntryState state =
                existingState(distribution, implementation, stripeCount);
        final SenkuIngestionMapBenchmark.KeyCursor cursor = cursor();

        for (int index = 0; index < 2 * TEST_ENTRY_COUNT; index++) {
            assertSame(NULL, benchmark.updateExistingEntry(state, cursor));
        }
        assertEquals(TEST_ENTRY_COUNT, state.size());
    }

    private static SenkuIngestionMapBenchmark.ExistingEntryState existingState(
            final String distribution, final String implementation,
            final int stripeCount) {
        final SenkuIngestionMapBenchmark.ExistingEntryState state =
                new SenkuIngestionMapBenchmark.ExistingEntryState();
        state.distribution = distribution;
        state.implementation = implementation;
        state.stripeCount = stripeCount;
        state.entryCount = TEST_ENTRY_COUNT;
        state.setup();
        return state;
    }

    private static SenkuIngestionMapBenchmark.KeyCursor cursor() {
        final SenkuIngestionMapBenchmark.KeyCursor cursor =
                new SenkuIngestionMapBenchmark.KeyCursor();
        cursor.reset(0);
        return cursor;
    }

    private static List<String> implementations() {
        return List.of("hash-map", "mixed-open-addressed");
    }

    private static List<Integer> stripeCounts() {
        return List.of(32, 64, 128);
    }
}
