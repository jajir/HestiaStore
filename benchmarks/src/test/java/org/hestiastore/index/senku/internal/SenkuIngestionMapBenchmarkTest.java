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
        assertUniqueBuild("collision-heavy");
        assertUniqueBuild("bit-board");
        assertUniqueBuild("randomized");
    }

    @Test
    void existingUpdatesRemainBoundedForEveryDistribution() {
        assertExistingUpdates("collision-heavy");
        assertExistingUpdates("bit-board");
        assertExistingUpdates("randomized");
    }

    private void assertUniqueBuild(final String distribution) {
        final SenkuIngestionMapBenchmark.UniqueBuildState state =
                new SenkuIngestionMapBenchmark.UniqueBuildState();
        state.distribution = distribution;
        state.entryCount = TEST_ENTRY_COUNT;
        state.setup();

        final List<Map<Long, NullValue>> maps = benchmark
                .buildUniqueEntries(state);

        assertEquals(TEST_ENTRY_COUNT,
                maps.stream().mapToInt(Map::size).sum());
    }

    private void assertExistingUpdates(final String distribution) {
        final SenkuIngestionMapBenchmark.ExistingEntryState state =
                new SenkuIngestionMapBenchmark.ExistingEntryState();
        state.distribution = distribution;
        state.entryCount = TEST_ENTRY_COUNT;
        state.setup();
        final SenkuIngestionMapBenchmark.KeyCursor cursor =
                new SenkuIngestionMapBenchmark.KeyCursor();
        cursor.reset(0);

        for (int index = 0; index < 2 * TEST_ENTRY_COUNT; index++) {
            assertSame(NULL, benchmark.updateExistingEntry(state, cursor));
        }
        assertEquals(TEST_ENTRY_COUNT, state.size());
    }
}
