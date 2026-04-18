package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class SegmentRegistryCacheStatsTest {

    @Test
    void constructorStoresProvidedValues() {
        final SegmentRegistryCacheStats stats = new SegmentRegistryCacheStats(1,
                2, 3, 4, 5, 6);

        assertEquals(1L, stats.hitCount());
        assertEquals(2L, stats.missCount());
        assertEquals(3L, stats.loadCount());
        assertEquals(4L, stats.evictionCount());
        assertEquals(5, stats.size());
        assertEquals(6, stats.limit());
    }

    @Test
    void constructorRejectsNegativeCounters() {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentRegistryCacheStats(-1, 0, 0, 0, 0, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentRegistryCacheStats(0, -1, 0, 0, 0, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentRegistryCacheStats(0, 0, -1, 0, 0, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentRegistryCacheStats(0, 0, 0, -1, 0, 0));
    }

    @Test
    void constructorRejectsNegativeSizeOrLimit() {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentRegistryCacheStats(0, 0, 0, 0, -1, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentRegistryCacheStats(0, 0, 0, 0, 0, -1));
    }

    @Test
    void emptyReturnsZeroedSnapshot() {
        final SegmentRegistryCacheStats stats = SegmentRegistryCacheStats
                .empty();

        assertEquals(0L, stats.hitCount());
        assertEquals(0L, stats.missCount());
        assertEquals(0L, stats.loadCount());
        assertEquals(0L, stats.evictionCount());
        assertEquals(0, stats.size());
        assertEquals(0, stats.limit());
    }
}
