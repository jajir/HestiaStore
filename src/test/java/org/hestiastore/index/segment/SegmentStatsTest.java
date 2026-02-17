package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class SegmentStatsTest {

    @Test
    void getNumberOfKeys_sumsSegmentAndDeltaCache() {
        final SegmentStats stats = new SegmentStats(2L, 5L, 1L);

        assertEquals(7L, stats.getNumberOfKeys());
    }

    @Test
    void gettersExposeConfiguredValues() {
        final SegmentStats stats = new SegmentStats(3L, 4L, 2L);

        assertEquals(3L, stats.getNumberOfKeysInDeltaCache());
        assertEquals(4L, stats.getNumberOfKeysInSegment());
        assertEquals(2L, stats.getNumberOfKeysInScarceIndex());
    }
}
