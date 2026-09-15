package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.Test;

class SenkuShardIndexTest {

    @Test
    void constructorCopiesArraysAndExposesShardScopedValues() {
        final long[] positions = { LargeFilePosition.of(0, 16).getPacked(), 0L };
        final long[] counts = { 2L, 0L };
        final SenkuShardIndex index = new SenkuShardIndex(positions, counts);
        positions[0] = 0L;
        counts[0] = 0L;

        assertEquals(2, index.shardCount());
        assertEquals(0L, index.position(0).getPartNumber());
        assertEquals(16, index.position(0).getLocalPosition());
        assertEquals(2L, index.recordCount(0));
        assertEquals(0L, index.recordCount(1));
        assertEquals(2L, index.totalRecordCount());
    }

    @Test
    void constructorRejectsInvalidArraysAndValues() {
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuShardIndex(new long[0], new long[0]));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuShardIndex(new long[1], new long[2]));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuShardIndex(new long[] { -1L },
                        new long[] { 1L }));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuShardIndex(new long[] { 0L },
                        new long[] { -1L }));
    }

    @Test
    void accessorsRejectShardOutsideIndex() {
        final SenkuShardIndex index = new SenkuShardIndex(new long[] { 0L },
                new long[] { 1L });

        assertThrows(IllegalArgumentException.class,
                () -> index.recordCount(-1));
        assertThrows(IllegalArgumentException.class,
                () -> index.position(1));
    }

    @Test
    void totalRecordCountRejectsOverflow() {
        final SenkuShardIndex index = new SenkuShardIndex(
                new long[] { 0L, 0L },
                new long[] { Long.MAX_VALUE, 1L });

        assertThrows(IndexException.class, index::totalRecordCount);
    }
}
