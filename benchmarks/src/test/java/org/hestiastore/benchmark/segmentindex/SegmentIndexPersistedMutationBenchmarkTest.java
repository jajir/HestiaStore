package org.hestiastore.benchmark.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.benchmark.segmentindex.SegmentIndexPersistedMutationBenchmark.MutationCursor;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.infra.ThreadParams;

class SegmentIndexPersistedMutationBenchmarkTest {

    @Test
    void mutationCursorPartitionsKeysAndStaggersFlushes() {
        final MutationCursor cursor = new MutationCursor();
        cursor.setup(threadParams(3, 16));

        assertEquals(103, cursor.nextPutKey(100));
        assertEquals(119, cursor.nextPutKey(100));
        assertEquals(3, cursor.nextDeleteKey(35));
        assertEquals(19, cursor.nextDeleteKey(35));
        assertEquals(3, cursor.nextDeleteKey(35));
        assertFalse(cursor.shouldFlush(16));
        assertFalse(cursor.shouldFlush(16));
        assertFalse(cursor.shouldFlush(16));
        assertTrue(cursor.shouldFlush(16));
        for (int i = 0; i < 15; i++) {
            assertFalse(cursor.shouldFlush(16));
        }
        assertTrue(cursor.shouldFlush(16));
    }

    private static ThreadParams threadParams(final int threadIndex,
            final int threadCount) {
        return new ThreadParams(threadIndex, threadCount, 0, 1, 0, 1,
                threadIndex, threadCount, threadIndex, threadCount);
    }
}
