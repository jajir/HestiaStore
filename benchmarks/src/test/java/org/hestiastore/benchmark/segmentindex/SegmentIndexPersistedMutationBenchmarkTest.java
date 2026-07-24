package org.hestiastore.benchmark.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.hestiastore.benchmark.segmentindex.SegmentIndexPersistedMutationBenchmark.MutationCursor;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.infra.ThreadParams;

class SegmentIndexPersistedMutationBenchmarkTest {

    @Test
    void mutationCursorPartitionsKeys() {
        final MutationCursor cursor = new MutationCursor();
        cursor.setup(threadParams(3, 16));

        assertEquals(103, cursor.nextPutKey(100));
        assertEquals(119, cursor.nextPutKey(100));
        assertEquals(3, cursor.nextDeleteKey(35));
        assertEquals(19, cursor.nextDeleteKey(35));
        assertEquals(3, cursor.nextDeleteKey(35));
    }

    private static ThreadParams threadParams(final int threadIndex,
            final int threadCount) {
        return new ThreadParams(threadIndex, threadCount, 0, 1, 0, 1,
                threadIndex, threadCount, threadIndex, threadCount);
    }
}
