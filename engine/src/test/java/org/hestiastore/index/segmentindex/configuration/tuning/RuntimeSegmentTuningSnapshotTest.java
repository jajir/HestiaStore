package org.hestiastore.index.segmentindex.configuration.tuning;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class RuntimeSegmentTuningSnapshotTest {

    @Test
    void exposesPrimitiveSectionValues() {
        final RuntimeSegmentTuningSnapshot snapshot =
                new RuntimeSegmentTuningSnapshot(10, 3);

        assertEquals(10, snapshot.cacheKeyLimit());
        assertEquals(3, snapshot.cachedSegmentLimit());
    }
}
