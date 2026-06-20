package org.hestiastore.index.segmentindex.configuration.tuning;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class RuntimeTuningKeyTest {

    @Test
    void exposesEffectiveStylePaths() {
        assertEquals("segment.cacheKeyLimit",
                RuntimeTuningKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE.path());
        assertEquals("segment.cachedSegmentLimit",
                RuntimeTuningKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE.path());
        assertEquals("writePath.segmentWriteCacheKeyLimitDuringMaintenance",
                RuntimeTuningKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE
                        .path());
    }
}
