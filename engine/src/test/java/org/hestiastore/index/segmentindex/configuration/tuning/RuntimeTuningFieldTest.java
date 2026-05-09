package org.hestiastore.index.segmentindex.configuration.tuning;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class RuntimeTuningFieldTest {

    @Test
    void exposesEffectiveStylePathsAndValueTypes() {
        assertEquals("segment.cacheKeyLimit",
                RuntimeTuningField.SEGMENT_CACHE_KEY_LIMIT.path());
        assertEquals(RuntimeTuningValueType.INT,
                RuntimeTuningField.SEGMENT_CACHE_KEY_LIMIT.valueType());
        assertEquals("segment.cachedSegmentLimit",
                RuntimeTuningField.SEGMENT_CACHED_SEGMENT_LIMIT.path());
        assertEquals("writePath.segmentWriteCacheKeyLimitDuringMaintenance",
                RuntimeTuningField.WRITE_PATH_SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE
                        .path());
    }
}
