package org.hestiastore.index.segmentindex.core.streaming;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class SegmentStreamingServiceTest {

    @Test
    void builderReturnsBuilder() {
        assertNotNull(SegmentStreamingService.builder());
    }
}
