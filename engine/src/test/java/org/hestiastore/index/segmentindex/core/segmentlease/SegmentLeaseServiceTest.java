package org.hestiastore.index.segmentindex.core.segmentlease;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class SegmentLeaseServiceTest {

    @Test
    void builderReturnsBuilder() {
        assertNotNull(SegmentLeaseService.builder());
    }
}
