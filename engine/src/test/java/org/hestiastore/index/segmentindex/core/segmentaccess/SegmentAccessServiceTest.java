package org.hestiastore.index.segmentindex.core.segmentaccess;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class SegmentAccessServiceTest {

    @Test
    void builderReturnsBuilder() {
        assertNotNull(SegmentAccessService.builder());
    }
}
