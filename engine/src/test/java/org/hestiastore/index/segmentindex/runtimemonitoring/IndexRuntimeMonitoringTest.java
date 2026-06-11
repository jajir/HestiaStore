package org.hestiastore.index.segmentindex.runtimemonitoring;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class IndexRuntimeMonitoringTest {

    @Test
    void builderReturnsIndexRuntimeMonitoringBuilder() {
        assertNotNull(IndexRuntimeMonitoring.builder());
    }
}
