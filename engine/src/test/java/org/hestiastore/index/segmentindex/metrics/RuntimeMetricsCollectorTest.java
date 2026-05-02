package org.hestiastore.index.segmentindex.metrics;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class RuntimeMetricsCollectorTest {

    @Test
    void builderReturnsRuntimeMetricsCollectorBuilder() {
        assertNotNull(RuntimeMetricsCollector.builder());
    }
}
