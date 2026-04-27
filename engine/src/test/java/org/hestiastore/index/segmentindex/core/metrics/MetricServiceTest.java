package org.hestiastore.index.segmentindex.core.metrics;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class MetricServiceTest {

    @Test
    void builderReturnsMetricServiceBuilder() {
        assertNotNull(MetricService.builder());
    }
}
