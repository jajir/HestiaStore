package org.hestiastore.index.segmentindex.monitoring;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

class SegmentIndexRuntimeMonitoringTest {

    @Test
    void doesNotExposeBuilderFactory() {
        final boolean hasBuilderFactory = Arrays
                .stream(SegmentIndexRuntimeMonitoring.class.getDeclaredMethods())
                .anyMatch(method -> "builder".equals(method.getName()));

        assertFalse(hasBuilderFactory);
    }
}
