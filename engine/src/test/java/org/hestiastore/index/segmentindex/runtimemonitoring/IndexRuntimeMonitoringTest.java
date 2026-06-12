package org.hestiastore.index.segmentindex.runtimemonitoring;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

class IndexRuntimeMonitoringTest {

    @Test
    void doesNotExposeBuilderFactory() {
        final boolean hasBuilderFactory = Arrays
                .stream(IndexRuntimeMonitoring.class.getDeclaredMethods())
                .anyMatch(method -> "builder".equals(method.getName()));

        assertFalse(hasBuilderFactory);
    }
}
