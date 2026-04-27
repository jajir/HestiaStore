package org.hestiastore.index.segmentindex.core.executorregistry;

import org.hestiastore.index.segmentindex.IndexConfiguration;

/**
 * Test helper for creating executor registries from full index configuration
 * objects.
 */
public final class ExecutorRegistryFixture {

    private ExecutorRegistryFixture() {
    }

    public static ExecutorRegistry from(
            final IndexConfiguration<?, ?> configuration) {
        return ExecutorRegistry.builder()
                .withIndexName(configuration.getIndexName())
                .withContextLoggingEnabled(
                        Boolean.TRUE.equals(
                                configuration.isContextLoggingEnabled()))
                .withIndexMaintenanceThreads(
                        configuration.getNumberOfIndexMaintenanceThreads())
                .withSplitMaintenanceThreads(
                        configuration.getNumberOfIndexMaintenanceThreads())
                .withSegmentMaintenanceThreads(
                        configuration.getNumberOfSegmentMaintenanceThreads())
                .withRegistryMaintenanceThreads(
                        configuration.getNumberOfRegistryLifecycleThreads())
                .build();
    }
}
