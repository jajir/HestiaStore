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
                .withIndexName(configuration.identity().name())
                .withContextLoggingEnabled(
                        Boolean.TRUE.equals(
                                configuration.logging().contextEnabled()))
                .withIndexMaintenanceThreads(
                        configuration.maintenance().indexThreads())
                .withSplitMaintenanceThreads(
                        configuration.maintenance().indexThreads())
                .withSegmentMaintenanceThreads(
                        configuration.maintenance().segmentThreads())
                .withRegistryMaintenanceThreads(
                        configuration.maintenance().registryLifecycleThreads())
                .build();
    }
}
