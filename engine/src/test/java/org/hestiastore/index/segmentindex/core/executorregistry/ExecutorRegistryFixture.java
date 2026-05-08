package org.hestiastore.index.segmentindex.core.executorregistry;

import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationResolver;
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
        return from(EffectiveIndexConfigurationResolver.resolveForCreate(
                configuration));
    }

    public static ExecutorRegistry from(
            final EffectiveIndexConfiguration<?, ?> configuration) {
        return ExecutorRegistry.builder()
                .withIndexName(configuration.identity().name())
                .withContextLoggingEnabled(
                        configuration.logging().contextEnabled())
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
