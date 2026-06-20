package org.hestiastore.index.segmentindex.core.executorregistry;

import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationResolver;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;

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
        return ExecutorRegistry.create(configuration.identity().name(),
                configuration.logging().contextEnabled(),
                configuration.maintenance().indexThreads(),
                configuration.maintenance().indexThreads(),
                configuration.maintenance().segmentThreads(),
                configuration.maintenance().registryLifecycleThreads(),
                configuration.maintenance().busyTimeoutMillis());
    }
}
