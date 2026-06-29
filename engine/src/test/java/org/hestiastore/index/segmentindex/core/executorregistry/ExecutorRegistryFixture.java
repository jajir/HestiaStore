package org.hestiastore.index.segmentindex.core.executorregistry;

import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationResolver;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;

/**
 * Test helper for creating executor registries from full index configuration
 * objects.
 */
public final class ExecutorRegistryFixture implements AutoCloseable {

    private final ExecutorRegistry executorRegistry;
    private final RuntimeExecutorPools runtimeExecutorPools;

    private ExecutorRegistryFixture(final ExecutorRegistry executorRegistry,
            final RuntimeExecutorPools runtimeExecutorPools) {
        this.executorRegistry = executorRegistry;
        this.runtimeExecutorPools = runtimeExecutorPools;
    }

    public static ExecutorRegistryFixture from(
            final IndexConfiguration<?, ?> configuration) {
        return from(EffectiveIndexConfigurationResolver.resolveForCreate(
                configuration));
    }

    public static ExecutorRegistryFixture from(
            final EffectiveIndexConfiguration<?, ?> configuration) {
        final RuntimeExecutorPools runtimePools = RuntimeExecutorPools.create(
                "hestia-test", 1, 1,
                configuration.maintenance().busyTimeoutMillis());
        return new ExecutorRegistryFixture(ExecutorRegistry.create(
                configuration.identity().name(),
                configuration.logging().contextEnabled(),
                configuration.maintenance().indexThreads(),
                runtimePools,
                configuration.maintenance().registryLifecycleThreads(),
                configuration.maintenance().busyTimeoutMillis()),
                runtimePools);
    }

    public ExecutorRegistry executorRegistry() {
        return executorRegistry;
    }

    @Override
    public void close() {
        if (!executorRegistry.wasClosed()) {
            executorRegistry.close();
        }
        if (!runtimeExecutorPools.wasClosed()) {
            runtimeExecutorPools.close();
        }
    }
}
