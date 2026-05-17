package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;

/**
 * Creates and owns failed-startup cleanup for the executor registry.
 */
final class BootstrapStepCreateExecutorRegistry<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    private ExecutorRegistry executorRegistry;

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        final EffectiveIndexConfiguration<K, V> configuration = state
                .getConfiguration();
        executorRegistry = ExecutorRegistry.builder()
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
                .withShutdownTimeoutMillis(
                        configuration.maintenance().busyTimeoutMillis())
                .build();
        state.setExecutorRegistry(executorRegistry);
    }

    @Override
    void closeResource() {
        if (executorRegistry != null && !executorRegistry.wasClosed()) {
            executorRegistry.close();
        }
    }
}
