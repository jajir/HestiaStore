package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.config.DataTypeDescriptorRegistry;

/**
 * Creates the managed runtime index and close wrappers once lifecycle resources
 * are
 * already opened.
 */
final class SegmentIndexManagedIndexFactory {

    private SegmentIndexManagedIndexFactory() {
    }

    static <M, N> SegmentIndex<M, N> create(
            final SegmentIndexLifecycle<M, N> lifecycle) {
        final SegmentIndexLifecycle<M, N> openedLifecycle = Vldtn.requireNonNull(
                lifecycle, "lifecycle");
        final SegmentIndexLifecycleResources<M, N> openedResources = openedLifecycle
                .openedResources();
        return new IndexDirectoryClosingAdapter<>(
                createManagedIndex(openedResources),
                openedResources.managedDirectory(),
                new SegmentIndexLifecycleCloseResource(openedLifecycle));
    }

    private static <M, N> SegmentIndex<M, N> createManagedIndex(
            final SegmentIndexLifecycleResources<M, N> resources) {
        final SegmentIndexLifecycleResources<M, N> openedResources = Vldtn
                .requireNonNull(resources, "resources")
                .requireOpened();
        final IndexConfiguration<M, N> configuration = openedResources
                .indexConfiguration();
        return createRuntimeIndexWithContextLoggingIfEnabled(openedResources,
                configuration);
    }

    private static <M> TypeDescriptor<M> resolveKeyTypeDescriptor(
            final IndexConfiguration<M, ?> configuration) {
        return DataTypeDescriptorRegistry
                .makeInstance(configuration.identity().keyTypeDescriptor());
    }

    private static <N> TypeDescriptor<N> resolveValueTypeDescriptor(
            final IndexConfiguration<?, N> configuration) {
        return DataTypeDescriptorRegistry
                .makeInstance(configuration.identity().valueTypeDescriptor());
    }

    private static <M, N> SegmentIndex<M, N> createRuntimeIndexWithContextLoggingIfEnabled(
            final SegmentIndexLifecycleResources<M, N> resources,
            final IndexConfiguration<M, N> configuration) {
        if (!Boolean.TRUE.equals(configuration.logging().contextEnabled())) {
            return createRuntimeIndex(resources, configuration);
        }
        Vldtn.requireNotBlank(configuration.identity().name(), "indexName");
        final IndexContextScopeRunner contextScopeRunner = new IndexContextScopeRunner(
                configuration.identity().name());
        return contextScopeRunner.supply(() -> new IndexContextLoggingAdapter<>(
                configuration, createRuntimeIndex(resources, configuration)));
    }

    private static <M, N> SegmentIndex<M, N> createRuntimeIndex(
            final SegmentIndexLifecycleResources<M, N> resources,
            final IndexConfiguration<M, N> configuration) {
        return new IndexInternalConcurrent<>(
                resources.managedDirectory(),
                resolveKeyTypeDescriptor(configuration),
                resolveValueTypeDescriptor(configuration), configuration,
                resources.runtimeConfiguration(),
                resources.executorRegistry());
    }
}
