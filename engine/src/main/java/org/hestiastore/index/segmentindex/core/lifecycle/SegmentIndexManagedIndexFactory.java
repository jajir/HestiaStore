package org.hestiastore.index.segmentindex.core.lifecycle;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.config.DataTypeDescriptorRegistry;
import org.hestiastore.index.segmentindex.core.internal.IndexInternalConcurrent;

/**
 * Creates the managed runtime index and close wrappers once lifecycle resources are
 * already opened.
 */
final class SegmentIndexManagedIndexFactory {

    private SegmentIndexManagedIndexFactory() {
    }

    static <M, N> SegmentIndex<M, N> create(
            final SegmentIndexLifecycle<M, N> lifecycle) {
        final SegmentIndexLifecycle<M, N> openedLifecycle = Vldtn.requireNonNull(
                lifecycle, "lifecycle");
        final SegmentIndexLifecycleResources<M, N> openedResources =
                openedLifecycle.openedResources();
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
        return wrapWithContextLoggingIfEnabled(configuration,
                new IndexInternalConcurrent<>(
                        openedResources.managedDirectory(),
                        resolveKeyTypeDescriptor(configuration),
                        resolveValueTypeDescriptor(configuration), configuration,
                        openedResources.runtimeConfiguration(),
                        openedResources.executorRegistry()));
    }

    private static <M> TypeDescriptor<M> resolveKeyTypeDescriptor(
            final IndexConfiguration<M, ?> configuration) {
        return DataTypeDescriptorRegistry
                .makeInstance(configuration.getKeyTypeDescriptor());
    }

    private static <N> TypeDescriptor<N> resolveValueTypeDescriptor(
            final IndexConfiguration<?, N> configuration) {
        return DataTypeDescriptorRegistry
                .makeInstance(configuration.getValueTypeDescriptor());
    }

    private static <M, N> SegmentIndex<M, N> wrapWithContextLoggingIfEnabled(
            final IndexConfiguration<M, N> configuration,
            final SegmentIndex<M, N> runtimeIndex) {
        if (!Boolean.TRUE.equals(configuration.isContextLoggingEnabled())) {
            return runtimeIndex;
        }
        Vldtn.requireNotBlank(configuration.getIndexName(), "indexName");
        return new IndexContextLoggingAdapter<>(configuration, runtimeIndex);
    }
}
