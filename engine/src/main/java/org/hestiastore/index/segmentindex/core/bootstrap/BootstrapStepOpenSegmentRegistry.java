package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Opens the physical segment registry.
 */
final class BootstrapStepOpenSegmentRegistry<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    private SegmentIndexBootstrapState<K, V> state;
    private SegmentRegistry<K, V> segmentRegistry;

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        this.state = state;
        segmentRegistry = SegmentRegistry.<K, V>builder()
                .withDirectoryFacade(request.getDirectory())
                .withKeyTypeDescriptor(state.getKeyTypeDescriptor())
                .withValueTypeDescriptor(state.getValueTypeDescriptor())
                .withConfiguration(state.getConfiguration())
                .withSegmentMaintenanceExecutor(
                        state.getExecutorRegistry()
                                .getStableSegmentMaintenanceExecutor())
                .withRegistryMaintenanceExecutor(
                        state.getExecutorRegistry()
                                .getRegistryMaintenanceExecutor())
                .withChunkStoreCache(state.getChunkStoreCache())
                .build();
        state.setSegmentRegistry(segmentRegistry);
    }

    @Override
    void closeResource() {
        if (state == null || state.runtimeCloseOwnershipTransferred()
                || segmentRegistry == null) {
            return;
        }
        segmentRegistry.close();
    }
}
