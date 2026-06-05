package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.segmentindex.core.storage.SegmentIndexCoreStorage;
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexCoreStorageFactory;
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexCoreStorageOpenSpec;

/**
 * Opens core storage and owns rollback cleanup until the runtime takes ownership.
 */
final class BootstrapStepOpenCoreStorage<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    private SegmentIndexBootstrapState<K, V> state;
    private SegmentIndexCoreStorage<K, V> coreStorage;

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        this.state = state;
        final SegmentIndexCoreStorageOpenSpec<K, V> openSpec =
                new SegmentIndexCoreStorageOpenSpec<>(
                        request.getDirectory(), state.getKeyTypeDescriptor(),
                        state.getValueTypeDescriptor(), state.getConfiguration(),
                        state.getExecutorRegistry());
        coreStorage = new SegmentIndexCoreStorageFactory<>(openSpec).create();
        state.setCoreStorage(coreStorage);
    }

    @Override
    void closeResource() {
        if (state == null || state.indexRuntimeWasCreated()
                || coreStorage == null) {
            return;
        }
        coreStorage.close();
    }
}
