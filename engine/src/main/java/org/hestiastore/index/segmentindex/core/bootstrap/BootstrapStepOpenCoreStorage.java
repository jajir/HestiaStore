package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexMaintenanceConfiguration;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.storage.CoreStorageRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageService;

/**
 * Creates the storage package entry point from already opened storage
 * resources.
 */
final class BootstrapStepOpenCoreStorage<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        final EffectiveIndexConfiguration<K, V> conf =
                state.getConfiguration();
        final RuntimeTuningState runtimeTuningState = RuntimeTuningState
                .fromConfiguration(conf);
        final EffectiveIndexMaintenanceConfiguration maintenance =
                conf.maintenance();
        final StorageService<K, V> storageService =
                StorageService.<K, V>builder()
                        .withDirectoryFacade(request.getDirectory())
                        .withKeyToSegmentMap(state.getKeyToSegmentMap())
                        .withSegmentRegistry(state.getSegmentRegistry())
                        .withKeyTypeDescriptor(state.getKeyTypeDescriptor())
                        .withStorageCleanupBusyBackoffMillis(
                                maintenance.busyBackoffMillis())
                        .withStorageCleanupBusyTimeoutMillis(
                                maintenance.busyTimeoutMillis())
                        .withWalBackpressureBusyBackoffMillis(
                                maintenance.busyBackoffMillis())
                        .withWalBackpressureBusyTimeoutMillis(
                                maintenance.busyTimeoutMillis())
                        .build();
        state.setCoreStorageRuntime(new CoreStorageRuntime<>(
                runtimeTuningState, storageService,
                state.getSegmentRegistry(), state.getKeyToSegmentMap()));
    }
}
