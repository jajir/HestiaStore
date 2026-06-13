package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;
import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.hestiastore.index.segmentindex.core.storage.WalRuntimeInitialization;

/**
 * Initializes storage-owned WAL coordination after maintenance is available for
 * durable-state flushing.
 */
final class BootstrapStepInitializeWal<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    private final SegmentIndexSessionResources<K, V> sessionResources;

    BootstrapStepInitializeWal(
            final SegmentIndexSessionResources<K, V> sessionResources) {
        this.sessionResources = Vldtn.requireNonNull(sessionResources,
                "sessionResources");
    }

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        final StorageService<K, V> storageService = state.getStorageService();
        storageService.initializeWal(new WalRuntimeInitialization<>(
                state.getConfiguration(),
                state.hasRuntimeWalRuntime() ? state.getRuntimeWalRuntime() : null,
                new BootstrapWalDurableState(
                        state.getRuntimeMaintenanceService()),
                sessionResources, sessionResources, state.lastAppliedWalLsn()));
        state.getRuntimeMaintenanceCheckpoint()
                .bindStorageService(storageService);
    }
}
