package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexMaintenanceConfiguration;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationAccess;

/**
 * Creates the runtime maintenance service and its deferred WAL checkpoint
 * collaborator.
 */
final class BootstrapStepCreateMaintenance<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    private final SegmentIndexSessionResources<K, V> sessionResources;

    BootstrapStepCreateMaintenance(
            final SegmentIndexSessionResources<K, V> sessionResources) {
        this.sessionResources = Vldtn.requireNonNull(sessionResources,
                "sessionResources");
    }

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        final BootstrapWalCheckpoint checkpoint = new BootstrapWalCheckpoint();
        state.setRuntimeMaintenanceService(newMaintenance(state, checkpoint));
        state.setRuntimeMaintenanceCheckpoint(checkpoint);
    }

    private MaintenanceService newMaintenance(
            final SegmentIndexBootstrapState<K, V> state,
            final BootstrapWalCheckpoint checkpoint) {
        final EffectiveIndexMaintenanceConfiguration maintenance =
                state.getConfiguration().maintenance();
        return MaintenanceService.<K, V>builder()
                .keyToSegmentMap(state.getKeyToSegmentMap())
                .stableSegmentGateway(StableSegmentOperationAccess.create(
                        state.getSegmentRegistry()))
                .splitService(state.getRuntimeSplitService())
                .busyBackoffMillis(maintenance.busyBackoffMillis())
                .busyTimeoutMillis(maintenance.busyTimeoutMillis())
                .statsRecorder(sessionResources.maintenanceStatsRecorder())
                .maintenanceExecutor(
                        state.getExecutorRegistry()
                                .getIndexMaintenanceExecutor())
                .checkpoint(checkpoint)
                .build();
    }
}
