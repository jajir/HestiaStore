package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;

/**
 * Holds runtime services that sit on top of storage and split state.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexRuntimeServices<K, V> {

    private final SegmentIndexOperationAccess<K, V> operationAccess;
    private final MaintenanceService maintenance;
    private final IndexRuntimeMonitoring runtimeMonitoring;
    private final RuntimeTuning runtimeConfiguration;

    public SegmentIndexRuntimeServices(
            final SegmentIndexOperationAccess<K, V> operationAccess,
            final MaintenanceService maintenance,
            final IndexRuntimeMonitoring runtimeMonitoring,
            final RuntimeTuning runtimeConfiguration) {
        this.operationAccess = Vldtn.requireNonNull(
                operationAccess, "operationAccess");
        this.maintenance = Vldtn.requireNonNull(maintenance, "maintenance");
        this.runtimeMonitoring = Vldtn.requireNonNull(runtimeMonitoring,
                "runtimeMonitoring");
        this.runtimeConfiguration = Vldtn.requireNonNull(runtimeConfiguration,
                "runtimeConfiguration");
    }

    SegmentIndexOperationAccess<K, V> operationAccess() {
        return operationAccess;
    }

    MaintenanceService maintenance() {
        return maintenance;
    }

    IndexRuntimeMonitoring runtimeMonitoring() {
        return runtimeMonitoring;
    }

    RuntimeTuning runtimeTuning() {
        return runtimeConfiguration;
    }
}
