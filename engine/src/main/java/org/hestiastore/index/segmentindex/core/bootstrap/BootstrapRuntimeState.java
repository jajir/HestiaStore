package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
import org.hestiastore.index.segmentindex.core.segmentlease.SegmentLeaseService;
import org.hestiastore.index.segmentindex.core.session.SegmentTopologyRuntimeAccess;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segmentindex.wal.WalRuntime;

/**
 * Holds runtime collaborators created during bootstrap after storage has opened.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class BootstrapRuntimeState<K, V> {

    private SegmentLeaseService<K, V> segmentLeaseService;
    private SplitService splitService;
    private SegmentTopologyRuntimeAccess<K, V> topologyRuntime;
    private WalRuntime<K, V> walRuntime;
    private BootstrapWalCheckpoint maintenanceCheckpoint;
    private MaintenanceService maintenanceService;
    private SegmentIndexOperationAccess<K, V> operationAccess;
    private RuntimeTuning runtimeTuning;
    private IndexRuntimeMonitoring runtimeMonitoring;
    private Boolean closeOwnershipTransferred = Boolean.FALSE;

    void setSegmentLeaseService(
            final SegmentLeaseService<K, V> segmentLeaseService) {
        this.segmentLeaseService = Vldtn.requireNonNull(segmentLeaseService,
                "segmentLeaseService");
    }

    SegmentLeaseService<K, V> getSegmentLeaseService() {
        return requireInitialized(segmentLeaseService, "segmentLeaseService");
    }

    void setSplitService(final SplitService splitService) {
        this.splitService = Vldtn.requireNonNull(splitService, "splitService");
    }

    boolean hasSplitService() {
        return splitService != null;
    }

    SplitService getSplitService() {
        return requireInitialized(splitService, "splitService");
    }

    void setTopologyRuntime(
            final SegmentTopologyRuntimeAccess<K, V> topologyRuntime) {
        this.topologyRuntime = Vldtn.requireNonNull(topologyRuntime,
                "topologyRuntime");
    }

    SegmentTopologyRuntimeAccess<K, V> getTopologyRuntime() {
        return requireInitialized(topologyRuntime, "topologyRuntime");
    }

    void setWalRuntime(final WalRuntime<K, V> walRuntime) {
        this.walRuntime = Vldtn.requireNonNull(walRuntime, "walRuntime");
    }

    boolean hasWalRuntime() {
        return walRuntime != null;
    }

    WalRuntime<K, V> getWalRuntime() {
        return requireInitialized(walRuntime, "walRuntime");
    }

    void setMaintenanceCheckpoint(
            final BootstrapWalCheckpoint maintenanceCheckpoint) {
        this.maintenanceCheckpoint = Vldtn.requireNonNull(
                maintenanceCheckpoint, "maintenanceCheckpoint");
    }

    BootstrapWalCheckpoint getMaintenanceCheckpoint() {
        return requireInitialized(maintenanceCheckpoint,
                "maintenanceCheckpoint");
    }

    void setMaintenanceService(final MaintenanceService maintenanceService) {
        this.maintenanceService = Vldtn.requireNonNull(maintenanceService,
                "maintenanceService");
    }

    MaintenanceService getMaintenanceService() {
        return requireInitialized(maintenanceService, "maintenanceService");
    }

    void setOperationAccess(
            final SegmentIndexOperationAccess<K, V> operationAccess) {
        this.operationAccess = Vldtn.requireNonNull(operationAccess,
                "operationAccess");
    }

    SegmentIndexOperationAccess<K, V> getOperationAccess() {
        return requireInitialized(operationAccess, "operationAccess");
    }

    void setRuntimeTuning(final RuntimeTuning runtimeTuning) {
        this.runtimeTuning = Vldtn.requireNonNull(runtimeTuning,
                "runtimeTuning");
    }

    RuntimeTuning getRuntimeTuning() {
        return requireInitialized(runtimeTuning, "runtimeTuning");
    }

    void setRuntimeMonitoring(
            final IndexRuntimeMonitoring runtimeMonitoring) {
        this.runtimeMonitoring = Vldtn.requireNonNull(runtimeMonitoring,
                "runtimeMonitoring");
    }

    IndexRuntimeMonitoring getRuntimeMonitoring() {
        return requireInitialized(runtimeMonitoring, "runtimeMonitoring");
    }

    void markCloseOwnershipTransferred() {
        closeOwnershipTransferred = Boolean.TRUE;
    }

    boolean closeOwnershipTransferred() {
        return Boolean.TRUE.equals(closeOwnershipTransferred);
    }

    private static <T> T requireInitialized(final T value,
            final String fieldName) {
        if (value == null) {
            throw new IllegalStateException(
                    fieldName + " was not initialized.");
        }
        return value;
    }
}
