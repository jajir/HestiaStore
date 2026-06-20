package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.core.execution.MappedSegmentMaintenanceService;
import org.hestiastore.index.segmentindex.core.execution.SegmentIteratorService;
import org.hestiastore.index.segmentindex.core.split.SplitRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageCoordinator;
import org.hestiastore.index.segmentindex.SegmentIndexMaintenance;

/**
 * Applies session lifecycle and operation tracking around maintenance calls.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class MaintenanceApiAdapter<K, V>
        implements SegmentIndexMaintenance {

    private final MappedSegmentMaintenanceService<K, V> maintenanceService;
    private final StorageCoordinator<K, V> storageService;
    private final SplitRuntime<K, V> splitService;
    private final SegmentIteratorService<K, V> streamingService;
    private final SegmentIndexStateMachine stateMachine;
    private final SessionOperationGate operationGate;

    /**
     * Creates a maintenance adapter that applies session operation tracking and
     * invalidates open iterators after successful maintenance commands.
     *
     * @param maintenanceService segment maintenance command service
     * @param storageService storage service used for consistency repair
     * @param splitService split service used after consistency repair
     * @param streamingService streaming service used to invalidate iterators
     * @param stateMachine lifecycle state checked before each operation
     * @param operationGate operation gate that tracks active work
     */
    MaintenanceApiAdapter(
            final MappedSegmentMaintenanceService<K, V> maintenanceService,
            final StorageCoordinator<K, V> storageService,
            final SplitRuntime<K, V> splitService,
            final SegmentIteratorService<K, V> streamingService,
            final SegmentIndexStateMachine stateMachine,
            final SessionOperationGate operationGate) {
        this.maintenanceService = Vldtn.requireNonNull(maintenanceService,
                "maintenanceService");
        this.storageService = Vldtn.requireNonNull(storageService,
                "storageService");
        this.splitService = Vldtn.requireNonNull(splitService,
                "splitService");
        this.streamingService = Vldtn.requireNonNull(streamingService,
                "streamingService");
        this.stateMachine = Vldtn.requireNonNull(stateMachine,
                "stateMachine");
        this.operationGate = Vldtn.requireNonNull(operationGate,
                "operationGate");
    }

    @Override
    public void compact() {
        beginOperationalOperation();
        try {
            maintenanceService.compact();
            streamingService.invalidateIterators();
        } finally {
            operationGate.endOperation();
        }
    }

    @Override
    public void compactAndWait() {
        beginOperationalOperation();
        try {
            maintenanceService.compactAndWait();
            streamingService.invalidateIterators();
        } finally {
            operationGate.endOperation();
        }
    }

    @Override
    public void flush() {
        beginOperationalOperation();
        try {
            maintenanceService.flush();
            streamingService.invalidateIterators();
        } finally {
            operationGate.endOperation();
        }
    }

    @Override
    public void flushAndWait() {
        beginOperationalOperation();
        try {
            maintenanceService.flushAndWait();
            streamingService.invalidateIterators();
        } finally {
            operationGate.endOperation();
        }
    }

    @Override
    public void checkAndRepairConsistency() {
        beginOperationalOperation();
        try {
            storageService.checkAndRepairConsistency();
            splitService.requestFullSplitScan();
            streamingService.invalidateIterators();
        } finally {
            operationGate.endOperation();
        }
    }

    private void beginOperationalOperation() {
        operationGate.beginOperation();
        try {
            stateMachine.ensureOperational();
        } catch (final RuntimeException e) {
            operationGate.endOperation();
            throw e;
        }
    }
}
