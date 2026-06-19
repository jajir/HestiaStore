package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.F;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStats;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.storage.CoreStorageRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns the ordered close sequence for index runtime collaborators.
 */
final class IndexCloseCoordinator<K, V> {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(IndexCloseCoordinator.class);

    private final String indexName;
    private final SegmentIndexStateMachine stateMachine;
    private final SegmentIndexOperationGate operationGate;
    private final IndexOperationStatsRecorder operationStatsRecorder;
    private final SegmentTopologyRuntimeAccess<K, V> topologyRuntime;
    private final MaintenanceService<K, V> maintenance;
    private final CoreStorageRuntime<K, V> coreStorageRuntime;
    private final StorageService<K, V> storageService;
    private final ExecutorRegistry executorRegistry;
    private final IndexDirectoryLock directoryLock;

    IndexCloseCoordinator(final String indexName,
            final SegmentIndexStateMachine stateMachine,
            final SegmentIndexOperationGate operationGate,
            final IndexOperationStatsRecorder operationStatsRecorder,
            final SegmentTopologyRuntimeAccess<K, V> topologyRuntime,
            final MaintenanceService<K, V> maintenance,
            final CoreStorageRuntime<K, V> coreStorageRuntime,
            final StorageService<K, V> storageService,
            final ExecutorRegistry executorRegistry,
            final IndexDirectoryLock directoryLock) {
        this.indexName = Vldtn.requireNonNull(indexName, "indexName");
        this.stateMachine = Vldtn.requireNonNull(stateMachine,
                "stateMachine");
        this.operationGate = Vldtn.requireNonNull(operationGate,
                "operationGate");
        this.operationStatsRecorder = Vldtn.requireNonNull(
                operationStatsRecorder, "operationStatsRecorder");
        this.topologyRuntime = Vldtn.requireNonNull(topologyRuntime,
                "topologyRuntime");
        this.maintenance = Vldtn.requireNonNull(maintenance, "maintenance");
        this.coreStorageRuntime = Vldtn.requireNonNull(coreStorageRuntime,
                "coreStorageRuntime");
        this.storageService = Vldtn.requireNonNull(storageService,
                "storageService");
        this.executorRegistry = Vldtn.requireNonNull(executorRegistry,
                "executorRegistry");
        this.directoryLock = Vldtn.requireNonNull(directoryLock,
                "directoryLock");
    }

    void close() {
        LOGGER.debug("Closing index '{}'.", indexName);
        try {
            stateMachine.beginClose();
            closeRuntime();
            LOGGER.debug("Index '{}' closed.", indexName);
        } catch (final RuntimeException e) {
            stateMachine.markRuntimeFailure(e);
            throw e;
        }
    }

    private void closeRuntime() {
        RuntimeException firstFailure = null;
        try {
            operationGate.awaitOperationDrain();
        } catch (final RuntimeException failure) {
            firstFailure = recordFailure(firstFailure, failure);
        }
        try {
            topologyRuntime.closeSplitRuntime();
        } catch (final RuntimeException failure) {
            firstFailure = recordFailure(firstFailure, failure);
        }
        try {
            maintenance.sealAsyncMaintenanceAndWait();
        } catch (final RuntimeException failure) {
            firstFailure = recordFailure(firstFailure, failure);
        }
        try {
            maintenance.flushAndWait();
        } catch (final RuntimeException failure) {
            firstFailure = recordFailure(firstFailure, failure);
        }
        try {
            coreStorageRuntime.closeCoreStorage();
        } catch (final RuntimeException failure) {
            firstFailure = recordFailure(firstFailure, failure);
        }
        try {
            logOperationCounts();
        } catch (final RuntimeException failure) {
            firstFailure = recordFailure(firstFailure, failure);
        }
        try {
            storageService.closeWal();
        } catch (final RuntimeException failure) {
            firstFailure = recordFailure(firstFailure, failure);
        }
        try {
            executorRegistry.close();
        } catch (final RuntimeException failure) {
            firstFailure = recordFailure(firstFailure, failure);
        }
        try {
            stateMachine.completeClose();
        } catch (final RuntimeException failure) {
            firstFailure = recordFailure(firstFailure, failure);
        }
        try {
            directoryLock.close();
        } catch (final RuntimeException failure) {
            firstFailure = recordFailure(firstFailure, failure);
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    private void logOperationCounts() {
        if (!LOGGER.isDebugEnabled()) {
            return;
        }
        final IndexOperationStats stats = operationStatsRecorder.statsSnapshot();
        LOGGER.debug(String.format(
                "Index is closing, where was %s gets, %s puts and %s deletes.",
                F.fmt(stats.getGetCount()), F.fmt(stats.getPutCount()),
                F.fmt(stats.getDeleteCount())));
    }

    private static RuntimeException recordFailure(
            final RuntimeException firstFailure,
            final RuntimeException failure) {
        if (firstFailure == null) {
            return failure;
        }
        firstFailure.addSuppressed(failure);
        return firstFailure;
    }
}
