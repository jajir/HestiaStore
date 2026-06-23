package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.F;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.execution.MappedSegmentMaintenanceService;
import org.hestiastore.index.segmentindex.core.execution.OperationStatsSnapshot;
import org.hestiastore.index.segmentindex.core.execution.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.split.SplitRuntime;
import org.hestiastore.index.segmentindex.core.storage.OpenedStorageRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns the ordered close sequence for index runtime collaborators.
 */
final class SessionCloseCoordinator<K, V> {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(SessionCloseCoordinator.class);

    private final String indexName;
    private final SegmentIndexStateMachine stateMachine;
    private final SessionOperationGate operationGate;
    private final IndexOperationStatsRecorder operationStatsRecorder;
    private final SplitRuntime<K, V> splitService;
    private final MappedSegmentMaintenanceService<K, V> maintenance;
    private final OpenedStorageRuntime<K, V> coreStorageRuntime;
    private final StorageCoordinator<K, V> storageService;
    private final ExecutorRegistry executorRegistry;
    private final SegmentIndexRuntimeHandle runtimeHandle;
    private final IndexDirectoryLock directoryLock;

    SessionCloseCoordinator(final String indexName,
            final SegmentIndexStateMachine stateMachine,
            final SessionOperationGate operationGate,
            final IndexOperationStatsRecorder operationStatsRecorder,
            final SplitRuntime<K, V> splitService,
            final MappedSegmentMaintenanceService<K, V> maintenance,
            final OpenedStorageRuntime<K, V> coreStorageRuntime,
            final StorageCoordinator<K, V> storageService,
            final ExecutorRegistry executorRegistry,
            final SegmentIndexRuntimeHandle runtimeHandle,
            final IndexDirectoryLock directoryLock) {
        this.indexName = Vldtn.requireNonNull(indexName, "indexName");
        this.stateMachine = Vldtn.requireNonNull(stateMachine,
                "stateMachine");
        this.operationGate = Vldtn.requireNonNull(operationGate,
                "operationGate");
        this.operationStatsRecorder = Vldtn.requireNonNull(
                operationStatsRecorder, "operationStatsRecorder");
        this.splitService = Vldtn.requireNonNull(splitService,
                "splitService");
        this.maintenance = Vldtn.requireNonNull(maintenance, "maintenance");
        this.coreStorageRuntime = Vldtn.requireNonNull(coreStorageRuntime,
                "coreStorageRuntime");
        this.storageService = Vldtn.requireNonNull(storageService,
                "storageService");
        this.executorRegistry = Vldtn.requireNonNull(executorRegistry,
                "executorRegistry");
        this.runtimeHandle = Vldtn.requireNonNull(runtimeHandle,
                "runtimeHandle");
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
            splitService.close();
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
            runtimeHandle.close();
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
        final OperationStatsSnapshot stats = operationStatsRecorder.statsSnapshot();
        LOGGER.debug(
                "Index is closing, where was {} gets, {} puts and {} deletes.",
                F.fmt(stats.getGetCount()), F.fmt(stats.getPutCount()),
                F.fmt(stats.getDeleteCount()));
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
