package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.F;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStats;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
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
    private final IndexOperationTrackingAccess operationTracker;
    private final IndexOperationStatsRecorder operationStatsRecorder;
    private final SegmentIndexRuntime<K, V> runtime;
    private final ExecutorRegistry executorRegistry;
    private final IndexDirectoryLock directoryLock;

    IndexCloseCoordinator(final String indexName,
            final SegmentIndexStateMachine stateMachine,
            final IndexOperationTrackingAccess operationTracker,
            final IndexOperationStatsRecorder operationStatsRecorder,
            final SegmentIndexRuntime<K, V> runtime,
            final ExecutorRegistry executorRegistry,
            final IndexDirectoryLock directoryLock) {
        this.indexName = Vldtn.requireNonNull(indexName, "indexName");
        this.stateMachine = Vldtn.requireNonNull(stateMachine,
                "stateMachine");
        this.operationTracker = Vldtn.requireNonNull(operationTracker,
                "operationTracker");
        this.operationStatsRecorder = Vldtn.requireNonNull(
                operationStatsRecorder, "operationStatsRecorder");
        this.runtime = Vldtn.requireNonNull(runtime, "runtime");
        this.executorRegistry = Vldtn.requireNonNull(executorRegistry,
                "executorRegistry");
        this.directoryLock = Vldtn.requireNonNull(directoryLock,
                "directoryLock");
    }

    void close() {
        close(stateMachine::beginClose);
    }

    private void close(final Runnable beginClose) {
        LOGGER.debug("Closing index '{}'.", indexName);
        try {
            beginClose.run();
            awaitForegroundOperations();
            closeSplitRuntime();
            sealAsyncMaintenanceAndWait();
            sealAndFlushRuntimeState();
            logOperationCounts();
            releaseWalRuntime();
            closeExecutorRegistry();
            finishClosedState();
            releaseDirectoryLock();
            LOGGER.debug("Index '{}' closed.", indexName);
        } catch (final RuntimeException e) {
            stateMachine.markRuntimeFailure(e);
            throw e;
        }
    }

    private void awaitForegroundOperations() {
        operationTracker.awaitOperations();
    }

    private void closeSplitRuntime() {
        runtime.closeSplitRuntime();
    }

    private void sealAsyncMaintenanceAndWait() {
        runtime.sealAsyncMaintenanceAndWait();
    }

    private void sealAndFlushRuntimeState() {
        runtime.flushAndWait();
        runtime.closeSegmentRegistry();
        runtime.closeKeyToSegmentMapIfOpen();
    }

    private void finishClosedState() {
        stateMachine.completeClose();
    }

    private void releaseWalRuntime() {
        runtime.closeWalRuntime();
    }

    private void closeExecutorRegistry() {
        executorRegistry.close();
    }

    private void releaseDirectoryLock() {
        directoryLock.close();
    }

    private void logOperationCounts() {
        if (!LOGGER.isDebugEnabled()) {
            return;
        }
        final IndexOperationStats stats =
                operationStatsRecorder.statsSnapshot();
        LOGGER.debug(String.format(
                "Index is closing, where was %s gets, %s puts and %s deletes.",
                F.fmt(stats.getGetCount()),
                F.fmt(stats.getPutCount()),
                F.fmt(stats.getDeleteCount())));
    }
}
