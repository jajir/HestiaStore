package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.F;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.metrics.Stats;
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
    private final Stats stats;
    private final SegmentIndexRuntime<K, V> runtime;
    private final IndexDirectoryLock directoryLock;

    IndexCloseCoordinator(final String indexName,
            final SegmentIndexStateMachine stateMachine,
            final IndexOperationTrackingAccess operationTracker,
            final Stats stats,
            final SegmentIndexRuntime<K, V> runtime,
            final IndexDirectoryLock directoryLock) {
        this.indexName = Vldtn.requireNonNull(indexName, "indexName");
        this.stateMachine = Vldtn.requireNonNull(stateMachine,
                "stateMachine");
        this.operationTracker = Vldtn.requireNonNull(operationTracker,
                "operationTracker");
        this.stats = Vldtn.requireNonNull(stats, "stats");
        this.runtime = Vldtn.requireNonNull(runtime, "runtime");
        this.directoryLock = Vldtn.requireNonNull(directoryLock,
                "directoryLock");
    }

    void close() {
        close(stateMachine::beginClose);
    }

    void closeAfterFailedStartup() {
        close(stateMachine::beginFailedStartupClose);
    }

    private void close(final Runnable beginClose) {
        LOGGER.debug("Closing index '{}'.", indexName);
        try {
            beginClose.run();
            awaitForegroundOperations();
            closeSplitRuntime();
            sealAndFlushRuntimeState();
            logOperationCounts();
            LOGGER.debug("Index '{}' closed.", indexName);
        } finally {
            try {
                finishClosedState();
                releaseWalRuntime();
            } finally {
                releaseDirectoryLock();
            }
        }
    }

    private void awaitForegroundOperations() {
        operationTracker.awaitOperations();
    }

    private void closeSplitRuntime() {
        runtime.closeSplitRuntime();
    }

    private void sealAndFlushRuntimeState() {
        runtime.flushAndWait();
        runtime.closeSegmentRegistry();
    }

    private void finishClosedState() {
        stateMachine.completeClose();
    }

    private void releaseWalRuntime() {
        runtime.closeWalRuntime();
    }

    private void releaseDirectoryLock() {
        directoryLock.close();
    }

    private void logOperationCounts() {
        if (!LOGGER.isDebugEnabled()) {
            return;
        }
        LOGGER.debug(String.format(
                "Index is closing, where was %s gets, %s puts and %s deletes.",
                F.fmt(stats.getGetCount()),
                F.fmt(stats.getPutCount()),
                F.fmt(stats.getDeleteCount())));
    }
}
