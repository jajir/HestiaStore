package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.F;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
import org.hestiastore.index.segmentindex.core.routing.IndexOperationTrackingAccess;
import org.hestiastore.index.segmentindex.core.session.state.IndexStateCoordinator;
import org.slf4j.Logger;

/**
 * Owns the ordered close sequence for index runtime collaborators.
 */
final class IndexCloseCoordinator<K, V> {

    private final Logger logger;
    private final String indexName;
    private final IndexStateCoordinator<K, V> stateCoordinator;
    private final IndexOperationTrackingAccess operationTracker;
    private final Stats stats;
    private final SegmentIndexRuntime<K, V> runtime;

    IndexCloseCoordinator(final Logger logger, final String indexName,
            final IndexStateCoordinator<K, V> stateCoordinator,
            final IndexOperationTrackingAccess operationTracker,
            final Stats stats,
            final SegmentIndexRuntime<K, V> runtime) {
        this.logger = Vldtn.requireNonNull(logger, "logger");
        this.indexName = Vldtn.requireNonNull(indexName, "indexName");
        this.stateCoordinator = Vldtn.requireNonNull(stateCoordinator,
                "stateCoordinator");
        this.operationTracker = Vldtn.requireNonNull(operationTracker,
                "operationTracker");
        this.stats = Vldtn.requireNonNull(stats, "stats");
        this.runtime = Vldtn.requireNonNull(runtime, "runtime");
    }

    void close() {
        logger.debug("Closing index '{}'.", indexName);
        try {
            stateCoordinator.beginClose();
            awaitForegroundOperations();
            closeSplitRuntime();
            sealAndFlushRuntimeState();
            logOperationCounts();
            logger.debug("Index '{}' closed.", indexName);
        } finally {
            finishClosedState();
            releaseWalRuntime();
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
        stateCoordinator.completeCloseStateTransition();
    }

    private void releaseWalRuntime() {
        runtime.closeWalRuntime();
    }

    private void logOperationCounts() {
        if (!logger.isDebugEnabled()) {
            return;
        }
        logger.debug(String.format(
                "Index is closing, where was %s gets, %s puts and %s deletes.",
                F.fmt(stats.getGetCount()),
                F.fmt(stats.getPutCount()),
                F.fmt(stats.getDeleteCount())));
    }
}
