package org.hestiastore.index.segmentindex.core.lifecycle;

import org.hestiastore.index.F;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.observability.Stats;
import org.hestiastore.index.segmentindex.core.operation.IndexOperationTrackingAccess;
import org.hestiastore.index.segmentindex.core.state.IndexStateCoordinator;
import org.slf4j.Logger;

/**
 * Owns the ordered close sequence for index runtime collaborators.
 */
public final class IndexCloseCoordinator<K, V> {

    private final Logger logger;
    private final String indexName;
    private final IndexStateCoordinator<K, V> stateCoordinator;
    private final IndexOperationTrackingAccess operationTracker;
    private final Stats stats;
    private final IndexCloseAccess<K, V> closeAccess;

    public IndexCloseCoordinator(final Logger logger, final String indexName,
            final IndexStateCoordinator<K, V> stateCoordinator,
            final IndexOperationTrackingAccess operationTracker,
            final Stats stats,
            final IndexCloseAccess<K, V> closeAccess) {
        this.logger = Vldtn.requireNonNull(logger, "logger");
        this.indexName = Vldtn.requireNonNull(indexName, "indexName");
        this.stateCoordinator = Vldtn.requireNonNull(stateCoordinator,
                "stateCoordinator");
        this.operationTracker = Vldtn.requireNonNull(operationTracker,
                "operationTracker");
        this.stats = Vldtn.requireNonNull(stats, "stats");
        this.closeAccess = Vldtn.requireNonNull(closeAccess, "closeAccess");
    }

    public void close() {
        logger.debug("Closing index '{}'.", indexName);
        try {
            stateCoordinator.beginClose();
            awaitForegroundOperations();
            quiesceBackgroundSplits();
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

    private void quiesceBackgroundSplits() {
        closeAccess.awaitBackgroundSplitsExhausted();
    }

    private void sealAndFlushRuntimeState() {
        closeAccess.flushStableSegmentsWithSplitSchedulingPaused();
        closeAccess.closeSegmentRegistry();
        closeAccess.flushKeyToSegmentMap();
        closeAccess.checkpointWal();
    }

    private void finishClosedState() {
        stateCoordinator.completeCloseStateTransition();
    }

    private void releaseWalRuntime() {
        closeAccess.closeWalRuntime();
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
