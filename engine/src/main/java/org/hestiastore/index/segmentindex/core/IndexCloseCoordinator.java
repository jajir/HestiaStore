package org.hestiastore.index.segmentindex.core;

import java.util.function.LongSupplier;

import org.hestiastore.index.F;
import org.slf4j.Logger;

/**
 * Owns the ordered close sequence for index runtime collaborators.
 */
@SuppressWarnings("java:S107")
final class IndexCloseCoordinator {

    private final Logger logger;
    private final String indexName;
    private final Runnable beginCloseTransition;
    private final Runnable awaitOperations;
    private final Runnable prepareDurableState;
    private final Runnable awaitBackgroundSplitsIdle;
    private final Runnable markClosed;
    private final Runnable flushStableSegmentsWithSplitPaused;
    private final Runnable closeSegmentRegistry;
    private final Runnable flushKeyToSegmentMap;
    private final Runnable checkpointWal;
    private final LongSupplier getReadCount;
    private final LongSupplier getWriteCount;
    private final LongSupplier getDeleteCount;
    private final Runnable finishCloseTransition;
    private final Runnable closeWalRuntime;

    IndexCloseCoordinator(final Logger logger, final String indexName,
            final Runnable beginCloseTransition,
            final Runnable awaitOperations,
            final Runnable prepareDurableState,
            final Runnable awaitBackgroundSplitsIdle,
            final Runnable markClosed,
            final Runnable flushStableSegmentsWithSplitPaused,
            final Runnable closeSegmentRegistry,
            final Runnable flushKeyToSegmentMap, final Runnable checkpointWal,
            final LongSupplier getReadCount,
            final LongSupplier getWriteCount,
            final LongSupplier getDeleteCount,
            final Runnable finishCloseTransition,
            final Runnable closeWalRuntime) {
        this.logger = logger;
        this.indexName = indexName;
        this.beginCloseTransition = beginCloseTransition;
        this.awaitOperations = awaitOperations;
        this.prepareDurableState = prepareDurableState;
        this.awaitBackgroundSplitsIdle = awaitBackgroundSplitsIdle;
        this.markClosed = markClosed;
        this.flushStableSegmentsWithSplitPaused = flushStableSegmentsWithSplitPaused;
        this.closeSegmentRegistry = closeSegmentRegistry;
        this.flushKeyToSegmentMap = flushKeyToSegmentMap;
        this.checkpointWal = checkpointWal;
        this.getReadCount = getReadCount;
        this.getWriteCount = getWriteCount;
        this.getDeleteCount = getDeleteCount;
        this.finishCloseTransition = finishCloseTransition;
        this.closeWalRuntime = closeWalRuntime;
    }

    void close() {
        logger.debug("Closing index '{}'.", indexName);
        try {
            beginCloseTransition.run();
            awaitOperations.run();
            awaitBackgroundWorkSettled();
            markClosed.run();
            flushStableSegmentsWithSplitPaused.run();
            closeSegmentRegistry.run();
            flushKeyToSegmentMap.run();
            checkpointWal.run();
            logOperationCounts();
            logger.debug("Index '{}' closed.", indexName);
        } finally {
            try {
                finishCloseTransition.run();
            } finally {
                closeWalRuntime.run();
            }
        }
    }

    private void awaitBackgroundWorkSettled() {
        prepareDurableState.run();
        awaitBackgroundSplitsIdle.run();
        prepareDurableState.run();
        awaitBackgroundSplitsIdle.run();
    }

    private void logOperationCounts() {
        if (!logger.isDebugEnabled()) {
            return;
        }
        logger.debug(String.format(
                "Index is closing, where was %s gets, %s puts and %s deletes.",
                F.fmt(getReadCount.getAsLong()), F.fmt(getWriteCount.getAsLong()),
                F.fmt(getDeleteCount.getAsLong())));
    }
}
