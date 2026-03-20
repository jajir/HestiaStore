package org.hestiastore.index.segmentindex.core;

import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.hestiastore.index.F;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.hestiastore.index.segmentregistry.SegmentRegistryResultStatus;
import org.slf4j.Logger;

/**
 * Owns the ordered close sequence for index runtime collaborators.
 */
@SuppressWarnings("java:S107")
final class IndexCloseCoordinator {

    private static final String CLOSE_OPERATION = "close";

    private final Logger logger;
    private final String indexName;
    private final Runnable beginCloseTransition;
    private final Runnable awaitOperations;
    private final Runnable drainPartitions;
    private final Runnable awaitBackgroundSplitExhausted;
    private final Runnable markClosed;
    private final Runnable flushStableSegmentsWithSplitPaused;
    private final Supplier<SegmentRegistryResult<Void>> closeSegmentRegistry;
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
            final Runnable drainPartitions,
            final Runnable awaitBackgroundSplitExhausted,
            final Runnable markClosed,
            final Runnable flushStableSegmentsWithSplitPaused,
            final Supplier<SegmentRegistryResult<Void>> closeSegmentRegistry,
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
        this.drainPartitions = drainPartitions;
        this.awaitBackgroundSplitExhausted = awaitBackgroundSplitExhausted;
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
            settleBackgroundWork();
            markClosed.run();
            flushStableSegmentsWithSplitPaused.run();
            verifyRegistryCloseResult(closeSegmentRegistry.get());
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

    private void settleBackgroundWork() {
        drainPartitions.run();
        awaitBackgroundSplitExhausted.run();
        drainPartitions.run();
        awaitBackgroundSplitExhausted.run();
    }

    private void verifyRegistryCloseResult(
            final SegmentRegistryResult<Void> closeResult) {
        final SegmentRegistryResultStatus status = closeResult.getStatus();
        if (status == SegmentRegistryResultStatus.OK
                || status == SegmentRegistryResultStatus.CLOSED) {
            return;
        }
        throw new IndexException(String.format(
                "Index operation '%s' failed: %s", CLOSE_OPERATION, status));
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
