package org.hestiastore.index.segmentindex.core.maintenance;

import java.util.concurrent.ExecutorService;
import java.util.function.LongSupplier;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationAccess;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationResult;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationStatus;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.slf4j.Logger;

/**
 * Owns foreground flush and compaction orchestration across mapped stable
 * segments.
 */
final class MaintenanceServiceImpl<K, V> implements MaintenanceService {

    private static final String COMPACT_OPERATION_ID = "compact";
    private static final String COMPACT_OPERATION_LABEL = "Compact";
    private static final String FLUSH_OPERATION_ID = "flush";
    private static final String FLUSH_OPERATION_LABEL = "Flush";
    private final Logger logger;
    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final StableSegmentOperationAccess<K, V> stableSegmentGateway;
    private final SplitService<K, V> splitService;
    private final IndexRetryPolicy retryPolicy;
    private final Stats stats;
    private final ExecutorService maintenanceExecutor;
    private final Runnable checkpointAction;
    private final LongSupplier nanoTimeSupplier;

    MaintenanceServiceImpl(final Logger logger,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final StableSegmentOperationAccess<K, V> stableSegmentGateway,
            final SplitService<K, V> splitService,
            final IndexRetryPolicy retryPolicy, final Stats stats,
            final ExecutorService maintenanceExecutor,
            final Runnable checkpointAction) {
        this(logger, keyToSegmentMap, stableSegmentGateway, splitService,
                retryPolicy, stats, maintenanceExecutor, checkpointAction,
                System::nanoTime);
    }

    MaintenanceServiceImpl(final Logger logger,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final StableSegmentOperationAccess<K, V> stableSegmentGateway,
            final SplitService<K, V> splitService,
            final IndexRetryPolicy retryPolicy, final Stats stats,
            final ExecutorService maintenanceExecutor,
            final Runnable checkpointAction,
            final LongSupplier nanoTimeSupplier) {
        this.logger = Vldtn.requireNonNull(logger, "logger");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.stableSegmentGateway = Vldtn.requireNonNull(stableSegmentGateway,
                "stableSegmentGateway");
        this.splitService = Vldtn.requireNonNull(splitService, "splitService");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
        this.stats = Vldtn.requireNonNull(stats, "stats");
        this.maintenanceExecutor = Vldtn.requireNonNull(maintenanceExecutor,
                "maintenanceExecutor");
        this.checkpointAction = Vldtn.requireNonNull(checkpointAction,
                "checkpointAction");
        this.nanoTimeSupplier = Vldtn.requireNonNull(nanoTimeSupplier,
                "nanoTimeSupplier");
    }

    @Override
    public void compact() {
        executeAsync(COMPACT_OPERATION_ID, () -> forEachMappedSegment(
                segmentId -> compactSegment(segmentId, false)));
    }

    @Override
    public void compactAndWait() {
        splitService.awaitQuiescence();
        compactMappedSegmentsAndFlush();
        finalizeSettledMaintenance(this::compactMappedSegmentsAndFlush);
    }

    @Override
    public void flush() {
        executeAsync(FLUSH_OPERATION_ID, () -> {
            flushSegments(false);
            keyToSegmentMap.flushIfDirty();
        });
    }

    @Override
    public void flushAndWait() {
        splitService.awaitQuiescence();
        flushMappedSegmentsAndWait();
        finalizeSettledMaintenance(this::flushMappedSegmentsAndWait);
    }

    void flushSegments(final boolean waitForCompletion) {
        forEachMappedSegment(
                segmentId -> flushSegment(segmentId, waitForCompletion));
    }

    void flushMappedSegmentsAndWait() {
        forEachMappedSegment(segmentId -> flushSegment(segmentId, true));
    }

    void compactMappedSegmentsAndFlush() {
        forEachMappedSegment(segmentId -> compactSegment(segmentId, true));
        flushSegments(true);
    }

    void compactSegment(final SegmentId segmentId,
            final boolean waitForCompletion) {
        runStableSegmentOperation(segmentId, waitForCompletion,
                COMPACT_OPERATION_ID, COMPACT_OPERATION_LABEL,
                stats::recordCompactBusyRetry,
                stats::recordCompactAcceptedToReadyNanos,
                stableSegmentGateway::compact);
    }

    void flushSegment(final SegmentId segmentId,
            final boolean waitForCompletion) {
        stats.recordFlushRequest();
        runStableSegmentOperation(segmentId, waitForCompletion,
                FLUSH_OPERATION_ID, FLUSH_OPERATION_LABEL,
                stats::recordFlushBusyRetry,
                stats::recordFlushAcceptedToReadyNanos,
                stableSegmentGateway::flush);
    }

    private void runStableSegmentOperation(final SegmentId segmentId,
            final boolean waitForCompletion, final String operationId,
            final String operationLabel, final Runnable busyRetryRecorder,
            final java.util.function.LongConsumer acceptedToReadyLatencyRecorder,
            final java.util.function.Function<SegmentId,
                    StableSegmentOperationResult<BlockingSegment<K, V>>> operationRunner) {
        logger.debug("{} attempt started: segment='{}' wait='{}'",
                operationLabel, segmentId, waitForCompletion);
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final StableSegmentOperationResult<BlockingSegment<K, V>> result = operationRunner
                    .apply(segmentId);
            final StableSegmentOperationStatus status = result.getStatus();
            if (status == StableSegmentOperationStatus.OK) {
                completeAcceptedOperation(segmentId, waitForCompletion,
                        operationId, operationLabel,
                        acceptedToReadyLatencyRecorder, result.getValue());
                return;
            }
            if (status == StableSegmentOperationStatus.CLOSED) {
                logOperation(
                        "{} skipped because segment is closed: segment='{}'",
                        operationLabel, segmentId);
                return;
            }
            if (status == StableSegmentOperationStatus.BUSY) {
                if (handleBusyOperation(segmentId, waitForCompletion,
                        operationLabel)) {
                    return;
                }
                busyRetryRecorder.run();
                retryPolicy.backoffOrThrow(startNanos, operationId, segmentId);
                continue;
            }
            if (shouldIgnoreUnmappedError(status, segmentId, operationLabel)) {
                return;
            }
            throw newIndexException(operationId, segmentId, status);
        }
    }

    private void completeAcceptedOperation(final SegmentId segmentId,
            final boolean waitForCompletion, final String operationId,
            final String operationLabel,
            final java.util.function.LongConsumer acceptedToReadyLatencyRecorder,
            final BlockingSegment<K, V> segment) {
        logger.debug("{} attempt accepted: segment='{}' wait='{}'",
                operationLabel, segmentId, waitForCompletion);
        logOperation("{} accepted: segment='{}' wait='{}' state='{}'",
                operationLabel, segmentId, waitForCompletion,
                segment == null ? null : segment.getRuntime().getState());
        if (waitForCompletion && segment != null) {
            acceptedToReadyLatencyRecorder.accept(awaitAcceptedToReadyNanos(
                    segmentId, operationId, segment));
        }
        logOperation("{} completed: segment='{}' wait='{}'",
                operationLabel, segmentId, waitForCompletion);
    }

    private boolean handleBusyOperation(final SegmentId segmentId,
            final boolean waitForCompletion, final String operationLabel) {
        if (!isSegmentStillMapped(segmentId)) {
            logOperation(
                    "{} aborted because segment is no longer mapped: segment='{}'",
                    operationLabel, segmentId);
            return true;
        }
        if (!waitForCompletion) {
            logOperation(
                    "{} coalesced because segment is already busy: segment='{}'",
                    operationLabel, segmentId);
            return true;
        }
        logOperation("{} busy, retrying: segment='{}'", operationLabel,
                segmentId);
        return false;
    }

    private boolean shouldIgnoreUnmappedError(final StableSegmentOperationStatus status,
            final SegmentId segmentId, final String operationLabel) {
        if (status != StableSegmentOperationStatus.ERROR || isSegmentStillMapped(segmentId)) {
            return false;
        }
        logOperation(
                "{} ignored error because segment is no longer mapped: segment='{}'",
                operationLabel, segmentId);
        return true;
    }

    private IndexException newIndexException(final String operation,
            final SegmentId segmentId, final StableSegmentOperationStatus status) {
        final String target = segmentId == null ? ""
                : String.format(" on segment '%s'", segmentId);
        return new IndexException(
                String.format("Index operation '%s' failed%s: %s", operation,
                        target, status));
    }

    private long awaitAcceptedToReadyNanos(final SegmentId segmentId,
            final String operation, final BlockingSegment<K, V> segment) {
        final long acceptedAtNanos = nanoTimeSupplier.getAsLong();
        awaitSegmentReady(segmentId, operation, segment);
        return nanoTimeSupplier.getAsLong() - acceptedAtNanos;
    }

    private void awaitSegmentReady(final SegmentId segmentId,
            final String operation, final BlockingSegment<K, V> segment) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentState state = segment.getRuntime().getState();
            if (state == SegmentState.READY || state == SegmentState.CLOSED) {
                return;
            }
            if (state == SegmentState.ERROR) {
                throw new IndexException(
                        String.format("Segment '%s' failed during %s.",
                                segmentId, operation));
            }
            retryPolicy.backoffOrThrow(startNanos, operation, segmentId);
        }
    }

    private boolean isSegmentStillMapped(final SegmentId segmentId) {
        return keyToSegmentMap.getSegmentIds().contains(segmentId);
    }

    private void forEachMappedSegment(
            final java.util.function.Consumer<SegmentId> segmentAction) {
        keyToSegmentMap.getSegmentIds().forEach(
                Vldtn.requireNonNull(segmentAction, "segmentAction"));
    }

    private void finalizeSettledMaintenance(final Runnable rerunAction) {
        final long topologyVersion = keyToSegmentMap.snapshot().version();
        splitService.awaitQuiescence();
        rerunIfTopologyChanged(topologyVersion, rerunAction);
        keyToSegmentMap.flushIfDirty();
        checkpointAction.run();
    }

    private void rerunIfTopologyChanged(final long topologyVersion,
            final Runnable rerunAction) {
        if (keyToSegmentMap.isAtVersion(topologyVersion)) {
            return;
        }
        rerunAction.run();
    }

    private void executeAsync(final String operation, final Runnable action) {
        try {
            maintenanceExecutor.execute(() -> runAsync(operation, action));
        } catch (final RuntimeException e) {
            throw new IndexException(String.format(
                    "Unable to schedule index operation '%s'.", operation), e);
        }
    }

    private void runAsync(final String operation, final Runnable action) {
        try {
            action.run();
        } catch (final RuntimeException e) {
            logger.error("Index operation '{}' failed.", operation, e);
        }
    }

    private void logOperation(final String message, final Object... args) {
        if (logger.isDebugEnabled()) {
            logger.debug(message, args);
        }
    }
}
