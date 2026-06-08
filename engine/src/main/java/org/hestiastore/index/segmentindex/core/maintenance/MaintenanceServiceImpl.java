package org.hestiastore.index.segmentindex.core.maintenance;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationAccess;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationResult;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationStatus;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns foreground flush and compaction orchestration across mapped stable
 * segments.
 */
final class MaintenanceServiceImpl<K, V> implements MaintenanceService {

    private static final String COMPACT_OPERATION_ID = "compact";
    private static final String COMPACT_OPERATION_LABEL = "Compact";
    private static final String FLUSH_OPERATION_ID = "flush";
    private static final String FLUSH_OPERATION_LABEL = "Flush";
    private static final Logger LOGGER = LoggerFactory
            .getLogger(MaintenanceServiceImpl.class);

    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final StableSegmentOperationAccess<K, V> stableSegmentGateway;
    private final SplitService splitService;
    private final MaintenanceRetryPolicy retryPolicy;
    private final MaintenanceStatsRecorder statsRecorder;
    private final ExecutorService maintenanceExecutor;
    private final Runnable checkpointAction;
    private final LongSupplier nanoTimeSupplier;
    private final AtomicBoolean asyncMaintenanceClosed =
            new AtomicBoolean(false);
    private final AtomicInteger asyncMaintenanceInFlight =
            new AtomicInteger();

    MaintenanceServiceImpl(
            final KeyToSegmentMap<K> keyToSegmentMap,
            final StableSegmentOperationAccess<K, V> stableSegmentGateway,
            final SplitService splitService,
            final MaintenanceRetryPolicy retryPolicy,
            final MaintenanceStatsRecorder statsRecorder,
            final ExecutorService maintenanceExecutor,
            final Runnable checkpointAction) {
        this(keyToSegmentMap, stableSegmentGateway, splitService,
                retryPolicy, statsRecorder, maintenanceExecutor,
                checkpointAction, System::nanoTime);
    }

    MaintenanceServiceImpl(
            final KeyToSegmentMap<K> keyToSegmentMap,
            final StableSegmentOperationAccess<K, V> stableSegmentGateway,
            final SplitService splitService,
            final MaintenanceRetryPolicy retryPolicy,
            final MaintenanceStatsRecorder statsRecorder,
            final ExecutorService maintenanceExecutor,
            final Runnable checkpointAction,
            final LongSupplier nanoTimeSupplier) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.stableSegmentGateway = Vldtn.requireNonNull(stableSegmentGateway,
                "stableSegmentGateway");
        this.splitService = Vldtn.requireNonNull(splitService, "splitService");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
        this.statsRecorder = Vldtn.requireNonNull(statsRecorder,
                "statsRecorder");
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

    @Override
    public void sealAsyncMaintenanceAndWait() {
        asyncMaintenanceClosed.set(true);
        final long startNanos = retryPolicy.startNanos();
        while (asyncMaintenanceInFlight.get() > 0) {
            retryPolicy.backoffOrThrow(startNanos, "asyncMaintenanceClose",
                    asyncMaintenanceInFlight.get());
        }
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
        statsRecorder.recordCompactRequest();
        runStableSegmentOperation(segmentId, waitForCompletion,
                COMPACT_OPERATION_ID, COMPACT_OPERATION_LABEL,
                statsRecorder::recordCompactBusyRetry,
                statsRecorder::recordCompactAcceptedToReadyNanos,
                stableSegmentGateway::compact);
    }

    void flushSegment(final SegmentId segmentId,
            final boolean waitForCompletion) {
        statsRecorder.recordFlushRequest();
        runStableSegmentOperation(segmentId, waitForCompletion,
                FLUSH_OPERATION_ID, FLUSH_OPERATION_LABEL,
                statsRecorder::recordFlushBusyRetry,
                statsRecorder::recordFlushAcceptedToReadyNanos,
                stableSegmentGateway::flush);
    }

    private void runStableSegmentOperation(final SegmentId segmentId,
            final boolean waitForCompletion, final String operationId,
            final String operationLabel, final Runnable busyRetryRecorder,
            final java.util.function.LongConsumer acceptedToReadyLatencyRecorder,
            final java.util.function.Function<SegmentId,
                    StableSegmentOperationResult<BlockingSegment<K, V>>> operationRunner) {
        LOGGER.debug("{} attempt started: segment='{}' wait='{}'",
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
        LOGGER.debug("{} attempt accepted: segment='{}' wait='{}'",
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
        enterAsyncMaintenance(operation);
        try {
            maintenanceExecutor
                    .execute(() -> runAsyncMaintenance(operation, action));
        } catch (final RuntimeException e) {
            exitAsyncMaintenance();
            throw new IndexException(String.format(
                    "Unable to schedule index operation '%s'.", operation), e);
        }
    }

    private void enterAsyncMaintenance(final String operation) {
        if (asyncMaintenanceClosed.get()) {
            throw new IndexException(String.format(
                    "Index operation '%s' cannot be scheduled because maintenance is closing.",
                    operation));
        }
        asyncMaintenanceInFlight.incrementAndGet();
        if (asyncMaintenanceClosed.get()) {
            exitAsyncMaintenance();
            throw new IndexException(String.format(
                    "Index operation '%s' cannot be scheduled because maintenance is closing.",
                    operation));
        }
    }

    private void runAsyncMaintenance(final String operation,
            final Runnable action) {
        try {
            runAsync(operation, action);
        } finally {
            exitAsyncMaintenance();
        }
    }

    private void exitAsyncMaintenance() {
        asyncMaintenanceInFlight.decrementAndGet();
    }

    private void runAsync(final String operation, final Runnable action) {
        try {
            action.run();
        } catch (final RuntimeException e) {
            LOGGER.error("Index operation '{}' failed.", operation, e);
        }
    }

    private void logOperation(final String message, final Object... args) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(message, args);
        }
    }
}
