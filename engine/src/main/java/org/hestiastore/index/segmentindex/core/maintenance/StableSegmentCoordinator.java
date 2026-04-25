package org.hestiastore.index.segmentindex.core.maintenance;

import java.util.Optional;
import java.util.function.LongSupplier;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
import org.hestiastore.index.segmentindex.core.routing.IndexResult;
import org.hestiastore.index.segmentindex.core.routing.IndexResultStatus;
import org.hestiastore.index.segmentindex.core.routing.StableSegmentAccess;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentHandle;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.slf4j.Logger;

/**
 * Coordinates retry-aware access to stable segments through the registry/core
 * boundary.
 */
final class StableSegmentCoordinator<K, V>
        implements StableSegmentMaintenanceAccess<K, V> {

    private static final String COMPACT_OPERATION_ID = "compact";
    private static final String COMPACT_OPERATION_LABEL = "Compact";
    private static final String FLUSH_OPERATION_ID = "flush";
    private static final String FLUSH_OPERATION_LABEL = "Flush";
    private final Logger logger;
    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final StableSegmentAccess<K, V> stableSegmentGateway;
    private final IndexRetryPolicy retryPolicy;
    private final Stats stats;
    private final LongSupplier nanoTimeSupplier;

    StableSegmentCoordinator(final Logger logger,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final StableSegmentAccess<K, V> stableSegmentGateway,
            final IndexRetryPolicy retryPolicy, final Stats stats) {
        this(logger, keyToSegmentMap, segmentRegistry, stableSegmentGateway,
                retryPolicy, stats, System::nanoTime);
    }

    StableSegmentCoordinator(final Logger logger,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final StableSegmentAccess<K, V> stableSegmentGateway,
            final IndexRetryPolicy retryPolicy, final Stats stats,
            final LongSupplier nanoTimeSupplier) {
        this.logger = Vldtn.requireNonNull(logger, "logger");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.stableSegmentGateway = Vldtn.requireNonNull(stableSegmentGateway,
                "stableSegmentGateway");
        final IndexRetryPolicy nonNullRetryPolicy = Vldtn
                .requireNonNull(retryPolicy, "retryPolicy");
        this.stats = Vldtn.requireNonNull(stats, "stats");
        this.retryPolicy = nonNullRetryPolicy;
        this.nanoTimeSupplier = Vldtn.requireNonNull(nanoTimeSupplier,
                "nanoTimeSupplier");
    }

    void putEntryForDrain(final SegmentId segmentId, final K key,
            final V value) {
        segmentRegistry.loadSegment(segmentId).put(key, value);
    }

    @Override
    public void flushSegments(final boolean waitForCompletion) {
        forEachMappedSegment(
                segmentId -> flushSegment(segmentId, waitForCompletion));
    }

    @Override
    public void flushMappedSegmentsAndWait() {
        forEachMappedSegment(segmentId -> flushSegment(segmentId, true));
    }

    @Override
    public void compactMappedSegmentsAndFlush() {
        forEachMappedSegment(segmentId -> compactSegment(segmentId, true));
        flushSegments(true);
    }

    @Override
    public void compactSegment(final SegmentId segmentId,
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

    @Override
    public EntryIterator<K, V> openIteratorWithRetry(final SegmentId segmentId,
            final SegmentIteratorIsolation isolation) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final IndexResult<EntryIterator<K, V>> result = stableSegmentGateway
                    .openIterator(segmentId, isolation);
            if (result.getStatus() == IndexResultStatus.OK) {
                return result.getValue();
            }
            if (result.getStatus() == IndexResultStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, "openIterator",
                        segmentId);
                continue;
            }
            throw new IndexException(String.format(
                    "Index operation 'openIterator' failed on segment '%s': %s",
                    segmentId, result.getStatus()));
        }
    }

    @Override
    public void invalidateIterators() {
        forEachMappedSegment(this::invalidateIteratorsForSegment);
    }

    private void runStableSegmentOperation(final SegmentId segmentId,
            final boolean waitForCompletion, final String operationId,
            final String operationLabel, final Runnable busyRetryRecorder,
            final java.util.function.LongConsumer acceptedToReadyLatencyRecorder,
            final java.util.function.Function<SegmentId, IndexResult<SegmentHandle<K, V>>> operationRunner) {
        logger.debug("{} attempt started: segment='{}' wait='{}'",
                operationLabel, segmentId, waitForCompletion);
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final IndexResult<SegmentHandle<K, V>> result = operationRunner
                    .apply(segmentId);
            final IndexResultStatus status = result.getStatus();
            if (status == IndexResultStatus.OK) {
                completeAcceptedOperation(segmentId, waitForCompletion,
                        operationId, operationLabel,
                        acceptedToReadyLatencyRecorder, result.getValue());
                return;
            }
            if (status == IndexResultStatus.CLOSED) {
                logOperation(
                        "{} skipped because segment is closed: segment='{}'",
                        operationLabel, segmentId);
                return;
            }
            if (status == IndexResultStatus.BUSY) {
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
            final SegmentHandle<K, V> segment) {
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

    private boolean shouldIgnoreUnmappedError(final IndexResultStatus status,
            final SegmentId segmentId, final String operationLabel) {
        if (status != IndexResultStatus.ERROR || isSegmentStillMapped(segmentId)) {
            return false;
        }
        logOperation(
                "{} ignored error because segment is no longer mapped: segment='{}'",
                operationLabel, segmentId);
        return true;
    }

    private IndexException newIndexException(final String operation,
            final SegmentId segmentId, final IndexResultStatus status) {
        final String target = segmentId == null ? ""
                : String.format(" on segment '%s'", segmentId);
        return new IndexException(
                String.format("Index operation '%s' failed%s: %s", operation,
                        target, status));
    }

    private long awaitAcceptedToReadyNanos(final SegmentId segmentId,
            final String operation, final SegmentHandle<K, V> segment) {
        final long acceptedAtNanos = nanoTimeSupplier.getAsLong();
        awaitSegmentReady(segmentId, operation, segment);
        return nanoTimeSupplier.getAsLong() - acceptedAtNanos;
    }

    private void awaitSegmentReady(final SegmentId segmentId,
            final String operation, final SegmentHandle<K, V> segment) {
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

    private void invalidateIteratorsForSegment(final SegmentId segmentId) {
        try {
            handleLoadedSegment(segmentId, segmentRegistry.tryGetSegment(segmentId));
        } catch (final IndexException e) {
            logIteratorInvalidationLookupFailure(segmentId, e);
        }
    }

    private void handleLoadedSegment(final SegmentId segmentId,
            final Optional<SegmentHandle<K, V>> loadedSegment) {
        if (loadedSegment.isPresent()) {
            loadedSegment.get().invalidateIterators();
            return;
        }
        logMissingSegment(segmentId);
    }

    private void logMissingSegment(final SegmentId segmentId) {
        if (!isSegmentStillMapped(segmentId)) {
            return;
        }
        logger.debug(
                "Skipping iterator invalidation for segment '{}' because it is not immediately available.",
                segmentId);
    }

    private void logIteratorInvalidationLookupFailure(final SegmentId segmentId,
            final IndexException exception) {
        if (!isSegmentStillMapped(segmentId)) {
            return;
        }
        logger.debug(
                "Skipping iterator invalidation for segment '{}' because registry lookup failed.",
                segmentId, exception);
    }

    private boolean isSegmentStillMapped(final SegmentId segmentId) {
        return keyToSegmentMap.getSegmentIds().contains(segmentId);
    }

    private void forEachMappedSegment(final java.util.function.Consumer<SegmentId> segmentAction) {
        keyToSegmentMap.getSegmentIds().forEach(
                Vldtn.requireNonNull(segmentAction, "segmentAction"));
    }

    private void logOperation(final String message, final Object... args) {
        if (logger.isDebugEnabled()) {
            logger.debug(message, args);
        }
    }
}
