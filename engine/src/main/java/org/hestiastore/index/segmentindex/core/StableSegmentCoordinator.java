package org.hestiastore.index.segmentindex.core;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.hestiastore.index.segmentregistry.SegmentRegistryResultStatus;
import org.slf4j.Logger;

/**
 * Coordinates retry-aware access to stable segments through the registry/core
 * boundary.
 */
final class StableSegmentCoordinator<K, V> {

    private static final String OPERATION_COMPACT = "compact";
    private static final String OPERATION_FLUSH = "flush";
    private static final String OPERATION_DRAIN = "drain";

    private final Logger logger;
    private final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator;
    private final SegmentIndexCore<K, V> core;
    private final IndexRetryPolicy retryPolicy;
    private final Stats stats;

    StableSegmentCoordinator(final Logger logger,
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator,
            final SegmentIndexCore<K, V> core,
            final IndexRetryPolicy retryPolicy, final Stats stats) {
        this.logger = Vldtn.requireNonNull(logger, "logger");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.backgroundSplitCoordinator = Vldtn.requireNonNull(
                backgroundSplitCoordinator, "backgroundSplitCoordinator");
        this.core = Vldtn.requireNonNull(core, "core");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
        this.stats = Vldtn.requireNonNull(stats, "stats");
    }

    void putEntryForDrain(final SegmentId segmentId, final K key,
            final V value) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentRegistryResult<Segment<K, V>> loaded = segmentRegistry
                    .getSegment(segmentId);
            if (loaded.getStatus() == SegmentRegistryResultStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, OPERATION_DRAIN,
                        segmentId);
            } else if (loaded.getStatus() != SegmentRegistryResultStatus.OK
                    || loaded.getValue() == null) {
                throw new IndexException(String.format(
                        "Segment '%s' failed to load for drain: %s", segmentId,
                        loaded.getStatus()));
            } else {
                final SegmentResult<Void> putResult = loaded.getValue().put(key,
                        value);
                if (putResult.getStatus() == SegmentResultStatus.OK) {
                    return;
                }
                if (putResult.getStatus() == SegmentResultStatus.BUSY
                        || putResult.getStatus() == SegmentResultStatus.CLOSED) {
                    retryPolicy.backoffOrThrow(startNanos, OPERATION_DRAIN,
                            segmentId);
                } else {
                    throw new IndexException(String.format(
                            "Segment '%s' failed to accept drain entry: %s",
                            segmentId, putResult.getStatus()));
                }
            }
        }
    }

    void flushSegments(final boolean waitForCompletion) {
        keyToSegmentMap.getSegmentIds().forEach(
                segmentId -> flushSegment(segmentId, waitForCompletion));
    }

    void flushMappedSegmentsAndWait() {
        backgroundSplitCoordinator
                .runWithSplitSchedulingPaused(() -> flushSegments(true));
    }

    void compactMappedSegmentsAndFlush() {
        backgroundSplitCoordinator.runWithSplitSchedulingPaused(() -> {
            keyToSegmentMap.getSegmentIds()
                    .forEach(segmentId -> compactSegment(segmentId, true));
            flushSegments(true);
        });
    }

    void compactSegment(final SegmentId segmentId,
            final boolean waitForCompletion) {
        if (logger.isDebugEnabled()) {
            logger.debug("Compact attempt started: segment='{}' wait='{}'",
                    segmentId, waitForCompletion);
        }
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final IndexResult<Segment<K, V>> result = core.compact(segmentId);
            final IndexResultStatus status = result.getStatus();
            if (status == IndexResultStatus.OK) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Compact accepted: segment='{}' wait='{}' state='{}'",
                            segmentId, waitForCompletion,
                            result.getValue() == null ? null
                                    : result.getValue().getState());
                }
                if (waitForCompletion) {
                    final Segment<K, V> segment = result.getValue();
                    if (segment != null) {
                        awaitSegmentReady(segmentId, OPERATION_COMPACT,
                                segment);
                    }
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Compact completed: segment='{}' wait='{}'",
                            segmentId, waitForCompletion);
                }
                return;
            }
            if (status == IndexResultStatus.CLOSED) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Compact skipped because segment is closed: segment='{}'",
                            segmentId);
                }
                return;
            }
            if (status == IndexResultStatus.BUSY) {
                if (!isSegmentStillMapped(segmentId)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                                "Compact aborted because segment is no longer mapped: segment='{}'",
                                segmentId);
                    }
                    return;
                }
                if (!waitForCompletion) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                                "Compact coalesced because segment is already busy: segment='{}'",
                                segmentId);
                    }
                    return;
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Compact busy, retrying: segment='{}'",
                            segmentId);
                }
                retryPolicy.backoffOrThrow(startNanos, OPERATION_COMPACT,
                        segmentId);
                continue;
            }
            if (status == IndexResultStatus.ERROR
                    && !isSegmentStillMapped(segmentId)) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Compact ignored error because segment is no longer mapped: segment='{}'",
                            segmentId);
                }
                return;
            }
            throw newIndexException(OPERATION_COMPACT, segmentId, status);
        }
    }

    void flushSegment(final SegmentId segmentId,
            final boolean waitForCompletion) {
        stats.incFlushRequestCx();
        if (logger.isDebugEnabled()) {
            logger.debug("Flush attempt started: segment='{}' wait='{}'",
                    segmentId, waitForCompletion);
        }
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final IndexResult<Segment<K, V>> result = core.flush(segmentId);
            final IndexResultStatus status = result.getStatus();
            if (status == IndexResultStatus.OK) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Flush accepted: segment='{}' wait='{}' state='{}'",
                            segmentId, waitForCompletion,
                            result.getValue() == null ? null
                                    : result.getValue().getState());
                }
                if (waitForCompletion) {
                    final Segment<K, V> segment = result.getValue();
                    if (segment != null) {
                        awaitSegmentReady(segmentId, OPERATION_FLUSH, segment);
                    }
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Flush completed: segment='{}' wait='{}'",
                            segmentId, waitForCompletion);
                }
                return;
            }
            if (status == IndexResultStatus.CLOSED) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Flush skipped because segment is closed: segment='{}'",
                            segmentId);
                }
                return;
            }
            if (status == IndexResultStatus.BUSY) {
                if (!isSegmentStillMapped(segmentId)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                                "Flush aborted because segment is no longer mapped: segment='{}'",
                                segmentId);
                    }
                    return;
                }
                if (!waitForCompletion) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                                "Flush coalesced because segment is already busy: segment='{}'",
                                segmentId);
                    }
                    return;
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Flush busy, retrying: segment='{}'",
                            segmentId);
                }
                retryPolicy.backoffOrThrow(startNanos, OPERATION_FLUSH,
                        segmentId);
                continue;
            }
            if (status == IndexResultStatus.ERROR
                    && !isSegmentStillMapped(segmentId)) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Flush ignored error because segment is no longer mapped: segment='{}'",
                            segmentId);
                }
                return;
            }
            throw newIndexException(OPERATION_FLUSH, segmentId, status);
        }
    }

    EntryIterator<K, V> openIteratorWithRetry(final SegmentId segmentId,
            final SegmentIteratorIsolation isolation) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final IndexResult<EntryIterator<K, V>> result = core
                    .openIterator(segmentId, isolation);
            if (result.getStatus() == IndexResultStatus.OK) {
                return result.getValue();
            }
            if (result.getStatus() == IndexResultStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, "openIterator",
                        segmentId);
                continue;
            }
            throw newIndexException("openIterator", segmentId,
                    result.getStatus());
        }
    }

    void invalidateIterators() {
        keyToSegmentMap.getSegmentIds().forEach(segmentId -> {
            final SegmentRegistryResult<Segment<K, V>> loaded = segmentRegistry
                    .getSegment(segmentId);
            if (loaded.getStatus() == SegmentRegistryResultStatus.OK
                    && loaded.getValue() != null) {
                loaded.getValue().invalidateIterators();
                return;
            }
            if (!isSegmentStillMapped(segmentId)) {
                return;
            }
            if (loaded.getStatus() == SegmentRegistryResultStatus.BUSY) {
                logger.debug(
                        "Skipping iterator invalidation for segment '{}' because it is BUSY.",
                        segmentId);
                return;
            }
            logger.debug(
                    "Skipping iterator invalidation for segment '{}' because registry returned status '{}'.",
                    segmentId, loaded.getStatus());
        });
    }

    private void awaitSegmentReady(final SegmentId segmentId,
            final String operation, final Segment<K, V> segment) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentState state = segment.getState();
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

    private IndexException newIndexException(final String operation,
            final SegmentId segmentId, final IndexResultStatus status) {
        final String target = segmentId == null ? ""
                : String.format(" on segment '%s'", segmentId);
        return new IndexException(
                String.format("Index operation '%s' failed%s: %s", operation,
                        target, status));
    }
}
