package org.hestiastore.index.segmentindex.split;

import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.function.BooleanSupplier;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.hestiastore.index.segment.SegmentRuntimeLimits;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexConfigurationContract;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.hestiastore.index.segmentregistry.SegmentRegistryResultStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinates route-first split materialization and route-map publish.
 * <p>
 * Child segments are materialized from the parent segment snapshot first and
 * only then published into the key-to-segment map.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class RouteSplitCoordinator<K, V> {

    private static final String SEGMENT_ARG = "segment";

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final IndexConfiguration<K, V> conf;
    private final Comparator<K> keyComparator;
    private final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final SegmentIndexSplitPolicy<K, V> splitPolicy;
    private final IndexRetryPolicy retryPolicy;

    @FunctionalInterface
    public interface SplitPublishRunner {
        boolean run(BooleanSupplier action);
    }

    public RouteSplitCoordinator(final IndexConfiguration<K, V> conf,
            final Comparator<K> keyComparator,
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry) {
        this(conf, keyComparator, keyToSegmentMap, segmentRegistry,
                new SegmentIndexSplitPolicyThreshold<>());
    }

    public RouteSplitCoordinator(final IndexConfiguration<K, V> conf,
            final Comparator<K> keyComparator,
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final SegmentIndexSplitPolicy<K, V> splitPolicy) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.keyComparator = Vldtn.requireNonNull(keyComparator,
                "keyComparator");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.splitPolicy = Vldtn.requireNonNull(splitPolicy, "splitPolicy");
        this.retryPolicy = new IndexRetryPolicy(
                conf.getIndexBusyBackoffMillis(),
                conf.getIndexBusyTimeoutMillis());
    }

    public boolean trySplit(final Segment<K, V> segment,
            final long splitThreshold) {
        return trySplit(segment, splitThreshold, BooleanSupplier::getAsBoolean);
    }

    public boolean trySplit(final Segment<K, V> segment,
            final long splitThreshold,
            final SplitPublishRunner splitPublishRunner) {
        Vldtn.requireNonNull(segment, SEGMENT_ARG);
        Vldtn.requireNonNull(splitPublishRunner, "splitPublishRunner");
        final long estimatedVisibleKeys = segment.getNumberOfKeysInCache();
        final boolean splitFeasible = estimatedVisibleKeys >= 2L;
        if (!isSplitEligible(segment, estimatedVisibleKeys, splitThreshold,
                splitFeasible)) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Route split skipped: segment='{}' estimatedKeys='{}' threshold='{}' splitFeasible='{}'",
                        segment.getId(), estimatedVisibleKeys, splitThreshold,
                        splitFeasible);
            }
            return false;
        }
        return splitAndPublishRoute(segment, splitThreshold,
                splitPublishRunner);
    }

    public boolean shouldSplit(final Segment<K, V> segment,
            final long splitThreshold) {
        return splitPolicy.shouldSplit(segment, splitThreshold);
    }

    private boolean splitAndPublishRoute(final Segment<K, V> segment,
            final long splitThreshold,
            final SplitPublishRunner splitPublishRunner) {
        final SegmentId segmentId = segment.getId();
        logger.debug("Route split started: segment='{}' threshold='{}'",
                segmentId, splitThreshold);
        final SegmentRegistryResult<Segment<K, V>> currentResult = segmentRegistry
                .getSegment(segmentId);
        if (currentResult.getStatus() != SegmentRegistryResultStatus.OK
                || currentResult.getValue() == null) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Route split aborted before validation: segment='{}' registryStatus='{}'",
                        segmentId, currentResult.getStatus());
            }
            return false;
        }
        final Segment<K, V> currentSegment = currentResult.getValue();
        if (currentSegment != segment) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Route split aborted because loaded segment changed: segment='{}'",
                        segmentId);
            }
            return false;
        }
        final RouteSplitPlan<K> splitPlan = buildRouteSplitPlan(segment,
                splitThreshold);
        if (splitPlan == null) {
            return false;
        }
        if (!materializeChildSegments(segment, splitPlan)) {
            return false;
        }
        final boolean published = splitPublishRunner
                .run(() -> publishRouteSplit(splitPlan));
        if (!published) {
            deleteChildSegments(splitPlan.getLowerSegmentId(),
                    splitPlan.getUpperSegmentId().orElse(null));
            return false;
        }
        keyToSegmentMap.optionalyFlush();
        deleteRetiredParentSegment(splitPlan.getReplacedSegmentId());
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Route split applied: replacedSegmentId='{}' lowerSegmentId='{}' upperSegmentId='{}' lowerMaxKey='{}'",
                    splitPlan.getReplacedSegmentId(),
                    splitPlan.getLowerSegmentId(),
                    splitPlan.getUpperSegmentId().orElse(null),
                    splitPlan.getLowerMaxKey());
        }
        return true;
    }

    private RouteSplitPlan<K> buildRouteSplitPlan(final Segment<K, V> segment,
            final long splitThreshold) {
        final SplitBoundary<K> boundary = computeSplitBoundary(segment);
        if (boundary == null) {
            return null;
        }
        if (boundary.visibleCount() < splitThreshold
                || boundary.visibleCount() < 2L) {
            return null;
        }
        final SegmentRegistryResult<Segment<K, V>> lowerCreated = createChildSegment();
        if (!lowerCreated.isOk() || lowerCreated.getValue() == null) {
            return null;
        }
        final SegmentRegistryResult<Segment<K, V>> upperCreated = createChildSegment();
        if (!upperCreated.isOk() || upperCreated.getValue() == null) {
            deleteChildSegments(lowerCreated.getValue().getId(), null);
            return null;
        }
        return new RouteSplitPlan<>(segment.getId(),
                lowerCreated.getValue().getId(), upperCreated.getValue().getId(),
                boundary.minKey(), boundary.maxLowerKey(),
                RouteSplitPlan.SplitMode.SPLIT);
    }

    private SplitBoundary<K> computeSplitBoundary(final Segment<K, V> segment) {
        final long visibleCount = countVisibleEntries(segment);
        if (visibleCount < 2L) {
            return null;
        }
        final long targetLowerCount = Math.max(1L, visibleCount / 2L);
        try (EntryIterator<K, V> iterator = openIteratorWithRetry(segment,
                SegmentIteratorIsolation.FULL_ISOLATION)) {
            if (iterator == null) {
                return null;
            }
            K minKey = null;
            K maxLowerKey = null;
            long lowerCount = 0L;
            while (iterator.hasNext() && lowerCount < targetLowerCount) {
                final Entry<K, V> entry = iterator.next();
                if (minKey == null) {
                    minKey = entry.getKey();
                }
                maxLowerKey = entry.getKey();
                lowerCount++;
            }
            if (minKey == null || maxLowerKey == null
                    || lowerCount >= visibleCount) {
                return null;
            }
            return new SplitBoundary<>(minKey, maxLowerKey, visibleCount);
        }
    }

    private long countVisibleEntries(final Segment<K, V> segment) {
        try (EntryIterator<K, V> iterator = openIteratorWithRetry(segment,
                SegmentIteratorIsolation.FULL_ISOLATION)) {
            if (iterator == null) {
                return 0L;
            }
            long count = 0L;
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
            return count;
        }
    }

    private boolean publishRouteSplit(final RouteSplitPlan<K> splitPlan) {
        Vldtn.requireNonNull(splitPlan, "splitPlan");
        try {
            if (!keyToSegmentMap.applyRouteSplit(splitPlan)) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Route split publish returned false: replacedSegmentId='{}' lowerSegmentId='{}' upperSegmentId='{}'",
                            splitPlan.getReplacedSegmentId(),
                            splitPlan.getLowerSegmentId(),
                            splitPlan.getUpperSegmentId().orElse(null));
                }
                return false;
            }
            return true;
        } catch (final RuntimeException e) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Route split publish failed: replacedSegmentId='{}' lowerSegmentId='{}' upperSegmentId='{}'",
                        splitPlan.getReplacedSegmentId(),
                        splitPlan.getLowerSegmentId(),
                        splitPlan.getUpperSegmentId().orElse(null), e);
            }
            return false;
        }
    }

    private boolean materializeChildSegments(final Segment<K, V> parentSegment,
            final RouteSplitPlan<K> splitPlan) {
        Vldtn.requireNonNull(parentSegment, SEGMENT_ARG);
        Vldtn.requireNonNull(splitPlan, "splitPlan");
        pinChildSegments(splitPlan);
        boolean deleteChildSegments = true;
        try {
            final Segment<K, V> lowerSegment = loadSegmentWithRetry(
                    splitPlan.getLowerSegmentId(), "splitChildLoad");
            final Segment<K, V> upperSegment = splitPlan.getUpperSegmentId()
                    .map(segmentId -> loadSegmentWithRetry(segmentId,
                            "splitChildLoad"))
                    .orElse(null);
            final SegmentRuntimeLimits defaultLimits = defaultRuntimeLimits();
            final SegmentRuntimeLimits materializationLimits = childMaterializationRuntimeLimits(
                    parentSegment, defaultLimits);
            lowerSegment.applyRuntimeLimits(materializationLimits);
            if (upperSegment != null) {
                upperSegment.applyRuntimeLimits(materializationLimits);
            }
            final EntryIterator<K, V> openedIterator = openIteratorWithRetry(
                    parentSegment, SegmentIteratorIsolation.FULL_ISOLATION);
            if (openedIterator == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Route split aborted because parent segment closed before child materialization completed: segment='{}'",
                            parentSegment.getId());
                }
                return false;
            }
            try (EntryIterator<K, V> iterator = openedIterator) {
                while (iterator.hasNext()) {
                    final Entry<K, V> entry = iterator.next();
                    final Segment<K, V> childSegment = selectChildSegment(
                            splitPlan, entry.getKey(), lowerSegment,
                            upperSegment);
                    copyEntryToChildSegment(childSegment, entry.getKey(),
                            entry.getValue());
                }
            } catch (final RuntimeException e) {
                if (isIteratorInvalidated(e)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                                "Route split aborted because parent iterator was invalidated during child materialization: segment='{}'",
                                parentSegment.getId());
                    }
                    return false;
                }
                throw e;
            }
            flushChildSegment(lowerSegment);
            if (upperSegment != null) {
                flushChildSegment(upperSegment);
            }
            lowerSegment.applyRuntimeLimits(defaultLimits);
            if (upperSegment != null) {
                upperSegment.applyRuntimeLimits(defaultLimits);
            }
            deleteChildSegments = false;
            return true;
        } finally {
            unpinChildSegments(splitPlan);
            if (deleteChildSegments) {
                deleteChildSegments(splitPlan.getLowerSegmentId(),
                        splitPlan.getUpperSegmentId().orElse(null));
            }
        }
    }

    private void pinChildSegments(final RouteSplitPlan<K> splitPlan) {
        segmentRegistry.pinSegment(splitPlan.getLowerSegmentId());
        splitPlan.getUpperSegmentId().ifPresent(segmentRegistry::pinSegment);
    }

    private void unpinChildSegments(final RouteSplitPlan<K> splitPlan) {
        segmentRegistry.unpinSegment(splitPlan.getLowerSegmentId());
        splitPlan.getUpperSegmentId().ifPresent(segmentRegistry::unpinSegment);
    }

    private SegmentRuntimeLimits defaultRuntimeLimits() {
        final int segmentCacheLimit = positiveOrFallback(
                conf.getMaxNumberOfKeysInSegmentCache(),
                IndexConfigurationContract.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE);
        final int segmentWriteCacheLimit = positiveOrFallback(
                conf.getMaxNumberOfKeysInActivePartition(),
                IndexConfigurationContract.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE
                        / 2);
        final int maintenanceWriteCacheLimit = Math.max(
                segmentWriteCacheLimit + 1,
                positiveOrFallback(conf.getMaxNumberOfKeysInPartitionBuffer(),
                        segmentWriteCacheLimit + 1));
        return new SegmentRuntimeLimits(segmentCacheLimit,
                segmentWriteCacheLimit, maintenanceWriteCacheLimit);
    }

    private SegmentRuntimeLimits childMaterializationRuntimeLimits(
            final Segment<K, V> parentSegment,
            final SegmentRuntimeLimits defaultLimits) {
        final int widenedSegmentCacheLimit = saturatingToPositiveInt(
                Math.max(parentSegment.getNumberOfKeysInCache() + 1L,
                        defaultLimits.maxNumberOfKeysInSegmentCache()));
        return new SegmentRuntimeLimits(widenedSegmentCacheLimit,
                defaultLimits.maxNumberOfKeysInSegmentWriteCache(),
                defaultLimits.maxNumberOfKeysInSegmentWriteCacheDuringMaintenance());
    }

    private int positiveOrFallback(final Integer value, final int fallback) {
        if (value != null && value.intValue() > 0) {
            return value.intValue();
        }
        return Math.max(1, fallback);
    }

    private int saturatingToPositiveInt(final long value) {
        if (value <= 0L) {
            return 1;
        }
        if (value >= Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) value;
    }

    private EntryIterator<K, V> openIteratorWithRetry(
            final Segment<K, V> segment,
            final SegmentIteratorIsolation isolation) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentResult<EntryIterator<K, V>> result = segment
                    .openIterator(isolation);
            if (result.getStatus() == SegmentResultStatus.OK
                    && result.getValue() != null) {
                return result.getValue();
            }
            if (result.getStatus() == SegmentResultStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, "openIterator",
                        segment.getId());
                continue;
            }
            if (result.getStatus() == SegmentResultStatus.CLOSED) {
                return null;
            }
            throw new IndexException(String.format(
                    "Segment '%s' failed to open iterator for child materialization: %s",
                    segment.getId(), result.getStatus()));
        }
    }

    private Segment<K, V> loadSegmentWithRetry(final SegmentId segmentId,
            final String operation) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentRegistryResult<Segment<K, V>> loaded = segmentRegistry
                    .getSegment(segmentId);
            if (loaded.getStatus() == SegmentRegistryResultStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, operation, segmentId);
                continue;
            }
            if (loaded.getStatus() == SegmentRegistryResultStatus.OK
                    && loaded.getValue() != null) {
                return loaded.getValue();
            }
            throw new IndexException(String.format(
                    "Segment '%s' failed to load for split materialization: %s",
                    segmentId, loaded.getStatus()));
        }
    }

    private void copyEntryToChildSegment(final Segment<K, V> segment,
            final K key, final V value) {
        final SegmentId segmentId = segment.getId();
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentResult<Void> putResult = segment.put(key, value);
            if (putResult.getStatus() == SegmentResultStatus.OK) {
                return;
            }
            if (putResult.getStatus() == SegmentResultStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, "splitPut", segmentId);
                continue;
            }
            if (putResult.getStatus() == SegmentResultStatus.CLOSED) {
                throw new IndexException(String.format(
                        "Segment '%s' closed during split materialization.",
                        segmentId));
            }
            throw new IndexException(String.format(
                    "Segment '%s' failed to accept split materialization entry: %s",
                    segmentId, putResult.getStatus()));
        }
    }

    private void flushChildSegment(final Segment<K, V> segment) {
        final SegmentId segmentId = segment.getId();
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentResult<Void> flushResult = segment.flush();
            if (flushResult.getStatus() == SegmentResultStatus.OK) {
                awaitSegmentReady(segmentId, "splitFlush", segment);
                return;
            }
            if (flushResult.getStatus() == SegmentResultStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, "splitFlush",
                        segmentId);
                continue;
            }
            if (flushResult.getStatus() == SegmentResultStatus.CLOSED) {
                throw new IndexException(String.format(
                        "Segment '%s' closed during split flush.", segmentId));
            }
            throw new IndexException(String.format(
                    "Segment '%s' failed to flush during split materialization: %s",
                    segmentId, flushResult.getStatus()));
        }
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
                throw new IndexException(String.format(
                        "Segment '%s' failed during %s.", segmentId,
                        operation));
            }
            retryPolicy.backoffOrThrow(startNanos, operation, segmentId);
        }
    }

    private Segment<K, V> selectChildSegment(final RouteSplitPlan<K> splitPlan,
            final K key, final Segment<K, V> lowerSegment,
            final Segment<K, V> upperSegment) {
        if (!splitPlan.isSplit()) {
            return lowerSegment;
        }
        if (keyComparator.compare(key, splitPlan.getLowerMaxKey()) <= 0) {
            return lowerSegment;
        }
        return upperSegment == null ? lowerSegment : upperSegment;
    }

    private boolean isIteratorInvalidated(final RuntimeException e) {
        return e instanceof NoSuchElementException;
    }

    private void deleteRetiredParentSegment(final SegmentId segmentId) {
        final SegmentRegistryResultStatus status = segmentRegistry
                .deleteSegment(segmentId).getStatus();
        if (status == SegmentRegistryResultStatus.BUSY) {
            logger.warn(
                    "Retired parent segment '{}' remained on disk because delete was busy after split publish.",
                    segmentId);
            return;
        }
        if (status != SegmentRegistryResultStatus.OK
                && status != SegmentRegistryResultStatus.CLOSED) {
            logger.warn(
                    "Retired parent segment '{}' could not be deleted after split publish: {}",
                    segmentId, status);
        }
    }

    private SegmentRegistryResult<Segment<K, V>> createChildSegment() {
        final long startNanos = retryPolicy.startNanos();
        SegmentRegistryResult<Segment<K, V>> result = segmentRegistry
                .createSegment();
        while (result.getStatus() == SegmentRegistryResultStatus.BUSY) {
            retryPolicy.backoffOrThrow(startNanos, "createSegment", null);
            result = segmentRegistry.createSegment();
        }
        return result;
    }

    private void deleteChildSegments(final SegmentId lowerSegmentId,
            final SegmentId upperSegmentId) {
        if (lowerSegmentId != null) {
            segmentRegistry.deleteSegment(lowerSegmentId);
        }
        if (upperSegmentId != null) {
            segmentRegistry.deleteSegment(upperSegmentId);
        }
    }

    private boolean isSplitEligible(final Segment<K, V> segment,
            final long estimatedVisibleKeys, final long splitThreshold,
            final boolean splitFeasible) {
        if (estimatedVisibleKeys < splitThreshold || !splitFeasible) {
            return false;
        }
        return shouldSplit(segment, splitThreshold);
    }

    private record SplitBoundary<K>(K minKey, K maxLowerKey, long visibleCount) {
    }
}
