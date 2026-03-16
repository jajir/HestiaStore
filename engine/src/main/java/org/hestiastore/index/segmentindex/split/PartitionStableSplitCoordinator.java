package org.hestiastore.index.segmentindex.split;

import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentRuntimeLimits;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexConfigurationContract;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.partition.PartitionRuntime;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.hestiastore.index.segmentregistry.SegmentRegistryResultStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinator for route-first partition splits.
 * <p>
 * The split builds child stable segments from the parent stable snapshot first
 * and only then remaps routed writes/reads to child segment ids. Buffered
 * overlay data are reassigned during the final short apply phase.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class PartitionStableSplitCoordinator<K, V> {

    private static final String SEGMENT_ARG = "segment";

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final IndexConfiguration<K, V> conf;
    private final Comparator<K> keyComparator;
    private final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final PartitionRuntime<K, V> partitionRuntime;
    private final SegmentIndexSplitPolicy<K, V> splitPolicy;
    private final IndexRetryPolicy retryPolicy;

    @FunctionalInterface
    public interface SplitApplyRunner {
        boolean run(Supplier<Boolean> action);
    }

    public PartitionStableSplitCoordinator(final IndexConfiguration<K, V> conf,
            final Comparator<K> keyComparator,
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final PartitionRuntime<K, V> partitionRuntime) {
        this(conf, keyComparator, keyToSegmentMap, segmentRegistry,
                partitionRuntime,
                new SegmentIndexSplitPolicyThreshold<>());
    }

    public PartitionStableSplitCoordinator(final IndexConfiguration<K, V> conf,
            final Comparator<K> keyComparator,
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final PartitionRuntime<K, V> partitionRuntime,
            final SegmentIndexSplitPolicy<K, V> splitPolicy) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.keyComparator = Vldtn.requireNonNull(keyComparator,
                "keyComparator");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.partitionRuntime = Vldtn.requireNonNull(partitionRuntime,
                "partitionRuntime");
        this.splitPolicy = Vldtn.requireNonNull(splitPolicy, "splitPolicy");
        this.retryPolicy = new IndexRetryPolicy(
                conf.getIndexBusyBackoffMillis(),
                conf.getIndexBusyTimeoutMillis());
    }

    public boolean optionallySplit(final Segment<K, V> segment,
            final long maxNumberOfKeysInSegment) {
        return optionallySplit(segment, maxNumberOfKeysInSegment,
                Supplier::get);
    }

    public boolean optionallySplit(final Segment<K, V> segment,
            final long maxNumberOfKeysInSegment,
            final SplitApplyRunner splitApplyRunner) {
        Vldtn.requireNonNull(segment, SEGMENT_ARG);
        Vldtn.requireNonNull(splitApplyRunner, "splitApplyRunner");
        final PartitionSplitPlan<K, V> estimatedPlan = buildPartitionSplitPlan(
                segment);
        if (!isEligibleForSplit(segment, estimatedPlan,
                maxNumberOfKeysInSegment)) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Route split skipped: segment='{}' estimatedKeys='{}' threshold='{}' splitFeasible='{}'",
                        segment.getId(), estimatedPlan.getEstimatedNumberOfKeys(),
                        maxNumberOfKeysInSegment, estimatedPlan.isSplitFeasible());
            }
            return false;
        }
        return splitByRouteRemap(segment, maxNumberOfKeysInSegment,
                splitApplyRunner);
    }

    public boolean shouldBeSplit(final Segment<K, V> segment,
            final long maxNumberOfKeysInSegment) {
        return splitPolicy.shouldSplit(segment, maxNumberOfKeysInSegment);
    }

    private boolean splitByRouteRemap(final Segment<K, V> segment,
            final long maxNumberOfKeysInSegment,
            final SplitApplyRunner splitApplyRunner) {
        final SegmentId segmentId = segment.getId();
        logger.debug("Route split started: segment='{}' threshold='{}'",
                segmentId, maxNumberOfKeysInSegment);
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
        final Segment<K, V> current = currentResult.getValue();
        if (current != segment) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Route split aborted because loaded segment changed: segment='{}'",
                        segmentId);
            }
            return false;
        }
        final PartitionSplitApplyPlan<K> applyPlan = buildPartitionSplitApplyPlan(
                segment, maxNumberOfKeysInSegment);
        if (applyPlan == null) {
            return false;
        }
        if (!materializeStableChildren(segment, applyPlan)) {
            return false;
        }
        final boolean applied = splitApplyRunner
                .run(() -> applyPartitionSplitPlan(applyPlan));
        if (!applied) {
            deleteSplitSegments(applyPlan.getLowerSegmentId(),
                    applyPlan.getUpperSegmentId().orElse(null));
            return false;
        }
        keyToSegmentMap.optionalyFlush();
        deleteRetiredParentSegment(applyPlan.getOldSegmentId());
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Route split applied: oldSegmentId='{}' lowerSegmentId='{}' upperSegmentId='{}' splitKey='{}'",
                    applyPlan.getOldSegmentId(), applyPlan.getLowerSegmentId(),
                    applyPlan.getUpperSegmentId().orElse(null),
                    applyPlan.getMaxKey());
        }
        return true;
    }

    private PartitionSplitApplyPlan<K> buildPartitionSplitApplyPlan(
            final Segment<K, V> segment,
            final long maxNumberOfKeysInSegment) {
        final SplitBoundary<K> boundary = computeSplitBoundary(segment);
        if (boundary == null) {
            return null;
        }
        if (boundary.visibleCount() < maxNumberOfKeysInSegment
                || boundary.visibleCount() < 2L) {
            return null;
        }
        final SegmentRegistryResult<Segment<K, V>> lowerCreated = createSegment();
        if (!lowerCreated.isOk() || lowerCreated.getValue() == null) {
            return null;
        }
        final SegmentRegistryResult<Segment<K, V>> upperCreated = createSegment();
        if (!upperCreated.isOk() || upperCreated.getValue() == null) {
            deleteSplitSegments(lowerCreated.getValue().getId(), null);
            return null;
        }
        return new PartitionSplitApplyPlan<>(segment.getId(),
                lowerCreated.getValue().getId(), upperCreated.getValue().getId(),
                boundary.minKey(), boundary.maxLowerKey(),
                PartitionSplitResult.PartitionSplitStatus.SPLIT);
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

    private boolean applyPartitionSplitPlan(
            final PartitionSplitApplyPlan<K> plan) {
        Vldtn.requireNonNull(plan, "plan");
        try {
            if (!keyToSegmentMap.applyPartitionSplitPlan(plan)) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Route split map apply returned false: oldSegmentId='{}' lowerSegmentId='{}' upperSegmentId='{}'",
                            plan.getOldSegmentId(), plan.getLowerSegmentId(),
                            plan.getUpperSegmentId().orElse(null));
                }
                return false;
            }
            partitionRuntime.reassignOverlayAfterPartitionSplit(plan);
            return true;
        } catch (final RuntimeException e) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Route split map apply failed: oldSegmentId='{}' lowerSegmentId='{}' upperSegmentId='{}'",
                        plan.getOldSegmentId(), plan.getLowerSegmentId(),
                        plan.getUpperSegmentId().orElse(null), e);
            }
            return false;
        }
    }

    private boolean materializeStableChildren(final Segment<K, V> parentSegment,
            final PartitionSplitApplyPlan<K> plan) {
        Vldtn.requireNonNull(parentSegment, SEGMENT_ARG);
        Vldtn.requireNonNull(plan, "plan");
        pinSplitTargets(plan);
        boolean deleteTargets = true;
        try {
            final Segment<K, V> lowerSegment = loadSegmentWithRetry(
                    plan.getLowerSegmentId(), "splitChildLoad");
            final Segment<K, V> upperSegment = plan.getUpperSegmentId()
                    .map(segmentId -> loadSegmentWithRetry(segmentId,
                            "splitChildLoad"))
                    .orElse(null);
            final SegmentRuntimeLimits defaultLimits = defaultRuntimeLimits();
            final SegmentRuntimeLimits materializationLimits = splitMaterializationRuntimeLimits(
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
                    final Segment<K, V> targetSegment = resolveMaterializationTarget(
                            plan, entry.getKey(), lowerSegment, upperSegment);
                    putStableEntry(targetSegment, entry.getKey(),
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
            flushSegment(lowerSegment);
            if (upperSegment != null) {
                flushSegment(upperSegment);
            }
            lowerSegment.applyRuntimeLimits(defaultLimits);
            if (upperSegment != null) {
                upperSegment.applyRuntimeLimits(defaultLimits);
            }
            deleteTargets = false;
            return true;
        } finally {
            unpinSplitTargets(plan);
            if (deleteTargets) {
                deleteSplitSegments(plan.getLowerSegmentId(),
                        plan.getUpperSegmentId().orElse(null));
            }
        }
    }

    private void pinSplitTargets(final PartitionSplitApplyPlan<K> plan) {
        segmentRegistry.pinSegment(plan.getLowerSegmentId());
        plan.getUpperSegmentId().ifPresent(segmentRegistry::pinSegment);
    }

    private void unpinSplitTargets(final PartitionSplitApplyPlan<K> plan) {
        segmentRegistry.unpinSegment(plan.getLowerSegmentId());
        plan.getUpperSegmentId().ifPresent(segmentRegistry::unpinSegment);
    }

    private SegmentRuntimeLimits defaultRuntimeLimits() {
        final int segmentCacheLimit = positiveOrFallback(
                conf.getMaxNumberOfKeysInSegmentCache(),
                IndexConfigurationContract.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE);
        final int writeCacheLimit = positiveOrFallback(
                conf.getMaxNumberOfKeysInActivePartition(),
                IndexConfigurationContract.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE
                        / 2);
        final int writeCacheDuringMaintenance = Math.max(writeCacheLimit + 1,
                positiveOrFallback(conf.getMaxNumberOfKeysInPartitionBuffer(),
                        writeCacheLimit + 1));
        return new SegmentRuntimeLimits(segmentCacheLimit, writeCacheLimit,
                writeCacheDuringMaintenance);
    }

    private SegmentRuntimeLimits splitMaterializationRuntimeLimits(
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

    private void putStableEntry(final Segment<K, V> segment, final K key,
            final V value) {
        final SegmentId segmentId = segment.getId();
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentResult<Void> putResult = segment.put(key, value);
            if (putResult.getStatus() == SegmentResultStatus.OK) {
                return;
            }
            if (putResult.getStatus() == SegmentResultStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, "splitPut",
                        segmentId);
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

    private void flushSegment(final Segment<K, V> segment) {
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

    private Segment<K, V> resolveMaterializationTarget(
            final PartitionSplitApplyPlan<K> plan, final K key,
            final Segment<K, V> lowerSegment, final Segment<K, V> upperSegment) {
        if (plan.getStatus() != PartitionSplitResult.PartitionSplitStatus.SPLIT) {
            return lowerSegment;
        }
        if (keyComparator.compare(key, plan.getMaxKey()) <= 0) {
            return lowerSegment;
        }
        return upperSegment == null ? lowerSegment : upperSegment;
    }

    private boolean isIteratorInvalidated(final RuntimeException e) {
        return e instanceof NoSuchElementException;
    }

    private void deleteRetiredParentSegment(final SegmentId segmentId) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentRegistryResult<Void> deleteResult = segmentRegistry
                    .deleteSegment(segmentId);
            if (deleteResult.getStatus() == SegmentRegistryResultStatus.OK
                    || deleteResult
                            .getStatus() == SegmentRegistryResultStatus.CLOSED) {
                return;
            }
            if (deleteResult.getStatus() == SegmentRegistryResultStatus.BUSY) {
                try {
                    retryPolicy.backoffOrThrow(startNanos, "deleteRetiredSplit",
                            segmentId);
                } catch (final IndexException timeout) {
                    logger.warn(
                            "Retired parent segment '{}' remained on disk because delete timed out after split publish.",
                            segmentId);
                    return;
                }
                continue;
            }
            logger.warn(
                    "Retired parent segment '{}' could not be deleted after split publish: {}",
                    segmentId, deleteResult.getStatus());
            return;
        }
    }

    private SegmentRegistryResult<Segment<K, V>> createSegment() {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentRegistryResult<Segment<K, V>> result = segmentRegistry
                    .createSegment();
            if (result.getStatus() == SegmentRegistryResultStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, "createSegment", null);
                continue;
            }
            return result;
        }
    }

    private void deleteSplitSegments(final SegmentId lowerSegmentId,
            final SegmentId upperSegmentId) {
        if (lowerSegmentId != null) {
            segmentRegistry.deleteSegment(lowerSegmentId);
        }
        if (upperSegmentId != null) {
            segmentRegistry.deleteSegment(upperSegmentId);
        }
    }

    private PartitionSplitPlan<K, V> buildPartitionSplitPlan(
            final Segment<K, V> segment) {
        return PartitionSplitPlan
                .fromPolicy(new PartitionSplitPolicy(segment
                        .getNumberOfKeysInCache()));
    }

    private boolean isEligibleForSplit(final Segment<K, V> segment,
            final PartitionSplitPlan<K, V> plan,
            final long maxNumberOfKeysInSegment) {
        if (plan.getEstimatedNumberOfKeys() < maxNumberOfKeysInSegment) {
            return false;
        }
        if (!plan.isSplitFeasible()) {
            return false;
        }
        return shouldBeSplit(segment, maxNumberOfKeysInSegment);
    }

    private record SplitBoundary<K>(K minKey, K maxLowerKey, long visibleCount) {
    }
}
