package org.hestiastore.index.segmentindex.split;

import java.util.function.Supplier;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.hestiastore.index.segmentindex.IndexConfiguration;
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
 * The split first remaps routed writes/reads to child segment ids and leaves
 * stable child materialization for a later explicit publish step.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class PartitionStableSplitCoordinator<K, V> {

    private static final String SEGMENT_ARG = "segment";

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final IndexConfiguration<K, V> conf;
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
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final PartitionRuntime<K, V> partitionRuntime) {
        this(conf, keyToSegmentMap, segmentRegistry, partitionRuntime,
                new SegmentIndexSplitPolicyThreshold<>());
    }

    public PartitionStableSplitCoordinator(final IndexConfiguration<K, V> conf,
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final PartitionRuntime<K, V> partitionRuntime,
            final SegmentIndexSplitPolicy<K, V> splitPolicy) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
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
        final SegmentSplitterPlan<K, V> estimatedPlan = buildPlan(segment);
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
        final SegmentSplitApplyPlan<K> applyPlan = buildApplyPlan(segment,
                maxNumberOfKeysInSegment);
        if (applyPlan == null) {
            return false;
        }
        final boolean applied = splitApplyRunner
                .run(() -> applySplitPlan(applyPlan));
        if (!applied) {
            deleteSplitSegments(applyPlan.getLowerSegmentId(),
                    applyPlan.getUpperSegmentId().orElse(null));
            return false;
        }
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Route split applied: oldSegmentId='{}' lowerSegmentId='{}' upperSegmentId='{}' splitKey='{}'",
                    applyPlan.getOldSegmentId(), applyPlan.getLowerSegmentId(),
                    applyPlan.getUpperSegmentId().orElse(null),
                    applyPlan.getMaxKey());
        }
        return true;
    }

    private SegmentSplitApplyPlan<K> buildApplyPlan(final Segment<K, V> segment,
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
        return new SegmentSplitApplyPlan<>(segment.getId(),
                lowerCreated.getValue().getId(), upperCreated.getValue().getId(),
                boundary.minKey(), boundary.maxLowerKey(),
                SegmentSplitterResult.SegmentSplittingStatus.SPLIT);
    }

    private SplitBoundary<K> computeSplitBoundary(final Segment<K, V> segment) {
        final long visibleCount = countVisibleEntries(segment);
        if (visibleCount < 2L) {
            return null;
        }
        final long targetLowerCount = Math.max(1L, visibleCount / 2L);
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentResult<EntryIterator<K, V>> result = segment
                    .openIterator(SegmentIteratorIsolation.FAIL_FAST);
            if (result.getStatus() == SegmentResultStatus.OK
                    && result.getValue() != null) {
                try (EntryIterator<K, V> iterator = result.getValue()) {
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
                    return new SplitBoundary<>(minKey, maxLowerKey,
                            visibleCount);
                }
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
                    "Segment '%s' failed to open iterator for route split: %s",
                    segment.getId(), result.getStatus()));
        }
    }

    private long countVisibleEntries(final Segment<K, V> segment) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentResult<EntryIterator<K, V>> result = segment
                    .openIterator(SegmentIteratorIsolation.FAIL_FAST);
            if (result.getStatus() == SegmentResultStatus.OK
                    && result.getValue() != null) {
                try (EntryIterator<K, V> iterator = result.getValue()) {
                    long count = 0L;
                    while (iterator.hasNext()) {
                        iterator.next();
                        count++;
                    }
                    return count;
                }
            }
            if (result.getStatus() == SegmentResultStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, "openIterator",
                        segment.getId());
                continue;
            }
            if (result.getStatus() == SegmentResultStatus.CLOSED) {
                return 0L;
            }
            throw new IndexException(String.format(
                    "Segment '%s' failed to count entries for route split: %s",
                    segment.getId(), result.getStatus()));
        }
    }

    private boolean applySplitPlan(final SegmentSplitApplyPlan<K> plan) {
        Vldtn.requireNonNull(plan, "plan");
        try {
            if (!keyToSegmentMap.applySplitPlan(plan)) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Route split map apply returned false: oldSegmentId='{}' lowerSegmentId='{}' upperSegmentId='{}'",
                            plan.getOldSegmentId(), plan.getLowerSegmentId(),
                            plan.getUpperSegmentId().orElse(null));
                }
                return false;
            }
            partitionRuntime.reassignOverlayAfterSplit(plan);
            partitionRuntime.registerPendingStableSplit(plan);
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

    private SegmentSplitterPlan<K, V> buildPlan(final Segment<K, V> segment) {
        return SegmentSplitterPlan
                .fromPolicy(new SegmentSplitterPolicy(segment
                        .getNumberOfKeysInCache()));
    }

    private boolean isEligibleForSplit(final Segment<K, V> segment,
            final SegmentSplitterPlan<K, V> plan,
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
