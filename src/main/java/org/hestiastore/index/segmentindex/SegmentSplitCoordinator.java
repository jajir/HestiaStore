package org.hestiastore.index.segmentindex;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.hestiastore.index.segmentregistry.SegmentHandlerLockStatus;
import org.hestiastore.index.segmentregistry.SegmentRegistryImpl;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinator for splitting segments based on the number of keys.
 * 
 * @param <K> Key type
 * @param <V> Value type
 */
class SegmentSplitCoordinator<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final IndexConfiguration<K, V> conf;
    private final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap;
    private final SegmentRegistryImpl<K, V> segmentRegistry;
    private final SegmentIndexSplitPolicy<K, V> splitPolicy;
    private final IndexRetryPolicy retryPolicy;
    private static final boolean DEBUG_SPLIT_LOSS = Boolean
            .getBoolean("hestiastore.debugSplitLoss");

    SegmentSplitCoordinator(final IndexConfiguration<K, V> conf,
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentRegistryImpl<K, V> segmentRegistry) {
        this(conf, keyToSegmentMap, segmentRegistry,
                new SegmentIndexSplitPolicyThreshold<>());
    }

    SegmentSplitCoordinator(final IndexConfiguration<K, V> conf,
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentRegistryImpl<K, V> segmentRegistry,
            final SegmentIndexSplitPolicy<K, V> splitPolicy) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.splitPolicy = Vldtn.requireNonNull(splitPolicy, "splitPolicy");
        this.retryPolicy = new IndexRetryPolicy(
                conf.getIndexBusyBackoffMillis(),
                conf.getIndexBusyTimeoutMillis());
    }

    /**
     * If number of keys reach threshold split segment into two.
     * 
     * @param segment required simple data file
     * @return
     */
    boolean optionallySplit(final Segment<K, V> segment) {
        return optionallySplit(segment, conf.getMaxNumberOfKeysInSegment());
    }

    boolean optionallySplit(final Segment<K, V> segment,
            final long maxNumberOfKeysInSegment) {
        Vldtn.requireNonNull(segment, "segment");
        final SegmentSplitterPlan<K, V> plan = buildPlan(segment);
        if (!isEligibleForSplit(segment, plan, maxNumberOfKeysInSegment)) {
            return false;
        }
        return splitWithLock(segment, maxNumberOfKeysInSegment);
    }

    boolean shouldBeSplit(final Segment<K, V> segment,
            final long maxNumberOfKeysInSegment) {
        return splitPolicy.shouldSplit(segment, maxNumberOfKeysInSegment);
    }

    private boolean splitWithLock(final Segment<K, V> segment,
            final long maxNumberOfKeysInSegment) {
        final SegmentId segmentId = segment.getId();
        logger.debug("Splitting of '{}' started.", segmentId);
        if (!segmentRegistry.isSegmentInstance(segmentId, segment)) {
            return false;
        }
        final SegmentHandlerLockStatus lockStatus = segmentRegistry
                .lockSegmentHandler(segmentId, segment);
        if (lockStatus != SegmentHandlerLockStatus.OK) {
            return false;
        }
        try {
            final SegmentSplitterPlan<K, V> plan = buildPlan(segment);
            if (!isEligibleForSplit(segment, plan, maxNumberOfKeysInSegment)) {
                return false;
            }
            if (!hasLiveEntries(segment)) {
                return false;
            }
            return splitLocked(segment, plan);
        } finally {
            segmentRegistry.unlockSegmentHandler(segmentId, segment);
        }
    }

    private boolean splitLocked(final Segment<K, V> segment,
            final SegmentSplitterPlan<K, V> plan) {
        final SegmentId segmentId = segment.getId();
        final SegmentId lowerSegmentId = keyToSegmentMap.findNewSegmentId();
        final SegmentId upperSegmentId = keyToSegmentMap.findNewSegmentId();
        final SegmentWriterTxFactory<K, V> writerTxFactory = id -> segmentRegistry
                .newSegmentBuilder(id).openWriterTx();
        final SegmentSplitter<K, V> splitter = new SegmentSplitter<>(segment,
                writerTxFactory);
        final SegmentSplitApplyPlan<K, V> applyPlan;
        final SegmentRegistryResult<Segment<K, V>> applyResult;
        final Segment<K, V> removed;
        try (SegmentSplitter.SplitExecution<K, V> execution = splitter
                .splitWithIterator(lowerSegmentId, upperSegmentId, plan)) {
            applyPlan = toApplyPlan(segmentId, upperSegmentId,
                    execution.getResult());
            applyResult = applySplitPlan(applyPlan);
            if (!applyResult.isOk()) {
                if (DEBUG_SPLIT_LOSS) {
                    logger.warn(
                            "Split debug: apply failed for segment '{}', status '{}'.",
                            segmentId, applyResult.getStatus());
                }
                deleteSplitSegments(lowerSegmentId, upperSegmentId);
                return false;
            }
            removed = applyResult.getValue();
        } catch (final RuntimeException e) {
            deleteSplitSegments(lowerSegmentId, upperSegmentId);
            throw e;
        }
        if (removed != null) {
            segmentRegistry.closeSegmentInstance(removed);
        }
        segmentRegistry.deleteSegmentFiles(applyPlan.getOldSegmentId());
        return true;
    }

    private void deleteSplitSegments(final SegmentId lowerSegmentId,
            final SegmentId upperSegmentId) {
        segmentRegistry.deleteSegmentFiles(lowerSegmentId);
        if (upperSegmentId != null) {
            segmentRegistry.deleteSegmentFiles(upperSegmentId);
        }
    }

    static <K, V> SegmentSplitApplyPlan<K, V> toApplyPlan(
            final SegmentId oldSegmentId, final SegmentId upperSegmentId,
            final SegmentSplitterResult<K, V> result) {
        Vldtn.requireNonNull(oldSegmentId, "oldSegmentId");
        Vldtn.requireNonNull(result, "result");
        final SegmentId resolvedUpperId;
        if (result.isSplit()) {
            resolvedUpperId = Vldtn.requireNonNull(upperSegmentId,
                    "upperSegmentId");
        } else {
            resolvedUpperId = null;
        }
        return new SegmentSplitApplyPlan<>(oldSegmentId, result.getSegmentId(),
                resolvedUpperId, result.getMinKey(), result.getMaxKey(),
                result.getStatus());
    }

    private SegmentSplitterPolicy<K, V> createPolicy(
            final Segment<K, V> segment) {
        final long estimatedNumberOfKeys = segment.getNumberOfKeysInCache();
        return new SegmentSplitterPolicy<>(estimatedNumberOfKeys);
    }

    private SegmentSplitterPlan<K, V> buildPlan(final Segment<K, V> segment) {
        final SegmentSplitterPolicy<K, V> policy = createPolicy(segment);
        return SegmentSplitterPlan.fromPolicy(policy);
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

    private boolean hasLiveEntries(final Segment<K, V> segment) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentResult<EntryIterator<K, V>> result = segment
                    .openIterator(SegmentIteratorIsolation.FAIL_FAST);
            if (result.getStatus() == SegmentResultStatus.OK) {
                try (EntryIterator<K, V> iterator = result.getValue()) {
                    return iterator.hasNext();
                }
            }
            if (result.getStatus() == SegmentResultStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, "openIterator",
                        segment.getId());
                continue;
            }
            throw new IndexException(
                    String.format("Segment '%s' failed to open iterator: %s",
                            segment.getId(), result.getStatus()));
        }
    }

    SegmentRegistryResult<Segment<K, V>> applySplitPlan(
            final SegmentSplitApplyPlan<K, V> plan) {
        Vldtn.requireNonNull(plan, "plan");
        return segmentRegistry.applySplitPlan(plan, null, null, () -> {
            if (!keyToSegmentMap.applySplitPlan(plan)) {
                return false;
            }
            keyToSegmentMap.optionalyFlush();
            return true;
        });
    }
}
