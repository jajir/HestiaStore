package org.hestiastore.index.segmentindex;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentregistry.SegmentHandlerLockStatus;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryFreeze;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.hestiastore.index.segmentregistry.SegmentRegistryResultStatus;
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
    private final SegmentRegistry<K, V> segmentRegistry;
    private final SegmentRegistryAccess<K, V> registryAccess;
    private final SegmentWriterTxFactory<K, V> writerTxFactory;
    private final SegmentIndexSplitPolicy<K, V> splitPolicy;
    private final IndexRetryPolicy retryPolicy;
    private static final boolean DEBUG_SPLIT_LOSS = Boolean
            .getBoolean("hestiastore.debugSplitLoss");

    SegmentSplitCoordinator(final IndexConfiguration<K, V> conf,
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final SegmentRegistryAccess<K, V> registryAccess,
            final SegmentWriterTxFactory<K, V> writerTxFactory) {
        this(conf, keyToSegmentMap, segmentRegistry, registryAccess,
                writerTxFactory,
                new SegmentIndexSplitPolicyThreshold<>());
    }

    SegmentSplitCoordinator(final IndexConfiguration<K, V> conf,
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final SegmentRegistryAccess<K, V> registryAccess,
            final SegmentWriterTxFactory<K, V> writerTxFactory,
            final SegmentIndexSplitPolicy<K, V> splitPolicy) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.registryAccess = Vldtn.requireNonNull(registryAccess,
                "registryAccess");
        this.writerTxFactory = Vldtn.requireNonNull(writerTxFactory,
                "writerTxFactory");
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
        if (!registryAccess.isSegmentInstance(segmentId, segment)) {
            return false;
        }
        final SegmentHandlerLockStatus lockStatus = registryAccess
                .lockSegmentHandler(segmentId, segment);
        if (lockStatus != SegmentHandlerLockStatus.OK) {
            return false;
        }
        boolean split = false;
        try {
            final SegmentSplitterPlan<K, V> plan = buildPlan(segment);
            if (isEligibleForSplit(segment, plan, maxNumberOfKeysInSegment)
                    && hasLiveEntries(segment)) {
                split = splitLocked(segment, plan);
            }
        } finally {
            registryAccess.unlockSegmentHandler(segmentId, segment);
        }
        if (split) {
            final SegmentRegistryResult<Void> deleteResult = registryAccess
                    .deleteSegmentFiles(segmentId);
            if (!deleteResult.isOk()) {
                logger.warn(
                        "Split cleanup: unable to delete segment files for '{}', status '{}'.",
                        segmentId, deleteResult.getStatus());
            }
        }
        return split;
    }

    private boolean splitLocked(final Segment<K, V> segment,
            final SegmentSplitterPlan<K, V> plan) {
        final SegmentId segmentId = segment.getId();
        final SegmentId lowerSegmentId = allocateSegmentId();
        final SegmentId upperSegmentId = allocateSegmentId();
        if (lowerSegmentId == null || upperSegmentId == null) {
            return false;
        }
        final SegmentSplitter<K, V> splitter = new SegmentSplitter<>(segment,
                writerTxFactory);
        final SegmentSplitApplyPlan<K, V> applyPlan;
        final SegmentRegistryResult<Void> applyResult;
        boolean applied = false;
        try (SegmentSplitter.SplitExecution<K, V> execution = splitter
                .splitWithIterator(lowerSegmentId, upperSegmentId, plan)) {
            applyPlan = toApplyPlan(segmentId, upperSegmentId,
                    execution.getResult());
            applyResult = applySplitPlan(applyPlan, segment);
            if (!applyResult.isOk()) {
                if (DEBUG_SPLIT_LOSS) {
                    logger.warn(
                            "Split debug: apply failed for segment '{}', status '{}'.",
                            segmentId, applyResult.getStatus());
                }
                deleteSplitSegments(lowerSegmentId, upperSegmentId);
                return false;
            }
            applied = true;
        } catch (final SegmentSplitAbortException e) {
            deleteSplitSegments(lowerSegmentId, upperSegmentId);
            return false;
        } catch (final RuntimeException e) {
            deleteSplitSegments(lowerSegmentId, upperSegmentId);
            throw e;
        }
        if (applied) {
            closeSegmentAfterSplit(segment);
        }
        return true;
    }

    private void deleteSplitSegments(final SegmentId lowerSegmentId,
            final SegmentId upperSegmentId) {
        segmentRegistry.deleteSegment(lowerSegmentId);
        if (upperSegmentId != null) {
            segmentRegistry.deleteSegment(upperSegmentId);
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

    SegmentRegistryResult<Void> applySplitPlan(
            final SegmentSplitApplyPlan<K, V> plan,
            final Segment<K, V> segment) {
        Vldtn.requireNonNull(plan, "plan");
        Vldtn.requireNonNull(segment, "segment");
        final SegmentRegistryResult<SegmentRegistryFreeze> freezeResult = registryAccess
                .tryEnterFreeze();
        if (freezeResult.getStatus() != SegmentRegistryResultStatus.OK) {
            if (freezeResult.getStatus() == SegmentRegistryResultStatus.CLOSED) {
                return SegmentRegistryResult.closed();
            }
            if (freezeResult.getStatus() == SegmentRegistryResultStatus.ERROR) {
                return SegmentRegistryResult.error();
            }
            return SegmentRegistryResult.busy();
        }
        try (SegmentRegistryFreeze guard = freezeResult.getValue()) {
            if (!keyToSegmentMap.applySplitPlan(plan)) {
                registryAccess.failRegistry();
                return SegmentRegistryResult.error();
            }
            keyToSegmentMap.optionalyFlush();
            final SegmentRegistryResult<Void> evictResult = registryAccess
                    .evictSegmentFromCache(plan.getOldSegmentId(), segment);
            if (!evictResult.isOk()) {
                registryAccess.failRegistry();
                return SegmentRegistryResult.error();
            }
            return SegmentRegistryResult.ok();
        } catch (final RuntimeException e) {
            registryAccess.failRegistry();
            return SegmentRegistryResult.error();
        }
    }

    private void closeSegmentAfterSplit(final Segment<K, V> segment) {
        Vldtn.requireNonNull(segment, "segment");
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentState state = segment.getState();
            if (state == SegmentState.CLOSED) {
                return;
            }
            if (state == SegmentState.ERROR) {
                throw new IndexException(
                        String.format("Segment '%s' failed during close: %s",
                                segment.getId(), state));
            }
            final SegmentResult<Void> result = segment.close();
            final SegmentResultStatus status = result.getStatus();
            if (status == SegmentResultStatus.OK) {
                awaitSegmentClosed(segment, startNanos);
                return;
            }
            if (status == SegmentResultStatus.CLOSED) {
                return;
            }
            if (status == SegmentResultStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, "close",
                        segment.getId());
                continue;
            }
            throw new IndexException(
                    String.format("Segment '%s' failed during close: %s",
                            segment.getId(), status));
        }
    }

    private void awaitSegmentClosed(final Segment<K, V> segment,
            final long startNanos) {
        while (true) {
            final SegmentState state = segment.getState();
            if (state == SegmentState.CLOSED) {
                return;
            }
            if (state == SegmentState.ERROR) {
                throw new IndexException(
                        String.format("Segment '%s' failed during close: %s",
                                segment.getId(), state));
            }
            retryPolicy.backoffOrThrow(startNanos, "close", segment.getId());
        }
    }

    private SegmentId allocateSegmentId() {
        final SegmentRegistryResult<SegmentId> result = segmentRegistry
                .allocateSegmentId();
        if (result.getStatus() == SegmentRegistryResultStatus.OK) {
            return result.getValue();
        }
        return null;
    }
}
