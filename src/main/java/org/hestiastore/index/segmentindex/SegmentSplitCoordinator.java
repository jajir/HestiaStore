package org.hestiastore.index.segmentindex;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
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
    private final IndexRetryPolicy retryPolicy;

    SegmentSplitCoordinator(final IndexConfiguration<K, V> conf,
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentRegistryImpl<K, V> segmentRegistry) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
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
        final SegmentSplitterPolicy<K, V> policy = createPolicy(segment);
        final SegmentSplitterPlan<K, V> plan = SegmentSplitterPlan
                .fromPolicy(policy);
        if (plan.getEstimatedNumberOfKeys() < maxNumberOfKeysInSegment) {
            return false;
        }
        if (!shouldBeSplit(segment, maxNumberOfKeysInSegment)) {
            return false;
        }
        if (!hasLiveEntries(segment)) {
            return false;
        }
        if (!plan.isSplitFeasible()) {
            return false;
        }
        split(segment, plan);
        return true;
    }

    boolean shouldBeSplit(final Segment<K, V> segment,
            final long maxNumberOfKeysInSegment) {
        return segment.getNumberOfKeysInCache() >= maxNumberOfKeysInSegment;
    }

    private boolean split(final Segment<K, V> segment,
            final SegmentSplitterPlan<K, V> plan) {
        final SegmentId segmentId = segment.getId();
        logger.debug("Splitting of '{}' started.", segmentId);
        if (!segmentRegistry.isSegmentInstance(segmentId, segment)) {
            return false;
        }
        final long mapVersion = keyToSegmentMap.snapshot().version();
        final SegmentId lowerSegmentId = keyToSegmentMap.findNewSegmentId();
        final SegmentId upperSegmentId = keyToSegmentMap.findNewSegmentId();
        final SegmentWriterTxFactory<K, V> writerTxFactory = id -> segmentRegistry
                .newSegmentBuilder(id).openWriterTx();
        final SegmentSplitter<K, V> splitter = new SegmentSplitter<>(segment,
                writerTxFactory);
        final SegmentSplitApplyPlan<K, V> applyPlan;
        final SegmentRegistryResult<Segment<K, V>> applyResult;
        final Segment<K, V> removed;
        SegmentSplitter.SplitExecution<K, V> execution = splitter
                .splitWithIterator(lowerSegmentId, upperSegmentId, plan);
        try {
            applyPlan = toApplyPlan(
                    segmentId, upperSegmentId, execution.getResult());
            if (keyToSegmentMap.snapshot().version() != mapVersion) {
                deleteSplitSegments(lowerSegmentId, upperSegmentId);
                return false;
            }
            applyResult = applySplitPlan(applyPlan);
            if (!applyResult.isOk()) {
                deleteSplitSegments(lowerSegmentId, upperSegmentId);
                return false;
            }
            removed = applyResult.getValue();
        } finally {
            execution.close();
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

    private boolean hasLiveEntries(final Segment<K, V> segment) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentResult<EntryIterator<K, V>> result = segment
                    .openIterator(SegmentIteratorIsolation.FULL_ISOLATION);
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
            throw new IndexException(String.format(
                    "Segment '%s' failed to open iterator: %s",
                    segment.getId(), result.getStatus()));
        }
    }

    SegmentRegistryResult<Segment<K, V>> applySplitPlan(
            final SegmentSplitApplyPlan<K, V> plan) {
        Vldtn.requireNonNull(plan, "plan");
        return keyToSegmentMap.withWriteLock(() -> {
            final SegmentRegistryResult<Segment<K, V>> applyResult = segmentRegistry
                    .applySplitPlan(plan, null, null,
                            () -> keyToSegmentMap.applySplitPlan(plan));
            if (!applyResult.isOk()) {
                return applyResult;
            }
            keyToSegmentMap.optionalyFlush();
            return applyResult;
        });
    }
}
