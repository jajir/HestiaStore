package org.hestiastore.index.segmentindex.split;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.hestiastore.index.segment.SegmentState;
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
 * Split coordinator used by the partitioned ingest runtime.
 * <p>
 * Stable segment data are still rewritten into child segments, but overlay
 * state is reassigned to the new child routes as part of split apply.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class PartitionStableSplitCoordinator<K, V> {

    private static final String SEGMENT_ARG = "segment";
    private static final String SEGMENT_CLOSE_FAILED_FORMAT = "Segment '%s' failed during close: %s";
    private static final boolean DEBUG_SPLIT_LOSS = Boolean
            .getBoolean("hestiastore.debugSplitLoss");

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final IndexConfiguration<K, V> conf;
    private final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final SegmentWriterTxFactory<K, V> writerTxFactory;
    private final PartitionRuntime<K, V> partitionRuntime;
    private final SegmentIndexSplitPolicy<K, V> splitPolicy;
    private final IndexRetryPolicy retryPolicy;

    public PartitionStableSplitCoordinator(final IndexConfiguration<K, V> conf,
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final SegmentWriterTxFactory<K, V> writerTxFactory,
            final PartitionRuntime<K, V> partitionRuntime) {
        this(conf, keyToSegmentMap, segmentRegistry, writerTxFactory,
                partitionRuntime, new SegmentIndexSplitPolicyThreshold<>());
    }

    public PartitionStableSplitCoordinator(final IndexConfiguration<K, V> conf,
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final SegmentWriterTxFactory<K, V> writerTxFactory,
            final PartitionRuntime<K, V> partitionRuntime,
            final SegmentIndexSplitPolicy<K, V> splitPolicy) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.writerTxFactory = Vldtn.requireNonNull(writerTxFactory,
                "writerTxFactory");
        this.partitionRuntime = Vldtn.requireNonNull(partitionRuntime,
                "partitionRuntime");
        this.splitPolicy = Vldtn.requireNonNull(splitPolicy, "splitPolicy");
        this.retryPolicy = new IndexRetryPolicy(
                conf.getIndexBusyBackoffMillis(),
                conf.getIndexBusyTimeoutMillis());
    }

    public boolean optionallySplit(final Segment<K, V> segment,
            final long maxNumberOfKeysInSegment) {
        Vldtn.requireNonNull(segment, SEGMENT_ARG);
        final SegmentSplitterPlan<K, V> plan = buildPlan(segment);
        if (!isEligibleForSplit(segment, plan, maxNumberOfKeysInSegment)) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Partition split skipped: segment='{}' estimatedKeys='{}' maxKeysInSegment='{}' splitFeasible='{}'",
                        segment.getId(), plan.getEstimatedNumberOfKeys(),
                        maxNumberOfKeysInSegment, plan.isSplitFeasible());
            }
            return false;
        }
        return splitWithLock(segment, maxNumberOfKeysInSegment);
    }

    public boolean shouldBeSplit(final Segment<K, V> segment,
            final long maxNumberOfKeysInSegment) {
        return splitPolicy.shouldSplit(segment, maxNumberOfKeysInSegment);
    }

    private boolean splitWithLock(final Segment<K, V> segment,
            final long maxNumberOfKeysInSegment) {
        final SegmentId segmentId = segment.getId();
        logger.debug("Partition split started: segment='{}' threshold='{}'",
                segmentId, maxNumberOfKeysInSegment);
        final SegmentRegistryResult<Segment<K, V>> currentResult = segmentRegistry
                .getSegment(segmentId);
        if (currentResult.getStatus() != SegmentRegistryResultStatus.OK
                || currentResult.getValue() == null) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Partition split aborted before lock validation: segment='{}' registryStatus='{}'",
                        segmentId, currentResult.getStatus());
            }
            return false;
        }
        final Segment<K, V> current = currentResult.getValue();
        if (current != segment) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Partition split aborted because loaded segment changed: segment='{}'",
                        segmentId);
            }
            return false;
        }
        final SegmentSplitterPlan<K, V> plan = buildPlan(segment);
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Partition split lock validated: segment='{}' estimatedKeys='{}' threshold='{}'",
                    segmentId, plan.getEstimatedNumberOfKeys(),
                    maxNumberOfKeysInSegment);
        }
        final boolean split;
        if (isEligibleForSplit(segment, plan, maxNumberOfKeysInSegment)
                && hasLiveEntries(segment)) {
            split = splitLocked(segment, plan);
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Partition split aborted after eligibility/live-entry check: segment='{}' estimatedKeys='{}' threshold='{}'",
                        segmentId, plan.getEstimatedNumberOfKeys(),
                        maxNumberOfKeysInSegment);
            }
            split = false;
        }
        if (split) {
            final SegmentRegistryResult<Void> deleteResult = segmentRegistry
                    .deleteSegment(segmentId);
            if (deleteResult.getStatus() != SegmentRegistryResultStatus.OK
                    && deleteResult
                            .getStatus() != SegmentRegistryResultStatus.CLOSED) {
                logSplitCleanupFailure(segmentId, deleteResult.getStatus(),
                        segment, plan);
            }
        }
        return split;
    }

    private void logSplitCleanupFailure(final SegmentId segmentId,
            final SegmentRegistryResultStatus deleteStatus,
            final Segment<K, V> segment, final SegmentSplitterPlan<K, V> plan) {
        final SegmentState segmentState = segment.getState();
        final boolean mappedInKeyMap = keyToSegmentMap.getSegmentIds()
                .contains(segmentId);
        logger.warn(
                "Partition split cleanup: delete skipped for segment '{}' (deleteStatus='{}', segmentState='{}', mappedInKeyMap='{}', estimatedKeys='{}', index='{}'). BUSY usually means in-flight operations still reference the segment.",
                segmentId, deleteStatus, segmentState, mappedInKeyMap,
                plan.getEstimatedNumberOfKeys(), conf.getIndexName());
    }

    private boolean splitLocked(final Segment<K, V> segment,
            final SegmentSplitterPlan<K, V> plan) {
        final SegmentId segmentId = segment.getId();
        final SegmentId lowerSegmentId = allocateSegmentId();
        final SegmentId upperSegmentId = allocateSegmentId();
        if (lowerSegmentId == null || upperSegmentId == null) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Partition split aborted because new segment ids could not be allocated: segment='{}' lowerSegmentId='{}' upperSegmentId='{}'",
                        segmentId, lowerSegmentId, upperSegmentId);
            }
            return false;
        }
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Partition split ids allocated: segment='{}' lowerSegmentId='{}' upperSegmentId='{}' estimatedKeys='{}'",
                    segmentId, lowerSegmentId, upperSegmentId,
                    plan.getEstimatedNumberOfKeys());
        }
        final SegmentSplitter<K, V> splitter = new SegmentSplitter<>(segment,
                writerTxFactory, retryPolicy);
        final SegmentSplitApplyPlan<K> applyPlan;
        boolean applied = false;
        try (SegmentSplitter.SplitExecution<K, V> execution = splitter
                .splitWithIterator(lowerSegmentId, upperSegmentId, plan)) {
            applyPlan = SegmentSplitCoordinator.toApplyPlan(segmentId,
                    upperSegmentId, execution.getResult());
            if (!applySplitPlan(applyPlan, segment)) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Partition split apply failed: segment='{}' lowerSegmentId='{}' upperSegmentId='{}' status='{}'",
                            segmentId, applyPlan.getLowerSegmentId(),
                            applyPlan.getUpperSegmentId().orElse(null),
                            applyPlan.getStatus());
                }
                if (DEBUG_SPLIT_LOSS) {
                    logger.warn(
                            "Split debug: partition-aware apply failed for segment '{}'.",
                            segmentId);
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
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Partition split applied: segment='{}' lowerSegmentId='{}' upperSegmentId='{}' status='{}' minKey='{}' maxKey='{}'",
                        segmentId, applyPlan.getLowerSegmentId(),
                        applyPlan.getUpperSegmentId().orElse(null),
                        applyPlan.getStatus(), applyPlan.getMinKey(),
                        applyPlan.getMaxKey());
            }
            closeSegmentAfterSplit(segment);
        }
        return true;
    }

    private boolean applySplitPlan(final SegmentSplitApplyPlan<K> plan,
            final Segment<K, V> segment) {
        Vldtn.requireNonNull(plan, "plan");
        Vldtn.requireNonNull(segment, SEGMENT_ARG);
        try {
            if (!keyToSegmentMap.applySplitPlan(plan)) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Partition split map apply returned false: oldSegmentId='{}' lowerSegmentId='{}' upperSegmentId='{}' status='{}'",
                            plan.getOldSegmentId(), plan.getLowerSegmentId(),
                            plan.getUpperSegmentId().orElse(null),
                            plan.getStatus());
                }
                return false;
            }
            partitionRuntime.reassignOverlayAfterSplit(plan);
            keyToSegmentMap.optionalyFlush();
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Partition split map apply succeeded: oldSegmentId='{}' lowerSegmentId='{}' upperSegmentId='{}' status='{}'",
                        plan.getOldSegmentId(), plan.getLowerSegmentId(),
                        plan.getUpperSegmentId().orElse(null),
                        plan.getStatus());
            }
            return true;
        } catch (final RuntimeException e) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Partition split map apply failed with exception: oldSegmentId='{}' lowerSegmentId='{}' upperSegmentId='{}' status='{}'",
                        plan.getOldSegmentId(), plan.getLowerSegmentId(),
                        plan.getUpperSegmentId().orElse(null),
                        plan.getStatus(), e);
            }
            return false;
        }
    }

    private SegmentSplitterPolicy createPolicy(final Segment<K, V> segment) {
        final long estimatedNumberOfKeys = segment.getNumberOfKeysInCache();
        return new SegmentSplitterPolicy(estimatedNumberOfKeys);
    }

    private SegmentSplitterPlan<K, V> buildPlan(final Segment<K, V> segment) {
        final SegmentSplitterPolicy policy = createPolicy(segment);
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
            if (result.getStatus() == SegmentResultStatus.CLOSED) {
                return false;
            }
            throw new IndexException(
                    String.format("Segment '%s' failed to open iterator: %s",
                            segment.getId(), result.getStatus()));
        }
    }

    private void deleteSplitSegments(final SegmentId lowerSegmentId,
            final SegmentId upperSegmentId) {
        segmentRegistry.deleteSegment(lowerSegmentId);
        if (upperSegmentId != null) {
            segmentRegistry.deleteSegment(upperSegmentId);
        }
    }

    private void closeSegmentAfterSplit(final Segment<K, V> segment) {
        Vldtn.requireNonNull(segment, SEGMENT_ARG);
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentState state = segment.getState();
            if (state == SegmentState.CLOSED) {
                return;
            }
            if (state == SegmentState.ERROR) {
                throw new IndexException(
                        String.format(SEGMENT_CLOSE_FAILED_FORMAT,
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
                    String.format(SEGMENT_CLOSE_FAILED_FORMAT,
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
                        String.format(SEGMENT_CLOSE_FAILED_FORMAT,
                                segment.getId(), state));
            }
            retryPolicy.backoffOrThrow(startNanos, "close", segment.getId());
        }
    }

    private SegmentId allocateSegmentId() {
        final SegmentRegistryResult<SegmentId> result = segmentRegistry
                .allocateSegmentId();
        if (result.getStatus() != SegmentRegistryResultStatus.OK) {
            return null;
        }
        return result.getValue();
    }
}
