package org.hestiastore.index.segmentindex;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentFiles;
import org.hestiastore.index.segment.SegmentFilesRenamer;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentPropertiesManager;
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
public class SegmentSplitCoordinator<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final IndexConfiguration<K, V> conf;
    private final KeySegmentCache<K> keySegmentCache;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final SegmentFilesRenamer filesRenamer = new SegmentFilesRenamer();

    SegmentSplitCoordinator(final IndexConfiguration<K, V> conf,
            final KeySegmentCache<K> keySegmentCache,
            final SegmentRegistry<K, V> segmentRegistry) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.keySegmentCache = Vldtn.requireNonNull(keySegmentCache,
                "keySegmentCache");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
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
        SegmentSplitterPolicy<K, V> policy = createPolicy(segment);
        SegmentSplitterPlan<K, V> plan = SegmentSplitterPlan.fromPolicy(policy);
        if (plan.getEstimatedNumberOfKeys() < maxNumberOfKeysInSegment) {
            return false;
        }
        final boolean compactBeforeSplit = policy
                .shouldBeCompactedBeforeSplitting(maxNumberOfKeysInSegment,
                        plan.getEstimatedNumberOfKeys());
        if (compactBeforeSplit) {
            compactSegment(segment);
            policy = createPolicy(segment);
            plan = SegmentSplitterPlan.fromPolicy(policy);
            if (plan.getEstimatedNumberOfKeys() < maxNumberOfKeysInSegment) {
                return true;
            }
        } else if (!shouldBeSplit(segment, maxNumberOfKeysInSegment)) {
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
        final SegmentId lowerSegmentId = keySegmentCache.findNewSegmentId();
        final SegmentId upperSegmentId = keySegmentCache.findNewSegmentId();
        final SegmentWriterTxFactory<K, V> writerTxFactory = id -> segmentRegistry
                .newSegmentBuilder(id).openWriterTx();
        final SegmentSplitter<K, V> splitter = new SegmentSplitter<>(segment,
                writerTxFactory);
        final SegmentSplitter.SplitExecution<K, V> execution = splitter
                .splitWithIterator(lowerSegmentId, upperSegmentId, plan);
        try {
            final SplitOutcome outcome = doSplit(segment, segmentId,
                    upperSegmentId, execution.getResult());
            if (outcome != null) {
                if (outcome.evictSegmentId != null) {
                    segmentRegistry.evictSegmentIfSame(outcome.evictSegmentId,
                            segment);
                }
                if (outcome.removeSegmentId != null) {
                    segmentRegistry.removeSegment(outcome.removeSegmentId);
                }
                return outcome.splitApplied;
            }
            return false;
        } finally {
            execution.close();
        }
    }

    private void compactSegment(final Segment<K, V> segment) {
        while (true) {
            final SegmentResult<?> result = segment.compact();
            final SegmentResultStatus status = result.getStatus();
            if (status == SegmentResultStatus.OK
                    || status == SegmentResultStatus.CLOSED) {
                return;
            }
            if (status == SegmentResultStatus.BUSY) {
                continue;
            }
            throw new org.hestiastore.index.IndexException(String.format(
                    "Segment '%s' failed during compact: %s", segment.getId(),
                    status));
        }
    }

    private SplitOutcome doSplit(final Segment<K, V> segment,
            final SegmentId segmentId, final SegmentId upperSegmentId,
            final SegmentSplitterResult<K, V> result) {
        if (result.isSplit()) {
            replaceCurrentWithSegment(segmentId, upperSegmentId);
            keySegmentCache.insertSegment(result.getMaxKey(),
                    result.getSegmentId());
            keySegmentCache.optionalyFlush();
            logger.debug("Splitting of segment '{}' to '{}' is done.",
                    segmentId, result.getSegmentId());
            final SplitOutcome outcome = new SplitOutcome(true, segmentId,
                    null);
            if (!segment.wasClosed()) {
                segment.close();
            }
            return outcome;
        } else {
            replaceCurrentWithSegment(segmentId, result.getSegmentId());
            keySegmentCache.updateSegmentMaxKey(segmentId, result.getMaxKey());
            keySegmentCache.optionalyFlush();
            logger.debug(
                    "Splitting of segment '{}' is done, "
                            + "but at the end it was compacting.",
                    segmentId, result.getSegmentId());
            final SplitOutcome outcome = new SplitOutcome(true, segmentId,
                    result.getSegmentId());
            if (!segment.wasClosed()) {
                segment.close();
            }
            return outcome;
        }
    }

    private static final class SplitOutcome {
        private final boolean splitApplied;
        private final SegmentId evictSegmentId;
        private final SegmentId removeSegmentId;

        private SplitOutcome(final boolean splitApplied,
                final SegmentId evictSegmentId,
                final SegmentId removeSegmentId) {
            this.splitApplied = splitApplied;
            this.evictSegmentId = evictSegmentId;
            this.removeSegmentId = removeSegmentId;
        }
    }

    private SegmentSplitterPolicy<K, V> createPolicy(
            final Segment<K, V> segment) {
        final long estimatedNumberOfKeys = segment.getNumberOfKeysInCache();
        return new SegmentSplitterPolicy<>(estimatedNumberOfKeys, false);
    }

    private boolean hasLiveEntries(final Segment<K, V> segment) {
        while (true) {
            final SegmentResult<EntryIterator<K, V>> result = segment
                    .openIterator(SegmentIteratorIsolation.FULL_ISOLATION);
            if (result.getStatus() == SegmentResultStatus.OK) {
                try (EntryIterator<K, V> iterator = result.getValue()) {
                    return iterator.hasNext();
                }
            }
            if (result.getStatus() == SegmentResultStatus.BUSY) {
                continue;
            }
            throw new org.hestiastore.index.IndexException(String.format(
                    "Segment '%s' failed to open iterator: %s",
                    segment.getId(), result.getStatus()));
        }
    }

    private void replaceCurrentWithSegment(final SegmentId segmentId,
            final SegmentId replacementSegmentId) {
        segmentRegistry.executeWithRegistryLock(() -> {
            final SegmentPropertiesManager currentProperties = segmentRegistry
                    .newSegmentPropertiesManager(segmentId);
            final SegmentFiles<K, V> currentFiles = segmentRegistry
                    .newSegmentFiles(segmentId);
            currentFiles.deleteAllFiles(currentProperties);

            final SegmentFiles<K, V> replacementFiles = segmentRegistry
                    .newSegmentFiles(replacementSegmentId);
            final SegmentPropertiesManager replacementProperties = segmentRegistry
                    .newSegmentPropertiesManager(replacementSegmentId);
            filesRenamer.renameFiles(replacementFiles, currentFiles,
                    replacementProperties);
        });
    }
}
