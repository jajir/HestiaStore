package org.hestiastore.index.sst;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentSplitter;
import org.hestiastore.index.segment.SegmentSplitterPlan;
import org.hestiastore.index.segment.SegmentSplitterPolicy;
import org.hestiastore.index.segment.SegmentSplitterResult;
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

    SegmentSplitCoordinator(final IndexConfiguration<K, V> conf,
            final KeySegmentCache<K> keySegmentCache) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.keySegmentCache = Vldtn.requireNonNull(keySegmentCache,
                "keySegmentCache");
    }

    /**
     * If number of keys reach threshold split segment into two.
     * 
     * @param segment required simple data file
     * @return
     */
    public void optionallySplit(final Segment<K, V> segment) {
        Vldtn.requireNonNull(segment, "segment");
        final SegmentSplitter<K, V> segmentSplitter = segment.getSegmentSplitter();
        final SegmentSplitterPolicy<K, V> policy = segment.getSegmentSplitterPolicy();
        final long maxNumberOfKeysInSegment = conf.getMaxNumberOfKeysInSegment();
        SegmentSplitterPlan<K, V> plan = SegmentSplitterPlan.fromPolicy(policy);
        if (plan.getEstimatedNumberOfKeys() < maxNumberOfKeysInSegment) {
            return;
        }
        if (policy.shouldBeCompactedBeforeSplitting(maxNumberOfKeysInSegment,
                plan.getEstimatedNumberOfKeys())) {
            segment.forceCompact();
            if (!shouldBeSplit(segment)) {
                return;
            }
            plan = SegmentSplitterPlan.fromPolicy(policy);
            if (plan.getEstimatedNumberOfKeys() < maxNumberOfKeysInSegment) {
                return;
            }
        } else if (!shouldBeSplit(segment)) {
            return;
        }
        split(segment, segmentSplitter, plan);
    }

    boolean shouldBeSplit(final Segment<K, V> segment) {
        return segment.getNumberOfKeys() >= conf.getMaxNumberOfKeysInSegment();
    }

    private boolean split(final Segment<K, V> segment,
            final SegmentSplitter<K, V> segmentSplitter,
            final SegmentSplitterPlan<K, V> plan) {
        final SegmentId segmentId = segment.getId();
        logger.debug("Splitting of '{}' started.", segmentId);
        final SegmentId newSegmentId = keySegmentCache.findNewSegmentId();
        final SegmentSplitterResult<K, V> result = segmentSplitter
                .split(newSegmentId, plan);
        if (result.isSplit()) {
            keySegmentCache.insertSegment(result.getMaxKey(), newSegmentId);
            logger.debug("Splitting of segment '{}' to '{}' is done.",
                    segmentId, newSegmentId);
        } else {
            logger.debug(
                    "Splitting of segment '{}' is done, "
                            + "but at the end it was compacting.",
                    segmentId, newSegmentId);
        }
        return true;
    }
}
