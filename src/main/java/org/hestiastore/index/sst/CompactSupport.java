package org.hestiastore.index.sst;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.F;
import org.hestiastore.index.Pair;
import org.hestiastore.index.PairWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactSupport<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final List<Pair<K, V>> toSameSegment = new ArrayList<>();
    private final KeySegmentCache<K> keySegmentCache;
    private final SegmentRegistry<K, V> segmentRegistry;
    private SegmentId currentSegmentId = null;

    /**
     * List of segment's ids eligible for compacting.
     */
    private List<SegmentId> eligibleSegments = new ArrayList<>();

    CompactSupport(final SegmentRegistry<K, V> segmentRegistry,
            final KeySegmentCache<K> keySegmentCache) {
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.keySegmentCache = Vldtn.requireNonNull(keySegmentCache,
                "keySegmentCache");
    }

    public void compact(final Pair<K, V> pair) {
        Vldtn.requireNonNull(pair, "pair");
        final K segmentKey = pair.getKey();
        final SegmentId segmentId = keySegmentCache
                .insertKeyToSegment(segmentKey);
        if (currentSegmentId == null) {
            currentSegmentId = segmentId;
            toSameSegment.add(pair);
            return;
        }
        if (currentSegmentId == segmentId) {
            toSameSegment.add(pair);
        } else {
            /* Write all keys to index and clean cache and set new pageId */
            flushToCurrentSegment();
            toSameSegment.add(pair);
            currentSegmentId = segmentId;
        }
    }

    public void compactRest() {
        if (currentSegmentId == null) {
            return;
        }
        flushToCurrentSegment();
        currentSegmentId = null;
    }

    private void flushToCurrentSegment() {
        if (logger.isDebugEnabled()) {
            logger.debug("Flushing '{}' key value pairs into segment '{}'.",
                    F.fmt(toSameSegment.size()), currentSegmentId);
        }
        final Segment<K, V> segment = segmentRegistry
                .getSegment(currentSegmentId);
        try (PairWriter<K, V> writer = segment.openWriter()) {
            toSameSegment.forEach(writer::put);
        }
        eligibleSegments.add(currentSegmentId);
        toSameSegment.clear();
        logger.debug("Flushing to segment '{}' was done.", currentSegmentId);
    }

    /**
     * After compacting all keys to appropriate segment it allows to obtain list
     * of that segment.
     * 
     * @return list of segment eligible form compacting
     */
    public List<SegmentId> getEligibleSegmentIds() {
        return eligibleSegments;
    }

}
