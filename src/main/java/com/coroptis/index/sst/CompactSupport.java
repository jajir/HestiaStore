package com.coroptis.index.sst;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.segment.Segment;
import com.coroptis.index.segment.SegmentId;
import com.coroptis.index.segment.SegmentSearcher;

public class CompactSupport<K, V> {

    private final Logger logger = LoggerFactory.getLogger(CompactSupport.class);

    private final List<Pair<K, V>> toSameSegment = new ArrayList<>();
    private final KeySegmentCache<K> keySegmentCache;
    private final SegmentManager<K, V> segmentManager;
    private final SegmentSearcherCache<K, V> segmentSearcherCache;
    private SegmentId currentSegmentId = null;

    /**
     * List of segment's ids eligible for compacting.
     */
    private List<SegmentId> eligibleSegments = new ArrayList<>();

    CompactSupport(final SegmentManager<K, V> segmentManager,
            final KeySegmentCache<K> keySegmentCache,
            final SegmentSearcherCache<K, V> segmentSearcherCache) {
        this.segmentManager = Objects.requireNonNull(segmentManager);
        this.keySegmentCache = Objects.requireNonNull(keySegmentCache);
        this.segmentSearcherCache = Objects
                .requireNonNull(segmentSearcherCache);
    }

    public void compact(final Pair<K, V> pair) {
        Objects.requireNonNull(pair);
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
            return;
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
        logger.debug("Flushing '{}' key value pairs into segment '{}'.",
                toSameSegment.size(), currentSegmentId);
        final Optional<SegmentSearcher<K, V>> oSegmenSearcher = segmentSearcherCache
                .getOptionalSegmenSearcher(currentSegmentId);
        final SegmentSearcher<K, V> segmentSearcher = oSegmenSearcher
                .orElse(null);
        final Segment<K, V> segment = segmentManager
                .getSegment(currentSegmentId);
        try (final PairWriter<K, V> writer = segment
                .openWriter(segmentSearcher)) {
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
