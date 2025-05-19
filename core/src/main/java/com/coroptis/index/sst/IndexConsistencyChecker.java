package com.coroptis.index.sst;

import java.util.Comparator;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.IndexException;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.segment.Segment;
import com.coroptis.index.segment.SegmentId;

/**
 * Iterate through all segments in index. It verify data describing structures
 * and data itself are consistent.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class IndexConsistencyChecker<K, V> {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final SegmentManager<K, V> segmentManager;
    private final KeySegmentCache<K> keySegmentCache;
    private final Comparator<K> keyComparator;

    IndexConsistencyChecker(final KeySegmentCache<K> keySegmentCache,
            final SegmentManager<K, V> segmentManager,
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.segmentManager = Objects.requireNonNull(segmentManager);
        this.keySegmentCache = Objects.requireNonNull(keySegmentCache);
        Objects.requireNonNull(keyTypeDescriptor);
        this.keyComparator = keyTypeDescriptor.getComparator();
    }

    public void checkAndRepairConsistency() {
        keySegmentCache.getSegmentsAsStream().forEach(segmentPair -> {
            final K segmentKey = segmentPair.getKey();
            final SegmentId segmentId = segmentPair.getValue();
            final Segment<K, V> segment = segmentManager.getSegment(segmentId);
            if (segment == null) {
                throw new IndexException(String.format(
                        "Segment '%s' is not found in index.", segmentId));
            }
            final K maxKey = segment.checkAndRepairConsistency();
            if (keyComparator.compare(segmentKey, maxKey) != 0) {
                logger.error(
                        "Key '{}' of segment '{}' is not equal to max key '{}'.",
                        segmentKey, segmentId, maxKey);
            }
            logger.debug("Checking segment '{}'", segmentId);
        });
    }

}
