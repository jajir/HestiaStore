package org.hestiastore.index.sst;

import java.util.Comparator;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        this.segmentManager = Vldtn.requireNonNull(segmentManager,
                "segmentManager");
        this.keySegmentCache = Vldtn.requireNonNull(keySegmentCache,
                "keySegmentCache");
        Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
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
