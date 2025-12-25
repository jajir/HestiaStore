package org.hestiastore.index.segmentindex;

import java.util.Comparator;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentSynchronizationAdapter;
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
    private static final String ERROR_MSG = "Index is broken. "
            + "File 'index.map' containing information about segments is corrupted. ";
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final SegmentRegistry<K, V> segmentRegistry;
    private final KeySegmentCache<K> keySegmentCache;
    private final Comparator<K> keyComparator;

    IndexConsistencyChecker(final KeySegmentCache<K> keySegmentCache,
            final SegmentRegistry<K, V> segmentRegistry,
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.keySegmentCache = Vldtn.requireNonNull(keySegmentCache,
                "keySegmentCache");
        Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        this.keyComparator = keyTypeDescriptor.getComparator();
    }

    public void checkAndRepairConsistency() {
        keySegmentCache.getSegmentsAsStream().forEach(segmentPair -> {
            final K segmentKey = segmentPair.getKey();
            if (segmentKey == null) {
                throw new IndexException(ERROR_MSG + "Segment key is null.");
            }
            final SegmentId segmentId = segmentPair.getValue();
            if (segmentId == null) {
                throw new IndexException(ERROR_MSG + "Segment id is null.");
            }
            logger.debug("checking segment '{}'.", segmentId);
            final Segment<K, V> segment = segmentRegistry.getSegment(segmentId);
            if (segment == null) {
                throw new IndexException(String.format(
                        ERROR_MSG + "Segment '%s' is not found in index.",
                        segmentId));
            }
            final K maxKey = segment.checkAndRepairConsistency();
            if (maxKey == null) {
                removeEmptySegment(segmentId, segment);
                return;
            }
            if (keyComparator.compare(segmentKey, maxKey) < 0) {
                throw new IndexException(String.format(ERROR_MSG
                        + "Segment '%s' has a max key of '%s', "
                        + "which is less than the max key '%s' from the index data.",
                        segmentId, segmentKey, maxKey));
            }
            logger.debug("Checking segment '{}' id done.", segmentId);
        });
    }

    private void removeEmptySegment(final SegmentId segmentId,
            final Segment<K, V> segment) {
        if (segment instanceof SegmentSynchronizationAdapter<K, V> adapter) {
            adapter.executeWithWriteLock(() -> {
                final K maxKey = adapter.checkAndRepairConsistency();
                if (maxKey != null) {
                    return null;
                }
                logger.warn(
                        "Segment '{}' is empty. Removing it from index map.",
                        segmentId);
                keySegmentCache.removeSegment(segmentId);
                keySegmentCache.optionalyFlush();
                segmentRegistry.removeSegment(segmentId);
                return null;
            });
        } else {
            logger.warn("Segment '{}' is empty. Removing it from index map.",
                    segmentId);
            keySegmentCache.removeSegment(segmentId);
            keySegmentCache.optionalyFlush();
            segmentRegistry.removeSegment(segmentId);
        }
    }

}
