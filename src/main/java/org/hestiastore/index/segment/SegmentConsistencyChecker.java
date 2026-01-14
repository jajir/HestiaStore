package org.hestiastore.index.segment;

import java.util.Comparator;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SegmentConsistencyChecker<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Segment<K, V> segment;
    private final Comparator<K> keyComparator;

    SegmentConsistencyChecker(final Segment<K, V> segment,
            final Comparator<K> keyComparator) {
        this.segment = Vldtn.requireNonNull(segment, "segment");
        this.keyComparator = Vldtn.requireNonNull(keyComparator,
                "keyComparator");
    }

    /**
     * Checks the consistency of the segment by ensuring that keys are strictly
     * increasing. If an inconsistency is found, throws IndexException.
     * 
     * @return the last key in the segment if no inconsistencies are found
     * @throws IndexException if keys are not in strictly increasing order
     */
    public K checkAndRepairConsistency() {
        logger.debug("Checking segment '{}'", segment.getId());
        K previousKey = null;
        final SegmentResult<EntryIterator<K, V>> iteratorResult = segment
                .openIterator();
        if (!iteratorResult.isOk()) {
            throw new IndexException(String.format(
                    "Segment '%s' is not ready for consistency check: %s",
                    segment.getId(), iteratorResult.getStatus()));
        }
        try (EntryIterator<K, V> iterator = iteratorResult.getValue()) {
            while (iterator.hasNext()) {
                final Entry<K, V> entry = iterator.next();
                if (previousKey == null) {
                    previousKey = entry.getKey();
                    continue;
                }
                if (keyComparator.compare(previousKey, entry.getKey()) >= 0) {
                    throw new IndexException(String.format(
                            "Keys in segment '%s' are not sorted. "
                                    + "Key '%s' have to higher than key '%s'.",
                            segment.getId(), entry.getKey(), previousKey));
                }
                previousKey = entry.getKey();
            }
        }
        if (previousKey == null) {
            logger.warn("Segment '{}' is empty.", segment.getId());
            return null;
        }
        return previousKey;
    }

}
