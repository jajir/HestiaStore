package org.hestiastore.index.segment;

import java.util.Comparator;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.Vldtn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentConsistencyChecker<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final SegmentImpl<K, V> segment;
    private final Comparator<K> keyComparator;

    SegmentConsistencyChecker(final SegmentImpl<K, V> segment,
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
        try (PairIterator<K, V> iterator = segment.openIterator()) {
            while (iterator.hasNext()) {
                final Pair<K, V> pair = iterator.next();
                if (previousKey == null) {
                    previousKey = pair.getKey();
                    continue;
                }
                if (keyComparator.compare(previousKey, pair.getKey()) >= 0) {
                    throw new IndexException(String.format(
                            "Keys in segment '%s' are not sorted. "
                                    + "Key '%s' have to higher than key '%s'.",
                            segment.getId(), pair.getKey(), previousKey));
                }
                previousKey = pair.getKey();
            }
        }
        if (previousKey == null) {
            logger.warn("Segment '{}' is empty.", segment.getId());
            return null;
        }
        return previousKey;
    }

}
