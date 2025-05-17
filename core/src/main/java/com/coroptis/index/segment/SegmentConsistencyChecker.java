package com.coroptis.index.segment;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.ContextAwareLogger;
import com.coroptis.index.IndexException;
import com.coroptis.index.LoggingContext;
import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;

public class SegmentConsistencyChecker<K, V> {

    private final ContextAwareLogger logger;
    private final Segment<K, V> segment;
    private final Comparator<K> keyComparator;

    SegmentConsistencyChecker(final LoggingContext loggingContext,
            final Segment<K, V> segment, final Comparator<K> keyComparator) {
        this.logger = new ContextAwareLogger(SegmentConsistencyChecker.class,
                loggingContext);
        this.segment = Objects.requireNonNull(segment);
        this.keyComparator = Objects.requireNonNull(keyComparator);
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
