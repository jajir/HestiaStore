package org.hestiastore.index.segment;

import java.util.Comparator;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.OperationResult;
import org.hestiastore.index.Vldtn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates ordering consistency of segment entries.
 *
 * @param <K> key type
 * @param <V> value type
 */
class SegmentConsistencyChecker<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Segment<K, V> segment;
    private final Comparator<K> keyComparator;

    /**
     * Creates a checker for the given segment and key comparator.
     *
     * @param segment segment to validate
     * @param keyComparator comparator for key ordering
     */
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
        final OperationResult<K> result = tryCheckAndRepairConsistency();
        if (!result.isOk()) {
            throw new IndexException(String.format(
                    "Segment '%s' is not ready for consistency check: %s",
                    segment.getId(), result.getStatus()));
        }
        return result.getValue();
    }

    /**
     * Attempts a complete consistency scan under full segment isolation.
     *
     * @return successful result containing the last key, or the segment status
     *         when exclusive access cannot be acquired
     */
    OperationResult<K> tryCheckAndRepairConsistency() {
        logger.debug("Checking segment '{}'", segment.getId());
        final OperationResult<EntryIterator<K, V>> iteratorResult = segment
                .openIterator(SegmentIteratorIsolation.FULL_ISOLATION);
        if (!iteratorResult.isOk()) {
            return OperationResult.fromStatus(iteratorResult.getStatus());
        }
        try (EntryIterator<K, V> iterator = iteratorResult.getValue()) {
            return OperationResult.ok(checkEntries(iterator));
        }
    }

    private K checkEntries(final EntryIterator<K, V> iterator) {
        K previousKey = null;
        while (iterator.hasNext()) {
            final Entry<K, V> entry = iterator.next();
            if (previousKey != null
                    && keyComparator.compare(previousKey, entry.getKey()) >= 0) {
                throw new IndexException(String.format(
                        "Keys in segment '%s' are not sorted. "
                                + "Key '%s' have to higher than key '%s'.",
                        segment.getId(), entry.getKey(), previousKey));
            }
            previousKey = entry.getKey();
        }
        if (previousKey == null) {
            logger.warn("Segment '{}' is empty.", segment.getId());
        }
        return previousKey;
    }

}
