package org.hestiastore.index.segmentindex;

import java.util.Comparator;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.hestiastore.index.segmentregistry.SegmentHandler;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.hestiastore.index.segmentregistry.SegmentRegistryResultStatus;
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
class IndexConsistencyChecker<K, V> {
    private static final String ERROR_MSG = "Index is broken. "
            + "File 'index.map' containing information about segments is corrupted. ";
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final SegmentRegistry<K, V> segmentRegistry;
    private final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap;
    private final Comparator<K> keyComparator;

    IndexConsistencyChecker(final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        this.keyComparator = keyTypeDescriptor.getComparator();
    }

    private SegmentRegistryResult<Segment<K, V>> loadSegment(
            final SegmentId segmentId) {
        final SegmentRegistryResult<SegmentHandler<K, V>> handlerResult = segmentRegistry
                .getSegmentHandler(segmentId);
        if (handlerResult.getStatus() == SegmentRegistryResultStatus.OK) {
            return handlerResult.getValue().getSegmentIfReady();
        }
        if (handlerResult.getStatus() == SegmentRegistryResultStatus.CLOSED) {
            return SegmentRegistryResult.closed();
        }
        if (handlerResult.getStatus() == SegmentRegistryResultStatus.ERROR) {
            return SegmentRegistryResult.error();
        }
        return SegmentRegistryResult.busy();
    }

    /**
     * Scans all segments and verifies map-to-segment consistency, attempting to
     * repair obvious corruption when possible.
     */
    public void checkAndRepairConsistency() {
        keyToSegmentMap.getSegmentsAsStream().forEach(segmentPair -> {
            final K segmentKey = segmentPair.getKey();
            if (segmentKey == null) {
                throw new IndexException(ERROR_MSG + "Segment key is null.");
            }
            final SegmentId segmentId = segmentPair.getValue();
            if (segmentId == null) {
                throw new IndexException(ERROR_MSG + "Segment id is null.");
            }
            logger.debug("checking segment '{}'.", segmentId);
            final Segment<K, V> segment;
            while (true) {
                final SegmentRegistryResult<Segment<K, V>> segmentResult = loadSegment(
                        segmentId);
                if (segmentResult.getStatus() == SegmentRegistryResultStatus.BUSY) {
                    Thread.onSpinWait();
                    continue;
                }
                if (segmentResult.getStatus() != SegmentRegistryResultStatus.OK) {
                    throw new IndexException(String.format(
                            ERROR_MSG + "Segment '%s' is not found in index.",
                            segmentId));
                }
                segment = segmentResult.getValue();
                break;
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
        if (!confirmEmptyUnderIsolation(segment)) {
            return;
        }
        logger.warn("Segment '{}' is empty. Removing it from index map.",
                segmentId);
        keyToSegmentMap.removeSegment(segmentId);
        keyToSegmentMap.optionalyFlush();
        segmentRegistry.removeSegment(segmentId);
    }

    private boolean confirmEmptyUnderIsolation(final Segment<K, V> segment) {
        while (true) {
            final SegmentResult<EntryIterator<K, V>> result = segment
                    .openIterator(SegmentIteratorIsolation.FULL_ISOLATION);
            if (result.getStatus() == SegmentResultStatus.OK) {
                try (EntryIterator<K, V> iterator = result.getValue()) {
                    return !iterator.hasNext();
                }
            }
            if (result.getStatus() == SegmentResultStatus.BUSY) {
                Thread.onSpinWait();
                continue;
            }
            throw new IndexException(String.format(
                    "Segment '%s' failed to open iterator: %s", segment.getId(),
                    result.getStatus()));
        }
    }

}
