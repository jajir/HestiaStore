package org.hestiastore.index.segmentindex;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.hestiastore.index.F;
import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactSupport<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final List<Entry<K, V>> toSameSegment = new ArrayList<>();
    private final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final Comparator<K> keyComparator;
    private SegmentId currentSegmentId = null;
    private K currentBatchMaxKey = null;

    /**
     * List of segment's ids eligible for compacting.
     */
    private final List<SegmentId> eligibleSegments = new ArrayList<>();

    CompactSupport(final SegmentRegistry<K, V> segmentRegistry,
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final Comparator<K> keyComparator) {
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.keyComparator = Vldtn.requireNonNull(keyComparator,
                "keyComparator");
    }

    public void compact(final Entry<K, V> entry) {
        Vldtn.requireNonNull(entry, "entry");
        final K segmentKey = entry.getKey();
        final SegmentId segmentId = keyToSegmentMap
                .insertKeyToSegment(segmentKey);
        if (currentSegmentId == null) {
            currentSegmentId = segmentId;
            toSameSegment.add(entry);
            updateBatchMax(entry.getKey());
            return;
        }
        if (currentSegmentId.equals(segmentId)) {
            toSameSegment.add(entry);
            updateBatchMax(entry.getKey());
        } else {
            /* Write all keys to index and clean cache and set new pageId */
            flushToCurrentSegment();
            toSameSegment.add(entry);
            currentSegmentId = segmentId;
            currentBatchMaxKey = null;
            updateBatchMax(entry.getKey());
        }
    }

    public void flush() {
        if (currentSegmentId == null) {
            return;
        }
        flushToCurrentSegment();
        currentSegmentId = null;
    }

    private void flushToCurrentSegment() {
        if (logger.isDebugEnabled()) {
            logger.debug("Flushing '{}' key value entries into segment '{}'.",
                    F.fmt(toSameSegment.size()), currentSegmentId);
        }
        final Segment<K, V> segment;
        while (true) {
            final SegmentResult<Segment<K, V>> segmentResult = segmentRegistry
                    .getSegment(currentSegmentId);
            if (segmentResult.getStatus() == SegmentResultStatus.BUSY) {
                continue;
            }
            if (segmentResult.getStatus() != SegmentResultStatus.OK) {
                throw new org.hestiastore.index.IndexException(String.format(
                        "Segment '%s' failed to load: %s", currentSegmentId,
                        segmentResult.getStatus()));
            }
            segment = segmentResult.getValue();
            break;
        }
        toSameSegment.forEach(entry -> {
            while (true) {
                final SegmentResult<Void> result = segment.put(entry.getKey(),
                        entry.getValue());
                if (result.getStatus() == SegmentResultStatus.OK) {
                    break;
                }
                if (result.getStatus() == SegmentResultStatus.BUSY) {
                    continue;
                }
                throw new org.hestiastore.index.IndexException(String.format(
                        "Segment '%s' failed during put: %s", segment.getId(),
                        result.getStatus()));
            }
        });
        while (true) {
            final SegmentResult<?> result = segment.flush();
            final SegmentResultStatus status = result.getStatus();
            if (status == SegmentResultStatus.OK
                    || status == SegmentResultStatus.CLOSED) {
                break;
            }
            if (status == SegmentResultStatus.BUSY) {
                continue;
            }
            throw new org.hestiastore.index.IndexException(String.format(
                    "Segment '%s' failed during flush: %s", segment.getId(),
                    status));
        }
        if (KeyToSegmentMap.FIRST_SEGMENT_ID.equals(currentSegmentId)) {
            // Segment containing highest key.
            if (currentBatchMaxKey != null) {
                // Update segment cache with highest key.
                keyToSegmentMap.insertKeyToSegment(currentBatchMaxKey);
                keyToSegmentMap.optionalyFlush();
            }
        }

        eligibleSegments.add(currentSegmentId);
        toSameSegment.clear();
        currentBatchMaxKey = null;
        logger.debug("Flushing to segment '{}' was done.", currentSegmentId);
    }

    /**
     * After compacting all keys to appropriate segment it allows to obtain list
     * of that segment.
     * 
     * @return list of segment eligible form compacting
     */
    public List<SegmentId> getEligibleSegmentIds() {
        return List.copyOf(eligibleSegments);
    }

    private void updateBatchMax(final K key) {
        if (currentBatchMaxKey == null) {
            currentBatchMaxKey = key;
            return;
        }
        if (keyComparator.compare(key, currentBatchMaxKey) > 0) {
            currentBatchMaxKey = key;
        }
    }

}
