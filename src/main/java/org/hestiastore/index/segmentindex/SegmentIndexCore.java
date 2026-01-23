package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.hestiastore.index.segment.SegmentState;

/**
 * Single-attempt core for segment-index operations that returns status
 * wrappers instead of blocking on BUSY.
 */
final class SegmentIndexCore<K, V> {

    private final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final SegmentMaintenanceCoordinator<K, V> maintenanceCoordinator;

    SegmentIndexCore(final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final SegmentMaintenanceCoordinator<K, V> maintenanceCoordinator) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.maintenanceCoordinator = Vldtn.requireNonNull(
                maintenanceCoordinator, "maintenanceCoordinator");
    }

    IndexResult<V> get(final K key) {
        final SegmentId segmentId = keyToSegmentMap.findSegmentId(key);
        if (segmentId == null) {
            return IndexResult.ok(null);
        }
        final SegmentResult<Segment<K, V>> segmentResult = segmentRegistry
                .getSegment(segmentId);
        if (segmentResult.getStatus() == SegmentResultStatus.BUSY) {
            return IndexResult.busy();
        }
        if (segmentResult.getStatus() == SegmentResultStatus.CLOSED) {
            return IndexResult.closed();
        }
        if (segmentResult.getStatus() != SegmentResultStatus.OK) {
            return IndexResult.error();
        }
        final Segment<K, V> segment = segmentResult.getValue();
        final SegmentResult<V> result = segment.get(key);
        if (result.getStatus() == SegmentResultStatus.OK) {
            return IndexResult.ok(result.getValue());
        }
        if (result.getStatus() == SegmentResultStatus.BUSY) {
            return IndexResult.busy();
        }
        if (result.getStatus() == SegmentResultStatus.CLOSED) {
            return IndexResult.closed();
        }
        return IndexResult.error();
    }

    IndexResult<Void> put(final K key, final V value) {
        KeyToSegmentMap.Snapshot<K> snapshot = keyToSegmentMap.snapshot();
        SegmentId segmentId = snapshot.findSegmentId(key);
        if (segmentId == null) {
            if (!keyToSegmentMap.tryExtendMaxKey(key, snapshot)) {
                return IndexResult.busy();
            }
            snapshot = keyToSegmentMap.snapshot();
            segmentId = snapshot.findSegmentId(key);
            if (segmentId == null) {
                return IndexResult.busy();
            }
        }
        final SegmentResult<Segment<K, V>> segmentResult = segmentRegistry
                .getSegment(segmentId);
        if (segmentResult.getStatus() == SegmentResultStatus.BUSY) {
            return IndexResult.busy();
        }
        if (segmentResult.getStatus() == SegmentResultStatus.CLOSED) {
            return IndexResult.closed();
        }
        if (segmentResult.getStatus() != SegmentResultStatus.OK) {
            return IndexResult.error();
        }
        final Segment<K, V> segment = segmentResult.getValue();
        if (segment.getState() == SegmentState.CLOSED) {
            return IndexResult.busy();
        }
        if (!keyToSegmentMap.isMappingValid(key, segmentId,
                snapshot.version())) {
            return IndexResult.busy();
        }
        final SegmentResult<Void> result = segment.put(key, value);
        if (result.getStatus() == SegmentResultStatus.OK) {
            maintenanceCoordinator.handlePostWrite(segment, key, segmentId,
                    snapshot.version());
            return IndexResult.ok();
        }
        if (result.getStatus() == SegmentResultStatus.BUSY
                || result.getStatus() == SegmentResultStatus.CLOSED) {
            return IndexResult.busy();
        }
        return IndexResult.error();
    }

    IndexResult<EntryIterator<K, V>> openIterator(final SegmentId segmentId,
            final SegmentIteratorIsolation isolation) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        final SegmentResult<Segment<K, V>> segmentResult = segmentRegistry
                .getSegment(segmentId);
        if (segmentResult.getStatus() == SegmentResultStatus.BUSY) {
            return IndexResult.busy();
        }
        if (segmentResult.getStatus() == SegmentResultStatus.CLOSED) {
            return IndexResult.closed();
        }
        if (segmentResult.getStatus() != SegmentResultStatus.OK) {
            return IndexResult.error();
        }
        final Segment<K, V> segment = segmentResult.getValue();
        final SegmentResult<EntryIterator<K, V>> result = segment
                .openIterator(isolation);
        if (result.getStatus() == SegmentResultStatus.OK) {
            return IndexResult.ok(result.getValue());
        }
        if (result.getStatus() == SegmentResultStatus.BUSY) {
            return IndexResult.busy();
        }
        if (result.getStatus() == SegmentResultStatus.CLOSED) {
            return IndexResult.closed();
        }
        return IndexResult.error();
    }

    IndexResult<Segment<K, V>> compact(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        final SegmentResult<Segment<K, V>> segmentResult = segmentRegistry
                .getSegment(segmentId);
        if (segmentResult.getStatus() == SegmentResultStatus.BUSY) {
            return IndexResult.busy();
        }
        if (segmentResult.getStatus() == SegmentResultStatus.CLOSED) {
            return IndexResult.closed();
        }
        if (segmentResult.getStatus() != SegmentResultStatus.OK) {
            return IndexResult.error();
        }
        final Segment<K, V> segment = segmentResult.getValue();
        final SegmentResult<Void> result = segment.compact();
        if (result.getStatus() == SegmentResultStatus.OK) {
            return IndexResult.ok(segment);
        }
        if (result.getStatus() == SegmentResultStatus.BUSY) {
            return IndexResult.busy();
        }
        if (result.getStatus() == SegmentResultStatus.CLOSED) {
            return IndexResult.closed();
        }
        return IndexResult.error();
    }

    IndexResult<Segment<K, V>> flush(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        final SegmentResult<Segment<K, V>> segmentResult = segmentRegistry
                .getSegment(segmentId);
        if (segmentResult.getStatus() == SegmentResultStatus.BUSY) {
            return IndexResult.busy();
        }
        if (segmentResult.getStatus() == SegmentResultStatus.CLOSED) {
            return IndexResult.closed();
        }
        if (segmentResult.getStatus() != SegmentResultStatus.OK) {
            return IndexResult.error();
        }
        final Segment<K, V> segment = segmentResult.getValue();
        final SegmentResult<Void> result = segment.flush();
        if (result.getStatus() == SegmentResultStatus.OK) {
            return IndexResult.ok(segment);
        }
        if (result.getStatus() == SegmentResultStatus.BUSY) {
            return IndexResult.busy();
        }
        if (result.getStatus() == SegmentResultStatus.CLOSED) {
            return IndexResult.closed();
        }
        return IndexResult.error();
    }
}
