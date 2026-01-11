package org.hestiastore.index.segmentindex;

import java.util.concurrent.CompletionStage;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;

/**
 * Single-attempt core for segment-index operations that returns status
 * wrappers instead of blocking on BUSY.
 */
final class SegmentIndexCore<K, V> {

    private final KeySegmentCache<K> keySegmentCache;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final SegmentMaintenanceCoordinator<K, V> maintenanceCoordinator;

    SegmentIndexCore(final KeySegmentCache<K> keySegmentCache,
            final SegmentRegistry<K, V> segmentRegistry,
            final SegmentMaintenanceCoordinator<K, V> maintenanceCoordinator) {
        this.keySegmentCache = Vldtn.requireNonNull(keySegmentCache,
                "keySegmentCache");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.maintenanceCoordinator = Vldtn.requireNonNull(
                maintenanceCoordinator, "maintenanceCoordinator");
    }

    IndexResult<V> get(final K key) {
        final SegmentId segmentId = keySegmentCache.findSegmentId(key);
        if (segmentId == null) {
            return IndexResult.ok(null);
        }
        final Segment<K, V> segment = segmentRegistry.getSegment(segmentId);
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
        KeySegmentCache.Snapshot<K> snapshot = keySegmentCache.snapshot();
        SegmentId segmentId = snapshot.findSegmentId(key);
        if (segmentId == null) {
            if (!keySegmentCache.tryExtendMaxKey(key, snapshot)) {
                return IndexResult.busy();
            }
            snapshot = keySegmentCache.snapshot();
            segmentId = snapshot.findSegmentId(key);
            if (segmentId == null) {
                return IndexResult.busy();
            }
        }
        final Segment<K, V> segment = segmentRegistry.getSegment(segmentId);
        if (segment.wasClosed()) {
            return IndexResult.busy();
        }
        if (!keySegmentCache.isMappingValid(key, segmentId,
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
        final Segment<K, V> segment = segmentRegistry.getSegment(segmentId);
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

    IndexResult<CompletionStage<Void>> compact(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        final Segment<K, V> segment = segmentRegistry.getSegment(segmentId);
        final SegmentResult<CompletionStage<Void>> result = segment.compact();
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

    IndexResult<CompletionStage<Void>> flush(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        final Segment<K, V> segment = segmentRegistry.getSegment(segmentId);
        final SegmentResult<CompletionStage<Void>> result = segment.flush();
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
}
