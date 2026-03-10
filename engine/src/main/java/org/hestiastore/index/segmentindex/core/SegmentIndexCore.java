package org.hestiastore.index.segmentindex.core;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.hestiastore.index.segmentregistry.SegmentRegistryResultStatus;

/**
 * Single-attempt core for segment-index operations that returns status
 * wrappers instead of blocking on BUSY.
 */
final class SegmentIndexCore<K, V> {

    private static final String SEGMENT_ID_ARG = "segmentId";

    private final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;

    SegmentIndexCore(final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
    }

    IndexResult<V> get(final K key) {
        final KeyToSegmentMap.Snapshot<K> snapshot = keyToSegmentMap.snapshot();
        final SegmentId segmentId = snapshot.findSegmentId(key);
        if (segmentId == null) {
            return IndexResult.ok(null);
        }
        final SegmentRegistryResult<Segment<K, V>> loaded = segmentRegistry.getSegment(
                segmentId);
        if (loaded.getStatus() != SegmentRegistryResultStatus.OK
                || loaded.getValue() == null) {
            return fromRegistryStatus(loaded.getStatus());
        }
        final Segment<K, V> segment = loaded.getValue();
        final SegmentResult<V> result = segment.get(key);
        if (result.getStatus() == SegmentResultStatus.OK) {
            if (!keyToSegmentMap.isMappingValid(key, segmentId,
                    snapshot.version())) {
                return IndexResult.busy();
            }
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

    IndexResult<V> get(final SegmentId segmentId, final K key) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        final SegmentRegistryResult<Segment<K, V>> loaded = segmentRegistry
                .getSegment(segmentId);
        if (loaded.getStatus() != SegmentRegistryResultStatus.OK
                || loaded.getValue() == null) {
            return fromRegistryStatus(loaded.getStatus());
        }
        final Segment<K, V> segment = loaded.getValue();
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

    IndexResult<EntryIterator<K, V>> openIterator(final SegmentId segmentId,
            final SegmentIteratorIsolation isolation) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        final SegmentRegistryResult<Segment<K, V>> loaded = segmentRegistry.getSegment(
                segmentId);
        if (loaded.getStatus() != SegmentRegistryResultStatus.OK
                || loaded.getValue() == null) {
            return fromRegistryStatus(loaded.getStatus());
        }
        final Segment<K, V> segment = loaded.getValue();
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
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        final SegmentRegistryResult<Segment<K, V>> loaded = segmentRegistry.getSegment(
                segmentId);
        if (loaded.getStatus() != SegmentRegistryResultStatus.OK
                || loaded.getValue() == null) {
            return fromRegistryStatus(loaded.getStatus());
        }
        final Segment<K, V> segment = loaded.getValue();
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
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        final SegmentRegistryResult<Segment<K, V>> loaded = segmentRegistry.getSegment(
                segmentId);
        if (loaded.getStatus() != SegmentRegistryResultStatus.OK
                || loaded.getValue() == null) {
            return fromRegistryStatus(loaded.getStatus());
        }
        final Segment<K, V> segment = loaded.getValue();
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

    private static <T> IndexResult<T> fromRegistryStatus(
            final SegmentRegistryResultStatus status) {
        if (status == SegmentRegistryResultStatus.BUSY) {
            return IndexResult.busy();
        }
        if (status == SegmentRegistryResultStatus.CLOSED) {
            return IndexResult.closed();
        }
        return IndexResult.error();
    }
}
