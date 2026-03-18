package org.hestiastore.index.segmentindex.core;

import java.util.function.Function;

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
 * Single-attempt gateway for stable-segment operations that returns status
 * wrappers instead of blocking on BUSY.
 */
final class StableSegmentGateway<K, V> {

    private static final String SEGMENT_ID_ARG = "segmentId";

    private final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;

    StableSegmentGateway(
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
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
        return withLoadedSegment(segmentId,
                segment -> fromSegmentResult(segment.get(key)));
    }

    IndexResult<EntryIterator<K, V>> openIterator(final SegmentId segmentId,
            final SegmentIteratorIsolation isolation) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        return withLoadedSegment(segmentId,
                segment -> fromSegmentResult(segment.openIterator(isolation)));
    }

    IndexResult<Segment<K, V>> compact(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        return withLoadedSegment(segmentId, segment -> {
            final IndexResult<Void> result = fromSegmentResult(segment.compact());
            if (result.getStatus() == IndexResultStatus.OK) {
                return IndexResult.ok(segment);
            }
            return fromIndexStatus(result.getStatus());
        });
    }

    IndexResult<Segment<K, V>> flush(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        return withLoadedSegment(segmentId, segment -> {
            final IndexResult<Void> result = fromSegmentResult(segment.flush());
            if (result.getStatus() == IndexResultStatus.OK) {
                return IndexResult.ok(segment);
            }
            return fromIndexStatus(result.getStatus());
        });
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

    private static <T> IndexResult<T> fromSegmentResult(
            final SegmentResult<T> result) {
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

    private static <T> IndexResult<T> fromIndexStatus(
            final IndexResultStatus status) {
        if (status == IndexResultStatus.BUSY) {
            return IndexResult.busy();
        }
        if (status == IndexResultStatus.CLOSED) {
            return IndexResult.closed();
        }
        if (status == IndexResultStatus.OK) {
            return IndexResult.ok();
        }
        return IndexResult.error();
    }

    private <T> IndexResult<T> withLoadedSegment(final SegmentId segmentId,
            final Function<Segment<K, V>, IndexResult<T>> action) {
        final SegmentRegistryResult<Segment<K, V>> loaded = segmentRegistry
                .getSegment(segmentId);
        if (loaded.getStatus() != SegmentRegistryResultStatus.OK
                || loaded.getValue() == null) {
            return fromRegistryStatus(loaded.getStatus());
        }
        return action.apply(loaded.getValue());
    }
}
