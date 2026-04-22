package org.hestiastore.index.segmentindex.core.routing;

import java.util.Optional;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.OperationStatus;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.Snapshot;
import org.hestiastore.index.segmentregistry.SegmentHandle;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Single-attempt gateway for stable-segment operations that returns status
 * wrappers instead of blocking on BUSY.
 */
final class StableSegmentGateway<K, V> implements StableSegmentAccess<K, V> {

    private static final String SEGMENT_ID_ARG = "segmentId";

    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;

    StableSegmentGateway(
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
    }

    @Override
    public OperationResult<V> get(final K key) {
        final Snapshot<K> snapshot = keyToSegmentMap.snapshot();
        final SegmentId segmentId = snapshot.findSegmentIdForKey(key);
        if (segmentId == null) {
            return OperationResult.ok(null);
        }
        return get(key, segmentId, snapshot.version());
    }

    @Override
    public OperationResult<V> get(final K key, final SegmentId segmentId,
            final long expectedTopologyVersion) {
        Vldtn.requireNonNull(key, "key");
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        return withLoadedSegment(segmentId,
                segment -> readFromLoadedSegment(segment, key,
                        expectedTopologyVersion));
    }

    @Override
    public OperationResult<V> get(final SegmentId segmentId, final K key) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        return withLoadedSegment(segmentId,
                segment -> fromSegmentResult(segment.tryGet(key)));
    }

    @Override
    public OperationResult<Void> put(final SegmentId segmentId, final K key,
            final V value) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        Vldtn.requireNonNull(key, "key");
        Vldtn.requireNonNull(value, "value");
        return withLoadedSegment(segmentId,
                segment -> fromSegmentResult(segment.tryPut(key, value)));
    }

    @Override
    public OperationResult<EntryIterator<K, V>> openIterator(
            final SegmentId segmentId,
            final SegmentIteratorIsolation isolation) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        return withLoadedSegment(segmentId,
                segment -> fromSegmentResult(segment.tryOpenIterator(isolation)));
    }

    @Override
    public OperationResult<SegmentHandle<K, V>> compact(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        return withLoadedSegment(segmentId, segment -> {
            final OperationResult<Void> result = fromSegmentResult(
                    segment.tryCompact());
            if (result.getStatus() == OperationStatus.OK) {
                return OperationResult.ok(segment);
            }
            return fromIndexStatus(result.getStatus());
        });
    }

    @Override
    public OperationResult<SegmentHandle<K, V>> flush(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        return withLoadedSegment(segmentId, segment -> {
            final OperationResult<Void> result = fromSegmentResult(
                    segment.tryFlush());
            if (result.getStatus() == OperationStatus.OK) {
                return OperationResult.ok(segment);
            }
            return fromIndexStatus(result.getStatus());
        });
    }

    private OperationResult<V> readFromLoadedSegment(
            final SegmentHandle<K, V> segment,
            final K key, final long expectedTopologyVersion) {
        final OperationResult<V> result = segment.tryGet(key);
        if (result.getStatus() != OperationStatus.OK) {
            return fromSegmentResult(result);
        }
        if (!isTopologyCurrent(expectedTopologyVersion)) {
            return OperationResult.busy();
        }
        return OperationResult.ok(result.getValue());
    }

    private boolean isTopologyCurrent(final long expectedTopologyVersion) {
        return keyToSegmentMap.isSnapshotVersionCurrent(expectedTopologyVersion);
    }

    private <T> OperationResult<T> withLoadedSegment(final SegmentId segmentId,
            final java.util.function.Function<SegmentHandle<K, V>, OperationResult<T>> action) {
        final Optional<SegmentHandle<K, V>> loaded = segmentRegistry
                .tryGetSegment(segmentId);
        if (loaded.isEmpty()) {
            return OperationResult.busy();
        }
        return action.apply(loaded.get());
    }

    private static <T> OperationResult<T> fromSegmentResult(
            final OperationResult<T> result) {
        if (result.getStatus() == OperationStatus.OK) {
            return OperationResult.ok(result.getValue());
        }
        if (result.getStatus() == OperationStatus.BUSY) {
            return OperationResult.busy();
        }
        if (result.getStatus() == OperationStatus.CLOSED) {
            return OperationResult.closed();
        }
        return OperationResult.error();
    }

    private static <T> OperationResult<T> fromIndexStatus(
            final OperationStatus status) {
        if (status == OperationStatus.BUSY) {
            return OperationResult.busy();
        }
        if (status == OperationStatus.CLOSED) {
            return OperationResult.closed();
        }
        if (status == OperationStatus.OK) {
            return OperationResult.ok();
        }
        return OperationResult.error();
    }
}
