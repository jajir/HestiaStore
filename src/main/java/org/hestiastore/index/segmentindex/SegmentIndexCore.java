package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.hestiastore.index.segmentregistry.SegmentRegistryResultStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single-attempt core for segment-index operations that returns status
 * wrappers instead of blocking on BUSY.
 */
final class SegmentIndexCore<K, V> {

    private static final Logger logger = LoggerFactory
            .getLogger(SegmentIndexCore.class);
    private static final boolean DEBUG_SPLIT_LOSS = Boolean
            .getBoolean("hestiastore.debugSplitLoss");

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
        final KeyToSegmentMap.Snapshot<K> snapshot = keyToSegmentMap.snapshot();
        final SegmentId segmentId = snapshot.findSegmentId(key);
        if (segmentId == null) {
            return IndexResult.ok(null);
        }
        final SegmentRegistryResult<Segment<K, V>> segmentResult = segmentRegistry
                .getSegment(segmentId);
        if (segmentResult.getStatus() == SegmentRegistryResultStatus.BUSY) {
            return IndexResult.busy();
        }
        if (segmentResult.getStatus() == SegmentRegistryResultStatus.CLOSED) {
            return IndexResult.closed();
        }
        if (segmentResult.getStatus() != SegmentRegistryResultStatus.OK) {
            return IndexResult.error();
        }
        final Segment<K, V> segment = segmentResult.getValue();
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

    IndexResult<Void> put(final K key, final V value) {
        final KeyToSegmentMap.Snapshot<K> snapshot = keyToSegmentMap.snapshot();
        if (snapshot.findSegmentId(key) == null) {
            if (!keyToSegmentMap.tryExtendMaxKey(key, snapshot)) {
                return IndexResult.busy();
            }
        }
        final KeyToSegmentMap.Snapshot<K> stableSnapshot = keyToSegmentMap
                .snapshot();
        final SegmentId segmentId = stableSnapshot.findSegmentId(key);
        if (segmentId == null) {
            return IndexResult.busy();
        }
        final SegmentRegistryResult<Segment<K, V>> segmentResult = segmentRegistry
                .getSegment(segmentId);
        if (segmentResult.getStatus() == SegmentRegistryResultStatus.BUSY) {
            return IndexResult.busy();
        }
        if (segmentResult.getStatus() == SegmentRegistryResultStatus.CLOSED) {
            return IndexResult.closed();
        }
        if (segmentResult.getStatus() != SegmentRegistryResultStatus.OK) {
            return IndexResult.error();
        }
        final Segment<K, V> segment = segmentResult.getValue();
        if (segment.getState() == SegmentState.CLOSED) {
            return IndexResult.busy();
        }
        if (!keyToSegmentMap.isMappingValid(key, segmentId,
                stableSnapshot.version())) {
            return IndexResult.busy();
        }
        final SegmentResult<Void> result = segment.put(key, value);
        final IndexResult<Void> putResult;
        if (result.getStatus() == SegmentResultStatus.OK) {
            putResult = IndexResult.ok();
        } else if (result.getStatus() == SegmentResultStatus.BUSY
                || result.getStatus() == SegmentResultStatus.CLOSED) {
            putResult = IndexResult.busy();
        } else {
            putResult = IndexResult.error();
        }
        if (putResult.getStatus() == IndexResultStatus.OK) {
            maintenanceCoordinator.handlePostWrite(segment, key, segmentId,
                    stableSnapshot.version());
            if (DEBUG_SPLIT_LOSS) {
                final KeyToSegmentMap.Snapshot<K> currentSnapshot = keyToSegmentMap
                        .snapshot();
                final SegmentId currentSegmentId = currentSnapshot
                        .findSegmentId(key);
                if (!segmentId.equals(currentSegmentId)
                        || currentSnapshot.version() != stableSnapshot
                                .version()) {
                    logger.warn(
                            "Split debug: key '{}' wrote to segment '{}' at map version '{}', now mapped to '{}' (version '{}').",
                            key, segmentId, stableSnapshot.version(),
                            currentSegmentId, currentSnapshot.version());
                }
            }
        }
        return putResult;
    }

    IndexResult<EntryIterator<K, V>> openIterator(final SegmentId segmentId,
            final SegmentIteratorIsolation isolation) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        final SegmentRegistryResult<Segment<K, V>> segmentResult = segmentRegistry
                .getSegment(segmentId);
        if (segmentResult.getStatus() == SegmentRegistryResultStatus.BUSY) {
            return IndexResult.busy();
        }
        if (segmentResult.getStatus() == SegmentRegistryResultStatus.CLOSED) {
            return IndexResult.closed();
        }
        if (segmentResult.getStatus() != SegmentRegistryResultStatus.OK) {
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
        final SegmentRegistryResult<Segment<K, V>> segmentResult = segmentRegistry
                .getSegment(segmentId);
        if (segmentResult.getStatus() == SegmentRegistryResultStatus.BUSY) {
            return IndexResult.busy();
        }
        if (segmentResult.getStatus() == SegmentRegistryResultStatus.CLOSED) {
            return IndexResult.closed();
        }
        if (segmentResult.getStatus() != SegmentRegistryResultStatus.OK) {
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
        final SegmentRegistryResult<Segment<K, V>> segmentResult = segmentRegistry
                .getSegment(segmentId);
        if (segmentResult.getStatus() == SegmentRegistryResultStatus.BUSY) {
            return IndexResult.busy();
        }
        if (segmentResult.getStatus() == SegmentRegistryResultStatus.CLOSED) {
            return IndexResult.closed();
        }
        if (segmentResult.getStatus() != SegmentRegistryResultStatus.OK) {
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
