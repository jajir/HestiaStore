package org.hestiastore.index.segmentindex.core.segmentaccess;

import java.util.List;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology.RouteLease;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology.RouteLeaseResult;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.Snapshot;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

final class SegmentAccessServiceImpl<K, V>
        implements SegmentAccessService<K, V> {

    private static final String OPERATION_READ = "acquireForRead";
    private static final String OPERATION_WRITE = "acquireForWrite";

    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final SegmentTopology<K> segmentTopology;
    private final IndexRetryPolicy retryPolicy;

    SegmentAccessServiceImpl(
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final SegmentTopology<K> segmentTopology,
            final IndexRetryPolicy retryPolicy) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.segmentTopology = Vldtn.requireNonNull(segmentTopology,
                "segmentTopology");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
    }

    @Override
    public SegmentAccess<K, V> acquireForRead(final K key) {
        final K nonNullKey = requireKey(key);
        final RouteLease lease = acquireReadRouteLease(nonNullKey);
        if (lease == null) {
            return null;
        }
        return loadSegmentAccess(lease);
    }

    @Override
    public SegmentAccess<K, V> acquireForWrite(final K key) {
        final K nonNullKey = requireKey(key);
        return loadSegmentAccess(acquireWriteRouteLease(nonNullKey));
    }

    private SegmentAccess<K, V> loadSegmentAccess(final RouteLease lease) {
        try {
            return new SegmentAccessImpl<>(lease,
                    segmentRegistry.loadSegment(lease.segmentId()));
        } catch (final RuntimeException e) {
            lease.close();
            throw e;
        }
    }

    private RouteLease acquireReadRouteLease(final K key) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final Snapshot<K> snapshot = currentRouteSnapshot();
            final SegmentId routedSegmentId = snapshot.findSegmentIdForKey(key);
            if (routedSegmentId == null) {
                return null;
            }
            final RouteLease lease = tryAcquireRouteLease(routedSegmentId,
                    snapshot);
            if (lease != null) {
                return lease;
            }
            retryPolicy.backoffOrThrow(startNanos, OPERATION_READ,
                    routedSegmentId);
        }
    }

    private RouteLease acquireWriteRouteLease(final K key) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final Snapshot<K> snapshot = currentRouteSnapshot();
            final SegmentId routedSegmentId = snapshot.findSegmentIdForKey(key);
            final RouteLease lease = routedSegmentId == null
                    ? tryAcquireTailRoute(snapshot, key)
                    : tryAcquireRouteLease(routedSegmentId, snapshot);
            if (lease != null) {
                return lease;
            }
            retryPolicy.backoffOrThrow(startNanos, OPERATION_WRITE,
                    routedSegmentId);
        }
    }

    private RouteLease tryAcquireTailRoute(final Snapshot<K> snapshot,
            final K key) {
        final List<SegmentId> segmentIds = snapshot
                .getSegmentIds(SegmentWindow.unbounded());
        if (segmentIds.isEmpty()) {
            return tryAcquireBootstrapRoute(key);
        }
        final SegmentId tailSegmentId = segmentIds.get(segmentIds.size() - 1);
        final RouteLease lease = tryAcquireRouteLease(tailSegmentId, snapshot);
        if (lease == null) {
            return null;
        }
        if (!keyToSegmentMap.extendMaxKeyIfNeeded(key)) {
            lease.close();
            return null;
        }
        return lease;
    }

    private RouteLease tryAcquireBootstrapRoute(final K key) {
        if (!keyToSegmentMap.extendMaxKeyIfNeeded(key)) {
            return null;
        }
        final Snapshot<K> bootstrappedSnapshot = currentRouteSnapshot();
        segmentTopology.reconcile(bootstrappedSnapshot);
        final SegmentId bootstrappedSegmentId = bootstrappedSnapshot
                .findSegmentIdForKey(key);
        if (bootstrappedSegmentId == null) {
            return null;
        }
        return tryAcquireRouteLease(bootstrappedSegmentId,
                bootstrappedSnapshot);
    }

    private RouteLease tryAcquireRouteLease(final SegmentId segmentId,
            final Snapshot<K> snapshot) {
        final RouteLeaseResult result = segmentTopology.tryAcquire(segmentId,
                snapshot.version());
        if (result.isAcquired()) {
            return result.lease();
        }
        if (result.isStaleTopology()) {
            segmentTopology.reconcile(snapshot);
        }
        return null;
    }

    private K requireKey(final K key) {
        return Vldtn.requireNonNull(key, "key");
    }

    private Snapshot<K> currentRouteSnapshot() {
        return keyToSegmentMap.snapshot();
    }
}
