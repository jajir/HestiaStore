package org.hestiastore.index.segmentindex.core;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.partition.PartitionLookupResult;
import org.hestiastore.index.segmentindex.partition.PartitionRuntime;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Owns overlay-first reads and merged iterators across stable segments and
 * partition overlays.
 */
final class PartitionReadCoordinator<K, V> {

    private final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap;
    private final PartitionRuntime<K, V> partitionRuntime;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final SegmentIndexCore<K, V> core;
    private final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final IndexRetryPolicy retryPolicy;

    PartitionReadCoordinator(
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final PartitionRuntime<K, V> partitionRuntime,
            final SegmentRegistry<K, V> segmentRegistry,
            final SegmentIndexCore<K, V> core,
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexRetryPolicy retryPolicy) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.partitionRuntime = Vldtn.requireNonNull(partitionRuntime,
                "partitionRuntime");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.core = Vldtn.requireNonNull(core, "core");
        this.backgroundSplitCoordinator = Vldtn.requireNonNull(
                backgroundSplitCoordinator, "backgroundSplitCoordinator");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
    }

    IndexResult<V> get(final K key) {
        return backgroundSplitCoordinator
                .runWithStableWriteAdmission(() -> getBuffered(key));
    }

    EntryIterator<K, V> openWindowIterator(final SegmentWindow resolvedWindows,
            final SegmentIteratorIsolation isolation) {
        Vldtn.requireNonNull(resolvedWindows, "resolvedWindows");
        Vldtn.requireNonNull(isolation, "isolation");
        if (isolation == SegmentIteratorIsolation.FULL_ISOLATION) {
            return backgroundSplitCoordinator.runWithStableWriteAdmission(
                    () -> openMergedIteratorWithRouteSnapshot(resolvedWindows,
                            isolation));
        }
        return openMergedIterator(resolvedWindows, isolation);
    }

    private IndexResult<V> getBuffered(final K key) {
        final KeyToSegmentMap.Snapshot<K> snapshot = keyToSegmentMap.snapshot();
        final SegmentId segmentId = snapshot.findSegmentId(key);
        if (segmentId == null) {
            return IndexResult.ok(null);
        }
        partitionRuntime.ensurePartition(segmentId);
        final PartitionLookupResult<V> overlay = partitionRuntime.lookup(
                segmentId, key);
        if (overlay.isFound()) {
            final V value = overlay.getValue();
            return IndexResult.ok(
                    valueTypeDescriptor.isTombstone(value) ? null : value);
        }
        if (!keyToSegmentMap.isMappingValid(key, segmentId,
                snapshot.version())) {
            return IndexResult.busy();
        }
        return core.get(segmentId, key);
    }

    private EntryIterator<K, V> openMergedIterator(
            final SegmentWindow resolvedWindows,
            final SegmentIteratorIsolation isolation) {
        final List<SegmentId> segmentIds = keyToSegmentMap
                .getSegmentIds(resolvedWindows);
        return openMergedIterator(segmentIds, isolation);
    }

    private EntryIterator<K, V> openMergedIteratorWithRouteSnapshot(
            final SegmentWindow resolvedWindows,
            final SegmentIteratorIsolation isolation) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final KeyToSegmentMap.Snapshot<K> snapshot = keyToSegmentMap
                    .snapshot();
            final List<SegmentId> segmentIds = snapshot
                    .getSegmentIds(resolvedWindows);
            try {
                final EntryIterator<K, V> iterator = openMergedIterator(
                        segmentIds, isolation);
                if (keyToSegmentMap.isVersion(snapshot.version())) {
                    return iterator;
                }
                iterator.close();
            } catch (final IndexException e) {
                if (keyToSegmentMap.isVersion(snapshot.version())) {
                    throw e;
                }
            }
            retryPolicy.backoffOrThrow(startNanos,
                    SegmentIndexImpl.OPERATION_OPEN_FULL_ISOLATION_ITERATOR,
                    null);
        }
    }

    private EntryIterator<K, V> openMergedIterator(
            final List<SegmentId> segmentIds,
            final SegmentIteratorIsolation isolation) {
        final NavigableMap<K, V> merged = new TreeMap<>(
                keyTypeDescriptor.getComparator());
        try (EntryIterator<K, V> stableIterator = new SegmentsIterator<>(
                segmentIds, segmentRegistry, isolation, retryPolicy)) {
            while (stableIterator.hasNext()) {
                final Entry<K, V> entry = stableIterator.next();
                merged.put(entry.getKey(), entry.getValue());
            }
        }
        partitionRuntime.applyOverlaySnapshot(segmentIds, merged);
        final List<Entry<K, V>> visibleEntries = merged.entrySet().stream()
                .filter(entry -> !valueTypeDescriptor.isTombstone(
                        entry.getValue()))
                .map(entry -> Entry.of(entry.getKey(), entry.getValue()))
                .toList();
        return new EntryIteratorList<>(visibleEntries);
    }
}
