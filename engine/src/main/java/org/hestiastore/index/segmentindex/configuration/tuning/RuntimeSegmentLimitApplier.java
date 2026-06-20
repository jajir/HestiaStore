package org.hestiastore.index.segmentindex.configuration.tuning;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstorecache.ChunkStoreCache;
import org.hestiastore.index.chunkstorecache.LruChunkStoreCache;
import org.hestiastore.index.segment.SegmentRuntimeLimits;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Applies runtime tuning changes to segment registry cache limits and all
 * loaded stable segments.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class RuntimeSegmentLimitApplier<K, V> {

    private final SegmentRegistry<K, V> segmentRegistry;
    private final SegmentRegistry.Runtime<K, V> segmentRuntime;
    private final ChunkStoreCache<K, V> chunkStoreCache;

    public RuntimeSegmentLimitApplier(
            final SegmentRegistry<K, V> segmentRegistry,
            final SegmentRegistry.Runtime<K, V> segmentRuntime) {
        this(segmentRegistry, segmentRuntime, new LruChunkStoreCache<>(0));
    }

    public RuntimeSegmentLimitApplier(
            final SegmentRegistry<K, V> segmentRegistry,
            final SegmentRegistry.Runtime<K, V> segmentRuntime,
            final ChunkStoreCache<K, V> chunkStoreCache) {
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.segmentRuntime = Vldtn.requireNonNull(segmentRuntime,
                "segmentRuntime");
        this.chunkStoreCache = Vldtn.requireNonNull(chunkStoreCache,
                "chunkStoreCache");
    }

    public void apply(final RuntimeTuningSnapshot effective) {
        final int maxSegmentsInCache = effective.segment().cachedSegmentLimit();
        segmentRegistry.updateCacheLimit(maxSegmentsInCache);
        final int maxSegmentCache = effective.segment().cacheKeyLimit();
        final int maxSegmentWriteCache = effective.writePath()
                .segmentWriteCacheKeyLimit();
        final int maxMaintenanceWriteCache = effective.writePath()
                .segmentWriteCacheKeyLimitDuringMaintenance();
        final SegmentRuntimeLimits limits = new SegmentRuntimeLimits(
                maxSegmentCache, maxSegmentWriteCache,
                maxMaintenanceWriteCache);
        segmentRuntime.updateRuntimeLimits(limits);
        for (final BlockingSegment<K, V> segment : segmentRuntime
                .loadedSegmentsSnapshot()) {
            segment.getRuntime().updateRuntimeLimits(limits);
        }
        chunkStoreCache.updateLimit(effective.chunkStoreCache().pageLimit());
    }
}
