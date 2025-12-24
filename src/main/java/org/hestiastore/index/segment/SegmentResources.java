package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.scarceindex.ScarceSegmentIndex;

/**
 * Lazily loads and caches heavyweight segment resources such as the delta
 * cache, Bloom filter, and scarce index. Call {@link #invalidate()} to drop
 * the cached instances so the next access rebuilds them.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentResources<K, V>
        implements SegmentDataProvider<K, V> {

    private final SegmentDataSupplier<K, V> segmentDataSupplier;
    private SegmentDeltaCache<K, V> deltaCache;
    private BloomFilter<K> bloomFilter;
    private ScarceSegmentIndex<K> scarceIndex;
    private boolean loaded;

    public SegmentResources(final SegmentDataSupplier<K, V> segmentDataSupplier) {
        this.segmentDataSupplier = Vldtn.requireNonNull(segmentDataSupplier,
                "segmentDataSupplier");
    }

    @Override
    public boolean isLoaded() {
        return loaded;
    }

    @Override
    public SegmentDeltaCache<K, V> getSegmentDeltaCache() {
        loaded = true;
        if (deltaCache == null) {
            deltaCache = segmentDataSupplier.getSegmentDeltaCache();
        }
        return deltaCache;
    }

    @Override
    public BloomFilter<K> getBloomFilter() {
        loaded = true;
        if (bloomFilter == null) {
            bloomFilter = segmentDataSupplier.getBloomFilter();
        }
        return bloomFilter;
    }

    @Override
    public ScarceSegmentIndex<K> getScarceIndex() {
        loaded = true;
        if (scarceIndex == null) {
            scarceIndex = segmentDataSupplier.getScarceIndex();
        }
        return scarceIndex;
    }

    @Override
    public void invalidate() {
        if (bloomFilter != null) {
            bloomFilter.close();
            bloomFilter = null;
        }
        if (deltaCache != null) {
            deltaCache.evictAll();
            deltaCache = null;
        }
        if (scarceIndex != null) {
            scarceIndex = null;
        }
        loaded = false;
    }
}
