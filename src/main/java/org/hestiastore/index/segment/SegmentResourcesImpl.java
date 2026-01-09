package org.hestiastore.index.segment;

import java.util.concurrent.atomic.AtomicReference;

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
public final class SegmentResourcesImpl<K, V>
        implements SegmentResources<K, V> {

    private final SegmentDataSupplier<K, V> segmentDataSupplier;
    private final AtomicReference<SegmentDeltaCache<K, V>> deltaCache = new AtomicReference<>();
    private final AtomicReference<BloomFilter<K>> bloomFilter = new AtomicReference<>();
    private final AtomicReference<ScarceSegmentIndex<K>> scarceIndex = new AtomicReference<>();

    public SegmentResourcesImpl(
            final SegmentDataSupplier<K, V> segmentDataSupplier) {
        this.segmentDataSupplier = Vldtn.requireNonNull(segmentDataSupplier,
                "segmentDataSupplier");
    }

    @Override
    public SegmentDeltaCache<K, V> getSegmentDeltaCache() {
        while (true) {
            final SegmentDeltaCache<K, V> current = deltaCache.get();
            if (current != null) {
                return current;
            }
            final SegmentDeltaCache<K, V> loaded = Vldtn.requireNonNull(
                    segmentDataSupplier.getSegmentDeltaCache(),
                    "segmentDataSupplier.getSegmentDeltaCache()");
            if (deltaCache.compareAndSet(null, loaded)) {
                return loaded;
            }
            final SegmentDeltaCache<K, V> after = deltaCache.get();
            if (after != null) {
                return after;
            }
        }
    }

    @Override
    public BloomFilter<K> getBloomFilter() {
        while (true) {
            final BloomFilter<K> current = bloomFilter.get();
            if (current != null) {
                return current;
            }
            final BloomFilter<K> loaded = Vldtn.requireNonNull(
                    segmentDataSupplier.getBloomFilter(),
                    "segmentDataSupplier.getBloomFilter()");
            if (bloomFilter.compareAndSet(null, loaded)) {
                return loaded;
            }
            loaded.close();
            final BloomFilter<K> after = bloomFilter.get();
            if (after != null) {
                return after;
            }
        }
    }

    @Override
    public ScarceSegmentIndex<K> getScarceIndex() {
        while (true) {
            final ScarceSegmentIndex<K> current = scarceIndex.get();
            if (current != null) {
                return current;
            }
            final ScarceSegmentIndex<K> loaded = Vldtn.requireNonNull(
                    segmentDataSupplier.getScarceIndex(),
                    "segmentDataSupplier.getScarceIndex()");
            if (scarceIndex.compareAndSet(null, loaded)) {
                return loaded;
            }
            loaded.close();
            final ScarceSegmentIndex<K> after = scarceIndex.get();
            if (after != null) {
                return after;
            }
        }
    }

    @Override
    public void invalidate() {
        final BloomFilter<K> cachedBloom = bloomFilter.getAndSet(null);
        if (cachedBloom != null) {
            cachedBloom.close();
        }
        final SegmentDeltaCache<K, V> cachedDelta = deltaCache.getAndSet(null);
        if (cachedDelta != null) {
            cachedDelta.evictAll();
        }
        final ScarceSegmentIndex<K> cachedScarce = scarceIndex.getAndSet(null);
        if (cachedScarce != null) {
            cachedScarce.close();
        }
    }
}
