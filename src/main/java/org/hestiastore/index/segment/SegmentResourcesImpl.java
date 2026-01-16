package org.hestiastore.index.segment;

import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.scarceindex.ScarceSegmentIndex;

/**
 * Lazily loads and caches heavyweight segment resources such as the Bloom
 * filter and scarce index. Call {@link #invalidate()} to drop the cached
 * instances so the next access rebuilds them.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentResourcesImpl<K, V>
        implements SegmentResources<K, V> {

    private final SegmentDataSupplier<K, V> segmentDataSupplier;
    private final AtomicReference<BloomFilter<K>> bloomFilter = new AtomicReference<>();
    private final AtomicReference<ScarceSegmentIndex<K>> scarceIndex = new AtomicReference<>();

    /**
     * Creates a cached resource wrapper backed by the given supplier.
     *
     * @param segmentDataSupplier supplier for resource instances
     */
    public SegmentResourcesImpl(
            final SegmentDataSupplier<K, V> segmentDataSupplier) {
        this.segmentDataSupplier = Vldtn.requireNonNull(segmentDataSupplier,
                "segmentDataSupplier");
    }

    /**
     * Returns the cached Bloom filter, loading it on first access.
     *
     * @return Bloom filter instance
     */
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

    /**
     * Returns the cached scarce index, loading it on first access.
     *
     * @return scarce index instance
     */
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

    /**
     * Clears cached resources and closes any open handles.
     */
    @Override
    public void invalidate() {
        final BloomFilter<K> cachedBloom = bloomFilter.getAndSet(null);
        if (cachedBloom != null) {
            cachedBloom.close();
        }
        final ScarceSegmentIndex<K> cachedScarce = scarceIndex.getAndSet(null);
        if (cachedScarce != null) {
            cachedScarce.close();
        }
    }
}
