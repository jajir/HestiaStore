package org.hestiastore.index.segment;

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
    private volatile BloomFilter<K> bloomFilter;
    private volatile ScarceSegmentIndex<K> scarceIndex;

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
        BloomFilter<K> current = bloomFilter;
        if (current != null) {
            return current;
        }
        synchronized (this) {
            if (bloomFilter == null) {
                bloomFilter = Vldtn.requireNonNull(
                        segmentDataSupplier.getBloomFilter(),
                        "segmentDataSupplier.getBloomFilter()");
            }
            return bloomFilter;
        }
    }

    /**
     * Returns the cached scarce index, loading it on first access.
     *
     * @return scarce index instance
     */
    @Override
    public ScarceSegmentIndex<K> getScarceIndex() {
        ScarceSegmentIndex<K> current = scarceIndex;
        if (current != null) {
            return current;
        }
        synchronized (this) {
            if (scarceIndex == null) {
                scarceIndex = Vldtn.requireNonNull(
                        segmentDataSupplier.getScarceIndex(),
                        "segmentDataSupplier.getScarceIndex()");
            }
            return scarceIndex;
        }
    }

    /**
     * Clears cached resources and closes any open handles.
     */
    @Override
    public void invalidate() {
        synchronized (this) {
            if (bloomFilter != null) {
                bloomFilter.close();
                bloomFilter = null;
            }
            if (scarceIndex != null) {
                scarceIndex.close();
                scarceIndex = null;
            }
        }
    }
}
