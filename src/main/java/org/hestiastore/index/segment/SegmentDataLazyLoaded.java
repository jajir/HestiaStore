package org.hestiastore.index.segment;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.scarceindex.ScarceIndex;

/**
 * Provide cached lazy loaded instances of segment data objects.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public final class SegmentDataLazyLoaded<K, V>
        extends AbstractCloseableResource implements SegmentData<K, V> {

    private final SegmentDataSupplier<K, V> segmentDataSupplier;

    private SegmentDeltaCache<K, V> deltaCache;
    private BloomFilter<K> bloomFilter;
    private ScarceIndex<K> scarceIndex;

    public SegmentDataLazyLoaded(
            final SegmentDataSupplier<K, V> segmentDataSupplier) {
        this.segmentDataSupplier = Vldtn.requireNonNull(segmentDataSupplier,
                "segmentDataSupplier");
    }

    @Override
    public SegmentDeltaCache<K, V> getSegmentDeltaCache() {
        if (deltaCache == null) {
            deltaCache = segmentDataSupplier.getSegmentDeltaCache();
        }
        return deltaCache;
    }

    @Override
    public BloomFilter<K> getBloomFilter() {
        if (bloomFilter == null) {
            bloomFilter = segmentDataSupplier.getBloomFilter();
        }
        return bloomFilter;
    }

    @Override
    public ScarceIndex<K> getScarceIndex() {
        if (scarceIndex == null) {
            scarceIndex = segmentDataSupplier.getScarceIndex();
        }
        return scarceIndex;
    }

    @Override
    protected void doClose() {
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

    }

}
