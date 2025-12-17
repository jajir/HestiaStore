package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.scarceindex.ScarceSegmentIndex;

/**
 * SegmentDataProvider that lazily loads {@link SegmentData} from a supplier
 * and caches the heavy resources (delta cache, Bloom filter, scarce index) in
 * memory until invalidated.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class SegmentDataProviderLazyLoaded<K, V>
        implements SegmentDataProvider<K, V> {

    private final SegmentDataSupplier<K, V> segmentDataSupplier;
    private SegmentData<K, V> segmentData;

    public SegmentDataProviderLazyLoaded(
            final SegmentDataSupplier<K, V> segmentDataSupplier) {
        this.segmentDataSupplier = Vldtn.requireNonNull(segmentDataSupplier,
                "segmentDataSupplier");
    }

    private SegmentData<K, V> getSegmentData() {
        if (segmentData == null) {
            segmentData = new SegmentDataLazyLoaded<>(segmentDataSupplier);
        }
        return segmentData;
    }

    @Override
    public boolean isLoaded() {
        return segmentData != null;
    }

    @Override
    public SegmentDeltaCache<K, V> getSegmentDeltaCache() {
        return getSegmentData().getSegmentDeltaCache();
    }

    @Override
    public BloomFilter<K> getBloomFilter() {
        return getSegmentData().getBloomFilter();
    }

    @Override
    public ScarceSegmentIndex<K> getScarceIndex() {
        return getSegmentData().getScarceIndex();
    }

    @Override
    public void invalidate() {
        if (segmentData != null) {
            segmentData.close();
            segmentData = null;
        }
    }
}
