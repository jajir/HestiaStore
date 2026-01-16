package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.scarceindex.ScarceSegmentIndex;
import org.junit.jupiter.api.Test;

class SegmentResourcesTest {

    @Test
    void resourcesAreCachedAndInvalidated() {
        @SuppressWarnings("unchecked")
        final SegmentDataSupplier<Integer, String> supplier = (SegmentDataSupplier<Integer, String>) mock(
                SegmentDataSupplier.class);
        @SuppressWarnings("unchecked")
        final BloomFilter<Integer> bloomFilter1 = (BloomFilter<Integer>) mock(
                BloomFilter.class);
        @SuppressWarnings("unchecked")
        final BloomFilter<Integer> bloomFilter2 = (BloomFilter<Integer>) mock(
                BloomFilter.class);
        @SuppressWarnings("unchecked")
        final ScarceSegmentIndex<Integer> scarce1 = (ScarceSegmentIndex<Integer>) mock(
                ScarceSegmentIndex.class);
        @SuppressWarnings("unchecked")
        final ScarceSegmentIndex<Integer> scarce2 = (ScarceSegmentIndex<Integer>) mock(
                ScarceSegmentIndex.class);

        when(supplier.getBloomFilter()).thenReturn(bloomFilter1, bloomFilter2);
        when(supplier.getScarceIndex()).thenReturn(scarce1, scarce2);

        final SegmentResourcesImpl<Integer, String> resources = new SegmentResourcesImpl<>(
                supplier);

        assertSame(bloomFilter1, resources.getBloomFilter());
        assertSame(bloomFilter1, resources.getBloomFilter());
        assertSame(scarce1, resources.getScarceIndex());
        assertSame(scarce1, resources.getScarceIndex());

        resources.invalidate();
        verify(bloomFilter1).close();
        verify(scarce1).close();

        assertSame(bloomFilter2, resources.getBloomFilter());
        assertSame(scarce2, resources.getScarceIndex());
    }
}
