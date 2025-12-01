package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.scarceindex.ScarceSegmentIndex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentDataLazyLoaderTest {

    @Mock
    private SegmentDataSupplier<Integer, String> supplier;

    @Mock
    private BloomFilter<Integer> bloomFilter;

    @Mock
    private ScarceSegmentIndex<Integer> scarceIndex;

    @Mock
    private SegmentDeltaCache<Integer, String> segmentDeltaCache;

    @Test
    void test_close_not_initialized() {
        SegmentDataLazyLoaded<Integer, String> loader = new SegmentDataLazyLoaded<>(
                supplier);
        loader.close();

        verify(supplier, times(0)).getBloomFilter();
        verify(supplier, times(0)).getScarceIndex();
        verify(supplier, times(0)).getSegmentDeltaCache();
    }

    @Test
    void test_init_and_close_bloom_filter() {
        SegmentDataLazyLoaded<Integer, String> loader = new SegmentDataLazyLoaded<>(
                supplier);
        when(supplier.getBloomFilter()).thenReturn(bloomFilter);

        assertEquals(bloomFilter, loader.getBloomFilter());
        assertEquals(bloomFilter, loader.getBloomFilter());
        assertEquals(bloomFilter, loader.getBloomFilter());
        loader.close();

        verify(supplier, times(1)).getBloomFilter();
        verify(supplier, times(0)).getScarceIndex();
        verify(supplier, times(0)).getSegmentDeltaCache();
    }

    @Test
    void test_init_and_close_scarce_index() {
        SegmentDataLazyLoaded<Integer, String> loader = new SegmentDataLazyLoaded<>(
                supplier);
        when(supplier.getScarceIndex()).thenReturn(scarceIndex);

        assertEquals(scarceIndex, loader.getScarceIndex());
        assertEquals(scarceIndex, loader.getScarceIndex());
        assertEquals(scarceIndex, loader.getScarceIndex());
        loader.close();

        verify(supplier, times(0)).getBloomFilter();
        verify(supplier, times(1)).getScarceIndex();
        verify(supplier, times(0)).getSegmentDeltaCache();
    }

    @Test
    void test_init_and_close_segment_delta_cache() {
        SegmentDataLazyLoaded<Integer, String> loader = new SegmentDataLazyLoaded<>(
                supplier);
        when(supplier.getSegmentDeltaCache()).thenReturn(segmentDeltaCache);

        assertEquals(segmentDeltaCache, loader.getSegmentDeltaCache());
        assertEquals(segmentDeltaCache, loader.getSegmentDeltaCache());
        assertEquals(segmentDeltaCache, loader.getSegmentDeltaCache());
        loader.close();

        verify(supplier, times(0)).getBloomFilter();
        verify(supplier, times(0)).getScarceIndex();
        verify(supplier, times(1)).getSegmentDeltaCache();
        verify(segmentDeltaCache, times(1)).evictAll();
    }

    @Test
    void getters_throw_after_close() {
        final SegmentDataLazyLoaded<Integer, String> loader = new SegmentDataLazyLoaded<>(
                supplier);
        loader.close();

        assertThrows(IllegalStateException.class, loader::getBloomFilter);
        assertThrows(IllegalStateException.class, loader::getScarceIndex);
        assertThrows(IllegalStateException.class, loader::getSegmentDeltaCache);
        verify(supplier, times(0)).getBloomFilter();
        verify(supplier, times(0)).getScarceIndex();
        verify(supplier, times(0)).getSegmentDeltaCache();
    }

}
