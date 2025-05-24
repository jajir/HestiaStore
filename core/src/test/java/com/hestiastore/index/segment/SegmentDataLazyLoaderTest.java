package com.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.hestiastore.index.bloomfilter.BloomFilter;
import com.hestiastore.index.scarceindex.ScarceIndex;

@ExtendWith(MockitoExtension.class)
public class SegmentDataLazyLoaderTest {

    @Mock
    private SegmentDataSupplier<Integer, String> supplier;

    @Mock
    private BloomFilter<Integer> bloomFilter;

    @Mock
    private ScarceIndex<Integer> scarceIndex;

    @Mock
    private SegmentDeltaCache<Integer, String> segmentDeltaCache;

    @Test
    public void test_close_not_initialized() throws Exception {
        SegmentDataLazyLoaded<Integer, String> loader = new SegmentDataLazyLoaded<>(
                supplier);
        loader.close();

        verify(supplier, times(0)).getBloomFilter();
        verify(supplier, times(0)).getScarceIndex();
        verify(supplier, times(0)).getSegmentDeltaCache();
    }

    @Test
    public void test_init_and_close_bloom_filter() throws Exception {
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
    public void test_init_and_close_scarce_index() throws Exception {
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
    public void test_init_and_close_segment_delta_cache() throws Exception {
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

}
