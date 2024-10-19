package com.coroptis.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coroptis.index.bloomfilter.BloomFilter;
import com.coroptis.index.scarceindex.ScarceIndex;

@ExtendWith(MockitoExtension.class)
public class SegmentDataLazyLoaderTest {

    @Mock
    private SegmentDataProvider<Integer, String> provider;

    @Mock
    private BloomFilter<Integer> bloomFilter;

    @Mock
    private ScarceIndex<Integer> scarceIndex;

    @Mock
    private SegmentDeltaCache<Integer, String> segmentDeltaCache;

    @Test
    public void test_close_not_initialized() throws Exception {
        SegmentDataLazyLoader<Integer, String> loader = new SegmentDataLazyLoader<>(provider);
        loader.close();

        verify(provider, times(0)).getBloomFilter();
        verify(provider, times(0)).getScarceIndex();
        verify(provider, times(0)).getSegmentDeltaCache();
    }

    @Test
    public void test_init_and_close_bloom_filter() throws Exception {
        SegmentDataLazyLoader<Integer, String> loader = new SegmentDataLazyLoader<>(provider);
        when(provider.getBloomFilter()).thenReturn(bloomFilter);

        assertEquals(bloomFilter, loader.getBloomFilter());
        assertEquals(bloomFilter, loader.getBloomFilter());
        assertEquals(bloomFilter, loader.getBloomFilter());
        loader.close();

        verify(provider, times(1)).getBloomFilter();
        verify(provider, times(0)).getScarceIndex();
        verify(provider, times(0)).getSegmentDeltaCache();
    }

    @Test
    public void test_init_and_close_scarce_index() throws Exception {
        SegmentDataLazyLoader<Integer, String> loader = new SegmentDataLazyLoader<>(provider);
        when(provider.getScarceIndex()).thenReturn(scarceIndex);

        assertEquals(scarceIndex, loader.getScarceIndex());
        assertEquals(scarceIndex, loader.getScarceIndex());
        assertEquals(scarceIndex, loader.getScarceIndex());
        loader.close();

        verify(provider, times(0)).getBloomFilter();
        verify(provider, times(1)).getScarceIndex();
        verify(provider, times(0)).getSegmentDeltaCache();
    }

    @Test
    public void test_init_and_close_segment_delta_cache() throws Exception {
        SegmentDataLazyLoader<Integer, String> loader = new SegmentDataLazyLoader<>(provider);
        when(provider.getSegmentDeltaCache()).thenReturn(segmentDeltaCache);

        assertEquals(segmentDeltaCache, loader.getSegmentDeltaCache());
        assertEquals(segmentDeltaCache, loader.getSegmentDeltaCache());
        assertEquals(segmentDeltaCache, loader.getSegmentDeltaCache());
        loader.close();

        verify(provider, times(0)).getBloomFilter();
        verify(provider, times(0)).getScarceIndex();
        verify(provider, times(1)).getSegmentDeltaCache();
        verify(segmentDeltaCache, times(1)).evictAll();
    }

}
