package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.TestData;
import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.scarceindex.ScarceIndex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SegmentSearcherTest {

    @Mock
    private SegmentIndexSearcher<String, Long> segmentIndexSearcher;

    @Mock
    private SegmentDeltaCache<String, Long> segmentDeltaCache;

    @Mock
    private BloomFilter<String> bloomFilter;

    @Mock
    private ScarceIndex<String> scarceIndex;

    SegmentSearcher<String, Long> segmentSearcher;

    @BeforeEach
    void setUp() {
        segmentSearcher = new SegmentSearcher<>(TestData.TYPE_DESCRIPTOR_LONG,
                segmentIndexSearcher);
    }

    @Test
    void test_get_tombStone() {
        when(segmentDeltaCache.get("key"))
                .thenReturn(TypeDescriptorLong.TOMBSTONE_VALUE);
        assertNull(segmentSearcher.get("key", segmentDeltaCache, bloomFilter,
                scarceIndex));
    }

    @Test
    void test_get_inCache() {
        when(segmentDeltaCache.get("key")).thenReturn(867L);
        assertEquals(867L, segmentSearcher.get("key", segmentDeltaCache,
                bloomFilter, scarceIndex));
    }

    @Test
    void test_get_notInCache_notInBloomFilter() {
        when(segmentDeltaCache.get("key")).thenReturn(null);
        when(bloomFilter.isNotStored("key")).thenReturn(true);

        assertNull(segmentSearcher.get("key", segmentDeltaCache, bloomFilter,
                scarceIndex));
    }

    @Test
    void test_get_notInCache_inBloomFilter_notInScarceIndex() {
        when(segmentDeltaCache.get("key")).thenReturn(null);
        when(bloomFilter.isNotStored("key")).thenReturn(false);
        when(scarceIndex.get("key")).thenReturn(null);

        assertNull(segmentSearcher.get("key", segmentDeltaCache, bloomFilter,
                scarceIndex));
    }

    @Test
    void test_get_notInCache_inBloomFilter_inScarceIndex_notInSegmentSearcher() {
        when(segmentDeltaCache.get("key")).thenReturn(null);
        when(bloomFilter.isNotStored("key")).thenReturn(false);
        when(scarceIndex.get("key")).thenReturn(123);
        when(segmentIndexSearcher.search("key", 123)).thenReturn(null);

        assertNull(segmentSearcher.get("key", segmentDeltaCache, bloomFilter,
                scarceIndex));
        verify(bloomFilter, times(1)).incrementFalsePositive();
    }

    @Test
    void test_get_notInCache_inBloomFilter_inScarceIndex_inSegmentSearcher() {
        when(segmentDeltaCache.get("key")).thenReturn(null);
        when(bloomFilter.isNotStored("key")).thenReturn(false);
        when(scarceIndex.get("key")).thenReturn(123);
        when(segmentIndexSearcher.search("key", 123)).thenReturn(8633L);

        assertEquals(8633L, segmentSearcher.get("key", segmentDeltaCache,
                bloomFilter, scarceIndex));
    }

    void test_close() {
        segmentSearcher.close();
        verify(segmentIndexSearcher, times(1)).close();
    }

    @AfterEach
    void tearDown() {
        segmentSearcher = null;
    }

}
