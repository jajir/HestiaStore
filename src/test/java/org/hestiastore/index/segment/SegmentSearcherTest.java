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

    @Mock
    private SegmentDataProvider<String, Long> segmentDataProvider;

    SegmentSearcher<String, Long> segmentSearcher;

    @BeforeEach
    void setUp() {
        segmentSearcher = new SegmentSearcher<>(TestData.TYPE_DESCRIPTOR_LONG);
    }

    @Test
    void test_get_tombStone() {
        when(segmentDeltaCache.get("key"))
                .thenReturn(TypeDescriptorLong.TOMBSTONE_VALUE);
        when(segmentDataProvider.getSegmentDeltaCache())
                .thenReturn(segmentDeltaCache);
        assertNull(segmentSearcher.get("key", segmentDataProvider,
                segmentIndexSearcher));
    }

    @Test
    void test_get_inCache() {
        when(segmentDeltaCache.get("key")).thenReturn(867L);
        when(segmentDataProvider.getSegmentDeltaCache())
                .thenReturn(segmentDeltaCache);
        assertEquals(867L, segmentSearcher.get("key", segmentDataProvider,
                segmentIndexSearcher));
    }

    @Test
    void test_get_notInCache_notInBloomFilter() {
        when(segmentDeltaCache.get("key")).thenReturn(null);
        when(bloomFilter.isNotStored("key")).thenReturn(true);
        when(segmentDataProvider.getSegmentDeltaCache())
                .thenReturn(segmentDeltaCache);
        when(segmentDataProvider.getBloomFilter()).thenReturn(bloomFilter);

        assertNull(segmentSearcher.get("key", segmentDataProvider,
                segmentIndexSearcher));
    }

    @Test
    void test_get_notInCache_inBloomFilter_notInScarceIndex() {
        when(segmentDeltaCache.get("key")).thenReturn(null);
        when(bloomFilter.isNotStored("key")).thenReturn(false);
        when(scarceIndex.get("key")).thenReturn(null);
        when(segmentDataProvider.getSegmentDeltaCache())
                .thenReturn(segmentDeltaCache);
        when(segmentDataProvider.getBloomFilter()).thenReturn(bloomFilter);
        when(segmentDataProvider.getScarceIndex()).thenReturn(scarceIndex);

        assertNull(segmentSearcher.get("key", segmentDataProvider,
                segmentIndexSearcher));
    }

    @Test
    void test_get_notInCache_inBloomFilter_inScarceIndex_notInSegmentSearcher() {
        when(segmentDeltaCache.get("key")).thenReturn(null);
        when(bloomFilter.isNotStored("key")).thenReturn(false);
        when(scarceIndex.get("key")).thenReturn(123);
        when(segmentIndexSearcher.search("key", 123)).thenReturn(null);
        when(segmentDataProvider.getSegmentDeltaCache())
                .thenReturn(segmentDeltaCache);
        when(segmentDataProvider.getBloomFilter()).thenReturn(bloomFilter);
        when(segmentDataProvider.getScarceIndex()).thenReturn(scarceIndex);

        assertNull(segmentSearcher.get("key", segmentDataProvider,
                segmentIndexSearcher));
        verify(bloomFilter, times(1)).incrementFalsePositive();
    }

    @Test
    void test_get_notInCache_inBloomFilter_inScarceIndex_inSegmentSearcher() {
        when(segmentDeltaCache.get("key")).thenReturn(null);
        when(bloomFilter.isNotStored("key")).thenReturn(false);
        when(scarceIndex.get("key")).thenReturn(123);
        when(segmentIndexSearcher.search("key", 123)).thenReturn(8633L);
        when(segmentDataProvider.getSegmentDeltaCache())
                .thenReturn(segmentDeltaCache);
        when(segmentDataProvider.getBloomFilter()).thenReturn(bloomFilter);
        when(segmentDataProvider.getScarceIndex()).thenReturn(scarceIndex);

        assertEquals(8633L, segmentSearcher.get("key", segmentDataProvider,
                segmentIndexSearcher));
    }

    @AfterEach
    void tearDown() {
        segmentSearcher = null;
    }

}
