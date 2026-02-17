package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.scarceindex.ScarceSegmentIndex;
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
    private BloomFilter<String> bloomFilter;

    @Mock
    private ScarceSegmentIndex<String> scarceIndex;

    @Mock
    private SegmentResources<String, Long> segmentDataProvider;

    SegmentSearcher<String, Long> segmentSearcher;

    @BeforeEach
    void setUp() {
        segmentSearcher = new SegmentSearcher<>();
    }

    @Test
    void test_get_notInBloomFilter() {
        when(bloomFilter.isNotStored("key")).thenReturn(true);
        when(segmentDataProvider.getBloomFilter()).thenReturn(bloomFilter);

        assertNull(segmentSearcher.get("key", segmentDataProvider,
                segmentIndexSearcher));
    }

    @Test
    void test_get_inBloomFilter_notInScarceIndex() {
        when(bloomFilter.isNotStored("key")).thenReturn(false);
        when(scarceIndex.get("key")).thenReturn(null);
        when(segmentDataProvider.getBloomFilter()).thenReturn(bloomFilter);
        when(segmentDataProvider.getScarceIndex()).thenReturn(scarceIndex);

        assertNull(segmentSearcher.get("key", segmentDataProvider,
                segmentIndexSearcher));
    }

    @Test
    void test_get_inBloomFilter_inScarceIndex_notInIndex() {
        when(bloomFilter.isNotStored("key")).thenReturn(false);
        when(scarceIndex.get("key")).thenReturn(123);
        when(segmentIndexSearcher.search("key", 123)).thenReturn(null);
        when(segmentDataProvider.getBloomFilter()).thenReturn(bloomFilter);
        when(segmentDataProvider.getScarceIndex()).thenReturn(scarceIndex);

        assertNull(segmentSearcher.get("key", segmentDataProvider,
                segmentIndexSearcher));
        verify(bloomFilter, times(1)).incrementFalsePositive();
    }

    @Test
    void test_get_inBloomFilter_inScarceIndex_inIndex() {
        when(bloomFilter.isNotStored("key")).thenReturn(false);
        when(scarceIndex.get("key")).thenReturn(123);
        when(segmentIndexSearcher.search("key", 123)).thenReturn(8633L);
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
