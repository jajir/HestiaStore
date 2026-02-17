package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.scarceindex.ScarceSegmentIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSearcherContextTest {

    @Mock
    private SegmentResources<String, Long> dataProvider;
    @Mock
    private BloomFilter<String> bloomFilter;
    @Mock
    private ScarceSegmentIndex<String> scarceIndex;
    @Mock
    private SegmentIndexSearcher<String, Long> indexSearcher;

    private SegmentSearcherContext<String, Long> ctx;

    @BeforeEach
    void setUp() {
        ctx = SegmentSearcherContext.of("k", dataProvider, indexSearcher);
    }

    @Test
    void requires_non_null_arguments() {
        assertThrows(IllegalArgumentException.class,
                () -> SegmentSearcherContext.of(null, dataProvider,
                        indexSearcher));
        assertThrows(IllegalArgumentException.class,
                () -> SegmentSearcherContext.of("k", null, indexSearcher));
        assertThrows(IllegalArgumentException.class,
                () -> SegmentSearcherContext.of("k", dataProvider, null));
    }

    @Test
    void exposes_provider_resources() {
        when(dataProvider.getBloomFilter()).thenReturn(bloomFilter);
        when(dataProvider.getScarceIndex()).thenReturn(scarceIndex);
        assertEquals(bloomFilter, ctx.getBloomFilter());
        assertEquals(scarceIndex, ctx.getScarceIndex());
    }

    @Test
    void delegates_position_lookup_and_search() {
        when(dataProvider.getScarceIndex()).thenReturn(scarceIndex);
        when(scarceIndex.get("k")).thenReturn(5);
        when(indexSearcher.search("k", 5)).thenReturn(42L);

        assertEquals(5, ctx.getPositionFromScarceIndex());
        assertEquals(42L, ctx.searchInIndex(5));
    }

    @Test
    void increments_false_positive_through_bloom_filter() {
        when(dataProvider.getBloomFilter()).thenReturn(bloomFilter);
        ctx.incrementFalsePositive();
        verify(bloomFilter).incrementFalsePositive();
    }

    @Test
    void delegates_bloom_filter_check() {
        when(dataProvider.getBloomFilter()).thenReturn(bloomFilter);
        when(bloomFilter.isNotStored("k")).thenReturn(true);
        assertEquals(true, ctx.isNotStoredInBloomFilter());
    }

    @Test
    void retains_key() {
        assertEquals("k", ctx.getKey());
        assertNotNull(ctx.getSegmentIndexSearcher());
    }
}
