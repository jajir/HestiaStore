package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import org.hestiastore.index.TestData;
import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.scarceindex.ScarceSegmentIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class SegmentSearcherStepDeltaCacheTest {

    @Mock
    private SegmentDeltaCache<String, Long> deltaCache;
    @Mock
    private BloomFilter<String> bloomFilter;
    @Mock
    private ScarceSegmentIndex<String> scarceIndex;
    @Mock
    private SegmentIndexSearcher<String, Long> indexSearcher;
    @Mock
    private SegmentResources<String, Long> segmentDataProvider;

    private SegmentSearcherStepDeltaCache<String, Long> step;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        step = new SegmentSearcherStepDeltaCache<>(TestData.TYPE_DESCRIPTOR_LONG);
        when(segmentDataProvider.getSegmentDeltaCache()).thenReturn(deltaCache);
        when(segmentDataProvider.getBloomFilter()).thenReturn(bloomFilter);
        when(segmentDataProvider.getScarceIndex()).thenReturn(scarceIndex);
    }

    @Test
    void stops_on_tombstone() {
        when(deltaCache.get("k")).thenReturn(TypeDescriptorLong.TOMBSTONE_VALUE);
        final var ctx = SegmentSearcherContext.of("k", segmentDataProvider,
                indexSearcher);
        final var res = new SegmentSearcherResult<Long>();

        final boolean cont = step.filter(ctx, res);

        assertFalse(cont);
        assertNull(res.getValue());
    }

    @Test
    void stops_when_value_found() {
        when(deltaCache.get("k")).thenReturn(10L);
        final var ctx = SegmentSearcherContext.of("k", segmentDataProvider,
                indexSearcher);
        final var res = new SegmentSearcherResult<Long>();

        final boolean cont = step.filter(ctx, res);

        assertFalse(cont);
        org.junit.jupiter.api.Assertions.assertEquals(10L, res.getValue());
    }

    @Test
    void continues_when_not_in_cache() {
        when(deltaCache.get("k")).thenReturn(null);
        final var ctx = SegmentSearcherContext.of("k", segmentDataProvider,
                indexSearcher);
        final var res = new SegmentSearcherResult<Long>();

        final boolean cont = step.filter(ctx, res);

        assertTrue(cont);
        assertNull(res.getValue());
    }
}
