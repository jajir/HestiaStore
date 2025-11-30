package org.hestiastore.index.segmentindex;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.hestiastore.index.segment.SegmentData;
import org.hestiastore.index.segment.SegmentDataFactory;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentDataProviderFromMainCacheTest {

    @Mock
    private SegmentDataCache<Integer, String> cache;

    @Mock
    private SegmentDataFactory<Integer, String> factory;

    @Mock
    private SegmentData<Integer, String> data;

    private SegmentId segmentId = SegmentId.of(42);

    private SegmentDataProviderFromMainCache<Integer, String> provider;

    @BeforeEach
    void setUp() {
        provider = new SegmentDataProviderFromMainCache<>(segmentId, cache,
                factory);
    }

    @Test
    void invalidate_should_close_segment_data() {
        provider.invalidate();

        verify(cache, times(1)).invalidate(segmentId);
        verify(cache, never()).getSegmentData(segmentId);
    }
}
