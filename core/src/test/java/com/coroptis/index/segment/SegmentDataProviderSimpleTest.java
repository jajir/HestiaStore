package com.coroptis.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SegmentDataProviderSimpleTest {

    @Mock
    private SegmentDataFactory<Integer, String> segmentDataFactory;

    @Mock
    private SegmentData<Integer, String> segmentData;

    @Mock
    private SegmentDeltaCache<Integer, String> segmentDeltaCache;

    @Test
    void test_getSegmentDataFactoryIsNull() {
        final Exception err = assertThrows(NullPointerException.class,
                () -> new SegmentDataProviderSimple<>(null));

        assertEquals("segmentDataFactory cannot be null", err.getMessage());
    }

    @Test
    void test_getSegmentData() {
        final SegmentDataProviderSimple<Integer, String> provider = new SegmentDataProviderSimple<>(
                segmentDataFactory);
        when(segmentDataFactory.getSegmentData()).thenReturn(segmentData);
        when(segmentData.getSegmentDeltaCache()).thenReturn(segmentDeltaCache);

        assertEquals(segmentDeltaCache, provider.getSegmentDeltaCache());
    }

    @Test
    void test_getSegmentData_areLoadedOnlyOnce() {
        final SegmentDataProviderSimple<Integer, String> provider = new SegmentDataProviderSimple<>(
                segmentDataFactory);
        when(segmentDataFactory.getSegmentData()).thenReturn(segmentData);
        when(segmentData.getSegmentDeltaCache()).thenReturn(segmentDeltaCache);

        // verify that the segmentData is loaded only once
        assertSame(segmentDeltaCache, provider.getSegmentDeltaCache());
        assertSame(segmentDeltaCache, provider.getSegmentDeltaCache());

        verify(segmentDataFactory, times(1)).getSegmentData();
    }

    @Test
    void test_invalidate() {
        final SegmentDataProviderSimple<Integer, String> provider = new SegmentDataProviderSimple<>(
                segmentDataFactory);
        when(segmentDataFactory.getSegmentData()).thenReturn(segmentData);
        when(segmentData.getSegmentDeltaCache()).thenReturn(segmentDeltaCache);

        assertSame(segmentDeltaCache, provider.getSegmentDeltaCache());

        provider.invalidate();

        assertSame(segmentDeltaCache, provider.getSegmentDeltaCache());

        verify(segmentDataFactory, times(2)).getSegmentData();
    }

}
