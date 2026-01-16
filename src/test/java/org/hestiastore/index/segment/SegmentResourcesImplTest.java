package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.scarceindex.ScarceSegmentIndex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentResourcesImplTest {

    @Mock
    private SegmentDataSupplier<Integer, String> segmentDataSupplier;

    @Mock
    private BloomFilter<Integer> bloomFilter;

    @Mock
    private ScarceSegmentIndex<Integer> scarceIndex;

    @Test
    void constructorRejectsNullSupplier() {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentResourcesImpl<>(null));
    }

    @Test
    void invalidateWithoutLoadDoesNotTouchSupplier() {
        final SegmentResourcesImpl<Integer, String> resources = new SegmentResourcesImpl<>(
                segmentDataSupplier);

        resources.invalidate();

        verifyNoInteractions(segmentDataSupplier);
    }

    @Test
    void loadsResourcesLazilyAndCachesInstances() {
        final SegmentResourcesImpl<Integer, String> resources = new SegmentResourcesImpl<>(
                segmentDataSupplier);
        when(segmentDataSupplier.getBloomFilter()).thenReturn(bloomFilter);
        when(segmentDataSupplier.getScarceIndex()).thenReturn(scarceIndex);

        assertSame(bloomFilter, resources.getBloomFilter());
        assertSame(bloomFilter, resources.getBloomFilter());
        assertSame(scarceIndex, resources.getScarceIndex());
        assertSame(scarceIndex, resources.getScarceIndex());

        verify(segmentDataSupplier, times(1)).getBloomFilter();
        verify(segmentDataSupplier, times(1)).getScarceIndex();
    }

    @Test
    void invalidateClosesAndEvictsLoadedResources() {
        final SegmentResourcesImpl<Integer, String> resources = new SegmentResourcesImpl<>(
                segmentDataSupplier);
        when(segmentDataSupplier.getBloomFilter()).thenReturn(bloomFilter);
        when(segmentDataSupplier.getScarceIndex()).thenReturn(scarceIndex);

        resources.getBloomFilter();
        resources.getScarceIndex();
        resources.invalidate();

        verify(bloomFilter, times(1)).close();
        verify(scarceIndex, times(1)).close();
    }

    @Test
    void invalidateDoesNotTouchUnloadedResources() {
        final SegmentResourcesImpl<Integer, String> resources = new SegmentResourcesImpl<>(
                segmentDataSupplier);
        when(segmentDataSupplier.getBloomFilter()).thenReturn(bloomFilter);

        resources.getBloomFilter();
        resources.invalidate();

        verify(segmentDataSupplier, times(1)).getBloomFilter();
        verify(segmentDataSupplier, never()).getScarceIndex();
        verify(bloomFilter, times(1)).close();
        verify(scarceIndex, never()).close();
    }

    @Test
    void reloadsBloomFilterAfterInvalidation() {
        final SegmentResourcesImpl<Integer, String> resources = new SegmentResourcesImpl<>(
                segmentDataSupplier);
        final BloomFilter<Integer> bloomFilterSecond = org.mockito.Mockito
                .mock(BloomFilter.class);
        when(segmentDataSupplier.getBloomFilter()).thenReturn(bloomFilter,
                bloomFilterSecond);

        assertSame(bloomFilter, resources.getBloomFilter());
        resources.invalidate();
        assertSame(bloomFilterSecond, resources.getBloomFilter());

        verify(segmentDataSupplier, times(2)).getBloomFilter();
    }
}
