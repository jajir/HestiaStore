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
    private SegmentDeltaCache<Integer, String> deltaCache;

    @Mock
    private SegmentDeltaCache<Integer, String> deltaCacheSecond;

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
        when(segmentDataSupplier.getSegmentDeltaCache()).thenReturn(deltaCache);
        when(segmentDataSupplier.getBloomFilter()).thenReturn(bloomFilter);
        when(segmentDataSupplier.getScarceIndex()).thenReturn(scarceIndex);

        assertSame(deltaCache, resources.getSegmentDeltaCache());
        assertSame(deltaCache, resources.getSegmentDeltaCache());
        assertSame(bloomFilter, resources.getBloomFilter());
        assertSame(bloomFilter, resources.getBloomFilter());
        assertSame(scarceIndex, resources.getScarceIndex());
        assertSame(scarceIndex, resources.getScarceIndex());

        verify(segmentDataSupplier, times(1)).getSegmentDeltaCache();
        verify(segmentDataSupplier, times(1)).getBloomFilter();
        verify(segmentDataSupplier, times(1)).getScarceIndex();
    }

    @Test
    void invalidateClosesAndEvictsLoadedResources() {
        final SegmentResourcesImpl<Integer, String> resources = new SegmentResourcesImpl<>(
                segmentDataSupplier);
        when(segmentDataSupplier.getSegmentDeltaCache()).thenReturn(deltaCache);
        when(segmentDataSupplier.getBloomFilter()).thenReturn(bloomFilter);
        when(segmentDataSupplier.getScarceIndex()).thenReturn(scarceIndex);

        resources.getSegmentDeltaCache();
        resources.getBloomFilter();
        resources.getScarceIndex();
        resources.invalidate();

        verify(deltaCache, times(1)).evictAll();
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
        verify(segmentDataSupplier, never()).getSegmentDeltaCache();
        verify(segmentDataSupplier, never()).getScarceIndex();
        verify(bloomFilter, times(1)).close();
        verify(deltaCache, never()).evictAll();
        verify(scarceIndex, never()).close();
    }

    @Test
    void reloadsResourcesAfterInvalidation() {
        final SegmentResourcesImpl<Integer, String> resources = new SegmentResourcesImpl<>(
                segmentDataSupplier);
        when(segmentDataSupplier.getSegmentDeltaCache()).thenReturn(deltaCache,
                deltaCacheSecond);

        assertSame(deltaCache, resources.getSegmentDeltaCache());
        resources.invalidate();
        assertSame(deltaCacheSecond, resources.getSegmentDeltaCache());

        verify(segmentDataSupplier, times(2)).getSegmentDeltaCache();
    }
}
