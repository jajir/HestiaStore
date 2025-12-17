package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.scarceindex.ScarceSegmentIndex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentDataProviderLazyLoadedTest {

    @Mock
    private SegmentDataSupplier<Integer, String> segmentDataSupplier;

    @Mock
    private SegmentDeltaCache<Integer, String> deltaCache;

    @Mock
    private BloomFilter<Integer> bloomFilter;

    @Mock
    private ScarceSegmentIndex<Integer> scarceIndex;

    @Test
    void constructorRejectsNullSupplier() {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentDataProviderLazyLoaded<>(null));
    }

    @Test
    void loadsDataLazilyAndCachesResults() {
        final SegmentDataProviderLazyLoaded<Integer, String> provider = new SegmentDataProviderLazyLoaded<>(
                segmentDataSupplier);
        when(segmentDataSupplier.getSegmentDeltaCache()).thenReturn(deltaCache);
        when(segmentDataSupplier.getBloomFilter()).thenReturn(bloomFilter);
        when(segmentDataSupplier.getScarceIndex()).thenReturn(scarceIndex);

        assertFalse(provider.isLoaded());
        assertSame(deltaCache, provider.getSegmentDeltaCache());
        assertSame(bloomFilter, provider.getBloomFilter());
        assertSame(scarceIndex, provider.getScarceIndex());

        verify(segmentDataSupplier, times(1)).getSegmentDeltaCache();
        verify(segmentDataSupplier, times(1)).getBloomFilter();
        verify(segmentDataSupplier, times(1)).getScarceIndex();
    }

    @Test
    void invalidateClosesAndReloadsOnNextAccess() {
        final SegmentDataProviderLazyLoaded<Integer, String> provider = new SegmentDataProviderLazyLoaded<>(
                segmentDataSupplier);
        when(segmentDataSupplier.getSegmentDeltaCache()).thenReturn(deltaCache);

        assertSame(deltaCache, provider.getSegmentDeltaCache());
        provider.invalidate();

        // next access forces reload
        assertSame(deltaCache, provider.getSegmentDeltaCache());
        verify(segmentDataSupplier, times(2)).getSegmentDeltaCache();
    }
}
