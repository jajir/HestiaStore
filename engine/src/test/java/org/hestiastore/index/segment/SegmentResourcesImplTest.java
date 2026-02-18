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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
    private BloomFilter<Integer> bloomFilterSecond;

    @Mock
    private ScarceSegmentIndex<Integer> scarceIndex;

    private SegmentResourcesImpl<Integer, String> resources;

    @BeforeEach
    void setUp() {
        resources = new SegmentResourcesImpl<>(segmentDataSupplier);
    }

    @AfterEach
    void tearDown() {
        resources = null;
    }

    @Test
    void constructorRejectsNullSupplier() {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentResourcesImpl<>(null));
    }

    @Test
    void invalidateWithoutLoadDoesNotTouchSupplier() {
        resources.invalidate();

        verifyNoInteractions(segmentDataSupplier);
    }

    @Test
    void loadsResourcesLazilyAndCachesInstances() {
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
        when(segmentDataSupplier.getBloomFilter()).thenReturn(bloomFilter)
                .thenReturn(bloomFilterSecond);

        assertSame(bloomFilter, resources.getBloomFilter());
        resources.invalidate();
        assertSame(bloomFilterSecond, resources.getBloomFilter());

        verify(segmentDataSupplier, times(2)).getBloomFilter();
    }
}
