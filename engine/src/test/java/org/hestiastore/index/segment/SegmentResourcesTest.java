package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertSame;
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
class SegmentResourcesTest {

    @Mock
    private SegmentDataSupplier<Integer, String> supplier;
    @Mock
    private BloomFilter<Integer> bloomFilter1;
    @Mock
    private BloomFilter<Integer> bloomFilter2;
    @Mock
    private ScarceSegmentIndex<Integer> scarce1;
    @Mock
    private ScarceSegmentIndex<Integer> scarce2;

    private SegmentResourcesImpl<Integer, String> resources;

    @BeforeEach
    void setUp() {
        resources = new SegmentResourcesImpl<>(supplier);
    }

    @AfterEach
    void tearDown() {
        resources = null;
    }

    @Test
    void resourcesAreCachedAndInvalidated() {
        when(supplier.getBloomFilter()).thenReturn(bloomFilter1)
                .thenReturn(bloomFilter2);
        when(supplier.getScarceIndex()).thenReturn(scarce1).thenReturn(scarce2);

        assertSame(bloomFilter1, resources.getBloomFilter());
        assertSame(bloomFilter1, resources.getBloomFilter());
        assertSame(scarce1, resources.getScarceIndex());
        assertSame(scarce1, resources.getScarceIndex());

        resources.invalidate();
        verify(bloomFilter1).close();
        verify(scarce1).close();

        assertSame(bloomFilter2, resources.getBloomFilter());
        assertSame(scarce2, resources.getScarceIndex());
    }
}
