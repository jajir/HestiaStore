package org.hestiastore.index.sst;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentManagerTest {

    private final TypeDescriptor<Integer> keyTypeDescriptor = new TypeDescriptorInteger();

    private final TypeDescriptor<String> valueTypeDescriptor = new TypeDescriptorShortString();

    @Mock
    private Directory directory;

    @Mock
    private IndexConfiguration<Integer, String> conf;

    @Mock
    private SegmentDataCache<Integer, String> segmentDataCache;

    @Test
    void test_getting_same_segmentId() {
        final SegmentRegistry<Integer, String> segmentRegistry = new SegmentRegistry<>(
                directory, keyTypeDescriptor, valueTypeDescriptor, conf,
                segmentDataCache);
        when(conf.getMaxNumberOfKeysInSegmentCache()).thenReturn(2L);
        when(conf.getDiskIoBufferSize()).thenReturn(1024);

        final Segment<Integer, String> s1 = segmentRegistry
                .getSegment(SegmentId.of(1));
        assertNotNull(s1);

        final Segment<Integer, String> s2 = segmentRegistry
                .getSegment(SegmentId.of(1));
        assertNotNull(s1);

        /*
         * Verify that first object was cached and second time just returned
         * from map.
         */
        assertSame(s1, s2);
    }

    @Test
    void test_close() {
        final SegmentRegistry<Integer, String> segmentRegistry = new SegmentRegistry<>(
                directory, keyTypeDescriptor, valueTypeDescriptor, conf,
                segmentDataCache);
        segmentRegistry.close();

        verify(segmentDataCache).invalidateAll();
    }

}
