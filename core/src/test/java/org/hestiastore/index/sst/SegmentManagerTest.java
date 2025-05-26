package org.hestiastore.index.sst;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.sst.IndexConfiguration;
import org.hestiastore.index.sst.SegmentDataCache;
import org.hestiastore.index.sst.SegmentManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SegmentManagerTest {

    private final TypeDescriptor<Integer> keyTypeDescriptor = new TypeDescriptorInteger();

    private final TypeDescriptor<String> valueTypeDescriptor = new TypeDescriptorString();

    @Mock
    private Directory directory;

    @Mock
    private IndexConfiguration<Integer, String> conf;

    @Mock
    private SegmentDataCache<Integer, String> segmentDataCache;

    @Test
    void test_getting_same_segmentId() throws Exception {
        final SegmentManager<Integer, String> segmentManager = new SegmentManager<>(
                directory, keyTypeDescriptor, valueTypeDescriptor, conf,
                segmentDataCache);
        when(conf.getMaxNumberOfKeysInSegmentCache()).thenReturn(2L);

        final Segment<Integer, String> s1 = segmentManager
                .getSegment(SegmentId.of(1));
        assertNotNull(s1);

        final Segment<Integer, String> s2 = segmentManager
                .getSegment(SegmentId.of(1));
        assertNotNull(s1);

        /*
         * Verify that first object was cached and second time just returned
         * from map.
         */
        assertSame(s1, s2);
    }

    @Test
    void test_close() throws Exception {
        final SegmentManager<Integer, String> segmentManager = new SegmentManager<>(
                directory, keyTypeDescriptor, valueTypeDescriptor, conf,
                segmentDataCache);
        segmentManager.close();

        verify(segmentDataCache).invalidateAll();
    }

}
