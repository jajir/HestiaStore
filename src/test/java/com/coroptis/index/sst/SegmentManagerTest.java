package com.coroptis.index.sst;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.segment.Segment;
import com.coroptis.index.segment.SegmentId;

@ExtendWith(MockitoExtension.class)
public class SegmentManagerTest {

    @Mock
    private Directory directory;

    @Mock
    private TypeDescriptor<Integer> keyTypeDescriptor;

    @Mock
    private TypeDescriptor<String> valueTypeDescriptor;

    @Mock
    private SsstIndexConf conf;

    @Test
    void test_getting_same_segmentId() throws Exception {
        final SegmentManager<Integer, String> segmentManager = new SegmentManager<>(
                directory, keyTypeDescriptor, valueTypeDescriptor, conf);

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

}
