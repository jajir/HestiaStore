package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberValidation;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

public class SegmentBuilderFromConfTest {

    private static final SegmentId SEGMENT_ID = SegmentId.of(27);
    private static final Directory DIRECTORY = new MemDirectory();
    private static final TypeDescriptor<String> VALUE_TYPE_DESCRIPTOR = new TypeDescriptorShortString();
    private static final TypeDescriptor<Integer> KEY_TYPE_DESCRIPTOR = new TypeDescriptorInteger();

    @Test
    void test_verify_that_given_segment_conf_is_used() {
        SegmentConf conf = new SegmentConf(1000, // maxNumberOfKeysInSegmentDeltaCache
                500, // maxNumberOfKeysInSegmentWriteCache
                50, // maxNumberOfKeysInIndexPage
                3, // bloomFilterNumberOfHashFunctions
                1024, // bloomFilterIndexSizeInBytes
                0.01, // bloomFilterProbabilityOfFalsePositive
                1024, // diskIoBufferSize
                List.of(new ChunkFilterMagicNumberWriting()), //
                List.of(new ChunkFilterMagicNumberValidation())//
        );
        final SegmentBuilder<Integer, String> builder = Segment
                .<Integer, String>builder()//
                .withAsyncDirectory(
                        org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                                .wrap(DIRECTORY))//
                .withId(SEGMENT_ID)//
                .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)//
                .withSegmentConf(conf)//
        ;
        SegmentImpl<Integer, String> seg = builder.build();
        seg.put(1, "A");
        seg.flush();
        assertEquals("A", seg.get(1));
        seg.close();
    }

}
