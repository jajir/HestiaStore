package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberValidation;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.DirectoryFacade;
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
                2000, // maxNumberOfKeysInDeltaCacheDuringWriting
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
                .withDirectoryFacade(DirectoryFacade.of(DIRECTORY))//
                .withId(SEGMENT_ID)//
                .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)//
                .withSegmentConf(conf)//
        ;
        SegmentImpl<Integer, String> seg = builder.build();
        SegmentConf ret = seg.getSegmentConf();
        assertEquals(1024, ret.getDiskIoBufferSize());

        assertEquals(1, ret.getEncodingChunkFilters().size());
        assertEquals(1, ret.getDecodingChunkFilters().size());

        assertEquals(ChunkFilterMagicNumberWriting.class,
                ret.getEncodingChunkFilters().get(0).getClass());
        assertEquals(ChunkFilterMagicNumberValidation.class,
                ret.getDecodingChunkFilters().get(0).getClass());
    }

}
