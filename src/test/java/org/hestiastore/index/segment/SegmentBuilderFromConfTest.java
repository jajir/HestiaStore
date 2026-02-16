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
    void test_verify_that_builder_configuration_is_used() {
        final SegmentBuilder<Integer, String> builder = Segment
                .<Integer, String>builder(
                        org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                                .wrap(DIRECTORY))//
                .withId(SEGMENT_ID)//
                .withKeyTypeDescriptor(KEY_TYPE_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_TYPE_DESCRIPTOR)//
                .withMaxNumberOfKeysInSegmentWriteCache(500)//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(1000)//
                .withMaxNumberOfKeysInSegmentCache(5000)//
                .withMaxNumberOfKeysInSegmentChunk(50)//
                .withBloomFilterNumberOfHashFunctions(3)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01)//
                .withDiskIoBufferSize(1024)//
                .withMaintenancePolicy(SegmentMaintenancePolicy.none())//
                .withEncodingChunkFilters(
                        List.of(new ChunkFilterMagicNumberWriting()))//
                .withDecodingChunkFilters(
                        List.of(new ChunkFilterMagicNumberValidation()))//
        ;
        Segment<Integer, String> seg = builder.build().getValue();
        assertEquals(SegmentResultStatus.OK, seg.put(1, "A").getStatus());
        assertEquals(SegmentResultStatus.OK, seg.flush().getStatus());
        final SegmentResult<String> result = seg.get(1);
        assertEquals(SegmentResultStatus.OK, result.getStatus());
        assertEquals("A", result.getValue());
        seg.close();
    }

}
