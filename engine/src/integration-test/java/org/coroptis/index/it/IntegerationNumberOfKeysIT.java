package org.coroptis.index.it;

import static org.hestiastore.index.segment.SegmentTestHelper.closeAndAwait;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Random;

import org.hestiastore.index.chunkstore.ChunkFilterCrc32Validation;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Writing;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberValidation;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentMaintenancePolicy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IntegerationNumberOfKeysIT {
    private final SegmentId SEGMENT_ID = SegmentId.of(29);
    private static final Random RANDOM = new Random();
    private static final int NUMBER_OF_TESTING_ENTRIES = 1_000_000;
    private static final TypeDescriptor<String> TYPE_DESCRIPTOR_STRING = new TypeDescriptorShortString();
    private static final TypeDescriptor<Long> TYPE_DESCRIPTOR_LONG = new TypeDescriptorLong();

    private Directory directory;

    @BeforeEach
    public void beforeEach() {
        directory = new MemDirectory();
    }

    @Test
    void test_after_force_compact() {
        final Segment<String, Long> segment = getCommonBuilder();
        writeData(segment);
        segment.compact();

        assertEquals(NUMBER_OF_TESTING_ENTRIES, segment.getNumberOfKeys());
        closeAndAwait(segment);
    }

    @Test
    void test_after_closing() {
        Segment<String, Long> segment = getCommonBuilder();
        writeData(segment);
        segment.compact();
        closeAndAwait(segment);
        segment = getCommonBuilder();

        assertEquals(NUMBER_OF_TESTING_ENTRIES, segment.getNumberOfKeys());
    }

    @Test
    void test_after_writing() {
        Segment<String, Long> segment = getCommonBuilder();
        writeData(segment);
        assertEquals(NUMBER_OF_TESTING_ENTRIES, segment.getNumberOfKeys());
        closeAndAwait(segment);
    }

    private Segment<String, Long> getCommonBuilder() {
        return Segment.<String, Long>builder(
                directory)//
                .withId(SEGMENT_ID)//
                .withKeyTypeDescriptor(TYPE_DESCRIPTOR_STRING)//
                .withValueTypeDescriptor(TYPE_DESCRIPTOR_LONG)//
                .withMaintenancePolicy(SegmentMaintenancePolicy.none())//
                .withMaxNumberOfKeysInSegmentChunk(100)//
                .withBloomFilterIndexSizeInBytes(0)// disable bloom filter
                .withEncodingChunkFilters(//
                        List.of(new ChunkFilterMagicNumberWriting(), //
                                new ChunkFilterCrc32Writing(), //
                                new ChunkFilterDoNothing()//
                        ))//
                .withDecodingChunkFilters(//
                        List.of(new ChunkFilterMagicNumberValidation(), //
                                new ChunkFilterCrc32Validation(), //
                                new ChunkFilterDoNothing()//
                        ))//
                .build().getValue();
    }

    private void writeData(final Segment<String, Long> segment) {
        for (int i = 0; i < NUMBER_OF_TESTING_ENTRIES; i++) {
            segment.put(wrap(i), RANDOM.nextLong());
        }
        segment.flush();
    }

    /*
     * Wrap long to 10 zeros, results should look like 0000000001, 0000000002,
     * ...
     */
    private String wrap(final int l) {
        String out = String.valueOf(l);
        while (out.length() < 10) {
            out = "0" + out;
        }
        return out;
    }

}
