package org.coroptis.index.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Random;

import org.hestiastore.index.PairWriter;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IntegerationNumberOfKeysIT {
    private final SegmentId SEGMENT_ID = SegmentId.of(29);
    private static final Random RANDOM = new Random();
    private static final int NUMBER_OF_TESTING_PAIRS = 1_000_000;
    private static final TypeDescriptor<String> TYPE_DESCRIPTOR_STRING = new TypeDescriptorString();
    private static final TypeDescriptor<Long> TYPE_DESCRIPTOR_LONG = new TypeDescriptorLong();

    private Directory directory;

    @BeforeEach
    public void beforeEach() {
        directory = new MemDirectory();
    }

    @Test
    public void test_after_force_compact() {
        final Segment<String, Long> segment = getCommonBuilder();
        writeData(segment);
        segment.forceCompact();

        assertEquals(NUMBER_OF_TESTING_PAIRS, segment.getNumberOfKeys());
        segment.close();
    }

    @Test
    public void test_after_closing() {
        Segment<String, Long> segment = getCommonBuilder();
        writeData(segment);
        segment.forceCompact();
        segment.close();
        segment = getCommonBuilder();

        assertEquals(NUMBER_OF_TESTING_PAIRS, segment.getNumberOfKeys());
    }

    @Test
    public void test_after_writing() {
        Segment<String, Long> segment = getCommonBuilder();
        writeData(segment);
        assertEquals(NUMBER_OF_TESTING_PAIRS, segment.getNumberOfKeys());
        segment.close();
    }

    private Segment<String, Long> getCommonBuilder() {
        return Segment.<String, Long>builder()//
                .withDirectory(directory)//
                .withId(SEGMENT_ID)//
                .withKeyTypeDescriptor(TYPE_DESCRIPTOR_STRING)//
                .withValueTypeDescriptor(TYPE_DESCRIPTOR_LONG)//
                .withMaxNumberOfKeysInSegmentCache(3)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(100)//
                .withMaxNumberOfKeysInIndexPage(100)//
                .withBloomFilterIndexSizeInBytes(0)// disable bloom filter
                .withMaxNumberOfKeysInSegmentCache(1000)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(100_000)//
                .build();
    }

    private void writeData(final Segment<String, Long> segment) {
        try (PairWriter<String, Long> pairWriter = segment.openWriter()) {
            for (int i = 0; i < NUMBER_OF_TESTING_PAIRS; i++) {
                pairWriter.put(wrap(i), RANDOM.nextLong());
            }
        }
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
