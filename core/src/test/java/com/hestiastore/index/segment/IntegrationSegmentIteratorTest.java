package com.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hestiastore.index.Pair;
import com.hestiastore.index.PairIterator;
import com.hestiastore.index.datatype.TypeDescriptorInteger;
import com.hestiastore.index.datatype.TypeDescriptorString;
import com.hestiastore.index.directory.Directory;
import com.hestiastore.index.directory.MemDirectory;

/**
 * This test case verify high level segment contract describe in exmples in
 * documentation.
 */
public class IntegrationSegmentIteratorTest extends AbstractSegmentTest {

    private final TypeDescriptorString tds = new TypeDescriptorString();
    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final SegmentId id = SegmentId.of(29);
    private Directory directory;
    private Segment<String, Integer> segment;

    private final List<Pair<String, Integer>> indexFile = Arrays.asList(//
            Pair.of("a", 20), //
            Pair.of("b", 30), //
            Pair.of("c", 40));

    private final List<Pair<String, Integer>> deltaCache = Arrays.asList(//
            Pair.of("a", 25), //
            Pair.of("e", 28), //
            Pair.of("b", tdi.getTombstone()));

    private final List<Pair<String, Integer>> resultData = Arrays.asList(//
            Pair.of("a", 25), //
            Pair.of("c", 40), //
            Pair.of("e", 28));

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        segment = Segment.<String, Integer>builder()//
                .withDirectory(directory)//
                .withId(id)//
                .withKeyTypeDescriptor(tds)//
                .withValueTypeDescriptor(tdi)//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withBloomFilterIndexSizeInBytes(0)//
                .build();

        writePairs(segment, indexFile);
        segment.forceCompact();
        writePairs(segment, deltaCache);
        /*
         * Now Content of main sst index file and delta cache should be as
         * described in documentation
         */
    }

    @Test
    void test_case_1_read_data() {
        verifySegmentSearch(segment, resultData);
        verifySegmentData(segment, resultData);
    }

    @Test
    void test_case_5_compact_after_addding_pair() {
        try (final PairIterator<String, Integer> iterator = segment
                .openIterator()) {
            assertTrue(iterator.hasNext());
            assertEquals(Pair.of("a", 25), iterator.next());

            // write <c, 10>
            writePairs(segment, Arrays.asList(Pair.of("c", 10)));
            segment.forceCompact();

            assertFalse(iterator.hasNext());
        }
    }

}
