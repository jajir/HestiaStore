package com.coroptis.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.PairWriter;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

/**
 * This test case verify high level segment contract describe in exmples in
 * documentation.
 */
public class SegmentIteratorTest extends AbstractSegmentTest {

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
        segment = Segment
                .<String, Integer>builder()//
                .withDirectory(directory)//
                .withId(id)//
                .withKeyTypeDescriptor(tds)//
                .withValueTypeDescriptor(tdi)//
                .withMaxNumberOfKeysInSegmentMemory(10)//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .build();

        writePairs(segment, indexFile);
        segment.forceCompact();
        writePairs(segment, deltaCache);
        /*
         * Now Content of main sst index file and delta cache should be as described in
         * documentation
         */
    }

    @Test
    void test_case_1_read_data() {
        verifySegmentSearch(segment, resultData);
        verifySegmentData(segment, resultData);
    }

    @Test
    void test_case_2_change_existing_pair() {
        try(final PairIterator<String, Integer> iterator = segment.openIterator()) {
            assertTrue(iterator.hasNext());
            assertEquals(Pair.of("a",25), iterator.next());

            //write <c, 10>
            writePairs(segment,    Arrays.asList(Pair.of("c", 10)));

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of("c",10), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of("e",28), iterator.next());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_case_3_add_pair() {
        try(final PairIterator<String, Integer> iterator = segment.openIterator()) {
            assertTrue(iterator.hasNext());
            assertEquals(Pair.of("a",25), iterator.next());

            //write <c, 10>
            writePairs(segment,   Arrays.asList(Pair.of("d", 10)));

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of("c",40), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of("e",28), iterator.next());

            assertFalse(iterator.hasNext());
        }
    }

    
    @Test
    void test_case_4_delete_pair() {
        try(final PairIterator<String, Integer> iterator = segment.openIterator()) {
            assertTrue(iterator.hasNext());
            assertEquals(Pair.of("a",25), iterator.next());

            //delete <c>
            writePairs(segment,    Arrays.asList(Pair.of("c", tdi.getTombstone())));

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of("e",28), iterator.next());

            assertFalse(iterator.hasNext());
        }
    }


    @Test
    void test_case_5_compact_after_addding_pair() {
        try(final PairIterator<String, Integer> iterator = segment.openIterator()) {
            assertTrue(iterator.hasNext());
            assertEquals(Pair.of("a",25), iterator.next());

            //write <c, 10>
            writePairs(segment,    Arrays.asList(Pair.of("c", 10)));
            segment.forceCompact();

            assertFalse(iterator.hasNext());
        }
    }


}
