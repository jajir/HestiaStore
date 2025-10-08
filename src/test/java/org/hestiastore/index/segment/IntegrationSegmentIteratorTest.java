package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Validation;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Writing;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberValidation;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This test case verify high level segment contract describe in exmples in
 * documentation.
 */
class IntegrationSegmentIteratorTest extends AbstractSegmentTest {

    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();
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
        verifySegmentData(segment, resultData);
        verifySegmentSearch(segment, resultData);
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
