package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
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

    private final List<Entry<String, Integer>> indexFile = Arrays.asList(//
            Entry.of("a", 20), //
            Entry.of("b", 30), //
            Entry.of("c", 40));

    private final List<Entry<String, Integer>> deltaCache = Arrays.asList(//
            Entry.of("a", 25), //
            Entry.of("e", 28), //
            Entry.of("b", tdi.getTombstone()));

    private final List<Entry<String, Integer>> resultData = Arrays.asList(//
            Entry.of("a", 25), //
            Entry.of("c", 40), //
            Entry.of("e", 28));

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        segment = Segment.<String, Integer>builder(
                org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                        .wrap(directory))//
                .withId(id)//
                .withKeyTypeDescriptor(tds)//
                .withValueTypeDescriptor(tdi)//
                .withMaintenancePolicy(SegmentMaintenancePolicy.none())//
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
                .build().getValue();

        writeEntries(segment, indexFile);
        assertEquals(SegmentResultStatus.OK, segment.compact().getStatus());
        writeEntries(segment, deltaCache);
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
    void test_case_5_compact_after_addding_entry() {
        final SegmentResult<EntryIterator<String, Integer>> result = segment
                .openIterator();
        assertEquals(SegmentResultStatus.OK, result.getStatus());
        try (final EntryIterator<String, Integer> iterator = result
                .getValue()) {
            assertTrue(iterator.hasNext());
            assertEquals(Entry.of("a", 25), iterator.next());

            // write <c, 10>
            writeEntries(segment, Arrays.asList(Entry.of("c", 10)));
            assertEquals(SegmentResultStatus.OK,
                    segment.compact().getStatus());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_case_6_compact_includes_write_cache_and_clears_it() {
        assertEquals(SegmentResultStatus.OK,
                segment.put("g", 13).getStatus());
        assertEquals(SegmentResultStatus.OK, segment.compact().getStatus());

        verifySegmentSearch(segment,
                Arrays.asList(Entry.of("a", 25), Entry.of("c", 40),
                        Entry.of("e", 28), Entry.of("g", 13)));
        verifySegmentData(segment,
                Arrays.asList(Entry.of("a", 25), Entry.of("c", 40),
                        Entry.of("e", 28), Entry.of("g", 13)));

        assertEquals(0, segment.getNumberOfKeysInWriteCache());
    }

}
