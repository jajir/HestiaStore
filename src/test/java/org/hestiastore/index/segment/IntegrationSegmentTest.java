package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.hestiastore.index.AbstractDataTest;
import org.hestiastore.index.Entry;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Validation;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Writing;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberValidation;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class IntegrationSegmentTest extends AbstractSegmentTest {

    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();
    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    private final List<Entry<Integer, String>> testDataSet = Arrays.asList(
            Entry.of(2, "a"), Entry.of(5, "d"), Entry.of(10, "i"),
            Entry.of(8, "g"), Entry.of(7, "f"), Entry.of(3, "b"),
            Entry.of(4, "c"), Entry.of(6, "e"), Entry.of(9, "h"));
    private final List<Entry<Integer, String>> sortedTestDataSet = testDataSet
            .stream().sorted((entry1, entry2) -> {
                return entry1.getKey() - entry2.getKey();
            }).toList();

    @ParameterizedTest
    @MethodSource("segmentProvider")
    void test_empty_segment_stats(final TypeDescriptorInteger tdi,
            final TypeDescriptorShortString tds, final Directory directory,
            final Segment<Integer, String> seg,
            final int expectedNumberKeysInScarceIndex,
            int expectedNumberOfFiles) {

        assertEquals(SegmentResultStatus.OK, seg.compact().getStatus());
        verifyCacheFiles(directory);

        verifySegmentData(seg, Arrays.asList());

        final SegmentStats stats = seg.getStats();
        assertEquals(0, stats.getNumberOfKeys());
        assertEquals(0, stats.getNumberOfKeysInDeltaCache());
        assertEquals(0, stats.getNumberOfKeysInSegment());
        assertEquals(0, stats.getNumberOfKeysInScarceIndex());

        verifySegmentSearch(seg, Arrays.asList(// s
                Entry.of(1, null) //
        ));

        /*
         * Number of file's is constantly 0, because of compact method doesn't
         * run, because there are no canges in delta files.
         */
        assertEquals(4, numberOfFilesInDirectory(directory));

    }

    @ParameterizedTest
    @MethodSource("segmentProvider")
    void test_simple(final TypeDescriptorInteger tdi,
            final TypeDescriptorShortString tds, final Directory directory,
            final Segment<Integer, String> seg,
            final int expectedNumberKeysInScarceIndex,
            final int expectedNumberOfFiles) {

        /*
         * Writing operation is here intentionally duplicated. It verifies, that
         * index consistency is kept.
         */
        writeEntries(seg, testDataSet);
        writeEntries(seg, testDataSet);

        verifyTestDataSet(seg);

        /**
         * It's always 4 or 5 because only one or zero delta files could exists.
         */
        if (numberOfFilesInDirectory(directory) != 3
                && numberOfFilesInDirectory(directory) != 4) {
            fail("Invalid number of files "
                    + numberOfFilesInDirectory(directory));
        }

        assertEquals(SegmentResultStatus.OK, seg.compact().getStatus());
        assertEquals(9, seg.getStats().getNumberOfKeys());
        assertEquals(expectedNumberKeysInScarceIndex,
                seg.getStats().getNumberOfKeysInScarceIndex());
    }

    /**
     * When all data are written in separate delta file, even in this case are
     * data correctly processed.
     */
    @ParameterizedTest
    @MethodSource("segmentProvider")
    void test_multipleWrites(final TypeDescriptorInteger tdi,
            final TypeDescriptorShortString tds, final Directory directory,
            final Segment<Integer, String> seg,
            final int expectedNumberKeysInScarceIndex,
            final int expectedNumberOfFiles) {

        testDataSet.forEach(entry -> {
            writeEntries(seg, Arrays.asList(entry));
        });

        verifyTestDataSet(seg);

        verifyNumberOfFiles(directory, expectedNumberOfFiles);

        assertEquals(SegmentResultStatus.OK, seg.compact().getStatus());

        verifyNumberOfFiles(directory, 4);
        verifyTestDataSet(seg);
        assertEquals(9, seg.getStats().getNumberOfKeys());
        assertEquals(expectedNumberKeysInScarceIndex,
                seg.getStats().getNumberOfKeysInScarceIndex());
    }

    private void verifyTestDataSet(final Segment<Integer, String> seg) {

        verifySegmentData(seg, sortedTestDataSet);

        verifySegmentSearch(seg, Arrays.asList(// s
                Entry.of(11, null), //
                Entry.of(2, "a"), //
                Entry.of(3, "b"), //
                Entry.of(4, "c"), //
                Entry.of(5, "d")//
        ));

    }

    @Test
    void test_duplicities() {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final Segment<Integer, String> seg = Segment
                .<Integer, String>builder(
                        org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                                .wrap(directory))//
                .withId(id)//
                .withKeyTypeDescriptor(tdi)//
                .withBloomFilterIndexSizeInBytes(0)//
                .withValueTypeDescriptor(tds)//
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

        writeEntries(seg, Arrays.asList(//
                Entry.of(2, "a"), //
                Entry.of(3, "b"), //
                Entry.of(3, "bb"), //
                Entry.of(4, "c"), //
                Entry.of(5, "d"), //
                Entry.of(5, "dd"), //
                Entry.of(5, "ddd")//
        ));

        assertEquals(4, seg.getStats().getNumberOfKeys());
        assertEquals(4, seg.getStats().getNumberOfKeysInDeltaCache());
        assertEquals(0, seg.getStats().getNumberOfKeysInSegment());

        verifySegmentData(seg, Arrays.asList(//
                Entry.of(2, "a"), //
                Entry.of(3, "bb"), //
                Entry.of(4, "c"), //
                Entry.of(5, "ddd") //
        ));

        verifySegmentSearch(seg, Arrays.asList(// s
                Entry.of(6, null), //
                Entry.of(2, "a"), //
                Entry.of(3, "bb"), //
                Entry.of(4, "c"), //
                Entry.of(5, "ddd")//
        ));
    }

    @Test
    void test_write_unordered() {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final Segment<Integer, String> seg = Segment
                .<Integer, String>builder(
                        org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                                .wrap(directory))//
                .withId(id)//
                .withKeyTypeDescriptor(tdi)//
                .withBloomFilterIndexSizeInBytes(0)//
                .withValueTypeDescriptor(tds)//
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

        writeEntries(seg, Arrays.asList(//
                Entry.of(5, "d"), //
                Entry.of(3, "b"), //
                Entry.of(5, "dd"), //
                Entry.of(2, "a"), //
                Entry.of(3, "bb"), //
                Entry.of(4, "c"), //
                Entry.of(5, "ddd")//
        ));

        assertEquals(4, seg.getStats().getNumberOfKeys());
        assertEquals(4, seg.getStats().getNumberOfKeysInDeltaCache());
        assertEquals(0, seg.getStats().getNumberOfKeysInSegment());

        verifySegmentData(seg, Arrays.asList(//
                Entry.of(2, "a"), //
                Entry.of(3, "bb"), //
                Entry.of(4, "c"), //
                Entry.of(5, "ddd") //
        ));

        verifySegmentSearch(seg, Arrays.asList(//
                Entry.of(6, null), //
                Entry.of(2, "a"), //
                Entry.of(3, "bb"), //
                Entry.of(4, "c"), //
                Entry.of(5, "ddd")//
        ));
    }

    @Test
    void test_write_unordered_tombstone() {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final Segment<Integer, String> seg = Segment
                .<Integer, String>builder(
                        org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                                .wrap(directory))//
                .withId(id)//
                .withKeyTypeDescriptor(tdi)//
                .withValueTypeDescriptor(tds)//
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

        writeEntries(seg, Arrays.asList(//
                Entry.of(5, "d"), //
                Entry.of(3, "b"), //
                Entry.of(5, "dd"), //
                Entry.of(2, "a"), //
                Entry.of(3, "bb"), //
                Entry.of(4, "c"), //
                Entry.of(5, "ddd"), //
                Entry.of(5, TypeDescriptorShortString.TOMBSTONE_VALUE)//
        ));

        /**
         * There is a error in computing number of keys in cache. There are 3
         * keys, because one is deleted.
         */
        assertEquals(4, seg.getStats().getNumberOfKeys());
        assertEquals(4, seg.getStats().getNumberOfKeysInDeltaCache());
        assertEquals(0, seg.getStats().getNumberOfKeysInSegment());

        verifySegmentData(seg, Arrays.asList(//
                Entry.of(2, "a"), //
                Entry.of(3, "bb"), //
                Entry.of(4, "c") //
        ));

        verifySegmentSearch(seg, Arrays.asList(// s
                Entry.of(5, null), //
                Entry.of(2, "a"), //
                Entry.of(3, "bb"), //
                Entry.of(4, "c") //
        ));
    }

    @ParameterizedTest
    @MethodSource("segmentProvider")
    void test_write_delete_repeat_operations(final TypeDescriptorInteger tdi,
            final TypeDescriptorShortString tds, final Directory directory,
            final Segment<Integer, String> seg,
            final int expectedNumberKeysInScarceIndex,
            final int expectedNumberOfFiles) {
        for (int i = 0; i < 100; i++) {
            int a = i * 3;
            int b = i * 3 + 1;
            writeEntries(seg, Arrays.asList(//
                    Entry.of(a, "a"), //
                    Entry.of(b, "b") //
            ));
            writeEntries(seg, Arrays.asList(//
                    Entry.of(a, tds.getTombstone()), //
                    Entry.of(b, tds.getTombstone()) //
            ));
            verifySegmentData(seg, Arrays.asList(//
            ));
        }
    }

    @Test
    void test_write_delete_operations() {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final Segment<Integer, String> seg = Segment
                .<Integer, String>builder(
                        org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                                .wrap(directory))
                .withId(id).withKeyTypeDescriptor(tdi)
                .withBloomFilterIndexSizeInBytes(0)//
                .withValueTypeDescriptor(tds)//
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

        writeEntries(seg, Arrays.asList(//
                Entry.of(2, "a"), //
                Entry.of(2, tds.getTombstone()), //
                Entry.of(3, "b"), //
                Entry.of(3, "bb"), //
                Entry.of(3, tds.getTombstone()), //
                Entry.of(4, "c"), //
                Entry.of(4, tds.getTombstone()), //
                Entry.of(5, "d"), //
                Entry.of(5, "dd"), //
                Entry.of(5, "ddd"), //
                Entry.of(5, tds.getTombstone()) //
        ));

        assertEquals(4, seg.getStats().getNumberOfKeys());
        assertEquals(4, seg.getStats().getNumberOfKeysInDeltaCache());
        assertEquals(0, seg.getStats().getNumberOfKeysInSegment());

        verifySegmentData(seg, Arrays.asList(//
        ));

        verifySegmentSearch(seg, Arrays.asList(// s
                Entry.of(2, null), //
                Entry.of(3, null), //
                Entry.of(4, null), //
                Entry.of(5, null), //
                Entry.of(6, null)//
        ));
    }

    @Test
    void test_write_to_unloaded_segment() {
        final Directory directory = new MemDirectory();
        final AsyncDirectory asyncDirectory = org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                .wrap(directory);
        final SegmentId segmentId = SegmentId.of(27);

        final SegmentPropertiesManager segmentPropertiesManager = new SegmentPropertiesManager(
                asyncDirectory, segmentId);
        segmentPropertiesManager.setVersion(1L);
        segmentPropertiesManager
                .setState(SegmentPropertiesManager.SegmentDataState.ACTIVE);

        final Segment<Integer, String> seg = Segment
                .<Integer, String>builder(
                        org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                                .wrap(directory))//
                .withId(segmentId)//
                .withMaxNumberOfKeysInSegmentWriteCache(64)//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(128)//
                .withMaxNumberOfKeysInSegmentCache(256)//
                .withMaxNumberOfKeysInSegmentChunk(3)//
                .withKeyTypeDescriptor(tdi)//
                .withBloomFilterNumberOfHashFunctions(2)//
                .withBloomFilterIndexSizeInBytes(0)//
                .withBloomFilterProbabilityOfFalsePositive(0.01)//
                .withValueTypeDescriptor(tds)//
                .withDiskIoBufferSize(1024)//
                .withEncodingChunkFilters(
                        List.of(new ChunkFilterMagicNumberWriting(), //
                                new ChunkFilterCrc32Writing(), //
                                new ChunkFilterDoNothing()//
                        ))//
                .withDecodingChunkFilters(
                        List.of(new ChunkFilterMagicNumberValidation(), //
                                new ChunkFilterCrc32Validation(), //
                                new ChunkFilterDoNothing()//
                        ))//
                .build();

        writeEntries(seg, Arrays.asList(//
                Entry.of(11, "aaa"), //
                Entry.of(12, "aab"), //
                Entry.of(13, "aac"), //
                Entry.of(14, "aad"), //
                Entry.of(15, "aae"), //
                Entry.of(16, "aaf"), //
                Entry.of(17, "aag"), //
                Entry.of(18, "aah"), //
                Entry.of(19, "aai"), //
                Entry.of(20, "aaj"), //
                Entry.of(21, "aak"), //
                Entry.of(22, "aal"), //
                Entry.of(9, TypeDescriptorShortString.TOMBSTONE_VALUE)//
        ));

        verifySegmentSearch(seg, Arrays.asList(// s
                Entry.of(9, null), //
                Entry.of(12, "aab"), //
                Entry.of(13, "aac"), //
                Entry.of(14, "aad"), //
                Entry.of(15, "aae") //
        ));
        seg.close();
    }

    /**
     * This test could be used for manual verification that all open files are
     * closed. Should be done by adding debug breakpoint into
     * {@link MergeSpliterator#tryAdvance(java.util.function.Consumer)} than
     * check number of open files from command line.
     * 
     * 
     * 
     * Directory should be following: <code><pre>
     * new FsDirectory(new File("./target/tmp/"));
     * </pre></code>
     * 
     * @
     */
    @Test
    void test_write_unordered_tombstone_with_compact() {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final Segment<Integer, String> seg = Segment
                .<Integer, String>builder(
                        org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                                .wrap(directory))//
                .withId(id)//
                .withKeyTypeDescriptor(tdi)//
                .withBloomFilterIndexSizeInBytes(0)//
                .withValueTypeDescriptor(tds)//
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

        writeEntries(seg, Arrays.asList(//
                Entry.of(5, "d"), //
                Entry.of(3, "b"), //
                Entry.of(5, "dd"), //
                Entry.of(2, "a"), //
                Entry.of(3, "bb"), //
                Entry.of(4, "c"), //
                Entry.of(5, "ddd"), //
                Entry.of(5, TypeDescriptorShortString.TOMBSTONE_VALUE)//
        ));
        assertEquals(SegmentResultStatus.OK, seg.compact().getStatus());

        assertEquals(3, seg.getStats().getNumberOfKeys());
        assertEquals(0, seg.getStats().getNumberOfKeysInDeltaCache());
        assertEquals(3, seg.getStats().getNumberOfKeysInSegment());

        verifySegmentData(seg, Arrays.asList(//
                Entry.of(2, "a"), //
                Entry.of(3, "bb"), //
                Entry.of(4, "c") //
        ));

        verifySegmentSearch(seg, Arrays.asList(// s
                Entry.of(5, null), //
                Entry.of(2, "a"), //
                Entry.of(3, "bb"), //
                Entry.of(4, "c") //
        ));
    }

    /**
     * Test could be verified that search in disk data is perform correctly and
     * buffers have correct size.
     * 
     * 
     * Easiest way how to verify that is to add debug breakpoint into
     * DirectoryMem methods for gettin read and writer objecs. It's easy to
     * spot, that correct value was set buffer have strange value 3KB.
     * 
     * 
     */
    @Test
    void test_search_on_disk() {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final Segment<Integer, String> seg = Segment
                .<Integer, String>builder(
                        org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                                .wrap(directory))//
                .withId(id)//
                .withKeyTypeDescriptor(tdi)//
                .withBloomFilterIndexSizeInBytes(0)//
                .withMaxNumberOfKeysInSegmentChunk(3)//
                .withDiskIoBufferSize(3 * 1024)//
                .withValueTypeDescriptor(tds)//
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

        final List<Entry<Integer, String>> entries = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            final Entry<Integer, String> p = Entry.of(i, "Ahoj");
            assertEquals(SegmentResultStatus.OK,
                    seg.put(p.getKey(), p.getValue()).getStatus());
            entries.add(p);
        }
        assertEquals(SegmentResultStatus.OK, seg.flush().getStatus());
        assertEquals(SegmentResultStatus.OK, seg.compact().getStatus());

        AbstractDataTest.verifyIteratorData(entries, seg.openIterator());
        for (int i = 0; i < 1000; i++) {
            final SegmentResult<String> result = seg.get(i);
            assertEquals(SegmentResultStatus.OK, result.getStatus(),
                    "Invalid result status for key " + i);
            assertEquals("Ahoj", result.getValue(),
                    "Invalid value for key " + i);
        }
    }

    /**
     * Prepare data for tests. Directory object is shared between parameterized
     * tests.
     * 
     * @return
     */
    static Stream<Arguments> segmentProvider() {
        final Directory dir1 = new MemDirectory();
        final Directory dir2 = new MemDirectory();
        final Directory dir3 = new MemDirectory();
        final SegmentId id1 = SegmentId.of(29);
        final SegmentId id2 = SegmentId.of(23);
        final SegmentId id3 = SegmentId.of(17);
        final TypeDescriptorShortString tds = new TypeDescriptorShortString();
        final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
        final int smallCacheSize = 1024;
        final int smallWriteCache = 256;
        return Stream.of(arguments(tdi, tds, dir1, Segment
                .<Integer, String>builder(
                        org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                                .wrap(dir1))//
                .withId(id1)//
                .withKeyTypeDescriptor(tdi)//
                .withValueTypeDescriptor(tds)//
                .withMaxNumberOfKeysInSegmentCache(smallCacheSize)//
                .withMaxNumberOfKeysInSegmentWriteCache(smallWriteCache)//
                .withMaxNumberOfKeysInSegmentChunk(10)//
                .withBloomFilterIndexSizeInBytes(0)// .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSize(1 * 1024) //
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
                .build(), //
                1, // expectedNumberKeysInScarceIndex,
                10 // expectedNumberOfFile
        ), arguments(tdi, tds, dir2, Segment.<Integer, String>builder(
                org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                        .wrap(dir2))//
                .withId(id2)//
                .withKeyTypeDescriptor(tdi)//
                .withValueTypeDescriptor(tds)//
                .withMaxNumberOfKeysInSegmentCache(smallCacheSize)//
                .withMaxNumberOfKeysInSegmentWriteCache(smallWriteCache)//
                .withMaxNumberOfKeysInSegmentChunk(1)//
                .withBloomFilterIndexSizeInBytes(0)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSize(2 * 1024)//
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
                .build(), //
                9, // expectedNumberKeysInScarceIndex
                10// expectedNumberOfFile
        ), arguments(tdi, tds, dir3, Segment.<Integer, String>builder(
                org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                        .wrap(dir3))//
                .withId(id3)//
                .withKeyTypeDescriptor(tdi)//
                .withValueTypeDescriptor(tds)//
                .withMaxNumberOfKeysInSegmentCache(smallCacheSize)//
                .withMaxNumberOfKeysInSegmentWriteCache(smallWriteCache)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withBloomFilterIndexSizeInBytes(0)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSize(4 * 1024)//
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
                .build(), //
                5, // expectedNumberKeysInScarceIndex
                10 // expectedNumberOfFile
        ));
    }

}
