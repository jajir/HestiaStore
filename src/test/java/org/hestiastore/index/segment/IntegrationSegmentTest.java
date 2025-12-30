package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.hestiastore.index.AbstractDataTest;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryWriter;
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

    private static final SegmentId SEGMENT_37_ID = SegmentId.of(37);

    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();
    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    private final List<Entry<Integer, String>> testDataSet = Arrays.asList(
            Entry.of(2, "a"), Entry.of(5, "d"), Entry.of(10, "i"), Entry.of(8, "g"),
            Entry.of(7, "f"), Entry.of(3, "b"), Entry.of(4, "c"), Entry.of(6, "e"),
            Entry.of(9, "h"));
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

        seg.forceCompact();
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
         * Number of file's is constantly 0, because of forceCompact method
         * doesn't run, because there are no canges in delta files.
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
        if (numberOfFilesInDirectoryP(directory) != 4
                && numberOfFilesInDirectoryP(directory) != 5) {
            fail("Invalid number of files "
                    + numberOfFilesInDirectoryP(directory));
        }

        seg.forceCompact();
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

        assertEquals(expectedNumberOfFiles,
                numberOfFilesInDirectoryP(directory));

        seg.forceCompact();

        assertEquals(4, numberOfFilesInDirectoryP(directory));
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

    @ParameterizedTest
    @MethodSource("segmentProvider")
    void test_split(final TypeDescriptorInteger tdi,
            final TypeDescriptorShortString tds, final Directory directory,
            final Segment<Integer, String> seg,
            final int expectedNumberKeysInScarceIndex,
            final int expectedNumberOfFiles) {

        writeEntries(seg, Arrays.asList(Entry.of(2, "e"), Entry.of(3, "e"),
                Entry.of(4, "e")));
        writeEntries(seg, Arrays.asList(Entry.of(2, "a"), Entry.of(3, "b"),
                Entry.of(4, "c"), Entry.of(5, "d")));

        final SegmentId segId = SegmentId.of(3);
        final SegmentSplitterPolicy<Integer, String> policy = seg
                .getSegmentSplitterPolicy();
        final SegmentSplitterPlan<Integer, String> plan = SegmentSplitterPlan
                .fromPolicy(policy);
        final SegmentSplitterResult<Integer, String> result = seg.split(segId,
                plan);
        final Segment<Integer, String> smaller = result.getSegment();
        assertEquals(2, result.getMinKey());
        assertEquals(3, result.getMaxKey());

        verifySegmentData(seg, Arrays.asList(//
                Entry.of(4, "c"), //
                Entry.of(5, "d") //
        ));

        verifySegmentData(smaller, Arrays.asList(//
                Entry.of(2, "a"), //
                Entry.of(3, "b") //
        ));

        verifySegmentSearch(seg, Arrays.asList(//
                Entry.of(2, null), //
                Entry.of(3, null), //
                Entry.of(4, "c"), //
                Entry.of(5, "d") //
        ));

        verifySegmentSearch(smaller, Arrays.asList(//
                Entry.of(2, "a"), //
                Entry.of(3, "b"), //
                Entry.of(4, null), //
                Entry.of(5, null) //
        ));

        assertEquals(8, numberOfFilesInDirectoryP(directory));
    }

    @Test
    void test_duplicities() {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final Segment<Integer, String> seg = Segment.<Integer, String>builder()//
                .withAsyncDirectory(org.hestiastore.index.directory.async.AsyncDirectoryAdapter.wrap(directory))//
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
        final Segment<Integer, String> seg = Segment.<Integer, String>builder()//
                .withAsyncDirectory(org.hestiastore.index.directory.async.AsyncDirectoryAdapter.wrap(directory))//
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
        final Segment<Integer, String> seg = Segment.<Integer, String>builder()//
                .withAsyncDirectory(org.hestiastore.index.directory.async.AsyncDirectoryAdapter.wrap(directory))//
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
        final Segment<Integer, String> seg = Segment.<Integer, String>builder()
                .withAsyncDirectory(org.hestiastore.index.directory.async.AsyncDirectoryAdapter.wrap(directory)).withId(id).withKeyTypeDescriptor(tdi)
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
    void test_split_just_tombstones() {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final Segment<Integer, String> seg = Segment.<Integer, String>builder()//
                .withAsyncDirectory(org.hestiastore.index.directory.async.AsyncDirectoryAdapter.wrap(directory))//
                .withId(id)//
                .withMaxNumberOfKeysInSegmentCache(13)//
                .withMaxNumberOfKeysInSegmentChunk(3)//
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
                Entry.of(25, "d"), //
                Entry.of(15, "d"), //
                Entry.of(1, TypeDescriptorShortString.TOMBSTONE_VALUE), //
                Entry.of(2, TypeDescriptorShortString.TOMBSTONE_VALUE), //
                Entry.of(3, TypeDescriptorShortString.TOMBSTONE_VALUE), //
                Entry.of(4, TypeDescriptorShortString.TOMBSTONE_VALUE), //
                Entry.of(5, TypeDescriptorShortString.TOMBSTONE_VALUE), //
                Entry.of(6, TypeDescriptorShortString.TOMBSTONE_VALUE), //
                Entry.of(7, TypeDescriptorShortString.TOMBSTONE_VALUE), //
                Entry.of(8, TypeDescriptorShortString.TOMBSTONE_VALUE), //
                Entry.of(9, TypeDescriptorShortString.TOMBSTONE_VALUE)//
        ));
        final SegmentSplitterPolicy<Integer, String> segSplitPolicy = seg
                .getSegmentSplitterPolicy();
        assertTrue(segSplitPolicy.shouldBeCompactedBeforeSplitting(10));

        /**
         * Verify that split is not possible
         */
        final SegmentSplitterPlan<Integer, String> plan2 = SegmentSplitterPlan
                .fromPolicy(segSplitPolicy);
        final Exception err = assertThrows(IllegalStateException.class,
                () -> seg.split(SEGMENT_37_ID, plan2));
        assertEquals("Splitting failed. Number of keys is too low.",
                err.getMessage());
    }

    @Test
    void test_write_to_unloaded_segment() {
        final Directory directory = new MemDirectory();
        final AsyncDirectory asyncDirectory = org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                .wrap(directory);
        final SegmentId segmentId = SegmentId.of(27);

        SegmentConf segmentConf = new SegmentConf(13, 6, 17, 3, 2, 0, 0.01,
                1024, List.of(), List.of());

        final SegmentPropertiesManager segmentPropertiesManager = new SegmentPropertiesManager(
                asyncDirectory, segmentId);

        final SegmentFiles<Integer, String> segmentFiles = new SegmentFiles<>(
                asyncDirectory, segmentId, tdi, tds, 1024, //
                List.of(new ChunkFilterMagicNumberWriting(), //
                        new ChunkFilterCrc32Writing(), //
                        new ChunkFilterDoNothing()//
                ), //
                List.of(new ChunkFilterMagicNumberValidation(), //
                        new ChunkFilterCrc32Validation(), //
                        new ChunkFilterDoNothing()//
                )//
        );

        final SegmentDataSupplier<Integer, String> segmentDataSupplier = new SegmentDataSupplier<>(
                segmentFiles, segmentConf, segmentPropertiesManager);

        final SegmentResourcesImpl<Integer, String> dataProvider = new SegmentResourcesImpl<>(
                segmentDataSupplier);

        final Segment<Integer, String> seg = Segment.<Integer, String>builder()//
                .withAsyncDirectory(org.hestiastore.index.directory.async.AsyncDirectoryAdapter.wrap(directory))//
                .withId(segmentId)//
                .withSegmentConf(segmentConf)//
                .withSegmentFiles(segmentFiles)//
                .withSegmentPropertiesManager(segmentPropertiesManager)//
                .withSegmentResources(dataProvider)//
                .withMaxNumberOfKeysInSegmentCache(13)//
                .withMaxNumberOfKeysInSegmentChunk(3)//
                .withKeyTypeDescriptor(tdi)//
                .withBloomFilterIndexSizeInBytes(0)//
                .withValueTypeDescriptor(tds)//
                .build();

        assertFalse(dataProvider.isLoaded());

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
        /**
         * Writing to segment which doesn't require compaction doesn't load
         * segment data.
         */
        assertFalse(dataProvider.isLoaded());

        verifySegmentSearch(seg, Arrays.asList(// s
                Entry.of(9, null), //
                Entry.of(12, "aab"), //
                Entry.of(13, "aac"), //
                Entry.of(14, "aad"), //
                Entry.of(15, "aae") //
        ));

        /**
         * SegmentIndex search should lead to load segment data.
         */
        assertTrue(dataProvider.isLoaded());

        /**
         * Force unloading segment data
         */
        dataProvider.invalidate();

        assertFalse(dataProvider.isLoaded());
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
    void test_write_unordered_tombstone_with_forceCompact() {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final Segment<Integer, String> seg = Segment.<Integer, String>builder()//
                .withAsyncDirectory(org.hestiastore.index.directory.async.AsyncDirectoryAdapter.wrap(directory))//
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
        seg.forceCompact();

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
        final Segment<Integer, String> seg = Segment.<Integer, String>builder()//
                .withAsyncDirectory(org.hestiastore.index.directory.async.AsyncDirectoryAdapter.wrap(directory))//
                .withId(id)//
                .withKeyTypeDescriptor(tdi)//
                .withBloomFilterIndexSizeInBytes(0)//
                .withMaxNumberOfKeysInSegmentChunk(3)//
                .withMaxNumberOfKeysInSegmentCache(5)//
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
        try (EntryWriter<Integer, String> writer = seg.openDeltaCacheWriter()) {
            for (int i = 0; i < 1000; i++) {
                final Entry<Integer, String> p = Entry.of(i, "Ahoj");
                writer.write(p);
                entries.add(p);
            }
        }
        seg.forceCompact();

        AbstractDataTest.verifyIteratorData(entries, seg.openIterator());
        for (int i = 0; i < 1000; i++) {
            assertEquals("Ahoj", seg.get(i), "Invalid value for key " + i);
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
        return Stream.of(arguments(tdi, tds, dir1,
                Segment.<Integer, String>builder()//
                        .withAsyncDirectory(org.hestiastore.index.directory.async.AsyncDirectoryAdapter.wrap(dir1))//
                        .withId(id1)//
                        .withKeyTypeDescriptor(tdi)//
                        .withValueTypeDescriptor(tds)//
                        .withMaxNumberOfKeysInSegmentCache(10) //
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
        ), arguments(tdi, tds, dir2, Segment.<Integer, String>builder()//
                .withAsyncDirectory(org.hestiastore.index.directory.async.AsyncDirectoryAdapter.wrap(dir2))//
                .withId(id2)//
                .withKeyTypeDescriptor(tdi)//
                .withValueTypeDescriptor(tds)//
                .withMaxNumberOfKeysInSegmentCache(3)//
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
                5// expectedNumberOfFile
        ), arguments(tdi, tds, dir3, Segment.<Integer, String>builder()//
                .withAsyncDirectory(org.hestiastore.index.directory.async.AsyncDirectoryAdapter.wrap(dir3))//
                .withId(id3)//
                .withKeyTypeDescriptor(tdi)//
                .withValueTypeDescriptor(tds)//
                .withMaxNumberOfKeysInSegmentCache(5)//
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
                7 // expectedNumberOfFile
        ));
    }

}
