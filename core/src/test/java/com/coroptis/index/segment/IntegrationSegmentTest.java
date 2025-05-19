package com.coroptis.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

public class IntegrationSegmentTest extends AbstractSegmentTest {

    private final TypeDescriptorString tds = new TypeDescriptorString();
    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    private final List<Pair<Integer, String>> testDataSet = Arrays.asList(
            Pair.of(2, "a"), Pair.of(5, "d"), Pair.of(10, "i"), Pair.of(8, "g"),
            Pair.of(7, "f"), Pair.of(3, "b"), Pair.of(4, "c"), Pair.of(6, "e"),
            Pair.of(9, "h"));
    private final List<Pair<Integer, String>> sortedTestDataSet = testDataSet
            .stream().sorted((pair1, pair2) -> {
                return pair1.getKey() - pair2.getKey();
            }).collect(Collectors.toList());

    @ParameterizedTest
    @MethodSource("segmentProvider")
    void test_empty_segment_stats(final TypeDescriptorInteger tdi,
            final TypeDescriptorString tds, final Directory directory,
            final Segment<Integer, String> seg,
            final int expectedNumberKeysInScarceIndex,
            int expectedNumberOfFiles) throws Exception {

        seg.forceCompact();
        verifyCacheFiles(directory);

        verifySegmentData(seg, Arrays.asList());

        final SegmentStats stats = seg.getStats();
        assertEquals(0, stats.getNumberOfKeys());
        assertEquals(0, stats.getNumberOfKeysInDeltaCache());
        assertEquals(0, stats.getNumberOfKeysInIndex());
        assertEquals(0, stats.getNumberOfKeysInScarceIndex());

        verifySegmentSearch(seg, Arrays.asList(// s
                Pair.of(1, null) //
        ));

        /*
         * Number of file's is constantly 0, because of forceCompact method
         * doesn't run, because there are no canges in delta files.
         */
        assertEquals(0, numberOfFilesInDirectory(directory));

    }

    @ParameterizedTest
    @MethodSource("segmentProvider")
    void test_simple(final TypeDescriptorInteger tdi,
            final TypeDescriptorString tds, final Directory directory,
            final Segment<Integer, String> seg,
            final int expectedNumberKeysInScarceIndex,
            final int expectedNumberOfFiles) throws Exception {

        /*
         * Writing operation is here intentionally duplicated. It verifies, that
         * index consistency is kept.
         */
        writePairs(seg, testDataSet);
        writePairs(seg, testDataSet);

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
            final TypeDescriptorString tds, final Directory directory,
            final Segment<Integer, String> seg,
            final int expectedNumberKeysInScarceIndex,
            final int expectedNumberOfFiles) throws Exception {

        testDataSet.forEach(pair -> {
            writePairs(seg, Arrays.asList(pair));
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
                Pair.of(11, null), //
                Pair.of(2, "a"), //
                Pair.of(3, "b"), //
                Pair.of(4, "c"), //
                Pair.of(5, "d")//
        ));

    }

    @ParameterizedTest
    @MethodSource("segmentProvider")
    void test_split(final TypeDescriptorInteger tdi,
            final TypeDescriptorString tds, final Directory directory,
            final Segment<Integer, String> seg,
            final int expectedNumberKeysInScarceIndex,
            final int expectedNumberOfFiles) throws Exception {

        writePairs(seg, Arrays.asList(Pair.of(2, "e"), Pair.of(3, "e"),
                Pair.of(4, "e")));
        writePairs(seg, Arrays.asList(Pair.of(2, "a"), Pair.of(3, "b"),
                Pair.of(4, "c"), Pair.of(5, "d")));

        final SegmentId segId = SegmentId.of(3);
        final SegmentSplitter<Integer, String> splitter = seg
                .getSegmentSplitter();
        final SegmentSplitterResult<Integer, String> result = splitter
                .split(segId);
        final Segment<Integer, String> smaller = result.getSegment();
        assertEquals(2, result.getMinKey());
        assertEquals(3, result.getMaxKey());

        verifySegmentData(seg, Arrays.asList(//
                Pair.of(4, "c"), //
                Pair.of(5, "d") //
        ));

        verifySegmentData(smaller, Arrays.asList(//
                Pair.of(2, "a"), //
                Pair.of(3, "b") //
        ));

        verifySegmentSearch(seg, Arrays.asList(//
                Pair.of(2, null), //
                Pair.of(3, null), //
                Pair.of(4, "c"), //
                Pair.of(5, "d") //
        ));

        verifySegmentSearch(smaller, Arrays.asList(//
                Pair.of(2, "a"), //
                Pair.of(3, "b"), //
                Pair.of(4, null), //
                Pair.of(5, null) //
        ));

        assertEquals(8, numberOfFilesInDirectoryP(directory));
    }

    @Test
    void test_duplicities() throws Exception {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final Segment<Integer, String> seg = Segment.<Integer, String>builder()
                .withDirectory(directory).withId(id).withKeyTypeDescriptor(tdi)
                .withBloomFilterIndexSizeInBytes(0).withValueTypeDescriptor(tds)
                .build();

        writePairs(seg, Arrays.asList(//
                Pair.of(2, "a"), //
                Pair.of(3, "b"), //
                Pair.of(3, "bb"), //
                Pair.of(4, "c"), //
                Pair.of(5, "d"), //
                Pair.of(5, "dd"), //
                Pair.of(5, "ddd")//
        ));

        assertEquals(4, seg.getStats().getNumberOfKeys());
        assertEquals(4, seg.getStats().getNumberOfKeysInDeltaCache());
        assertEquals(0, seg.getStats().getNumberOfKeysInIndex());

        verifySegmentData(seg, Arrays.asList(//
                Pair.of(2, "a"), //
                Pair.of(3, "bb"), //
                Pair.of(4, "c"), //
                Pair.of(5, "ddd") //
        ));

        verifySegmentSearch(seg, Arrays.asList(// s
                Pair.of(6, null), //
                Pair.of(2, "a"), //
                Pair.of(3, "bb"), //
                Pair.of(4, "c"), //
                Pair.of(5, "ddd")//
        ));
    }

    @Test
    void test_write_unordered() throws Exception {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final Segment<Integer, String> seg = Segment.<Integer, String>builder()
                .withDirectory(directory).withId(id).withKeyTypeDescriptor(tdi)
                .withBloomFilterIndexSizeInBytes(0)//
                .withValueTypeDescriptor(tds).build();

        writePairs(seg, Arrays.asList(//
                Pair.of(5, "d"), //
                Pair.of(3, "b"), //
                Pair.of(5, "dd"), //
                Pair.of(2, "a"), //
                Pair.of(3, "bb"), //
                Pair.of(4, "c"), //
                Pair.of(5, "ddd")//
        ));

        assertEquals(4, seg.getStats().getNumberOfKeys());
        assertEquals(4, seg.getStats().getNumberOfKeysInDeltaCache());
        assertEquals(0, seg.getStats().getNumberOfKeysInIndex());

        verifySegmentData(seg, Arrays.asList(//
                Pair.of(2, "a"), //
                Pair.of(3, "bb"), //
                Pair.of(4, "c"), //
                Pair.of(5, "ddd") //
        ));

        verifySegmentSearch(seg, Arrays.asList(//
                Pair.of(6, null), //
                Pair.of(2, "a"), //
                Pair.of(3, "bb"), //
                Pair.of(4, "c"), //
                Pair.of(5, "ddd")//
        ));
    }

    @Test
    void test_write_unordered_tombstone() throws Exception {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final Segment<Integer, String> seg = Segment.<Integer, String>builder()
                .withDirectory(directory).withId(id).withKeyTypeDescriptor(tdi)
                .withValueTypeDescriptor(tds).build();

        writePairs(seg, Arrays.asList(//
                Pair.of(5, "d"), //
                Pair.of(3, "b"), //
                Pair.of(5, "dd"), //
                Pair.of(2, "a"), //
                Pair.of(3, "bb"), //
                Pair.of(4, "c"), //
                Pair.of(5, "ddd"), //
                Pair.of(5, TypeDescriptorString.TOMBSTONE_VALUE)//
        ));

        /**
         * There is a error in computing number of keys in cache. There are 3
         * keys, because one is deleted.
         */
        assertEquals(4, seg.getStats().getNumberOfKeys());
        assertEquals(4, seg.getStats().getNumberOfKeysInDeltaCache());
        assertEquals(0, seg.getStats().getNumberOfKeysInIndex());

        verifySegmentData(seg, Arrays.asList(//
                Pair.of(2, "a"), //
                Pair.of(3, "bb"), //
                Pair.of(4, "c") //
        ));

        verifySegmentSearch(seg, Arrays.asList(// s
                Pair.of(5, null), //
                Pair.of(2, "a"), //
                Pair.of(3, "bb"), //
                Pair.of(4, "c") //
        ));
    }

    @ParameterizedTest
    @MethodSource("segmentProvider")
    void test_write_delete_repeat_operations(final TypeDescriptorInteger tdi,
            final TypeDescriptorString tds, final Directory directory,
            final Segment<Integer, String> seg,
            final int expectedNumberKeysInScarceIndex,
            final int expectedNumberOfFiles) throws Exception {
        for (int i = 0; i < 100; i++) {
            int a = i * 3;
            int b = i * 3 + 1;
            writePairs(seg, Arrays.asList(//
                    Pair.of(a, "a"), //
                    Pair.of(b, "b") //
            ));
            writePairs(seg, Arrays.asList(//
                    Pair.of(a, tds.getTombstone()), //
                    Pair.of(b, tds.getTombstone()) //
            ));
            verifySegmentData(seg, Arrays.asList(//
            ));
        }
    }

    @Test
    void test_write_delete_operations() throws Exception {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final Segment<Integer, String> seg = Segment.<Integer, String>builder()
                .withDirectory(directory).withId(id).withKeyTypeDescriptor(tdi)
                .withBloomFilterIndexSizeInBytes(0)//
                .withValueTypeDescriptor(tds)//
                .build();

        writePairs(seg, Arrays.asList(//
                Pair.of(2, "a"), //
                Pair.of(2, tds.getTombstone()), //
                Pair.of(3, "b"), //
                Pair.of(3, "bb"), //
                Pair.of(3, tds.getTombstone()), //
                Pair.of(4, "c"), //
                Pair.of(4, tds.getTombstone()), //
                Pair.of(5, "d"), //
                Pair.of(5, "dd"), //
                Pair.of(5, "ddd"), //
                Pair.of(5, tds.getTombstone()) //
        ));

        assertEquals(4, seg.getStats().getNumberOfKeys());
        assertEquals(4, seg.getStats().getNumberOfKeysInDeltaCache());
        assertEquals(0, seg.getStats().getNumberOfKeysInIndex());

        verifySegmentData(seg, Arrays.asList(//
        ));

        verifySegmentSearch(seg, Arrays.asList(// s
                Pair.of(2, null), //
                Pair.of(3, null), //
                Pair.of(4, null), //
                Pair.of(5, null), //
                Pair.of(6, null)//
        ));
    }

    @Test
    void test_split_just_tombstones() {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final Segment<Integer, String> seg = Segment.<Integer, String>builder()//
                .withDirectory(directory)//
                .withId(id)//
                .withMaxNumberOfKeysInSegmentCache(13)//
                .withMaxNumberOfKeysInIndexPage(3)//
                .withKeyTypeDescriptor(tdi)//
                .withBloomFilterIndexSizeInBytes(0)//
                .withValueTypeDescriptor(tds)//
                .build();

        writePairs(seg, Arrays.asList(//
                Pair.of(25, "d"), //
                Pair.of(15, "d"), //
                Pair.of(1, TypeDescriptorString.TOMBSTONE_VALUE), //
                Pair.of(2, TypeDescriptorString.TOMBSTONE_VALUE), //
                Pair.of(3, TypeDescriptorString.TOMBSTONE_VALUE), //
                Pair.of(4, TypeDescriptorString.TOMBSTONE_VALUE), //
                Pair.of(5, TypeDescriptorString.TOMBSTONE_VALUE), //
                Pair.of(6, TypeDescriptorString.TOMBSTONE_VALUE), //
                Pair.of(7, TypeDescriptorString.TOMBSTONE_VALUE), //
                Pair.of(8, TypeDescriptorString.TOMBSTONE_VALUE), //
                Pair.of(9, TypeDescriptorString.TOMBSTONE_VALUE)//
        ));
        final SegmentSplitter<Integer, String> segSplitter = seg
                .getSegmentSplitter();
        assertTrue(segSplitter.shouldBeCompactedBeforeSplitting(10));

        /**
         * Verify that split is not possible
         */
        final Exception err = assertThrows(IllegalStateException.class,
                () -> seg.getSegmentSplitter().split(SegmentId.of(37)));
        assertEquals("Splitting failed. Number of keys is too low.",
                err.getMessage());
    }

    @Test
    void test_write_to_unloaded_segment() {
        final Directory directory = new MemDirectory();
        final SegmentId segmentId = SegmentId.of(27);

        SegmentConf segmentConf = new SegmentConf(13L, 17L, 3, 0, 0, 0.0, 1024);

        final SegmentPropertiesManager segmentPropertiesManager = new SegmentPropertiesManager(
                directory, segmentId);

        final SegmentFiles<Integer, String> segmentFiles = new SegmentFiles<>(
                directory, segmentId, tdi, tds, 1024);

        final SegmentDataSupplier<Integer, String> segmentDataSupplier = new SegmentDataSupplier<>(
                segmentFiles, segmentConf, segmentPropertiesManager);

        final SegmentDataFactory<Integer, String> segmentDataFactory = new SegmentDataFactoryImpl<>(
                segmentDataSupplier);

        final SegmentDataProviderSimple<Integer, String> dataProvider = new SegmentDataProviderSimple<>(
                segmentDataFactory);

        final Segment<Integer, String> seg = Segment.<Integer, String>builder()//
                .withDirectory(directory)//
                .withId(segmentId)//
                .withSegmentDataProvider(dataProvider)//
                .withSegmentConf(segmentConf)//
                .withSegmentFiles(segmentFiles)//
                .withSegmentPropertiesManager(segmentPropertiesManager)//
                .withSegmentDataProvider(dataProvider)//
                .withMaxNumberOfKeysInSegmentCache(13)//
                .withMaxNumberOfKeysInIndexPage(3)//
                .withKeyTypeDescriptor(tdi)//
                .withBloomFilterIndexSizeInBytes(0)//
                .withValueTypeDescriptor(tds)//
                .build();

        assertFalse(dataProvider.isLoaded());

        writePairs(seg, Arrays.asList(//
                Pair.of(11, "aaa"), //
                Pair.of(12, "aab"), //
                Pair.of(13, "aac"), //
                Pair.of(14, "aad"), //
                Pair.of(15, "aae"), //
                Pair.of(16, "aaf"), //
                Pair.of(17, "aag"), //
                Pair.of(18, "aah"), //
                Pair.of(19, "aai"), //
                Pair.of(20, "aaj"), //
                Pair.of(21, "aak"), //
                Pair.of(22, "aal"), //
                Pair.of(9, TypeDescriptorString.TOMBSTONE_VALUE)//
        ));
        /**
         * Writing to segment which doesn't require compaction doesn't load
         * segmrnt data.
         */
        assertFalse(dataProvider.isLoaded());

        verifySegmentSearch(seg, Arrays.asList(// s
                Pair.of(9, null), //
                Pair.of(12, "aab"), //
                Pair.of(13, "aac"), //
                Pair.of(14, "aad"), //
                Pair.of(15, "aae") //
        ));

        /**
         * Index search shoud lead to load segment data.
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
     * @throws Exception
     */
    @Test
    void test_write_unordered_tombstone_with_forceCompact() throws Exception {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final Segment<Integer, String> seg = Segment.<Integer, String>builder()
                .withDirectory(directory).withId(id).withKeyTypeDescriptor(tdi)
                .withBloomFilterIndexSizeInBytes(0).withValueTypeDescriptor(tds)
                .build();

        writePairs(seg, Arrays.asList(//
                Pair.of(5, "d"), //
                Pair.of(3, "b"), //
                Pair.of(5, "dd"), //
                Pair.of(2, "a"), //
                Pair.of(3, "bb"), //
                Pair.of(4, "c"), //
                Pair.of(5, "ddd"), //
                Pair.of(5, TypeDescriptorString.TOMBSTONE_VALUE)//
        ));
        seg.forceCompact();

        assertEquals(3, seg.getStats().getNumberOfKeys());
        assertEquals(0, seg.getStats().getNumberOfKeysInDeltaCache());
        assertEquals(3, seg.getStats().getNumberOfKeysInIndex());

        verifySegmentData(seg, Arrays.asList(//
                Pair.of(2, "a"), //
                Pair.of(3, "bb"), //
                Pair.of(4, "c") //
        ));

        verifySegmentSearch(seg, Arrays.asList(// s
                Pair.of(5, null), //
                Pair.of(2, "a"), //
                Pair.of(3, "bb"), //
                Pair.of(4, "c") //
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
     * @throws Exception
     */
    @Test
    void test_search_on_disk() throws Exception {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final Segment<Integer, String> seg = Segment.<Integer, String>builder()//
                .withDirectory(directory).withId(id)//
                .withKeyTypeDescriptor(tdi)//
                .withBloomFilterIndexSizeInBytes(0)//
                .withMaxNumberOfKeysInIndexPage(3)//
                .withMaxNumberOfKeysInSegmentCache(5)//
                .withDiskIoBufferSize(3 * 1024).withValueTypeDescriptor(tds)
                .build();

        try (PairWriter<Integer, String> writer = seg.openWriter()) {
            for (int i = 0; i < 1000; i++) {
                writer.put(Pair.of(i, "Ahoj"));
            }
        }
        seg.forceCompact();

        for (int i = 0; i < 1000; i++) {
            final String value = seg.get(i);
            assertEquals("Ahoj", value);
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
        final TypeDescriptorString tds = new TypeDescriptorString();
        final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
        return Stream.of(arguments(tdi, tds, dir1,
                Segment.<Integer, String>builder()//
                        .withDirectory(dir1)//
                        .withId(id1)//
                        .withKeyTypeDescriptor(tdi)//
                        .withValueTypeDescriptor(tds)//
                        .withMaxNumberOfKeysInSegmentCache(10) //
                        .withMaxNumberOfKeysInIndexPage(10)//
                        .withBloomFilterIndexSizeInBytes(0)//
                        .withDiskIoBufferSize(1 * 1024) //
                        .build(), //
                2, // expectedNumberKeysInScarceIndex,
                10 // expectedNumberOfFile
        ), arguments(tdi, tds, dir2, Segment.<Integer, String>builder()//
                .withDirectory(dir2)//
                .withId(id2)//
                .withKeyTypeDescriptor(tdi)//
                .withValueTypeDescriptor(tds)//
                .withMaxNumberOfKeysInSegmentCache(3)//
                .withMaxNumberOfKeysInIndexPage(1)//
                .withBloomFilterIndexSizeInBytes(0)//
                .withDiskIoBufferSize(2 * 1024)//
                .build(), //
                9, // expectedNumberKeysInScarceIndex
                5// expectedNumberOfFile
        ), arguments(tdi, tds, dir3, Segment.<Integer, String>builder()//
                .withDirectory(dir3)//
                .withId(id3)//
                .withKeyTypeDescriptor(tdi)//
                .withValueTypeDescriptor(tds)//
                .withMaxNumberOfKeysInSegmentCache(5)//
                .withMaxNumberOfKeysInIndexPage(2)//
                .withBloomFilterIndexSizeInBytes(0)//
                .withDiskIoBufferSize(4 * 1024)//
                .build(), //
                5, // expectedNumberKeysInScarceIndex
                7 // expectedNumberOfFile
        ));
    }

}
