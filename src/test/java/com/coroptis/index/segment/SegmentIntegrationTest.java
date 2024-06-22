package com.coroptis.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.PairWriter;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

public class SegmentIntegrationTest {

    private final Logger logger = LoggerFactory
            .getLogger(SegmentIntegrationTest.class);

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

        verifySegmentData(seg, Arrays.asList());

        final SegmentStats stats = seg.getStats();
        assertEquals(0, stats.getNumberOfKeys());
        assertEquals(0, stats.getNumberOfKeysInCache());
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
        final SegmentSplitter.Result<Integer, String> result = seg.split(segId);
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
                .withValueTypeDescriptor(tds).build();

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
        assertEquals(4, seg.getStats().getNumberOfKeysInCache());
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
        assertEquals(4, seg.getStats().getNumberOfKeysInCache());
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
        assertEquals(4, seg.getStats().getNumberOfKeysInCache());
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
        seg.forceCompact();

        assertEquals(3, seg.getStats().getNumberOfKeys());
        assertEquals(0, seg.getStats().getNumberOfKeysInCache());
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
                Segment.<Integer, String>builder().withDirectory(dir1)
                        .withId(id1).withKeyTypeDescriptor(tdi)
                        .withValueTypeDescriptor(tds)
                        .withMaxNumberOfKeysInSegmentMemory(10)
                        .withMaxNumberOfKeysInSegmentCache(10).build(),
                2, // expectedNumberKeysInScarceIndex,
                10 // expectedNumberOfFile
        ), arguments(tdi, tds, dir2,
                Segment.<Integer, String>builder().withDirectory(dir2)
                        .withId(id2).withKeyTypeDescriptor(tdi)
                        .withValueTypeDescriptor(tds)
                        .withMaxNumberOfKeysInSegmentCache(1)
                        .withMaxNumberOfKeysInSegmentMemory(1)
                        .withMaxNumberOfKeysInIndexPage(1).build(),
                9, // expectedNumberKeysInScarceIndex
                5// expectedNumberOfFile
        ), arguments(tdi, tds, dir3,
                Segment.<Integer, String>builder().withDirectory(dir3)
                        .withId(id3).withKeyTypeDescriptor(tdi)
                        .withValueTypeDescriptor(tds)
                        .withMaxNumberOfKeysInSegmentCache(2)
                        .withMaxNumberOfKeysInSegmentMemory(2)
                        .withMaxNumberOfKeysInIndexPage(2).build(),
                5, // expectedNumberKeysInScarceIndex
                4 // expectedNumberOfFile
        ));
    }

    private int numberOfFilesInDirectory(final Directory directory) {
        return (int) directory.getFileNames().count();
    }

    private int numberOfFilesInDirectoryP(final Directory directory) {
        final AtomicInteger cx = new AtomicInteger(0);
        directory.getFileNames().forEach(fileName -> {
            logger.debug("Found file name {}", fileName);
            cx.incrementAndGet();
        });
        return cx.get();
    }

    /**
     * Convert pair iterator data to list
     * 
     * @param <M>      key type
     * @param <N>      value type
     * @param iterator
     * @returnlist of pairs with data from list
     */
    private <M, N> List<Pair<M, N>> toList(final PairIterator<M, N> iterator) {
        final ArrayList<Pair<M, N>> out = new ArrayList<>();
        while (iterator.hasNext()) {
            out.add(iterator.next());
        }
        iterator.close();
        return out;
    }

    /**
     * Simplify filling segment with data.
     * 
     * @param <M>   key type
     * @param <N>   value type
     * @param seg   required segment
     * @param pairs required list of pairs
     */
    private <M, N> void writePairs(final Segment<M, N> seg,
            final List<Pair<M, N>> pairs) {
        try (PairWriter<M, N> writer = seg.openWriter()) {
            for (final Pair<M, N> pair : pairs) {
                writer.put(pair);
            }
        }
    }

    /**
     * Open segment search and verify that found value for given key is equals
     * to expected value
     * 
     * @param <M>   key type
     * @param <N>   value type
     * @param seg   required segment
     * @param pairs required list of pairs of key and expected value
     */
    private <M, N> void verifySegmentSearch(final Segment<M, N> seg,
            final List<Pair<M, N>> pairs) {
        try (SegmentSearcher<M, N> searcher = seg.openSearcher()) {
            pairs.forEach(pair -> {
                final M key = pair.getKey();
                final N expectedValue = pair.getValue();
                assertEquals(expectedValue, searcher.get(key));
            });
        }
    }

    /**
     * Open segment search and verify that found value for given key is equals
     * to expected value
     * 
     * @param <M>   key type
     * @param <N>   value type
     * @param seg   required segment
     * @param pairs required list of expected data in segment
     */
    private <M, N> void verifySegmentData(final Segment<M, N> seg,
            final List<Pair<M, N>> pairs) {
        final List<Pair<M, N>> data = toList(seg.openIterator());
        assertEquals(pairs.size(), data.size(), "Unexpected segment data size");
        for (int i = 0; i < pairs.size(); i++) {
            final Pair<M, N> expectedPair = pairs.get(i);
            final Pair<M, N> realPair = data.get(i);
            assertEquals(expectedPair, realPair);
        }
    }

}
