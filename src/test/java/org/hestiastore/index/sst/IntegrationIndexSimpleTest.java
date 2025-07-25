package org.hestiastore.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.log.LoggedKey;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IntegrationIndexSimpleTest {

    private final Logger logger = LoggerFactory
            .getLogger(IntegrationIndexSimpleTest.class);

    final Directory directory = new MemDirectory();
    final SegmentId id = SegmentId.of(27);
    final TypeDescriptorShortString tds = new TypeDescriptorShortString();
    final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    private final List<Pair<Integer, String>> testData = List.of(
            Pair.of(1, "bbb"), Pair.of(2, "ccc"), Pair.of(3, "dde"),
            Pair.of(4, "ddf"), Pair.of(5, "ddg"), Pair.of(6, "ddh"),
            Pair.of(7, "ddi"), Pair.of(8, "ddj"), Pair.of(9, "ddk"),
            Pair.of(10, "ddl"), Pair.of(11, "ddm"));

    @Test
    void testBasic() {
        final Index<Integer, String> index1 = makeSstIndex();

        testData.stream().forEach(index1::put);

        index1.compact();

        try (final Stream<Pair<Integer, String>> stream = testData.stream()) {
            stream.forEach(pair -> {
                final String value = index1.get(pair.getKey());
                assertEquals(pair.getValue(), value);
            });
        }
        index1.compact();

        index1.close();
        assertEquals(14, numberOfFilesInDirectoryP(directory));

        final Index<Integer, String> index2 = makeSstIndex();
        testData.stream().forEach(pair -> {
            final String value = index2.get(pair.getKey());
            assertEquals(pair.getValue(), value);
        });

        List<Pair<Integer, String>> pairs1 = getSegmentData(1);
        assertEquals(Pair.of(1, "bbb"), pairs1.get(0));
        assertEquals(Pair.of(2, "ccc"), pairs1.get(1));
        assertEquals(Pair.of(3, "dde"), pairs1.get(2));
        assertEquals(Pair.of(4, "ddf"), pairs1.get(3));
        assertEquals(4, pairs1.size());

        List<Pair<Integer, String>> pairs2 = getSegmentData(2);
        assertEquals(Pair.of(5, "ddg"), pairs2.get(0));
        assertEquals(Pair.of(6, "ddh"), pairs2.get(1));
        assertEquals(Pair.of(7, "ddi"), pairs2.get(2));
        assertEquals(3, pairs2.size());

        List<Pair<Integer, String>> pairs3 = getSegmentData(3);
        assertEquals(0, pairs3.size());

        List<Pair<Integer, String>> pairs4 = getSegmentData(0);
        assertEquals(Pair.of(8, "ddj"), pairs4.get(0));
        assertEquals(Pair.of(9, "ddk"), pairs4.get(1));
        assertEquals(Pair.of(10, "ddl"), pairs4.get(2));
        assertEquals(Pair.of(11, "ddm"), pairs4.get(3));
        assertEquals(4, pairs4.size());

    }

    @Test
    void test_fullLog() {

        Index<Integer, String> index1 = makeIndex(true);

        testData.stream().forEach(index1::put);

        // reopen index to make sure all log data at flushed at the disk
        index1.close();
        index1 = makeIndex(true);

        final List<Pair<LoggedKey<Integer>, String>> list = index1
                .getLogStreamer().stream().toList();
        assertEquals(testData.size(), list.size());
    }

    @Test
    void test_merging_values_from_cache_and_segment() {
        final Index<Integer, String> index1 = makeSstIndex();
        testData.stream().forEach(index1::put);
        index1.flush();

        try (final Stream<Pair<Integer, String>> stream = index1
                .getStream(SegmentWindow.unbounded())) {
            final List<Pair<Integer, String>> list = stream.toList();
            assertEquals(testData.size(), list.size());
        }

    }

    /**
     * Verify that stream could be read repeatedly without concurrent
     * modification problem.
     * 
     * @
     */
    @Test
    void test_repeated_read() {
        final Index<Integer, String> index1 = makeSstIndex();
        testData.stream().forEach(index1::put);
        index1.flush();

        final List<Pair<Integer, String>> list1 = index1
                .getStream(SegmentWindow.unbounded()).toList();
        final List<Pair<Integer, String>> list2 = index1
                .getStream(SegmentWindow.unbounded()).toList();
        assertEquals(testData.size(), list1.size());
        assertEquals(testData.size(), list2.size());
    }

    /**
     * Verify that data could be read from index after index is closed and new
     * one is opened.
     * 
     * @
     */
    @Test
    void test_read_from_reopend_index_multiple_records() {
        final Index<Integer, String> index1 = makeSstIndex();
        testData.stream().forEach(index1::put);
        index1.close();

        final Index<Integer, String> index2 = makeSstIndex();
        final List<Pair<Integer, String>> list1 = index2
                .getStream(SegmentWindow.unbounded()).toList();
        assertEquals(testData.size(), list1.size());
    }

    /**
     * Verify that single record could be written to index and after reopening
     * it's still there.
     * 
     * @
     */
    @Test
    void test_read_from_reopend_index_single_records() {
        final Index<Integer, String> index1 = makeSstIndex();
        index1.put(Pair.of(2, "duck"));
        index1.close();

        final Index<Integer, String> index2 = makeSstIndex();
        final List<Pair<Integer, String>> list1 = index2
                .getStream(SegmentWindow.unbounded()).toList();
        assertEquals(1, list1.size());
    }

    /**
     * Verify that changed data are correctly stored.
     * 
     * @
     */
    @Test
    void test_storing_of_modified_data_after_index_close() {
        // generate data
        final List<String> values = List.of("aaa", "bbb", "ccc", "ddd", "eee",
                "fff");
        final List<Pair<Integer, String>> data = IntStream
                .range(0, values.size() - 1)
                .mapToObj(i -> Pair.of(i, values.get(i))).toList();
        final List<Pair<Integer, String>> updatedData = IntStream
                .range(0, values.size() - 1)
                .mapToObj(i -> Pair.of(i, values.get(i + 1))).toList();

        final Index<Integer, String> index1 = makeSstIndex();
        data.stream().forEach(index1::put);
        index1.flush();
        verifyDataIndex(index1, data);
        index1.close();

        final Index<Integer, String> index2 = makeSstIndex();
        updatedData.stream().forEach(index2::put);
        verifyDataIndex(index2, updatedData);
        index2.close();
    }

    private void verifyDataIndex(final Index<Integer, String> index,
            final List<Pair<Integer, String>> data) {
        final List<Pair<Integer, String>> indexData = index
                .getStream(SegmentWindow.unbounded()).toList();
        assertEquals(data.size(), indexData.size());
        for (int i = 0; i < data.size(); i++) {
            final Pair<Integer, String> pairData = data.get(i);
            final Pair<Integer, String> pairIndex = indexData.get(i);
            assertEquals(pairData.getKey(), pairIndex.getKey());
            assertEquals(pairData.getValue(), pairIndex.getValue());
        }
    }

    /**
     * Verify that data could be read from index after index is closed and new
     * one is opened.
     * 
     * @
     */
    @Test
    void test_read_from_unclosed_index() {
        final Index<Integer, String> index1 = makeSstIndex();
        testData.stream().forEach(index1::put);

        assertThrows(IllegalStateException.class, () -> makeSstIndex());
    }

    private Index<Integer, String> makeSstIndex() {
        return makeIndex(false);
    }

    private Index<Integer, String> makeIndex(boolean withLog) {
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(tdi) //
                .withValueTypeDescriptor(tds) //
                .withMaxNumberOfKeysInSegment(5) //
                .withMaxNumberOfKeysInSegmentCache(3L) //
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(4L) //
                .withMaxNumberOfKeysInSegmentIndexPage(2) //
                .withMaxNumberOfKeysInCache(3) //
                .withBloomFilterIndexSizeInBytes(1000) //
                .withBloomFilterNumberOfHashFunctions(4) //
                .withLogEnabled(withLog) //
                .withName("test_index") //
                .build();
        return Index.create(directory, conf);
    }

    private int numberOfFilesInDirectoryP(final Directory directory) {
        final AtomicInteger cx = new AtomicInteger(0);
        directory.getFileNames().sorted().forEach(fileName -> {
            logger.debug("Found file name {}", fileName);
            cx.incrementAndGet();
        });
        return cx.get();
    }

    private List<Pair<Integer, String>> getSegmentData(final int segmentId) {
        final Segment<Integer, String> seg = makeSegment(segmentId);
        final List<Pair<Integer, String>> out = new ArrayList<>();
        try (PairIterator<Integer, String> iterator = seg.openIterator()) {
            while (iterator.hasNext()) {
                out.add(iterator.next());
            }
        }
        return out;
    }

    private Segment<Integer, String> makeSegment(final int segmentId) {
        return Segment.<Integer, String>builder()//
                .withDirectory(directory)//
                .withId(SegmentId.of(segmentId))//
                .withKeyTypeDescriptor(tdi)//
                .withValueTypeDescriptor(tds)//
                .withMaxNumberOfKeysInIndexPage(2)//
                .withBloomFilterIndexSizeInBytes(1000) //
                .withBloomFilterNumberOfHashFunctions(4) //
                .withMaxNumberOfKeysInSegmentCache(2)//
                .build();
    }

}
