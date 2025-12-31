package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IntegrationSegmentIndexSimpleTest {

    private static final int DISK_IO_BUFFER_SIZE = 6 * 1024; // 6KB
    private final Logger logger = LoggerFactory
            .getLogger(IntegrationSegmentIndexSimpleTest.class);

    final Directory directory = new MemDirectory();
    final SegmentId id = SegmentId.of(27);
    final TypeDescriptorShortString tds = new TypeDescriptorShortString();
    final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    private final List<Entry<Integer, String>> testData = List.of(
            Entry.of(1, "bbb"), Entry.of(2, "ccc"), Entry.of(3, "dde"),
            Entry.of(4, "ddf"), Entry.of(5, "ddg"), Entry.of(6, "ddh"),
            Entry.of(7, "ddi"), Entry.of(8, "ddj"), Entry.of(9, "ddk"),
            Entry.of(10, "ddl"), Entry.of(11, "ddm"));

    @Test
    void testBasic() {
        final SegmentIndex<Integer, String> index1 = makeSegmentIndex();

        testData.stream().forEach(index1::put);

        index1.compact();

        try (final Stream<Entry<Integer, String>> stream = testData.stream()) {
            stream.forEach(entry -> {
                final String value = index1.get(entry.getKey());
                assertEquals(entry.getValue(), value);
            });
        }
        index1.compact();

        index1.close();
        assertEquals(22, numberOfFilesInDirectoryP(directory));

        final SegmentIndex<Integer, String> index2 = makeSegmentIndex();
        testData.stream().forEach(entry -> {
            final String value = index2.get(entry.getKey());
            assertEquals(entry.getValue(), value);
        });

        final List<Entry<Integer, String>> combined = new ArrayList<>();
        final org.hestiastore.index.directory.async.AsyncDirectory asyncDirectory = org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                .wrap(directory);
        final KeySegmentCache<Integer> keySegmentCache = new KeySegmentCache<>(
                asyncDirectory, tdi);
        final List<SegmentId> segmentIds = keySegmentCache.getSegmentIds();
        keySegmentCache.close();
        asyncDirectory.close();
        for (final SegmentId segmentId : segmentIds) {
            combined.addAll(getSegmentData(segmentId));
        }
        combined.sort(java.util.Comparator.comparing(Entry<Integer, String>::getKey,
                tdi.getComparator()));
        assertEquals(testData.size(), combined.size());
        for (int i = 0; i < testData.size(); i++) {
            assertEquals(testData.get(i), combined.get(i));
        }

    }

    @Test
    void test_fullLog() {

        SegmentIndex<Integer, String> index1 = makeIndex(true);

        testData.stream().forEach(index1::put);

        // reopen index to make sure all log data at flushed at the disk
        index1.close();
        index1 = makeIndex(true);
    }

    @Test
    void test_merging_values_from_cache_and_segment() {
        final SegmentIndex<Integer, String> index1 = makeSegmentIndex();
        testData.stream().forEach(index1::put);
        index1.flush();

        try (final Stream<Entry<Integer, String>> stream = index1
                .getStream(SegmentWindow.unbounded())) {
            final List<Entry<Integer, String>> list = stream.toList();
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
        final SegmentIndex<Integer, String> index1 = makeSegmentIndex();
        testData.stream().forEach(index1::put);
        index1.flush();

        final List<Entry<Integer, String>> list1 = index1
                .getStream(SegmentWindow.unbounded()).toList();
        final List<Entry<Integer, String>> list2 = index1
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
        final SegmentIndex<Integer, String> index1 = makeSegmentIndex();
        testData.stream().forEach(index1::put);
        index1.close();

        final SegmentIndex<Integer, String> index2 = makeSegmentIndex();
        final List<Entry<Integer, String>> list1 = index2
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
        final SegmentIndex<Integer, String> index1 = makeSegmentIndex();
        index1.put(Entry.of(2, "duck"));
        index1.close();

        final SegmentIndex<Integer, String> index2 = makeSegmentIndex();
        final List<Entry<Integer, String>> list1 = index2
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
        final List<Entry<Integer, String>> data = IntStream
                .range(0, values.size() - 1)
                .mapToObj(i -> Entry.of(i, values.get(i))).toList();
        final List<Entry<Integer, String>> updatedData = IntStream
                .range(0, values.size() - 1)
                .mapToObj(i -> Entry.of(i, values.get(i + 1))).toList();

        final SegmentIndex<Integer, String> index1 = makeSegmentIndex();
        data.stream().forEach(index1::put);
        index1.flush();
        verifyDataIndex(index1, data);
        index1.close();

        final SegmentIndex<Integer, String> index2 = makeSegmentIndex();
        updatedData.stream().forEach(index2::put);
        verifyDataIndex(index2, updatedData);
        index2.close();
    }

    private void verifyDataIndex(final SegmentIndex<Integer, String> index,
            final List<Entry<Integer, String>> data) {
        final List<Entry<Integer, String>> indexData = index
                .getStream(SegmentWindow.unbounded()).toList();
        assertEquals(data.size(), indexData.size());
        for (int i = 0; i < data.size(); i++) {
            final Entry<Integer, String> entryData = data.get(i);
            final Entry<Integer, String> entryIndex = indexData.get(i);
            assertEquals(entryData.getKey(), entryIndex.getKey());
            assertEquals(entryData.getValue(), entryIndex.getValue());
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
        final SegmentIndex<Integer, String> index1 = makeSegmentIndex();
        testData.stream().forEach(index1::put);

        assertThrows(IllegalStateException.class, () -> makeSegmentIndex());
    }

    private SegmentIndex<Integer, String> makeSegmentIndex() {
        return makeIndex(false);
    }

    private SegmentIndex<Integer, String> makeIndex(boolean withLog) {
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(tdi) //
                .withValueTypeDescriptor(tds) //
                .withDiskIoBufferSizeInBytes(DISK_IO_BUFFER_SIZE)//
                .withMaxNumberOfKeysInSegment(5) //
                .withMaxNumberOfKeysInSegmentCache(3) //
                .withMaxNumberOfKeysInSegmentChunk(2) //
                .withMaxNumberOfKeysInCache(3) //
                .withBloomFilterIndexSizeInBytes(1000) //
                .withBloomFilterNumberOfHashFunctions(4) //
                .withContextLoggingEnabled(withLog) //
                .withName("test_index") //
                .build();
        return SegmentIndex.create(directory, conf);
    }

    private int numberOfFilesInDirectoryP(final Directory directory) {
        final AtomicInteger cx = new AtomicInteger(0);
        directory.getFileNames().sorted().forEach(fileName -> {
            logger.debug("Found file name {}", fileName);
            cx.incrementAndGet();
        });
        return cx.get();
    }

    private List<Entry<Integer, String>> getSegmentData(
            final SegmentId segmentId) {
        final Segment<Integer, String> seg = makeSegment(segmentId);
        final List<Entry<Integer, String>> out = new ArrayList<>();
        try (EntryIterator<Integer, String> iterator = seg.openIterator()) {
            while (iterator.hasNext()) {
                out.add(iterator.next());
            }
        }
        return out;
    }

    private Segment<Integer, String> makeSegment(final SegmentId segmentId) {
        return Segment.<Integer, String>builder()//
                .withAsyncDirectory(
                        org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                                .wrap(directory))//
                .withId(segmentId)//
                .withDiskIoBufferSize(DISK_IO_BUFFER_SIZE)//
                .withKeyTypeDescriptor(tdi)//
                .withValueTypeDescriptor(tds)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withBloomFilterIndexSizeInBytes(1000) //
                .withBloomFilterNumberOfHashFunctions(4) //
                .withMaxNumberOfKeysInSegmentCache(2)//
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
    }

}
