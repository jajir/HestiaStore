package com.coroptis.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

public class IntegrationTest {

    private final Logger logger = LoggerFactory
            .getLogger(IntegrationTest.class);

    private final TypeDescriptorString tds = new TypeDescriptorString();
    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    @ParameterizedTest
    @MethodSource("segmentProvider")
    void test_empty_segment_stats(final TypeDescriptorInteger tdi,
            final TypeDescriptorString tds, final Directory directory,
            final Segment<Integer, String> seg) throws Exception {

        seg.forceCompact();

        final List<Pair<Integer, String>> list = toList(seg.openIterator());
        assertEquals(0, list.size());
        final SegmentStats stats = seg.getStats();
        assertEquals(0, stats.getNumberOfKeys());
        assertEquals(0, stats.getNumberOfKeysInCache());
        assertEquals(0, stats.getNumberOfKeysInIndex());
        assertEquals(0, stats.getNumberOfKeysInScarceIndex());

        assertNull(seg.get(1));
        assertEquals(5, numberOfFilesInDirectory(directory));

    }

    @ParameterizedTest
    @MethodSource("segmentProvider")
    void test_simple(final TypeDescriptorInteger tdi,
            final TypeDescriptorString tds, final Directory directory,
            final Segment<Integer, String> seg,
            final int expectedNumberKeysInScarceIndex) throws Exception {

        try (final SegmentWriter<Integer, String> writer = seg.openWriter()) {
            writer.put(Pair.of(2, "a"));
            writer.put(Pair.of(3, "b"));
            writer.put(Pair.of(4, "c"));
            writer.put(Pair.of(5, "d"));
        }

        assertEquals(4, seg.getStats().getNumberOfKeys());

        final List<Pair<Integer, String>> list = toList(seg.openIterator());
        assertEquals(Pair.of(2, "a"), list.get(0));
        assertEquals(Pair.of(3, "b"), list.get(1));
        assertEquals(Pair.of(4, "c"), list.get(2));
        assertEquals(Pair.of(5, "d"), list.get(3));
        assertEquals(4, list.size());

        assertEquals(expectedNumberKeysInScarceIndex,
                seg.getStats().getNumberOfKeysInScarceIndex());

        assertNull(seg.get(6));
        assertEquals("a", seg.get(2));
        assertEquals("b", seg.get(3));
        assertEquals("c", seg.get(4));
        assertEquals("d", seg.get(5));

        assertEquals(5, numberOfFilesInDirectoryP(directory));
    }

    @ParameterizedTest
    @MethodSource("segmentProvider")
    void test_split(final TypeDescriptorInteger tdi,
            final TypeDescriptorString tds, final Directory directory,
            final Segment<Integer, String> seg,
            final int expectedNumberKeysInScarceIndex) throws Exception {

        try (final SegmentWriter<Integer, String> writer = seg.openWriter()) {
            writer.put(Pair.of(2, "a"));
            writer.put(Pair.of(3, "b"));
            writer.put(Pair.of(4, "c"));
            writer.put(Pair.of(5, "d"));
        }

        final SegmentId segId = SegmentId.of(3);
        final Segment<Integer, String> smaller = seg.split(segId);

        final List<Pair<Integer, String>> list1 = toList(seg.openIterator());
        assertEquals(Pair.of(4, "c"), list1.get(0));
        assertEquals(Pair.of(5, "d"), list1.get(1));
        assertEquals(2, list1.size());

        final List<Pair<Integer, String>> list2 = toList(
                smaller.openIterator());
        assertEquals(Pair.of(2, "a"), list2.get(0));
        assertEquals(Pair.of(3, "b"), list2.get(1));
        assertEquals(2, list2.size());

        assertNull(seg.get(2));
        assertNull(seg.get(3));
        assertEquals("c", seg.get(4));
        assertEquals("d", seg.get(5));

        assertNull(smaller.get(4));
        assertNull(smaller.get(5));
        assertEquals("a", smaller.get(2));
        assertEquals("b", smaller.get(3));

        assertEquals(10, numberOfFilesInDirectoryP(directory));
    }

    @Test
    void test_duplicities() throws Exception {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final Segment<Integer, String> seg = Segment.<Integer, String>builder()
                .withDirectory(directory).withId(id).withKeyTypeDescriptor(tdi)
                .withValueTypeDescriptor(tds).build();

        try (final SegmentWriter<Integer, String> writer = seg.openWriter()) {
            writer.put(Pair.of(2, "a"));
            writer.put(Pair.of(3, "b"));
            writer.put(Pair.of(3, "bb"));
            writer.put(Pair.of(4, "c"));
            writer.put(Pair.of(5, "d"));
            writer.put(Pair.of(5, "dd"));
            writer.put(Pair.of(5, "ddd"));
        }

        assertEquals(4, seg.getStats().getNumberOfKeys());
        assertEquals(4, seg.getStats().getNumberOfKeysInCache());
        assertEquals(0, seg.getStats().getNumberOfKeysInIndex());

        final List<Pair<Integer, String>> list = toList(seg.openIterator());
        assertEquals(Pair.of(2, "a"), list.get(0));
        assertEquals(Pair.of(3, "bb"), list.get(1));
        assertEquals(Pair.of(4, "c"), list.get(2));
        assertEquals(Pair.of(5, "ddd"), list.get(3));
        assertEquals(4, list.size());

        assertNull(seg.get(6));
        assertEquals("a", seg.get(2));
        assertEquals("bb", seg.get(3));
        assertEquals("c", seg.get(4));
        assertEquals("ddd", seg.get(5));
    }

    @Test
    void test_write_unordered() throws Exception {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final Segment<Integer, String> seg = Segment.<Integer, String>builder()
                .withDirectory(directory).withId(id).withKeyTypeDescriptor(tdi)
                .withValueTypeDescriptor(tds).build();

        try (final SegmentWriter<Integer, String> writer = seg.openWriter()) {
            writer.put(Pair.of(5, "d"));
            writer.put(Pair.of(3, "b"));
            writer.put(Pair.of(5, "dd"));
            writer.put(Pair.of(2, "a"));
            writer.put(Pair.of(3, "bb"));
            writer.put(Pair.of(4, "c"));
            writer.put(Pair.of(5, "ddd"));
        }

        assertEquals(4, seg.getStats().getNumberOfKeys());
        assertEquals(4, seg.getStats().getNumberOfKeysInCache());
        assertEquals(0, seg.getStats().getNumberOfKeysInIndex());

        final List<Pair<Integer, String>> list = toList(seg.openIterator());
        assertEquals(Pair.of(2, "a"), list.get(0));
        assertEquals(Pair.of(3, "bb"), list.get(1));
        assertEquals(Pair.of(4, "c"), list.get(2));
        assertEquals(Pair.of(5, "ddd"), list.get(3));
        assertEquals(4, list.size());

        assertNull(seg.get(6));
        assertEquals("a", seg.get(2));
        assertEquals("bb", seg.get(3));
        assertEquals("c", seg.get(4));
        assertEquals("ddd", seg.get(5));
    }

    @Test
    void test_write_unordered_tombstone() throws Exception {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final Segment<Integer, String> seg = Segment.<Integer, String>builder()
                .withDirectory(directory).withId(id).withKeyTypeDescriptor(tdi)
                .withValueTypeDescriptor(tds).build();

        try (final SegmentWriter<Integer, String> writer = seg.openWriter()) {
            writer.put(Pair.of(5, "d"));
            writer.put(Pair.of(3, "b"));
            writer.put(Pair.of(5, "dd"));
            writer.put(Pair.of(2, "a"));
            writer.put(Pair.of(3, "bb"));
            writer.put(Pair.of(4, "c"));
            writer.put(Pair.of(5, "ddd"));
            writer.put(Pair.of(5, TypeDescriptorString.TOMBSTONE_VALUE));
        }

        /**
         * There is a error in computing number of keys in cache. There are 3
         * keys, because one is deleted.
         */
        assertEquals(4, seg.getStats().getNumberOfKeys());
        assertEquals(4, seg.getStats().getNumberOfKeysInCache());
        assertEquals(0, seg.getStats().getNumberOfKeysInIndex());

        final List<Pair<Integer, String>> list = toList(seg.openIterator());
        assertEquals(Pair.of(2, "a"), list.get(0));
        assertEquals(Pair.of(3, "bb"), list.get(1));
        assertEquals(Pair.of(4, "c"), list.get(2));
        assertEquals(3, list.size());

        assertNull(seg.get(5));
        assertEquals("a", seg.get(2));
        assertEquals("bb", seg.get(3));
        assertEquals("c", seg.get(4));
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

        try (final SegmentWriter<Integer, String> writer = seg.openWriter()) {
            writer.put(Pair.of(5, "d"));
            writer.put(Pair.of(3, "b"));
            writer.put(Pair.of(5, "dd"));
            writer.put(Pair.of(2, "a"));
            writer.put(Pair.of(3, "bb"));
            writer.put(Pair.of(4, "c"));
            writer.put(Pair.of(5, "ddd"));
            writer.put(Pair.of(5, TypeDescriptorString.TOMBSTONE_VALUE));
        }
        seg.forceCompact();

        assertEquals(3, seg.getStats().getNumberOfKeys());
        assertEquals(0, seg.getStats().getNumberOfKeysInCache());
        assertEquals(3, seg.getStats().getNumberOfKeysInIndex());

        final List<Pair<Integer, String>> list = toList(seg.openIterator());
        assertEquals(Pair.of(2, "a"), list.get(0));
        assertEquals(Pair.of(3, "bb"), list.get(1));
        assertEquals(Pair.of(4, "c"), list.get(2));
        assertEquals(3, list.size());

        assertNull(seg.get(5));
        assertEquals("a", seg.get(2));
        assertEquals("bb", seg.get(3));
        assertEquals("c", seg.get(4));
    }

    /**
     * Prepare data for tests. Directory object is shared between parameterized
     * tests.
     * 
     * @return
     */
    static Stream<Arguments> segmentProvider() {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final TypeDescriptorString tds = new TypeDescriptorString();
        final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
        return Stream.of(

                arguments(tdi, tds, directory,
                        Segment.<Integer, String>builder()
                                .withDirectory(directory).withId(id)
                                .withKeyTypeDescriptor(tdi)
                                .withValueTypeDescriptor(tds)
                                .withMaxNumberOfKeysInSegmentCache(10).build(),
                        0),
                arguments(tdi, tds, directory,
                        Segment.<Integer, String>builder()
                                .withDirectory(directory).withId(id)
                                .withKeyTypeDescriptor(tdi)
                                .withValueTypeDescriptor(tds)
                                .withMaxNumberOfKeysInSegmentCache(1)
                                .withMaxNumberOfKeysInIndexPage(1).build(),
                        4),
                arguments(tdi, tds, directory,
                        Segment.<Integer, String>builder()
                                .withDirectory(directory).withId(id)
                                .withKeyTypeDescriptor(tdi)
                                .withValueTypeDescriptor(tds)
                                .withMaxNumberOfKeysInSegmentCache(2)
                                .withMaxNumberOfKeysInIndexPage(2).build(),
                        3));
    }

    private List<Pair<Integer, String>> toList(
            final PairIterator<Integer, String> iterator) {
        final ArrayList<Pair<Integer, String>> out = new ArrayList<>();
        while (iterator.hasNext()) {
            out.add(iterator.next());

        }
        iterator.close();
        return out;
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

}
