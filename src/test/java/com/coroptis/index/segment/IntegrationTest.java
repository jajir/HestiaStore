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
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

public class IntegrationTest {

    private final Logger logger = LoggerFactory
            .getLogger(IntegrationTest.class);

    private final TypeDescriptorString tds = new TypeDescriptorString();
    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    // TODO overit pocet souboru v directory, po testu

    @ParameterizedTest
    @MethodSource("segmentProvider")
    void test_empty_segment_stats(final TypeDescriptorInteger tdi,
            final TypeDescriptorString tds, final Directory directory,
            final Segment<Integer, String> seg) throws Exception {

        seg.forceCompact();

        final List<Pair<Integer, String>> list = toList(seg.getStream());
        assertEquals(0, list.size());
        final SegmentStats stats = seg.getStats();
        assertEquals(0, stats.getNumberOfKeys());
        assertEquals(0, stats.getNumberOfKeysInCache());
        assertEquals(0, stats.getNumberOfKeysInIndex());
        assertEquals(0, stats.getNumberOfKeysInScarceIndex());

        assertNull(seg.get(1));
        assertEquals(4, numberOfFilesInDirectory(directory));

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

        final List<Pair<Integer, String>> list = toList(seg.getStream());
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

        assertEquals(4, numberOfFilesInDirectoryP(directory));
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

        final List<Pair<Integer, String>> list = toList(seg.getStream());
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

        final List<Pair<Integer, String>> list = toList(seg.getStream());
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

        assertEquals(4, seg.getStats().getNumberOfKeys());
        assertEquals(4, seg.getStats().getNumberOfKeysInCache());
        assertEquals(0, seg.getStats().getNumberOfKeysInIndex());

        final List<Pair<Integer, String>> list = toList(seg.getStream());
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
                                .withMaxNumeberOfKeysInIndexPage(1).build(),
                        4),
                arguments(tdi, tds, directory,
                        Segment.<Integer, String>builder()
                                .withDirectory(directory).withId(id)
                                .withKeyTypeDescriptor(tdi)
                                .withValueTypeDescriptor(tds)
                                .withMaxNumberOfKeysInSegmentCache(2)
                                .withMaxNumeberOfKeysInIndexPage(2).build(),
                        3));
    }

    private List<Pair<Integer, String>> toList(
            final Stream<Pair<Integer, String>> stream) {
        final ArrayList<Pair<Integer, String>> out = new ArrayList<>();
        stream.forEach(out::add);
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
