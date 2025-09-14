package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.PairWriter;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class IntegrationSegmentWriteConsistencyTest {

    private static final TypeDescriptorShortString TDS = new TypeDescriptorShortString();
    private static final TypeDescriptorInteger TDI = new TypeDescriptorInteger();

    private final List<String> values = List.of("aaa", "bbb", "ccc", "ddd",
            "eee", "fff");
    private final List<Pair<Integer, String>> data = IntStream
            .range(0, values.size() - 1)
            .mapToObj(i -> Pair.of(i, values.get(i))).toList();
    private final List<Pair<Integer, String>> updatedData = IntStream
            .range(0, values.size() - 1)
            .mapToObj(i -> Pair.of(i, values.get(i + 1))).toList();

    /**
     * Test that updated data are correctly stored into index.
     * 
     * @
     */
    @Test
    void test_writing_updated_values() {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final Segment<Integer, String> seg1 = makeSegment(directory, id);
        try (PairWriter<Integer, String> writer = seg1.openWriter()) {
            data.forEach(writer::put);
        }
        verifyDataIndex(seg1, data);
        seg1.close();

        final Segment<Integer, String> seg2 = makeSegment(directory, id);
        try (PairWriter<Integer, String> writer = seg2.openWriter()) {
            updatedData.forEach(writer::put);
        }
        verifyDataIndex(seg2, updatedData);
        seg2.close();
        assertEquals(4, directory.getFileNames().count());
    }

    private Segment<Integer, String> makeSegment(final Directory directory,
            final SegmentId id) {
        return Segment.<Integer, String>builder().withDirectory(directory)//
                .withId(id)//
                .withKeyTypeDescriptor(TDI)//
                .withValueTypeDescriptor(TDS)//
                .withMaxNumberOfKeysInIndexPage(2)//
                .withMaxNumberOfKeysInSegmentCache(3)//
                .withBloomFilterIndexSizeInBytes(0)//
                .build();
    }

    // TODO move it to static utility class
    private void verifyDataIndex(final Segment<Integer, String> index,
            final List<Pair<Integer, String>> data) {
        final List<Pair<Integer, String>> indexData = toList(index);
        assertEquals(data.size(), indexData.size());
        for (int i = 0; i < data.size(); i++) {
            final Pair<Integer, String> pairData = data.get(i);
            final Pair<Integer, String> pairIndex = indexData.get(i);
            assertEquals(pairData.getKey(), pairIndex.getKey());
            assertEquals(pairData.getValue(), pairIndex.getValue());
        }
    }

    // TODO move it to static utility class
    private List<Pair<Integer, String>> toList(
            final Segment<Integer, String> segment) {
        try (PairIterator<Integer, String> iterator = segment.openIterator()) {
            final List<Pair<Integer, String>> out = new ArrayList<>();
            iterator.forEachRemaining(out::add);
            return out;
        }

    }

}
