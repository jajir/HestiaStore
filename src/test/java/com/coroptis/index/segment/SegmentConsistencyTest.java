package com.coroptis.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.PairWriter;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

public class SegmentConsistencyTest {

    final List<String> values = List.of("aaa", "bbb", "ccc", "ddd", "eee",
            "fff");
    final List<Pair<Integer, String>> data = IntStream
            .range(0, values.size() - 1)
            .mapToObj(i -> Pair.of(i, values.get(i)))
            .collect(Collectors.toList());
    final List<Pair<Integer, String>> updatedData = IntStream
            .range(0, values.size() - 1)
            .mapToObj(i -> Pair.of(i, values.get(i + 1)))
            .collect(Collectors.toList());

    private final TypeDescriptorString tds = new TypeDescriptorString();
    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    /**
     * Test that updated data are correctly stored into index.
     * 
     * @throws Exception
     */
    @Test
    void test_writing_updated_values() throws Exception {
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
    }

    private Segment<Integer, String> makeSegment(final Directory directory,
            final SegmentId id) {
        final Segment<Integer, String> seg = Segment.<Integer, String>builder()
                .withDirectory(directory).withId(id).withKeyTypeDescriptor(tdi)
                .withValueTypeDescriptor(tds).withMaxNumberOfKeysInIndexPage(2)
                .withMaxNumberOfKeysInSegmentCache(3).build();
        return seg;
    }

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

    private List<Pair<Integer, String>> toList(
            final Segment<Integer, String> index) {
        try (PairIterator<Integer, String> iterator = index.openIterator()) {
            final List<Pair<Integer, String>> data = new ArrayList<>();
            iterator.forEachRemaining(data::add);
            return data;
        }

    }

}
