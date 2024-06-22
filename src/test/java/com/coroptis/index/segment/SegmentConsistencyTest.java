package com.coroptis.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

/**
 * When iterator is opened and data are changed during iterating than updated
 * data should be returned from already opened iterator.
 * 
 * @author honza
 *
 */
public class SegmentConsistencyTest extends AbstractSegmentTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    final Directory dir = new MemDirectory();
    final SegmentId id = SegmentId.of(29);

    /**
     * Test verify that read operation provide latest values. Even writing to
     * segment during
     * 
     * @throws Exception
     */
    @Test
    void test_consistency() throws Exception {
        final Segment<Integer, Integer> seg = makeSegment();
        for (int i = 0; i < 100; i++) {
            writePairs(seg, makeList(i));
            verifySegmentData(seg, makeList(i));
        }
    }

    @Test
    void test_reading_of_updated_values() throws Exception {
        final Segment<Integer, Integer> seg = makeSegment();
        writePairs(seg, makeList(0));
        final PairIterator<Integer, Integer> iterator = seg.openIterator();
        for (int i = 1; i < 100; i++) {
            while (iterator.hasNext()) {
                writePairs(seg, makeList(i));
                final Pair<Integer, Integer> p = iterator.next();
                assertEquals(i, p.getValue());
                verifySegmentData(seg, makeList(i));
            }
        }
    }

    private List<Pair<Integer, Integer>> makeList(final int no) {
        final List<Pair<Integer, Integer>> out = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            out.add(Pair.of(i, no));
        }
        return out;
    }

    private Segment<Integer, Integer> makeSegment() {
        final Segment<Integer, Integer> seg = Segment
                .<Integer, Integer>builder()//
                .withDirectory(dir)//
                .withId(id)//
                .withKeyTypeDescriptor(tdi)//
                .withValueTypeDescriptor(tdi)//
                .withMaxNumberOfKeysInSegmentMemory(10)//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .build();
        return seg;
    }

}
