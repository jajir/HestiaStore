package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentConsistencyCheckerTest {

    private static final SegmentId SEGMENT_ID = SegmentId.of(13);
    private static final TypeDescriptor<Integer> TYPE_DESCRIPTOR_INTEGER = new TypeDescriptorInteger();
    private static final Pair<Integer, String> PAIR1 = Pair.of(1, "a");
    private static final Pair<Integer, String> PAIR2 = Pair.of(2, "b");
    private static final Pair<Integer, String> PAIR3 = Pair.of(3, "c");

    @Mock
    private Segment<Integer, String> segment;

    @Mock
    private PairIterator<Integer, String> iterator;

    private SegmentConsistencyChecker<Integer, String> checker;

    @Test
    void test_noData() {
        when(segment.openIterator()).thenReturn(iterator);
        when(segment.getId()).thenReturn(SEGMENT_ID);
        when(iterator.hasNext()).thenReturn(false);

        final Integer lastKey = checker.checkAndRepairConsistency();
        assertNull(lastKey);
    }

    @Test
    void test_3_records() {
        when(segment.openIterator()).thenReturn(iterator);
        when(segment.getId()).thenReturn(SEGMENT_ID);
        when(iterator.hasNext()).thenReturn(true, true, true, false);
        when(iterator.next()).thenReturn(PAIR1).thenReturn(PAIR2)
                .thenReturn(PAIR3);

        final Integer lastKey = checker.checkAndRepairConsistency();
        assertEquals(3, lastKey);
    }

    @Test
    void test_same_records() {
        when(segment.openIterator()).thenReturn(iterator);
        when(segment.getId()).thenReturn(SEGMENT_ID);
        when(iterator.hasNext()).thenReturn(true, true, true, false);
        when(iterator.next()).thenReturn(PAIR1).thenReturn(PAIR3)
                .thenReturn(PAIR3);

        final Exception e = assertThrows(IndexException.class,
                () -> checker.checkAndRepairConsistency());

        assertEquals(
                "Keys in segment 'segment-00013' are not sorted. "
                        + "Key '3' have to higher than key '3'.",
                e.getMessage());
    }

    @Test
    void test_invalid_order() {
        when(segment.openIterator()).thenReturn(iterator);
        when(segment.getId()).thenReturn(SEGMENT_ID);
        when(iterator.hasNext()).thenReturn(true, true, true, false);
        when(iterator.next()).thenReturn(PAIR1).thenReturn(PAIR3)
                .thenReturn(PAIR2);

        final Exception e = assertThrows(IndexException.class,
                () -> checker.checkAndRepairConsistency());

        assertEquals(
                "Keys in segment 'segment-00013' are not sorted. "
                        + "Key '2' have to higher than key '3'.",
                e.getMessage());
    }

    @BeforeEach
    void setUp() {
        checker = new SegmentConsistencyChecker<>(segment,
                TYPE_DESCRIPTOR_INTEGER.getComparator());
    }

    @AfterEach
    void tearDown() {
        checker = null;
    }

}
