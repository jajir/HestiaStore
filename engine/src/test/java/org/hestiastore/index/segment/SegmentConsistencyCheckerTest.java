package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.OperationResult;
import org.hestiastore.index.OperationStatus;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentConsistencyCheckerTest {

    private static final SegmentId SEGMENT_ID = SegmentId.of(13);
    private static final TypeDescriptor<Integer> TYPE_DESCRIPTOR_INTEGER = new TypeDescriptorInteger();
    private static final Entry<Integer, String> ENTRY1 = Entry.of(1, "a");
    private static final Entry<Integer, String> ENTRY2 = Entry.of(2, "b");
    private static final Entry<Integer, String> ENTRY3 = Entry.of(3, "c");

    @Mock
    private Segment<Integer, String> segment;

    @Mock
    private EntryIterator<Integer, String> iterator;

    private SegmentConsistencyChecker<Integer, String> checker;

    @Test
    void test_noData() {
        when(segment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(OperationResult.ok(iterator));
        when(iterator.hasNext()).thenReturn(false);

        final Integer lastKey = checker.checkAndRepairConsistency();

        assertNull(lastKey);
        verify(iterator).close();
    }

    @Test
    void test_3_records() {
        when(segment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(OperationResult.ok(iterator));
        when(iterator.hasNext()).thenReturn(true, true, true, false);
        when(iterator.next()).thenReturn(ENTRY1).thenReturn(ENTRY2)
                .thenReturn(ENTRY3);

        final Integer lastKey = checker.checkAndRepairConsistency();

        assertEquals(3, lastKey);
        verify(iterator).close();
    }

    @Test
    void test_same_records() {
        when(segment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(OperationResult.ok(iterator));
        when(iterator.hasNext()).thenReturn(true, true, true, false);
        when(iterator.next()).thenReturn(ENTRY1).thenReturn(ENTRY3)
                .thenReturn(ENTRY3);

        final Exception e = assertThrows(IndexException.class,
                () -> checker.checkAndRepairConsistency());

        assertEquals(
                "Keys in segment 'segment-00013' are not sorted. "
                        + "Key '3' have to higher than key '3'.",
                e.getMessage());
        verify(iterator).close();
    }

    @Test
    void test_invalid_order() {
        when(segment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(OperationResult.ok(iterator));
        when(iterator.hasNext()).thenReturn(true, true, true, false);
        when(iterator.next()).thenReturn(ENTRY1).thenReturn(ENTRY3)
                .thenReturn(ENTRY2);

        final Exception e = assertThrows(IndexException.class,
                () -> checker.checkAndRepairConsistency());

        assertEquals(
                "Keys in segment 'segment-00013' are not sorted. "
                        + "Key '2' have to higher than key '3'.",
                e.getMessage());
        verify(iterator).close();
    }

    @ParameterizedTest
    @EnumSource(value = OperationStatus.class,
            names = { "BUSY", "CLOSED", "ERROR" })
    void tryCheckAndRepairConsistency_returnsUnavailableStatus(
            final OperationStatus status) {
        when(segment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(OperationResult.fromStatus(status));

        final OperationResult<Integer> result = checker
                .tryCheckAndRepairConsistency();

        assertEquals(status, result.getStatus());
        assertNull(result.getValue());
    }

    @Test
    void checkAndRepairConsistency_reportsUnavailableStatus() {
        when(segment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(OperationResult.busy());

        final IndexException exception = assertThrows(IndexException.class,
                () -> checker.checkAndRepairConsistency());

        assertEquals(
                "Segment 'segment-00013' is not ready for consistency check: BUSY",
                exception.getMessage());
    }

    @BeforeEach
    void setUp() {
        when(segment.getId()).thenReturn(SEGMENT_ID);
        checker = new SegmentConsistencyChecker<>(segment,
                TYPE_DESCRIPTOR_INTEGER.getComparator());
    }

    @AfterEach
    void tearDown() {
        checker = null;
    }

}
