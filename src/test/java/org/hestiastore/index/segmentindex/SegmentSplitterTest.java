package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSplitterTest {

    private static final SegmentId SEGMENT_ID = SegmentId.of(27);
    private static final SegmentId LOWER_ID = SegmentId.of(28);
    private static final SegmentId UPPER_ID = SegmentId.of(29);

    private static final Entry<String, String> ENTRY1 = Entry.of("key1",
            "value1");
    private static final Entry<String, String> ENTRY2 = Entry.of("key2",
            "value2");
    private static final Entry<String, String> ENTRY3 = Entry.of("key3",
            "value3");

    @Mock
    private Segment<String, String> segment;
    @Mock
    private SegmentWriterTxFactory<String, String> writerTxFactory;
    @Mock
    private EntryIterator<String, String> iterator;
    @Mock
    private WriteTransaction<String, String> lowerTx;
    @Mock
    private WriteTransaction<String, String> currentTx;
    @Mock
    private EntryWriter<String, String> lowerWriter;
    @Mock
    private EntryWriter<String, String> currentWriter;

    private SegmentSplitter<String, String> splitter;

    @BeforeEach
    void setUp() {
        splitter = new SegmentSplitter<>(segment, writerTxFactory);
    }

    @Test
    void split_rejects_null_segment_id() {
        final SegmentSplitterPlan<String, String> plan = SegmentSplitterPlan
                .fromPolicy(new SegmentSplitterPolicy<>(4L, false));
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> splitter.split(null, UPPER_ID, plan));
        assertEquals("Property 'lowerSegmentId' must not be null.",
                err.getMessage());
    }

    @Test
    void split_rejects_null_plan() {
        final IllegalArgumentException err = assertThrows(
                IllegalArgumentException.class,
                () -> splitter.split(LOWER_ID, UPPER_ID, null));
        assertEquals("Property 'plan' must not be null.", err.getMessage());
    }

    @Test
    void split_fails_when_plan_is_not_feasible() {
        when(segment.getId()).thenReturn(SEGMENT_ID);
        final SegmentSplitterPlan<String, String> plan = SegmentSplitterPlan
                .fromPolicy(new SegmentSplitterPolicy<>(2L, false));
        final IllegalStateException err = assertThrows(
                IllegalStateException.class,
                () -> splitter.split(LOWER_ID, UPPER_ID, plan));
        assertEquals("Splitting failed. Number of keys is too low.",
                err.getMessage());
        verify(segment).invalidateIterators();
    }

    @Test
    void split_writes_lower_and_upper_entries() {
        when(segment.getId()).thenReturn(SEGMENT_ID);
        final SegmentSplitterPlan<String, String> plan = SegmentSplitterPlan
                .fromPolicy(new SegmentSplitterPolicy<>(4L, false));
        when(segment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, true, true, true, false);
        when(iterator.next())
            .thenReturn(ENTRY1)
            .thenReturn(ENTRY2)
            .thenReturn(ENTRY3);
        when(writerTxFactory.openWriterTx(LOWER_ID)).thenReturn(lowerTx);
        when(writerTxFactory.openWriterTx(UPPER_ID)).thenReturn(currentTx);
        when(lowerTx.open()).thenReturn(lowerWriter);
        when(currentTx.open()).thenReturn(currentWriter);

        final SegmentSplitterResult<String, String> result = splitter
                .split(LOWER_ID, UPPER_ID, plan);

        assertTrue(result.isSplit());
        assertEquals(LOWER_ID, result.getSegmentId());
        assertEquals(ENTRY1.getKey(), result.getMinKey());
        assertEquals(ENTRY2.getKey(), result.getMaxKey());
        verify(lowerWriter).write(ENTRY1);
        verify(lowerWriter).write(ENTRY2);
        verify(currentWriter).write(ENTRY3);
        verify(lowerTx).commit();
        verify(currentTx).commit();
        verify(segment).invalidateIterators();
    }

    @Test
    void split_compacts_when_no_upper_entries_remain() {
        when(segment.getId()).thenReturn(SEGMENT_ID);
        final SegmentSplitterPlan<String, String> plan = SegmentSplitterPlan
                .fromPolicy(new SegmentSplitterPolicy<>(4L, false));
        when(segment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, true, false, false);
        when(iterator.next()).thenReturn(ENTRY1).thenReturn(ENTRY2);
        when(writerTxFactory.openWriterTx(LOWER_ID)).thenReturn(lowerTx);
        when(lowerTx.open()).thenReturn(lowerWriter);

        final SegmentSplitterResult<String, String> result = splitter
                .split(LOWER_ID, UPPER_ID, plan);

        assertFalse(result.isSplit());
        assertEquals(SegmentSplitterResult.SegmentSplittingStatus.COMPACTED,
                result.getStatus());
        verify(lowerWriter).write(ENTRY1);
        verify(lowerWriter).write(ENTRY2);
        verify(lowerTx).commit();
        verify(writerTxFactory, never()).openWriterTx(UPPER_ID);
        verify(segment).invalidateIterators();
    }
}
