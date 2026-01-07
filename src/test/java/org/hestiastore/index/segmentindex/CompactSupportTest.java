package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CompactSupportTest {

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    @Mock
    private KeySegmentCache<Integer> keySegmentCache;

    @Mock
    private Segment<Integer, String> segment0;

    @Mock
    private Segment<Integer, String> segment1;

    private CompactSupport<Integer, String> newSupport() {
        return new CompactSupport<>(segmentRegistry, keySegmentCache,
                new TypeDescriptorInteger().getComparator());
    }

    @Test
    void compact_null_entry_throws() {
        final CompactSupport<Integer, String> cs = newSupport();
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> cs.compact(null));
        assertEquals("Property 'entry' must not be null.", e.getMessage());
    }

    @Test
    void first_entry_defers_flush() {
        final CompactSupport<Integer, String> cs = newSupport();
        when(keySegmentCache.insertKeyToSegment(1))
                .thenReturn(KeySegmentCache.FIRST_SEGMENT_ID);

        cs.compact(Entry.of(1, "A"));

        verify(segmentRegistry, never()).getSegment(any());
        assertEquals(List.of(), cs.getEligibleSegmentIds());
    }

    @Test
    void same_segment_multiple_entries_no_flush_until_change() {
        final CompactSupport<Integer, String> cs = newSupport();
        final SegmentId seg0 = KeySegmentCache.FIRST_SEGMENT_ID;
        when(keySegmentCache.insertKeyToSegment(1)).thenReturn(seg0);
        when(keySegmentCache.insertKeyToSegment(2)).thenReturn(seg0);

        cs.compact(Entry.of(1, "A"));
        cs.compact(Entry.of(2, "B"));

        verify(segmentRegistry, never()).getSegment(any());
        assertEquals(List.of(), cs.getEligibleSegmentIds());
    }

    @Test
    void segment_change_triggers_flush_and_records_first_segment() {
        final CompactSupport<Integer, String> cs = newSupport();
        final SegmentId seg0 = KeySegmentCache.FIRST_SEGMENT_ID;
        final SegmentId seg1 = SegmentId.of(1);

        when(keySegmentCache.insertKeyToSegment(1)).thenReturn(seg0);
        when(keySegmentCache.insertKeyToSegment(2)).thenReturn(seg0);
        when(keySegmentCache.insertKeyToSegment(100)).thenReturn(seg1);

        when(segmentRegistry.getSegment(seg0)).thenReturn(segment0);
        when(segment0.put(any(), any())).thenReturn(SegmentResult.ok());
        when(segment0.flush()).thenReturn(SegmentResult.ok());

        cs.compact(Entry.of(1, "A"));
        cs.compact(Entry.of(2, "B"));
        // segment changes here -> flush previous batch to seg0
        cs.compact(Entry.of(100, "C"));

        // two entries written to seg0 and flushed
        verify(segment0).put(1, "A");
        verify(segment0).put(2, "B");
        verify(segment0).flush();

        // because seg0 is FIRST_SEGMENT_ID, cache gets updated with max key (2) and flushed
        verify(keySegmentCache, atLeastOnce()).insertKeyToSegment(eq(2));
        verify(keySegmentCache).optionalyFlush();

        // eligible now contains seg0 only
        assertEquals(List.of(seg0), cs.getEligibleSegmentIds());
    }

    @Test
    void compactRest_flushes_pending_batch_and_tracks_segment() {
        final CompactSupport<Integer, String> cs = newSupport();
        final SegmentId seg1 = SegmentId.of(1);

        when(keySegmentCache.insertKeyToSegment(10)).thenReturn(seg1);
        when(keySegmentCache.insertKeyToSegment(11)).thenReturn(seg1);
        when(segmentRegistry.getSegment(seg1)).thenReturn(segment1);
        when(segment1.put(any(), any())).thenReturn(SegmentResult.ok());
        when(segment1.flush()).thenReturn(SegmentResult.ok());

        cs.compact(Entry.of(10, "X"));
        cs.compact(Entry.of(11, "Y"));

        // Flush remaining
        cs.flush();

        verify(segment1).put(10, "X");
        verify(segment1).put(11, "Y");
        verify(segment1).flush();

        assertEquals(List.of(seg1), cs.getEligibleSegmentIds());
    }

    @Test
    void compactRest_noop_when_nothing_pending() {
        final CompactSupport<Integer, String> cs = newSupport();
        cs.flush();
        // no interactions
        verify(segmentRegistry, never()).getSegment(any());
        assertEquals(List.of(), cs.getEligibleSegmentIds());
    }
}
