package org.hestiastore.index.segmentindex.core.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Optional;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentsIteratorTest {

    private static final SegmentId SEGMENT_ID_17 = SegmentId.of(17);
    private static final SegmentId SEGMENT_ID_23 = SegmentId.of(23);

    @Mock
    private SegmentRegistry<String, String> segmentRegistry;

    @Mock
    private BlockingSegment<String, String> handle17;

    @Mock
    private BlockingSegment<String, String> handle23;

    @Mock
    private EntryIterator<String, String> entryIterator17;

    @Mock
    private EntryIterator<String, String> entryIterator23;

    @Test
    void test_there_is_no_segment() {
        try (SegmentsIterator<String, String> iterator = new SegmentsIterator<>(
                new ArrayList<>(), segmentRegistry)) {
            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_segments_in_one() {
        when(segmentRegistry.tryGetSegment(SEGMENT_ID_17))
                .thenReturn(Optional.of(handle17));
        when(handle17.tryOpenIterator(SegmentIteratorIsolation.FAIL_FAST))
                .thenReturn(SegmentResult.ok(entryIterator17));
        when(entryIterator17.hasNext()).thenReturn(true, false);
        when(entryIterator17.next()).thenReturn(new Entry<>("key1", "value1"));

        final ArrayList<SegmentId> tst = new ArrayList<SegmentId>();
        tst.add(SEGMENT_ID_17);

        try (SegmentsIterator<String, String> iterator = new SegmentsIterator<>(
                tst, segmentRegistry)) {
            assertTrue(iterator.hasNext());
            final Entry<String, String> entry1 = iterator.next();
            assertEquals("key1", entry1.getKey());
            assertEquals("value1", entry1.getValue());

            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_segment_load_retries_when_registry_is_busy() {
        when(segmentRegistry.tryGetSegment(SEGMENT_ID_17))
                .thenReturn(Optional.of(handle17));
        when(handle17.tryOpenIterator(SegmentIteratorIsolation.FAIL_FAST))
                .thenReturn(SegmentResult.ok(entryIterator17));
        when(entryIterator17.hasNext()).thenReturn(true, false);
        when(entryIterator17.next()).thenReturn(new Entry<>("key1", "value1"));

        final ArrayList<SegmentId> ids = new ArrayList<>();
        ids.add(SEGMENT_ID_17);

        try (SegmentsIterator<String, String> iterator = new SegmentsIterator<>(
                ids, segmentRegistry)) {
            assertTrue(iterator.hasNext());
            final Entry<String, String> entry = iterator.next();
            assertEquals("key1", entry.getKey());
            assertEquals("value1", entry.getValue());
            assertFalse(iterator.hasNext());
        }

        verify(segmentRegistry).tryGetSegment(SEGMENT_ID_17);
    }

    @Test
    void test_fail_fast_skips_segment_when_registry_stays_busy() {
        when(segmentRegistry.tryGetSegment(SEGMENT_ID_17))
                .thenReturn(Optional.empty());

        final ArrayList<SegmentId> ids = new ArrayList<>();
        ids.add(SEGMENT_ID_17);

        try (SegmentsIterator<String, String> iterator = new SegmentsIterator<>(
                ids, segmentRegistry)) {
            assertFalse(iterator.hasNext());
        }

        verify(segmentRegistry).tryGetSegment(SEGMENT_ID_17);
    }

    @Test
    void test_open_iterator_retries_when_segment_is_busy() {
        when(segmentRegistry.tryGetSegment(SEGMENT_ID_17))
                .thenReturn(Optional.of(handle17));
        when(handle17.tryOpenIterator(SegmentIteratorIsolation.FAIL_FAST))
                .thenReturn(SegmentResult.busy())
                .thenReturn(SegmentResult.ok(entryIterator17));
        when(entryIterator17.hasNext()).thenReturn(true, false);
        when(entryIterator17.next()).thenReturn(new Entry<>("key1", "value1"));

        final ArrayList<SegmentId> ids = new ArrayList<>();
        ids.add(SEGMENT_ID_17);

        try (SegmentsIterator<String, String> iterator = new SegmentsIterator<>(
                ids, segmentRegistry)) {
            assertTrue(iterator.hasNext());
            iterator.next();
            assertFalse(iterator.hasNext());
        }

        verify(handle17, times(2))
                .tryOpenIterator(SegmentIteratorIsolation.FAIL_FAST);
    }

    @Test
    void test_fail_fast_skips_segment_when_open_iterator_stays_busy() {
        when(segmentRegistry.tryGetSegment(SEGMENT_ID_17))
                .thenReturn(Optional.of(handle17));
        when(handle17.tryOpenIterator(SegmentIteratorIsolation.FAIL_FAST))
                .thenReturn(SegmentResult.busy());

        final ArrayList<SegmentId> ids = new ArrayList<>();
        ids.add(SEGMENT_ID_17);

        try (SegmentsIterator<String, String> iterator = new SegmentsIterator<>(
                ids, segmentRegistry)) {
            assertFalse(iterator.hasNext());
        }

        verify(handle17, times(2))
                .tryOpenIterator(SegmentIteratorIsolation.FAIL_FAST);
    }

    @Test
    void test_segments_are_two() {
        when(segmentRegistry.tryGetSegment(SEGMENT_ID_17))
                .thenReturn(Optional.of(handle17));
        when(handle17.tryOpenIterator(SegmentIteratorIsolation.FAIL_FAST))
                .thenReturn(SegmentResult.ok(entryIterator17));
        when(entryIterator17.hasNext()).thenReturn(true, false);
        when(entryIterator17.next()).thenReturn(new Entry<>("key1", "value1"));

        when(segmentRegistry.tryGetSegment(SEGMENT_ID_23))
                .thenReturn(Optional.of(handle23));
        when(handle23.tryOpenIterator(SegmentIteratorIsolation.FAIL_FAST))
                .thenReturn(SegmentResult.ok(entryIterator23));
        when(entryIterator23.hasNext()).thenReturn(true, false);
        when(entryIterator23.next()).thenReturn(new Entry<>("key2", "value2"));

        final ArrayList<SegmentId> tst = new ArrayList<SegmentId>();
        tst.add(SEGMENT_ID_17);
        tst.add(SEGMENT_ID_23);

        try (SegmentsIterator<String, String> iterator = new SegmentsIterator<>(
                tst, segmentRegistry)) {
            assertTrue(iterator.hasNext());
            final Entry<String, String> entry1 = iterator.next();
            assertEquals("key1", entry1.getKey());
            assertEquals("value1", entry1.getValue());

            assertTrue(iterator.hasNext());
            final Entry<String, String> entry2 = iterator.next();
            assertEquals("key2", entry2.getKey());
            assertEquals("value2", entry2.getValue());

            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void testClose() {
        when(segmentRegistry.tryGetSegment(SEGMENT_ID_17))
                .thenReturn(Optional.of(handle17));
        when(handle17.tryOpenIterator(SegmentIteratorIsolation.FAIL_FAST))
                .thenReturn(SegmentResult.ok(entryIterator17));

        final ArrayList<SegmentId> tst = new ArrayList<SegmentId>();
        tst.add(SEGMENT_ID_17);

        SegmentsIterator<String, String> iterator = new SegmentsIterator<>(tst,
                segmentRegistry);
        iterator.close();

        verify(entryIterator17, atLeastOnce()).close();
        assertFalse(iterator.hasNext());
    }

    @Test
    void test_close_does_throw_when_already_closed() {
        when(segmentRegistry.tryGetSegment(SEGMENT_ID_17))
                .thenReturn(Optional.of(handle17));
        when(handle17.tryOpenIterator(SegmentIteratorIsolation.FAIL_FAST))
                .thenReturn(SegmentResult.ok(entryIterator17));

        final ArrayList<SegmentId> ids = new ArrayList<>();
        ids.add(SEGMENT_ID_17);

        SegmentsIterator<String, String> iterator = new SegmentsIterator<>(ids,
                segmentRegistry);
        iterator.close();

        assertThrows(IllegalStateException.class, iterator::close);
    }

    @Test
    void test_make_sure_that_lastSegmentIterator_in_not_closed_double_time() {
        when(segmentRegistry.tryGetSegment(SEGMENT_ID_17))
                .thenReturn(Optional.of(handle17));
        when(handle17.tryOpenIterator(SegmentIteratorIsolation.FAIL_FAST))
                .thenReturn(SegmentResult.ok(entryIterator17));
        when(entryIterator17.hasNext()).thenReturn(true, false);
        when(entryIterator17.next()).thenReturn(new Entry<>("key1", "value1"));

        final ArrayList<SegmentId> ids = new ArrayList<>();
        ids.add(SEGMENT_ID_17);

        SegmentsIterator<String, String> iterator = new SegmentsIterator<>(ids,
                segmentRegistry);
        assertTrue(iterator.hasNext());
        iterator.next();
        iterator.close();

        verify(entryIterator17, times(1)).close();
    }

    @Test
    void test_full_isolation_is_propagated_to_segment() {
        when(segmentRegistry.loadSegment(SEGMENT_ID_17)).thenReturn(handle17);
        when(handle17.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(entryIterator17);
        when(entryIterator17.hasNext()).thenReturn(true, false);
        when(entryIterator17.next()).thenReturn(new Entry<>("key1", "value1"));

        final ArrayList<SegmentId> ids = new ArrayList<>();
        ids.add(SEGMENT_ID_17);

        try (SegmentsIterator<String, String> iterator = new SegmentsIterator<>(
                ids, segmentRegistry, SegmentIteratorIsolation.FULL_ISOLATION)) {
            assertTrue(iterator.hasNext());
            iterator.next();
        }

        verify(handle17).openIterator(SegmentIteratorIsolation.FULL_ISOLATION);
    }

}
