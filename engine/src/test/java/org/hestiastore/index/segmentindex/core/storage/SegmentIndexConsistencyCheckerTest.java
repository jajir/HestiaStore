package org.hestiastore.index.segmentindex.core.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.Snapshot;
import org.hestiastore.index.segmentregistry.SegmentHandle;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexConsistencyCheckerTest {

    private static final TypeDescriptor<Integer> TYPE_DESCRIPTOR_INTEGER = new TypeDescriptorInteger();
    private static final SegmentId SEGMENT_ID = SegmentId.of(13);
    private static final Integer SEGMENT_MAX_KEY = 73;

    @Mock
    private KeyToSegmentMap<Integer> keyToSegmentMap;
    @Mock
    private Snapshot<Integer> snapshot;

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    @Mock
    private SegmentHandle<Integer, String> segmentHandle;

    private IndexConsistencyChecker<Integer, String> checker;

    @Test
    void test_noSegments() {
        when(snapshot.getSegmentIds(SegmentWindow.unbounded()))
                .thenReturn(List.of());

        checker.checkAndRepairConsistency();

        assertTrue(true);
    }

    @Test
    void test_missingSegment() {
        when(snapshot.getSegmentIds(SegmentWindow.unbounded()))
                .thenReturn(List.of(SEGMENT_ID));
        when(segmentRegistry.loadSegment(SEGMENT_ID))
                .thenThrow(new IndexException("boom"));

        final Exception e = assertThrows(IndexException.class,
                () -> checker.checkAndRepairConsistency());

        assertEquals(
                "Index is broken. File 'index.map' "
                        + "containing information about segments is corrupted. "
                        + "Segment 'segment-00013' is not found in index.",
                e.getMessage());
    }

    @Test
    void test_oneSegment_segmentId_is_null() {
        when(snapshot.getSegmentIds(SegmentWindow.unbounded()))
                .thenReturn(Collections.singletonList(null));

        final Exception e = assertThrows(IndexException.class,
                () -> checker.checkAndRepairConsistency());

        assertEquals(
                "Index is broken. " + "File 'index.map' containing information "
                        + "about segments is corrupted. Segment id is null.",
                e.getMessage());
    }

    @Test
    void test_oneSegment_segmentMaxKey_is_null_removes_segment() {
        when(snapshot.getSegmentIds(SegmentWindow.unbounded()))
                .thenReturn(List.of(SEGMENT_ID));
        when(segmentRegistry.loadSegment(SEGMENT_ID)).thenReturn(segmentHandle);
        when(segmentHandle.checkAndRepairConsistency()).thenReturn(null);
        when(segmentHandle.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(
                        EntryIterator.make(List.<Entry<Integer, String>>of()
                                .iterator()));

        checker.checkAndRepairConsistency();

        verify(segmentRegistry).deleteSegment(SEGMENT_ID);
        verify(keyToSegmentMap).removeSegmentRoute(SEGMENT_ID);
        verify(keyToSegmentMap).flushIfDirty();
    }

    @Test
    void test_oneSegment_segmentMaxKeyIsHigher() {
        when(snapshot.getSegmentIds(SegmentWindow.unbounded()))
                .thenReturn(List.of(SEGMENT_ID));
        when(segmentRegistry.loadSegment(SEGMENT_ID)).thenReturn(segmentHandle);
        when(segmentHandle.checkAndRepairConsistency())
                .thenReturn(SEGMENT_MAX_KEY + 1);
        when(snapshot.findSegmentIdForKey(SEGMENT_MAX_KEY + 1))
                .thenReturn(null);

        final Exception e = assertThrows(IndexException.class,
                () -> checker.checkAndRepairConsistency());

        assertEquals("Index is broken. File 'index.map' containing information "
                + "about segments is corrupted. Segment 'segment-00013' "
                + "contains max key '74', which routes to segment 'null' "
                + "in the index map.", e.getMessage());
    }

    @Test
    void test_oneSegment() {
        when(snapshot.getSegmentIds(SegmentWindow.unbounded()))
                .thenReturn(List.of(SEGMENT_ID));
        when(segmentRegistry.loadSegment(SEGMENT_ID)).thenReturn(segmentHandle);
        when(segmentHandle.checkAndRepairConsistency())
                .thenReturn(SEGMENT_MAX_KEY);
        when(snapshot.findSegmentIdForKey(SEGMENT_MAX_KEY))
                .thenReturn(SEGMENT_ID);

        checker.checkAndRepairConsistency();

        assertTrue(true);
    }

    @Test
    void test_segmentFilteredOut_isSkippedWithoutLoading() {
        when(snapshot.getSegmentIds(SegmentWindow.unbounded()))
                .thenReturn(List.of(SEGMENT_ID));
        checker = new IndexConsistencyChecker<>(keyToSegmentMap,
                segmentRegistry, TYPE_DESCRIPTOR_INTEGER, segmentId -> false);

        checker.checkAndRepairConsistency();

        verifyNoInteractions(segmentRegistry);
    }

    // no such segment

    @BeforeEach
    void setUp() {
        when(keyToSegmentMap.snapshot()).thenReturn(snapshot);
        checker = new IndexConsistencyChecker<>(keyToSegmentMap, segmentRegistry,
                TYPE_DESCRIPTOR_INTEGER);
    }

    @AfterEach
    void tearDown() {
        checker = null;
    }
}
