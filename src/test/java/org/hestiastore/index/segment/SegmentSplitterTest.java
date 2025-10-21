package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.PairWriter;
import org.hestiastore.index.WriteTransaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSplitterTest {

    private static final SegmentId SEGMENT_ID = SegmentId.of(27);

    private static final Pair<String, String> PAIR1 = Pair.of("key1", "value1");

    private static final Pair<String, String> PAIR2 = Pair.of("key2", "value2");

    private static final Pair<String, String> PAIR3 = Pair.of("key3", "value3");

    @Mock
    private SegmentImpl<String, String> segment;

    @Mock
    private SegmentImpl<String, String> lowerSegment;

    @Mock
    private VersionController versionController;

    @Mock
    private SegmentPropertiesManager segmentPropertiesManager;

    @Mock
    private SegmentDeltaCacheController<String, String> deltaCacheController;

    @Mock
    private SegmentStats segmentStats;

    @Mock
    private PairIterator<String, String> segmentIterator;

    @Mock
    private PairWriter<String, String> segmentFullWriter;

    @Mock
    private WriteTransaction<String, String> segmentFullWriterTx;

    @Mock
    private PairWriter<String, String> lowerSegmentFullWriter;

    @Mock
    private WriteTransaction<String, String> lowerSegmentWriteTx;

    @Mock
    private SegmentReplacer<String, String> splitApplier;

    private SegmentSplitter<String, String> splitter;

    @BeforeEach
    void setUp() {
        splitter = new SegmentSplitter<>(segment, versionController,
                splitApplier);
    }

    @Test
    void test_shouldBeCompactedBeforeSplitting_yes_lowEstimatedNumberOfKeys() {
        when(segmentPropertiesManager.getSegmentStats())
                .thenReturn(segmentStats);
        when(segmentStats.getNumberOfKeysInSegment()).thenReturn(2L);
        when(deltaCacheController.getDeltaCacheSizeWithoutTombstones())
                .thenReturn(1);

        final SegmentSplitterPolicy<String, String> policy = new SegmentSplitterPolicy<>(
                segmentPropertiesManager, deltaCacheController);
        final SegmentSplitterPlan<String, String> plan = SegmentSplitterPlan
                .fromPolicy(policy);

        assertTrue(
                policy.shouldBeCompactedBeforeSplitting(1000, plan.getEstimatedNumberOfKeys()));
        assertFalse(plan.getEstimatedNumberOfKeys() >= 1000);
    }

    @Test
    void test_shouldBeCompactedBeforeSplitting_no() {
        when(segmentPropertiesManager.getSegmentStats())
                .thenReturn(segmentStats);
        when(segmentStats.getNumberOfKeysInSegment()).thenReturn(1000L);
        when(deltaCacheController.getDeltaCacheSizeWithoutTombstones())
                .thenReturn(100);

        final SegmentSplitterPolicy<String, String> policy = new SegmentSplitterPolicy<>(
                segmentPropertiesManager, deltaCacheController);
        final SegmentSplitterPlan<String, String> plan = SegmentSplitterPlan
                .fromPolicy(policy);

        assertFalse(
                policy.shouldBeCompactedBeforeSplitting(1000, plan.getEstimatedNumberOfKeys()));
        assertTrue(plan.getEstimatedNumberOfKeys() >= 1000);
    }

    @Test
    void test_split_segmentId_is_null() {
        when(segmentPropertiesManager.getSegmentStats()).thenReturn(segmentStats);
        when(segmentStats.getNumberOfKeysInSegment()).thenReturn(2L);
        when(deltaCacheController.getDeltaCacheSizeWithoutTombstones())
                .thenReturn(1);
        final SegmentSplitterPolicy<String, String> policy = new SegmentSplitterPolicy<>(
                segmentPropertiesManager, deltaCacheController);
        final SegmentSplitterPlan<String, String> plan = SegmentSplitterPlan
                .fromPolicy(policy);
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> {
                    splitter.split(null, plan);
                });
        assertEquals("Property 'segmentId' must not be null.",
                err.getMessage());
    }

    @Test
    void test_split_plan_is_null() {
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> splitter.split(SEGMENT_ID, null));
        assertEquals("Property 'plan' must not be null.", err.getMessage());
    }

    @SuppressWarnings("unchecked")
    @Test
    void test_split() {
        when(segmentPropertiesManager.getSegmentStats())
                .thenReturn(segmentStats);
        when(segmentStats.getNumberOfKeysInSegment()).thenReturn(2L);
        when(deltaCacheController.getDeltaCacheSizeWithoutTombstones())
                .thenReturn(2);

        // main iterator behaviour
        when(segment.openIterator()).thenReturn(segmentIterator);
        when(segmentIterator.hasNext()).thenReturn(true, true, true, true,
                false);
        when(segmentIterator.next()).thenReturn(PAIR1, PAIR2, PAIR3);

        // mock writing lower part to new segment
        when(segment.createSegmentWithSameConfig(SEGMENT_ID)).thenReturn(lowerSegment);
        when(lowerSegment.openFullWriteTx()).thenReturn(lowerSegmentWriteTx);
        when(lowerSegmentWriteTx.open())
                .thenReturn(lowerSegmentFullWriter);

        // mock writing upper part to current segment

        when(segment.openFullWriteTx()).thenReturn(segmentFullWriterTx);
        when(segmentFullWriterTx.open()).thenReturn(segmentFullWriter);

        final SegmentSplitterPolicy<String, String> policy = new SegmentSplitterPolicy<>(
                segmentPropertiesManager, deltaCacheController);
        final SegmentSplitterPlan<String, String> plan = SegmentSplitterPlan
                .fromPolicy(policy);

        final SegmentSplitterResult<String, String> result = splitter
                .split(SEGMENT_ID, plan);

        assertNotNull(result);
        verify(lowerSegmentFullWriter, times(1)).write(PAIR1);
        verify(lowerSegmentFullWriter, times(1)).write(PAIR2);
        verify(lowerSegmentFullWriter, times(1)).close();
        verify(segmentFullWriter, times(1)).write(PAIR3);
        verify(segmentFullWriter, times(1)).close();
        verify(versionController, times(1)).changeVersion();
        verify(segmentIterator, times(1)).close();
        verify(lowerSegmentWriteTx, times(1)).commit();
        verify(segmentFullWriterTx, times(1)).commit();
    }

    @Test
    void test_split_half_is_zero() {
        when(segmentPropertiesManager.getSegmentStats())
                .thenReturn(segmentStats);
        when(segmentStats.getNumberOfKeysInSegment()).thenReturn(0L);
        when(deltaCacheController.getDeltaCacheSizeWithoutTombstones())
                .thenReturn(1);

        final SegmentSplitterPolicy<String, String> policy = new SegmentSplitterPolicy<>(
                segmentPropertiesManager, deltaCacheController);
        final SegmentSplitterPlan<String, String> plan = SegmentSplitterPlan
                .fromPolicy(policy);

        final Exception err = assertThrows(IllegalStateException.class,
                () -> splitter.split(SEGMENT_ID, plan));
        assertEquals("Splitting failed. Number of keys is too low.",
                err.getMessage());
    }
}
