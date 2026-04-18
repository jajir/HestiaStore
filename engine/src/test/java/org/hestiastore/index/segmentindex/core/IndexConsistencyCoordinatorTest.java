package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class IndexConsistencyCoordinatorTest {

    private static final SegmentId MATCHED_SEGMENT_ID = SegmentId.of(7);
    private static final SegmentId OTHER_SEGMENT_ID = SegmentId.of(8);

    @Test
    void checkAndRepairConsistency_runsDefaultFilterCleanupAndRescan() {
        final AtomicReference<Predicate<SegmentId>> filterRef = new AtomicReference<>();
        final StringBuilder calls = new StringBuilder();
        final IndexConsistencyCoordinator<Integer, String> coordinator = new IndexConsistencyCoordinator<>(
                () -> calls.append("verify>"), filterRef::set,
                () -> calls.append("cleanup>"), () -> calls.append("scan>"),
                MATCHED_SEGMENT_ID::equals);

        coordinator.checkAndRepairConsistency();

        assertNotNull(filterRef.get());
        assertTrue(filterRef.get().test(MATCHED_SEGMENT_ID));
        assertTrue(filterRef.get().test(OTHER_SEGMENT_ID));
        assertEquals("verify>cleanup>scan>", calls.toString());
    }

    @Test
    void runStartupConsistencyCheck_usesStartupSegmentFilter() {
        final AtomicReference<Predicate<SegmentId>> filterRef = new AtomicReference<>();
        final IndexConsistencyCoordinator<Integer, String> coordinator = new IndexConsistencyCoordinator<>(
                () -> {
                }, filterRef::set, () -> {
                }, () -> {
                }, MATCHED_SEGMENT_ID::equals);

        coordinator.runStartupConsistencyCheck(coordinator::checkAndRepairConsistency);

        assertTrue(filterRef.get().test(MATCHED_SEGMENT_ID));
        assertFalse(filterRef.get().test(OTHER_SEGMENT_ID));
    }

    @Test
    void runStartupConsistencyCheck_resetsStartupFilterAfterFailure() {
        final AtomicReference<Predicate<SegmentId>> filterRef = new AtomicReference<>();
        final IllegalStateException failure = new IllegalStateException("boom");
        final IndexConsistencyCoordinator<Integer, String> coordinator = new IndexConsistencyCoordinator<>(
                () -> {
                }, filterRef::set, () -> {
                }, () -> {
                }, MATCHED_SEGMENT_ID::equals);

        final IllegalStateException ex = assertThrows(
                IllegalStateException.class,
                () -> coordinator.runStartupConsistencyCheck(() -> {
                    coordinator.checkAndRepairConsistency();
                    throw failure;
                }));
        assertSame(failure, ex);

        coordinator.checkAndRepairConsistency();

        assertTrue(filterRef.get().test(MATCHED_SEGMENT_ID));
        assertTrue(filterRef.get().test(OTHER_SEGMENT_ID));
    }
}
