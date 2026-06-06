package org.hestiastore.index.segmentindex.core.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class IndexConsistencyCoordinatorTest {

    private static final SegmentId MATCHED_SEGMENT_ID = SegmentId.of(7);
    private static final SegmentId OTHER_SEGMENT_ID = SegmentId.of(8);

    @Test
    void checkAndRepairConsistency_runsDefaultFilterAndCleanup() {
        final AtomicReference<Predicate<SegmentId>> filterRef = new AtomicReference<>();
        final StringBuilder calls = new StringBuilder();
        final IndexConsistencyCoordinator<Integer, String> coordinator = new IndexConsistencyCoordinator<>(
                () -> calls.append("verify>"), filterRef::set,
                () -> calls.append("cleanup>"),
                MATCHED_SEGMENT_ID::equals);

        coordinator.checkAndRepairConsistency();

        assertNotNull(filterRef.get());
        assertTrue(filterRef.get().test(MATCHED_SEGMENT_ID));
        assertTrue(filterRef.get().test(OTHER_SEGMENT_ID));
        assertEquals("verify>cleanup>", calls.toString());
    }

    @Test
    void runStartupConsistencyCheck_usesStartupSegmentFilter() {
        final AtomicReference<Predicate<SegmentId>> filterRef = new AtomicReference<>();
        final IndexConsistencyCoordinator<Integer, String> coordinator = new IndexConsistencyCoordinator<>(
                () -> {
                }, filterRef::set, () -> {
                }, MATCHED_SEGMENT_ID::equals);

        coordinator.runStartupConsistencyCheck();

        assertTrue(filterRef.get().test(MATCHED_SEGMENT_ID));
        assertFalse(filterRef.get().test(OTHER_SEGMENT_ID));
    }

    @Test
    void runStartupConsistencyCheck_resetsStartupFilterAfterFailure() {
        final AtomicReference<Predicate<SegmentId>> filterRef = new AtomicReference<>();
        final AtomicBoolean fail = new AtomicBoolean(true);
        final IllegalStateException failure = new IllegalStateException("boom");
        final IndexConsistencyCoordinator<Integer, String> coordinator = new IndexConsistencyCoordinator<>(
                () -> {
                }, segmentFilter -> {
                    filterRef.set(segmentFilter);
                    if (fail.get()) {
                        throw failure;
                    }
                }, () -> {
                }, MATCHED_SEGMENT_ID::equals);

        final IllegalStateException ex = assertThrows(
                IllegalStateException.class,
                coordinator::runStartupConsistencyCheck);
        assertSame(failure, ex);

        fail.set(false);
        coordinator.checkAndRepairConsistency();

        assertTrue(filterRef.get().test(MATCHED_SEGMENT_ID));
        assertTrue(filterRef.get().test(OTHER_SEGMENT_ID));
    }
}
