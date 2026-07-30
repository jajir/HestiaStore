package org.hestiastore.benchmark.segmentindex;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class MutationFlushCoordinatorTest {

    @Test
    void startsOneFlushAtAggregateThreshold() {
        final MutationFlushCoordinator coordinator = new MutationFlushCoordinator();

        assertFalse(coordinator.recordAndTryStartFlush(3));
        assertFalse(coordinator.recordAndTryStartFlush(3));
        assertTrue(coordinator.recordAndTryStartFlush(3));
        assertFalse(coordinator.recordAndTryStartFlush(3));
        assertFalse(coordinator.recordAndTryStartFlush(3));
        assertFalse(coordinator.recordAndTryStartFlush(3));

        coordinator.finishFlush();
        assertTrue(coordinator.recordAndTryStartFlush(3));
        coordinator.finishFlush();
        assertFalse(coordinator.recordAndTryStartFlush(3));
    }

    @Test
    void resetClearsPendingMutations() {
        final MutationFlushCoordinator coordinator = new MutationFlushCoordinator();

        assertFalse(coordinator.recordAndTryStartFlush(2));
        coordinator.reset();

        assertFalse(coordinator.recordAndTryStartFlush(2));
        assertTrue(coordinator.recordAndTryStartFlush(2));
        coordinator.finishFlush();
    }
}
