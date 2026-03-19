package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
class IndexOpenCoordinatorTest {

    @Mock
    private Logger logger;

    @Mock
    private Runnable recoverWal;

    @Mock
    private Runnable cleanupOrphanedSegments;

    @Mock
    private Runnable markReady;

    @Mock
    private Runnable runStartupConsistencyCheck;

    @Mock
    private Runnable scheduleBackgroundSplitScan;

    private IndexOpenCoordinator coordinator;

    @BeforeEach
    void setUp() {
        coordinator = new IndexOpenCoordinator(logger, "test-index");
    }

    @Test
    void completeOpen_runsRecoveryReadyAndScanWithoutStartupConsistency() {
        coordinator.completeOpen(false, recoverWal, cleanupOrphanedSegments,
                markReady, runStartupConsistencyCheck,
                scheduleBackgroundSplitScan);

        final InOrder inOrder = inOrder(recoverWal, cleanupOrphanedSegments,
                markReady, scheduleBackgroundSplitScan);
        inOrder.verify(recoverWal).run();
        inOrder.verify(cleanupOrphanedSegments).run();
        inOrder.verify(markReady).run();
        inOrder.verify(scheduleBackgroundSplitScan).run();
        verify(runStartupConsistencyCheck, never()).run();
    }

    @Test
    void completeOpen_runsStartupConsistencyBeforeInitialScanWhenLockRecovered() {
        coordinator.completeOpen(true, recoverWal, cleanupOrphanedSegments,
                markReady, runStartupConsistencyCheck,
                scheduleBackgroundSplitScan);

        final InOrder inOrder = inOrder(recoverWal, cleanupOrphanedSegments,
                markReady, runStartupConsistencyCheck,
                scheduleBackgroundSplitScan);
        inOrder.verify(recoverWal).run();
        inOrder.verify(cleanupOrphanedSegments).run();
        inOrder.verify(markReady).run();
        inOrder.verify(runStartupConsistencyCheck).run();
        inOrder.verify(scheduleBackgroundSplitScan).run();
        verify(logger).info(
                "Recovered stale index lock (.lock). Index is going to be checked for consistency and unlocked.");
    }

    @Test
    void constructorRejectsNullIndexName() {
        assertThrows(IllegalArgumentException.class,
                () -> new IndexOpenCoordinator(logger, null));
    }
}
