package org.hestiastore.index.segmentindex.core.session;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.core.storage.IndexConsistencyCoordinator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexStartupCoordinatorTest {

    @Mock
    private SegmentIndexRuntime<Integer, String> runtime;

    @Mock
    private SegmentIndexStateMachine stateMachine;

    @Mock
    private IndexConsistencyCoordinator<Integer, String> consistencyCoordinator;

    private SegmentIndexStartupCoordinator<Integer, String> startupCoordinator;

    @BeforeEach
    void setUp() {
        startupCoordinator = new SegmentIndexStartupCoordinator<>(
                "startup-test", false, runtime, stateMachine,
                consistencyCoordinator);
    }

    @Test
    void completeStartup_recoversAndMarksReadyWithoutConsistencyCheck() {
        startupCoordinator.completeStartup();

        final InOrder inOrder = inOrder(runtime, stateMachine);
        inOrder.verify(runtime).recoverFromWal();
        inOrder.verify(runtime).cleanupOrphanedSegmentDirectories();
        inOrder.verify(stateMachine).markReady();
        inOrder.verify(runtime).requestFullSplitScan();
        verify(consistencyCoordinator, never()).runStartupConsistencyCheck(any());
    }

    @Test
    void completeStartup_runsConsistencyCheckAfterStaleLockRecovery() {
        startupCoordinator = new SegmentIndexStartupCoordinator<>(
                "startup-test", true, runtime, stateMachine,
                consistencyCoordinator);
        doAnswer(invocation -> {
            final Runnable consistencyCheck = invocation.getArgument(0);
            consistencyCheck.run();
            return null;
        }).when(consistencyCoordinator).runStartupConsistencyCheck(any());

        startupCoordinator.completeStartup();

        final InOrder inOrder = inOrder(runtime, stateMachine,
                consistencyCoordinator);
        inOrder.verify(runtime).recoverFromWal();
        inOrder.verify(runtime).cleanupOrphanedSegmentDirectories();
        inOrder.verify(stateMachine).markReady();
        inOrder.verify(consistencyCoordinator)
                .runStartupConsistencyCheck(any());
        inOrder.verify(consistencyCoordinator).checkAndRepairConsistency();
        inOrder.verify(runtime).requestFullSplitScan();
        verify(consistencyCoordinator).checkAndRepairConsistency();
    }
}
