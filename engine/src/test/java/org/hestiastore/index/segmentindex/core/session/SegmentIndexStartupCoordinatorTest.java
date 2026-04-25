package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.segmentindex.core.session.SegmentIndexImpl;
import org.hestiastore.index.segmentindex.core.storage.IndexConsistencyCoordinator;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexRuntime;
import org.hestiastore.index.segmentindex.core.session.state.IndexStateCoordinator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
class SegmentIndexStartupCoordinatorTest {

    @Mock
    private Logger logger;

    @Mock
    private SegmentIndexRuntime<Integer, String> runtime;

    @Mock
    private IndexStateCoordinator<Integer, String> stateCoordinator;

    @Mock
    private IndexConsistencyCoordinator<Integer, String> consistencyCoordinator;

    @Mock
    private SegmentIndexImpl<Integer, String> owner;

    private final AtomicInteger startupConsistencyChecks = new AtomicInteger();
    private SegmentIndexStartupCoordinator<Integer, String> startupCoordinator;

    @BeforeEach
    void setUp() {
        startupCoordinator = new SegmentIndexStartupCoordinator<>(logger,
                "startup-test", false, runtime, stateCoordinator,
                consistencyCoordinator);
    }

    @Test
    void completeStartup_recoversAndMarksReadyWithoutConsistencyCheck() {
        startupCoordinator.completeStartup(
                startupConsistencyChecks::incrementAndGet);

        final InOrder inOrder = inOrder(runtime, stateCoordinator);
        inOrder.verify(runtime).recoverFromWal();
        inOrder.verify(runtime).cleanupOrphanedSegmentDirectories();
        inOrder.verify(stateCoordinator).markReady();
        inOrder.verify(runtime).requestSplitReconciliation();
        verify(consistencyCoordinator, never()).runStartupConsistencyCheck(any());
        assertEquals(0, startupConsistencyChecks.get());
    }

    @Test
    void completeStartup_runsConsistencyCheckAfterStaleLockRecovery() {
        startupCoordinator = new SegmentIndexStartupCoordinator<>(logger,
                "startup-test", true, runtime, stateCoordinator,
                consistencyCoordinator);
        doAnswer(invocation -> {
            final Runnable consistencyCheck = invocation.getArgument(0);
            consistencyCheck.run();
            return null;
        }).when(consistencyCoordinator).runStartupConsistencyCheck(any());

        startupCoordinator.completeStartup(
                startupConsistencyChecks::incrementAndGet);

        final InOrder inOrder = inOrder(runtime, stateCoordinator,
                consistencyCoordinator);
        inOrder.verify(runtime).recoverFromWal();
        inOrder.verify(runtime).cleanupOrphanedSegmentDirectories();
        inOrder.verify(stateCoordinator).markReady();
        inOrder.verify(consistencyCoordinator)
                .runStartupConsistencyCheck(any());
        inOrder.verify(runtime).requestSplitReconciliation();
        assertEquals(1, startupConsistencyChecks.get());
    }
}
