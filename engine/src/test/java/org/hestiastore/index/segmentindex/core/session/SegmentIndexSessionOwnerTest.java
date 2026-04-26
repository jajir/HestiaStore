package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.session.state.IndexState;
import org.hestiastore.index.segmentindex.core.session.state.IndexStateCoordinator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexSessionOwnerTest {

    private IndexStateCoordinator<Integer, String> stateCoordinator;
    private SegmentIndexRuntime<Integer, String> runtime;
    private IndexCloseCoordinator<Integer, String> closeCoordinator;
    private SegmentIndexStartupCoordinator<Integer, String> startupCoordinator;
    private SegmentIndexSessionOwner<Integer, String> owner;

    @BeforeEach
    void setUp() {
        stateCoordinator = mock(IndexStateCoordinator.class);
        runtime = mock(SegmentIndexRuntime.class);
        closeCoordinator = mock(IndexCloseCoordinator.class);
        startupCoordinator = mock(SegmentIndexStartupCoordinator.class);
        owner = new SegmentIndexSessionOwner<>(stateCoordinator, runtime,
                closeCoordinator, startupCoordinator);
    }

    @Test
    void delegatesStateAndRuntimeViews() {
        final IndexState<Integer, String> indexState = mock(IndexState.class);
        final SegmentIndexMetricsSnapshot metricsSnapshot =
                mock(SegmentIndexMetricsSnapshot.class);
        final IndexControlPlane controlPlane = mock(IndexControlPlane.class);
        when(stateCoordinator.getIndexState()).thenReturn(indexState);
        when(stateCoordinator.getState()).thenReturn(SegmentIndexState.READY);
        when(runtime.metricsSnapshot()).thenReturn(metricsSnapshot);
        when(runtime.controlPlane()).thenReturn(controlPlane);

        assertSame(indexState, owner.getIndexState());
        assertSame(SegmentIndexState.READY, owner.getState());
        assertSame(metricsSnapshot, owner.metricsSnapshot());
        assertSame(controlPlane, owner.controlPlane());
        assertSame(stateCoordinator, owner.stateCoordinator());
        assertSame(runtime, owner.runtime());
    }

    @Test
    void completeStartupRunsOnlyOnce() {
        final SegmentIndexImpl<Integer, String> index = Mockito
                .mock(SegmentIndexImpl.class);
        final Runnable hook = mock(Runnable.class);

        owner.completeStartup(hook);
        owner.completeStartup(hook);

        verify(startupCoordinator).completeStartup(hook);
    }

    @Test
    void closeAndFailureDelegateToCollaborators() {
        final SegmentIndexImpl<Integer, String> index = Mockito
                .mock(SegmentIndexImpl.class);
        final Throwable failure = new IllegalStateException("boom");

        owner.close();
        owner.failWithError(failure);

        verify(closeCoordinator).close();
        verify(stateCoordinator).failWithError(failure);
    }

    @Test
    void ensureOperationalUsesCurrentIndexState() {
        final IndexState<Integer, String> indexState = mock(IndexState.class);
        when(stateCoordinator.getIndexState()).thenReturn(indexState);

        assertDoesNotThrow(owner::ensureOperational);

        verify(indexState).tryPerformOperation();
    }

    @Test
    void runMaintenanceOperationAppliesGuardsAndCleanup() {
        final IndexState<Integer, String> indexState = mock(IndexState.class);
        final Runnable action = mock(Runnable.class);
        when(stateCoordinator.getIndexState()).thenReturn(indexState);

        assertDoesNotThrow(() -> owner.runMaintenanceOperation(action));

        verify(indexState).tryPerformOperation();
        verify(action).run();
        verify(runtime).invalidateSegmentIterators();
    }
}
