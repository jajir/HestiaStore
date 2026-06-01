package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("unchecked")
class SegmentIndexSessionOwnerTest {

    private SegmentIndexStateMachine stateMachine;
    private SegmentIndexRuntime<Integer, String> runtime;
    private IndexCloseCoordinator<Integer, String> closeCoordinator;
    private SegmentIndexSessionOwner<Integer, String> owner;

    @BeforeEach
    void setUp() {
        stateMachine = mock(SegmentIndexStateMachine.class);
        runtime = mock(SegmentIndexRuntime.class);
        closeCoordinator = mock(IndexCloseCoordinator.class);
        owner = new SegmentIndexSessionOwner<>(stateMachine, runtime,
                closeCoordinator);
    }

    @Test
    void delegatesStateAndRuntimeViews() {
        final SegmentIndexMetricsSnapshot metricsSnapshot =
                mock(SegmentIndexMetricsSnapshot.class);
        final RuntimeTuning runtimeConfiguration = mock(RuntimeTuning.class);
        when(stateMachine.getState()).thenReturn(SegmentIndexState.READY);
        when(runtime.metricsSnapshot()).thenReturn(metricsSnapshot);
        when(runtime.runtimeTuning()).thenReturn(runtimeConfiguration);

        assertSame(SegmentIndexState.READY, owner.getState());
        assertSame(metricsSnapshot, owner.metricsSnapshot());
        assertSame(runtimeConfiguration, owner.runtimeTuning());
        assertSame(stateMachine, owner.stateMachine());
        assertSame(runtime, owner.runtime());
    }

    void closeDelegatesToCloseCoordinator() {
        owner.close();

        verify(closeCoordinator).close();
    }

    @Test
    void ensureOperationalUsesStateMachine() {
        assertDoesNotThrow(owner::ensureOperational);

        verify(stateMachine).ensureOperational();
    }

    @Test
    void invalidateSegmentIteratorsDelegatesToRuntime() {
        assertDoesNotThrow(owner::invalidateSegmentIterators);

        verify(runtime).invalidateSegmentIterators();
    }
}
