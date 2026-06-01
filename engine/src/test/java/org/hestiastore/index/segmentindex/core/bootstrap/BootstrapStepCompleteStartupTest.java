package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.effectiveConfiguration;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.stateWithConfiguration;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BootstrapStepCompleteStartupTest {

    @Mock
    private SegmentIndexSessionResources<Integer, String> sessionResources;

    private BootstrapStepCompleteStartup<Integer, String> step;

    @BeforeEach
    void setUp() {
        step = new BootstrapStepCompleteStartup<>(sessionResources);
    }

    @Test
    void constructor_rejectsNullSessionResources() {
        assertThrows(IllegalArgumentException.class,
                () -> new BootstrapStepCompleteStartup<Integer, String>(null));
    }

    @Test
    void apply_completesStartupWithoutConsistencyCheck() {
        final SegmentIndexBootstrapState<Integer, String> state =
                stateWithConfiguration(effectiveConfiguration(
                        "bootstrap-step-complete-startup"));

        assertDoesNotThrow(() -> step.apply(
                request(new MemDirectory(), SegmentIndexBootstrapMode.OPEN),
                state));

        final InOrder inOrder = inOrder(sessionResources);
        inOrder.verify(sessionResources).recoverFromWal();
        inOrder.verify(sessionResources).cleanupOrphanedSegmentDirectories();
        inOrder.verify(sessionResources).markReady();
        inOrder.verify(sessionResources).wasStaleLockRecovered();
        inOrder.verify(sessionResources).requestFullSplitScan();
        verifyNoMoreInteractions(sessionResources);
    }

    @Test
    void apply_runsConsistencyCheckAfterStaleLockRecovery() {
        final SegmentIndexBootstrapState<Integer, String> state =
                stateWithConfiguration(effectiveConfiguration(
                        "bootstrap-step-complete-startup"));
        when(sessionResources.wasStaleLockRecovered()).thenReturn(true);

        assertDoesNotThrow(() -> step.apply(
                request(new MemDirectory(), SegmentIndexBootstrapMode.OPEN),
                state));

        final InOrder inOrder = inOrder(sessionResources);
        inOrder.verify(sessionResources).recoverFromWal();
        inOrder.verify(sessionResources).cleanupOrphanedSegmentDirectories();
        inOrder.verify(sessionResources).markReady();
        inOrder.verify(sessionResources).wasStaleLockRecovered();
        inOrder.verify(sessionResources).runStartupConsistencyCheck();
        inOrder.verify(sessionResources).requestFullSplitScan();
        verifyNoMoreInteractions(sessionResources);
    }
}
