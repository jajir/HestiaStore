package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.junit.jupiter.api.Test;

class SegmentIndexSessionResourcesTest {

    @Test
    void accessors_returnOwnedRuntimeObjects() {
        final SegmentIndexSessionResources<Integer, String> resources =
                new SegmentIndexSessionResources<>();

        assertSame(resources.operationStatsRecorder(),
                resources.operationStatsRecorder());
        assertSame(resources.maintenanceStatsRecorder(),
                resources.maintenanceStatsRecorder());
        assertSame(resources.splitStatsRecorder(), resources.splitStatsRecorder());
        assertNotNull(resources.stateMachine());
        assertNotNull(resources.operationGate());
        assertNotNull(resources.trackedRunner());
        assertEquals(SegmentIndexState.OPENING, resources.currentState());
    }

    @Test
    void markReady_updatesOwnedStateMachine() {
        final SegmentIndexSessionResources<Integer, String> resources =
                new SegmentIndexSessionResources<>();

        resources.markReady();

        assertEquals(SegmentIndexState.READY, resources.currentState());
    }

    @Test
    void markRuntimeFailure_updatesOwnedStateMachine() {
        final SegmentIndexSessionResources<Integer, String> resources =
                new SegmentIndexSessionResources<>();

        resources.markRuntimeFailure(new RuntimeException("failure"));

        assertEquals(SegmentIndexState.ERROR, resources.currentState());
    }

    @Test
    void handleWalRuntimeFailure_updatesOwnedStateMachine() {
        final SegmentIndexSessionResources<Integer, String> resources =
                new SegmentIndexSessionResources<>();

        resources.handleWalRuntimeFailure(new RuntimeException("failure"));

        assertEquals(SegmentIndexState.ERROR, resources.currentState());
    }

    @Test
    void setExecutorRegistry_rejectsNull() {
        final SegmentIndexSessionResources<Integer, String> resources =
                new SegmentIndexSessionResources<>();

        assertThrows(IllegalArgumentException.class,
                () -> resources.setExecutorRegistry(null));
    }

    @Test
    void directoryLock_returnsAcquiredLock() {
        final SegmentIndexSessionResources<Integer, String> resources =
                new SegmentIndexSessionResources<>();

        resources.acquireDirectoryLock(new MemDirectory());

        assertSame(resources.directoryLock(), resources.directoryLock());
    }
}
