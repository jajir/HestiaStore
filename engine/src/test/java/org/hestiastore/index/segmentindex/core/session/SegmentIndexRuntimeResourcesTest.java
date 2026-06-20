package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.junit.jupiter.api.Test;

class SegmentIndexRuntimeResourcesTest {

    @Test
    void accessors_returnOwnedRuntimeObjects() {
        final SegmentIndexRuntimeResources<Integer, String> resources =
                new SegmentIndexRuntimeResources<>();

        assertSame(resources.operationStatsRecorder(),
                resources.operationStatsRecorder());
        assertSame(resources.maintenanceStatsRecorder(),
                resources.maintenanceStatsRecorder());
        assertSame(resources.splitStatsRecorder(), resources.splitStatsRecorder());
        assertNotNull(resources.stateMachine());
        assertNotNull(resources.operationGate());
        assertEquals(SegmentIndexState.OPENING, resources.currentState());
    }

    @Test
    void markReady_updatesOwnedStateMachine() {
        final SegmentIndexRuntimeResources<Integer, String> resources =
                new SegmentIndexRuntimeResources<>();

        resources.markReady();

        assertEquals(SegmentIndexState.READY, resources.currentState());
    }

    @Test
    void markRuntimeFailure_updatesOwnedStateMachine() {
        final SegmentIndexRuntimeResources<Integer, String> resources =
                new SegmentIndexRuntimeResources<>();

        resources.markRuntimeFailure(new RuntimeException("failure"));

        assertEquals(SegmentIndexState.ERROR, resources.currentState());
    }

    @Test
    void setExecutorRegistry_rejectsNull() {
        final SegmentIndexRuntimeResources<Integer, String> resources =
                new SegmentIndexRuntimeResources<>();

        assertThrows(IllegalArgumentException.class,
                () -> resources.setExecutorRegistry(null));
    }

    @Test
    void directoryLock_returnsAcquiredLock() {
        final SegmentIndexRuntimeResources<Integer, String> resources =
                new SegmentIndexRuntimeResources<>();

        resources.acquireDirectoryLock(new MemDirectory());

        assertSame(resources.directoryLock(), resources.directoryLock());
    }
}
