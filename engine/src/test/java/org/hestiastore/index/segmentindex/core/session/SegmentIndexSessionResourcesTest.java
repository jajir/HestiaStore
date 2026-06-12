package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.junit.jupiter.api.Test;

class SegmentIndexSessionResourcesTest {

    @Test
    void setSessionInfrastructure_rejectsNull() {
        final SegmentIndexSessionResources<Integer, String> resources =
                new SegmentIndexSessionResources<>();

        assertThrows(IllegalArgumentException.class,
                () -> resources.setSessionInfrastructure(null));
    }

    @Test
    void accessors_delegateToInstalledInfrastructure() {
        final SegmentIndexSessionResources<Integer, String> resources =
                new SegmentIndexSessionResources<>();
        final SegmentIndexSessionInfrastructure<Integer, String> infrastructure =
                SegmentIndexSessionInfrastructure.create();

        resources.setSessionInfrastructure(infrastructure);

        assertSame(infrastructure.operationStatsRecorder(),
                resources.operationStatsRecorder());
        assertSame(infrastructure.maintenanceStatsRecorder(),
                resources.maintenanceStatsRecorder());
        assertSame(infrastructure.splitStatsRecorder(),
                resources.splitStatsRecorder());
        assertEquals(SegmentIndexState.OPENING, resources.currentState());
    }

    @Test
    void markReady_delegatesToInstalledInfrastructure() {
        final SegmentIndexSessionResources<Integer, String> resources =
                resourcesWithInfrastructure();

        resources.markReady();

        assertEquals(SegmentIndexState.READY, resources.currentState());
    }

    @Test
    void markRuntimeFailure_delegatesToInstalledInfrastructure() {
        final SegmentIndexSessionResources<Integer, String> resources =
                resourcesWithInfrastructure();

        resources.markRuntimeFailure(new RuntimeException("failure"));

        assertEquals(SegmentIndexState.ERROR, resources.currentState());
    }

    @Test
    void setRuntime_rejectsNullRuntime() {
        final SegmentIndexSessionResources<Integer, String> resources =
                new SegmentIndexSessionResources<>();

        assertThrows(IllegalArgumentException.class,
                () -> resources.setRuntime(null,
                        mock(ExecutorRegistry.class)));
    }

    @Test
    void closeRuntimeAfterFailedInitialization_isNoopWithoutRuntime() {
        final SegmentIndexSessionResources<Integer, String> resources =
                new SegmentIndexSessionResources<>();

        assertDoesNotThrow(resources::closeRuntimeAfterFailedInitialization);
    }

    @Test
    void directoryLock_returnsAcquiredLock() {
        final SegmentIndexSessionResources<Integer, String> resources =
                new SegmentIndexSessionResources<>();

        resources.acquireDirectoryLock(new MemDirectory());

        assertSame(resources.directoryLock(), resources.directoryLock());
    }

    private SegmentIndexSessionResources<Integer, String> resourcesWithInfrastructure() {
        final SegmentIndexSessionResources<Integer, String> resources =
                new SegmentIndexSessionResources<>();
        resources.setSessionInfrastructure(
                SegmentIndexSessionInfrastructure.create());
        return resources;
    }
}
