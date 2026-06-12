package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.junit.jupiter.api.Test;

class SegmentIndexSessionInfrastructureTest {

    @Test
    void create_initializesSessionInfrastructure() {
        final SegmentIndexSessionInfrastructure<Integer, String> infrastructure =
                SegmentIndexSessionInfrastructure.create();

        assertEquals(SegmentIndexState.OPENING,
                infrastructure.currentState());
        assertNotNull(infrastructure.stateMachine());
        assertNotNull(infrastructure.operationStatsRecorder());
        assertNotNull(infrastructure.maintenanceStatsRecorder());
        assertNotNull(infrastructure.splitStatsRecorder());
        assertNotNull(infrastructure.operationGate());
        assertNotNull(infrastructure.trackedRunner());
    }

    @Test
    void markReady_updatesStateMachine() {
        final SegmentIndexSessionInfrastructure<Integer, String> infrastructure =
                SegmentIndexSessionInfrastructure.create();

        infrastructure.markReady();

        assertEquals(SegmentIndexState.READY, infrastructure.currentState());
    }

    @Test
    void markRuntimeFailure_updatesStateMachine() {
        final SegmentIndexSessionInfrastructure<Integer, String> infrastructure =
                SegmentIndexSessionInfrastructure.create();

        infrastructure.markRuntimeFailure(new RuntimeException("failure"));

        assertEquals(SegmentIndexState.ERROR, infrastructure.currentState());
    }
}
