package org.hestiastore.index.segmentindex.core.split;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexMaintenanceConfiguration;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.SegmentIndexRuntimeState;
import org.hestiastore.index.segmentindex.core.routing.MappedSegmentLeaseService;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SplitPolicySchedulerTest {

    @Mock
    private EffectiveIndexConfiguration<String, String> conf;

    @Mock
    private RuntimeTuningState runtimeTuningState;

    @Mock
    private SegmentRouteMap<String> keyToSegmentMap;

    @Mock
    private MappedSegmentLeaseService<String, String> segmentLeaseService;

    @Mock
    private SplitTaskCoordinator<String, String> splitExecutionCoordinator;

    @Mock
    private ScheduledExecutorService splitPolicyScheduler;

    @Mock
    private SegmentIndexRuntimeState runtimeState;

    @Test
    void workerSubmissionErrorReleasesReservedWorkers() {
        final SplitWorkerState policyState = new SplitWorkerState();
        final Executor workerExecutor = task -> {
            throw new OutOfMemoryError("worker submit failed");
        };
        when(conf.maintenance()).thenReturn(
                new EffectiveIndexMaintenanceConfiguration(3, 1, 1, 50, true));
        when(runtimeState.currentState()).thenReturn(SegmentIndexState.READY);
        final SplitPolicyScheduler<String, String> scheduler =
                new SplitPolicyScheduler<>(conf, runtimeTuningState,
                        keyToSegmentMap, segmentLeaseService,
                        splitExecutionCoordinator, workerExecutor,
                        splitPolicyScheduler, runtimeState,
                        new SplitStatsRecorder(), policyState,
                        new SplitCandidateQueue());

        assertThrows(OutOfMemoryError.class, scheduler::requestFullSplitScan);

        assertFalse(policyState.isWorkerActive());
    }
}
