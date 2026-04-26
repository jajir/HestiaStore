package org.hestiastore.index.segmentindex.core.executorregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.jupiter.api.Test;

class ExecutorTopologyTest {

    @Test
    void shutdownExecutorsInCloseOrderShutsDownAllExecutors() {
        final java.util.ArrayList<String> shutdownOrder =
                new java.util.ArrayList<>();
        final ExecutorTestSupport.RecordingExecutorService indexMaintenance =
                new ExecutorTestSupport.RecordingExecutorService("index",
                        shutdownOrder);
        final ExecutorTestSupport.RecordingExecutorService splitMaintenance =
                new ExecutorTestSupport.RecordingExecutorService("split",
                        shutdownOrder);
        final ScheduledExecutorService splitPolicyScheduler =
                new ExecutorTestSupport.RecordingScheduledExecutorService(
                        "scheduler", shutdownOrder);
        final ExecutorTestSupport.RecordingExecutorService stableSegmentMaintenance =
                new ExecutorTestSupport.RecordingExecutorService(
                        "stable", shutdownOrder);
        final ExecutorTestSupport.RecordingExecutorService registryMaintenance =
                new ExecutorTestSupport.RecordingExecutorService(
                        "registry", shutdownOrder);
        final ExecutorTopology topology = new ExecutorTopology(
                indexMaintenance, splitMaintenance,
                new LazyExecutorReference<>(() -> splitPolicyScheduler),
                stableSegmentMaintenance,
                new LazyExecutorReference<>(() -> registryMaintenance));

        topology.splitPolicyScheduler();
        topology.registryMaintenanceExecutor();

        final RuntimeException failure = topology.shutdownExecutorsInCloseOrder();

        assertEquals(null, failure);
        assertEquals(List.of("index", "split", "scheduler", "stable",
                "registry"), shutdownOrder);
        assertTrue(indexMaintenance.isShutdown());
        assertTrue(splitMaintenance.isShutdown());
        assertTrue(splitPolicyScheduler.isShutdown());
        assertTrue(stableSegmentMaintenance.isShutdown());
        assertTrue(registryMaintenance.isShutdown());
    }
}
