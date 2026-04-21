package org.hestiastore.index.segmentindex.core.infrastructure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.jupiter.api.Test;

class IndexExecutorTopologyTest {

    @Test
    void shutdownExecutorsInCloseOrderShutsDownAllExecutors() {
        final java.util.ArrayList<String> shutdownOrder =
                new java.util.ArrayList<>();
        final IndexExecutorTestSupport.RecordingExecutorService indexMaintenance =
                new IndexExecutorTestSupport.RecordingExecutorService("index",
                        shutdownOrder);
        final IndexExecutorTestSupport.RecordingExecutorService splitMaintenance =
                new IndexExecutorTestSupport.RecordingExecutorService("split",
                        shutdownOrder);
        final ScheduledExecutorService splitPolicyScheduler =
                new IndexExecutorTestSupport.RecordingScheduledExecutorService(
                        "scheduler", shutdownOrder);
        final IndexExecutorTestSupport.RecordingExecutorService stableSegmentMaintenance =
                new IndexExecutorTestSupport.RecordingExecutorService(
                        "stable", shutdownOrder);
        final IndexExecutorTestSupport.RecordingExecutorService registryMaintenance =
                new IndexExecutorTestSupport.RecordingExecutorService(
                        "registry", shutdownOrder);
        final IndexExecutorTopology topology = new IndexExecutorTopology(
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
