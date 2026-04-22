package org.hestiastore.index.segmentindex.core.maintenance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

class IndexExecutorTopologyTest {

    @Test
    void shutdownExecutorsInCloseOrderShutsDownAllExecutors() {
        final java.util.ArrayList<String> shutdownOrder =
                new java.util.ArrayList<>();
        final IndexExecutorTestSupport.RecordingExecutorService splitPlanner =
                new IndexExecutorTestSupport.RecordingExecutorService("planner",
                        shutdownOrder);
        final IndexExecutorTestSupport.RecordingExecutorService splitMaintenance =
                new IndexExecutorTestSupport.RecordingExecutorService("split",
                        shutdownOrder);
        final IndexExecutorTestSupport.RecordingExecutorService stableSegmentMaintenance =
                new IndexExecutorTestSupport.RecordingExecutorService(
                        "stable", shutdownOrder);
        final IndexExecutorTestSupport.RecordingExecutorService registryMaintenance =
                new IndexExecutorTestSupport.RecordingExecutorService(
                        "registry", shutdownOrder);
        final IndexExecutorTopology topology = new IndexExecutorTopology(
                splitPlanner, splitMaintenance, stableSegmentMaintenance,
                new LazyExecutorReference<>(() -> registryMaintenance));

        topology.registryMaintenanceExecutor();

        final RuntimeException failure = topology.shutdownExecutorsInCloseOrder();

        assertEquals(null, failure);
        assertEquals(List.of("planner", "split", "stable", "registry"),
                shutdownOrder);
        assertTrue(splitPlanner.isShutdown());
        assertTrue(splitMaintenance.isShutdown());
        assertTrue(stableSegmentMaintenance.isShutdown());
        assertTrue(registryMaintenance.isShutdown());
    }
}
