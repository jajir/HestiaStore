package org.hestiastore.index.segmentindex.core.executorregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
                splitPolicyScheduler,
                stableSegmentMaintenance,
                registryMaintenance,
                1_000);

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

    @Test
    void shutdownExecutorsInCloseOrderTimesOutAndCallsShutdownNow() {
        final java.util.ArrayList<String> shutdownOrder =
                new java.util.ArrayList<>();
        final NeverTerminatingExecutorService indexMaintenance =
                new NeverTerminatingExecutorService("index", shutdownOrder);
        final ExecutorTestSupport.RecordingExecutorService splitMaintenance =
                new ExecutorTestSupport.RecordingExecutorService("split",
                        shutdownOrder);
        final ExecutorTestSupport.RecordingExecutorService stableSegmentMaintenance =
                new ExecutorTestSupport.RecordingExecutorService(
                        "stable", shutdownOrder);
        final ScheduledExecutorService splitPolicyScheduler =
                new ExecutorTestSupport.RecordingScheduledExecutorService(
                        "scheduler", shutdownOrder);
        final ExecutorTestSupport.RecordingExecutorService registryMaintenance =
                new ExecutorTestSupport.RecordingExecutorService(
                        "registry", shutdownOrder);
        final ExecutorTopology topology = new ExecutorTopology(
                indexMaintenance, splitMaintenance, splitPolicyScheduler,
                stableSegmentMaintenance, registryMaintenance, 1);

        final RuntimeException failure = topology.shutdownExecutorsInCloseOrder();

        assertNotNull(failure);
        assertTrue(failure.getMessage().contains("indexMaintenance"));
        assertTrue(failure.getMessage().contains("1 ms"));
        assertTrue(indexMaintenance.shutdownNowCalled());
        assertEquals(List.of("index", "index", "split", "scheduler",
                "stable", "registry"),
                shutdownOrder);
    }

    private static final class NeverTerminatingExecutorService
            extends ExecutorTestSupport.RecordingExecutorService {

        private boolean shutdownNowCalled;

        private NeverTerminatingExecutorService(final String name,
                final List<String> shutdownOrder) {
            super(name, shutdownOrder);
        }

        @Override
        public java.util.List<Runnable> shutdownNow() {
            shutdownNowCalled = true;
            super.shutdownNow();
            return List.of();
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(final long timeout,
                final java.util.concurrent.TimeUnit unit) {
            return false;
        }

        boolean shutdownNowCalled() {
            return shutdownNowCalled;
        }
    }
}
