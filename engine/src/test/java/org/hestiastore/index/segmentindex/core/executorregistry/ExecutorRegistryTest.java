package org.hestiastore.index.segmentindex.core.executorregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.hestiastore.index.segmentindex.core.metrics.IndexExecutorMetricsAccess;
import org.hestiastore.index.segmentindex.core.metrics.IndexExecutorRuntimeAccess;
import org.junit.jupiter.api.Test;

class ExecutorRegistryTest {

    @Test
    void builderRejectsMissingIndexMaintenanceThreads() {
        final ExecutorRegistryBuilder builder = ExecutorRegistry.builder()
                .withSegmentMaintenanceThreads(1)
                .withSplitMaintenanceThreads(1)
                .withRegistryMaintenanceThreads(1);

        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, builder::build);
        assertEquals("Property 'indexMaintenanceThreads' must not be null.",
                ex.getMessage());
    }

    @Test
    void builderUsesProvidedValues() {
        final ExecutorRegistry registry = newRegistry(2, 3, 4);
        try {
            assertNotNull(registry.getIndexMaintenanceExecutor());
            assertNotNull(registry.getSplitMaintenanceExecutor());
            assertNotNull(registry.getSplitPolicyScheduler());
            assertNotNull(registry.getStableSegmentMaintenanceExecutor());
            assertNotNull(registry.getRegistryMaintenanceExecutor());
        } finally {
            registry.close();
        }
    }

    @Test
    void builderRejectsNonPositiveSegmentMaintenanceThreads() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> newRegistry(0, 1, 1));
        assertEquals(
                "Property 'numberOfSegmentMaintenanceThreads' must be greater than 0",
                ex.getMessage());
    }

    @Test
    void builderRejectsNonPositiveIndexMaintenanceThreads() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> newRegistry(1, 0, 1));
        assertEquals(
                "Property 'indexMaintenanceThreads' must be greater than 0",
                ex.getMessage());
    }

    @Test
    void builderRejectsNonPositiveSplitMaintenanceThreads() {
        final ExecutorRegistryBuilder builder = newRegistryBuilder(1, 1, 1)
                .withSplitMaintenanceThreads(0);

        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, builder::build);
        assertEquals(
                "Property 'splitMaintenanceThreads' must be greater than 0",
                ex.getMessage());
    }

    @Test
    void builderRejectsNonPositiveRegistryMaintenanceThreads() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> newRegistry(1, 1, 0));
        assertEquals(
                "Property 'registryMaintenanceThreads' must be greater than 0",
                ex.getMessage());
    }

    @Test
    void builderRejectsBlankIndexNameWhenContextLoggingEnabled() {
        final ExecutorRegistryBuilder builder = newRegistryBuilder(1, 1, 1)
                .withContextLoggingEnabled(true)
                .withIndexName("  ");

        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, builder::build);
        assertEquals("Property 'indexName' must not be blank.",
                ex.getMessage());
    }

    @Test
    void closeShutsDownAllExecutors() {
        final ExecutorRegistry registry = newRegistry(1, 1, 1);
        final ExecutorService indexMaintenance = registry
                .getIndexMaintenanceExecutor();
        final ExecutorService splitMaintenance = registry
                .getSplitMaintenanceExecutor();
        final ExecutorService splitPolicyScheduler = registry
                .getSplitPolicyScheduler();
        final ExecutorService stableSegmentMaintenance = registry
                .getStableSegmentMaintenanceExecutor();
        final ExecutorService registryMaintenance = registry
                .getRegistryMaintenanceExecutor();

        registry.close();

        assertTrue(indexMaintenance.isShutdown());
        assertTrue(splitMaintenance.isShutdown());
        assertTrue(splitPolicyScheduler.isShutdown());
        assertTrue(stableSegmentMaintenance.isShutdown());
        assertTrue(registryMaintenance.isShutdown());
    }

    @Test
    void gettersRejectCallsAfterClose() {
        final ExecutorRegistry registry = newRegistry(1, 1, 1);
        registry.close();

        assertThrows(IllegalStateException.class,
                registry::getIndexMaintenanceExecutor);
        assertThrows(IllegalStateException.class,
                registry::getSplitMaintenanceExecutor);
        assertThrows(IllegalStateException.class,
                registry::getSplitPolicyScheduler);
        assertThrows(IllegalStateException.class,
                registry::getStableSegmentMaintenanceExecutor);
        assertThrows(IllegalStateException.class,
                registry::getRegistryMaintenanceExecutor);
    }

    @Test
    void gettersReturnSameExecutorInstances() {
        final ExecutorRegistry registry = newRegistry(1, 1, 1);
        try {
            assertSame(registry.getIndexMaintenanceExecutor(),
                    registry.getIndexMaintenanceExecutor());
            assertSame(registry.getSplitMaintenanceExecutor(),
                    registry.getSplitMaintenanceExecutor());
            assertSame(registry.getSplitPolicyScheduler(),
                    registry.getSplitPolicyScheduler());
            assertSame(registry.getStableSegmentMaintenanceExecutor(),
                    registry.getStableSegmentMaintenanceExecutor());
            assertSame(registry.getRegistryMaintenanceExecutor(),
                    registry.getRegistryMaintenanceExecutor());
        } finally {
            registry.close();
        }
    }

    @Test
    void closeDoesNotNeedLazyExecutorsToBeRequestedFirst() {
        final ExecutorRegistry registry = newRegistry(1, 1, 1);

        final ExecutorService indexMaintenance = registry
                .getIndexMaintenanceExecutor();
        final ExecutorService splitMaintenance = registry
                .getSplitMaintenanceExecutor();
        final ExecutorService stableSegmentMaintenance = registry
                .getStableSegmentMaintenanceExecutor();

        registry.close();

        assertTrue(indexMaintenance.isShutdown());
        assertTrue(splitMaintenance.isShutdown());
        assertTrue(stableSegmentMaintenance.isShutdown());
    }

    @Test
    void executorsUseExpectedThreadNamesAndDaemonThreads()
            throws InterruptedException, ExecutionException {
        final ExecutorRegistry registry = newRegistry(1, 1, 1);
        try {
            final String indexMaintenanceName = registry
                    .getIndexMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().getName()).get();
            final String splitMaintenanceName = registry
                    .getSplitMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().getName()).get();
            final String splitPolicyName = registry
                    .getSplitPolicyScheduler()
                    .submit(() -> Thread.currentThread().getName()).get();
            final String stableSegmentMaintenanceName = registry
                    .getStableSegmentMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().getName()).get();
            final String registryMaintenanceName = registry
                    .getRegistryMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().getName()).get();

            final boolean indexMaintenanceDaemon = registry
                    .getIndexMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().isDaemon()).get();
            final boolean splitMaintenanceDaemon = registry
                    .getSplitMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().isDaemon()).get();
            final boolean splitPolicyDaemon = registry
                    .getSplitPolicyScheduler()
                    .submit(() -> Thread.currentThread().isDaemon()).get();
            final boolean stableSegmentMaintenanceDaemon = registry
                    .getStableSegmentMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().isDaemon()).get();
            final boolean registryMaintenanceDaemon = registry
                    .getRegistryMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().isDaemon()).get();

            assertTrue(indexMaintenanceName.startsWith("index-maintenance-"));
            assertTrue(splitMaintenanceName.startsWith("split-maintenance-"));
            assertTrue(splitPolicyName.startsWith("split-policy-"));
            assertTrue(stableSegmentMaintenanceName
                    .startsWith("segment-maintenance-"));
            assertTrue(
                    registryMaintenanceName.startsWith("registry-maintenance-"));
            assertTrue(indexMaintenanceDaemon);
            assertTrue(splitMaintenanceDaemon);
            assertTrue(splitPolicyDaemon);
            assertTrue(stableSegmentMaintenanceDaemon);
            assertTrue(registryMaintenanceDaemon);
        } finally {
            registry.close();
        }
    }

    @Test
    void stableSegmentMaintenanceExecutorUsesConfiguredThreadCount()
            throws InterruptedException {
        final int stableSegmentMaintenanceThreads = 4;
        final ExecutorRegistry registry = newRegistry(stableSegmentMaintenanceThreads, 1, 1);
        final CountDownLatch started = new CountDownLatch(
                stableSegmentMaintenanceThreads);
        final CountDownLatch release = new CountDownLatch(1);
        try {
            final List<java.util.concurrent.Future<String>> futures = java.util.stream.IntStream
                    .range(0, stableSegmentMaintenanceThreads)
                    .mapToObj(i -> registry
                            .getStableSegmentMaintenanceExecutor().submit(() -> {
                                started.countDown();
                                release.await();
                                return Thread.currentThread().getName();
                            }))
                    .toList();
            assertTrue(started.await(2, TimeUnit.SECONDS),
                    "Stable segment maintenance executor did not start all configured workers.");
            release.countDown();
            final Set<String> threadNames = futures.stream().map(future -> {
                try {
                    return future.get();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            "Interrupted while collecting stable segment maintenance worker names.",
                            e);
                } catch (final ExecutionException e) {
                    throw new IllegalStateException(
                            "Failed while collecting stable segment maintenance worker names.",
                            e.getCause());
                }
            }).collect(Collectors.toSet());
            assertEquals(stableSegmentMaintenanceThreads,
                    threadNames.size());
        } finally {
            release.countDown();
            registry.close();
        }
    }

    @Test
    void runtimeSnapshotTracksIndexMaintenanceQueuePressureAndRejections()
            throws InterruptedException {
        final ExecutorRegistry registry = newRegistry(1, 1, 1);
        final CountDownLatch workerStarted = new CountDownLatch(1);
        final CountDownLatch releaseWorker = new CountDownLatch(1);
        try {
            registry.getIndexMaintenanceExecutor().execute(() -> {
                workerStarted.countDown();
                awaitRelease(releaseWorker);
            });
            assertTrue(workerStarted.await(2, TimeUnit.SECONDS));
            for (int i = 0; i < 64; i++) {
                registry.getIndexMaintenanceExecutor().execute(() -> {
                });
            }
            final ExecutorService indexMaintenanceExecutor = registry.getIndexMaintenanceExecutor();

            assertThrows(RejectedExecutionException.class,
                    () -> indexMaintenanceExecutor.execute(() -> {
                    }));

            final IndexExecutorMetricsAccess snapshot = registry.runtimeSnapshot().getIndexMaintenance();
            assertEquals(1, snapshot.getActiveThreadCount());
            assertEquals(64, snapshot.getQueueSize());
            assertEquals(64, snapshot.getQueueCapacity());
            assertEquals(1L, snapshot.getRejectedTaskCount());
            assertEquals(0L, snapshot.getCallerRunsCount());
        } finally {
            releaseWorker.countDown();
            registry.close();
        }
    }

    @Test
    void runtimeSnapshotTracksCompletedTasksAndCallerRuns()
            throws InterruptedException, ExecutionException {
        final ExecutorRegistry registry = newRegistry(1, 1, 1);
        final CountDownLatch workerStarted = new CountDownLatch(1);
        final CountDownLatch releaseWorker = new CountDownLatch(1);
        final AtomicReference<String> callerRunThread = new AtomicReference<>();
        try {
            registry.getSplitMaintenanceExecutor().submit(() -> {
            }).get();
            registry.getStableSegmentMaintenanceExecutor().submit(() -> {
            }).get();

            registry.getStableSegmentMaintenanceExecutor().execute(() -> {
                workerStarted.countDown();
                awaitRelease(releaseWorker);
            });
            assertTrue(workerStarted.await(2, TimeUnit.SECONDS));
            for (int i = 0; i < 64; i++) {
                registry.getStableSegmentMaintenanceExecutor().execute(() -> {
                });
            }

            final String submittingThreadName = Thread.currentThread().getName();
            registry.getStableSegmentMaintenanceExecutor()
                    .execute(() -> callerRunThread
                            .set(Thread.currentThread().getName()));

            assertEquals(submittingThreadName, callerRunThread.get());

            final IndexExecutorRuntimeAccess runtimeSnapshot = registry.runtimeSnapshot();
            assertEquals(1L, runtimeSnapshot.getSplitMaintenance()
                    .getCompletedTaskCount());
            assertEquals(1L, runtimeSnapshot.getStableSegmentMaintenance()
                    .getCompletedTaskCount());
            assertEquals(1L, runtimeSnapshot.getStableSegmentMaintenance()
                    .getCallerRunsCount());
            assertEquals(1, runtimeSnapshot.getStableSegmentMaintenance()
                    .getActiveThreadCount());
            assertEquals(64, runtimeSnapshot.getStableSegmentMaintenance()
                    .getQueueSize());
            assertEquals(64, runtimeSnapshot.getStableSegmentMaintenance()
                    .getQueueCapacity());
        } finally {
            releaseWorker.countDown();
            registry.close();
        }
    }

    private static void awaitRelease(final CountDownLatch latch) {
        try {
            latch.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static ExecutorRegistry newRegistry(
            final int stableSegmentMaintenanceThreads,
            final int indexMaintenanceThreads,
            final int registryMaintenanceThreads) {
        return newRegistryBuilder(stableSegmentMaintenanceThreads,
                indexMaintenanceThreads, registryMaintenanceThreads).build();
    }

    private static ExecutorRegistryBuilder newRegistryBuilder(
            final int stableSegmentMaintenanceThreads,
            final int indexMaintenanceThreads,
            final int registryMaintenanceThreads) {
        return ExecutorRegistry.builder()
                .withIndexName("executor-registry-test")
                .withContextLoggingEnabled(false)
                .withIndexMaintenanceThreads(indexMaintenanceThreads)
                .withSplitMaintenanceThreads(indexMaintenanceThreads)
                .withSegmentMaintenanceThreads(stableSegmentMaintenanceThreads)
                .withRegistryMaintenanceThreads(registryMaintenanceThreads);
    }
}
