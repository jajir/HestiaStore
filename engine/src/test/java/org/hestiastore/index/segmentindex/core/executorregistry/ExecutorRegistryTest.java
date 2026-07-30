package org.hestiastore.index.segmentindex.core.executorregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ExecutorRegistryTest {

    private RuntimeExecutorPools runtimeExecutorPools;

    @AfterEach
    void tearDown() {
        if (runtimeExecutorPools != null
                && !runtimeExecutorPools.wasClosed()) {
            runtimeExecutorPools.close();
        }
    }

    @Test
    void createUsesProvidedValues() {
        final ExecutorRegistry registry = newRegistry(2, 3, 4);
        try {
            assertNotNull(registry.getIndexMaintenanceExecutor());
            assertNotNull(registry.getSplitMaintenanceExecutor());
            assertNotNull(registry.getSplitPolicyScheduler());
            assertNotNull(registry.getStableSegmentMaintenanceExecutor());
            assertNotNull(registry.getRegistryMaintenanceExecutor());
            assertNotNull(registry.getWalAppendThreadFactory());
        } finally {
            registry.close();
        }
    }

    @Test
    void createRejectsNonPositiveSegmentMaintenanceThreads() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> RuntimeExecutorPools.create("hestia-test", 0, 1,
                        30_000));
        assertEquals(
                "Property 'segmentMaintenanceThreads' must be greater than 0",
                ex.getMessage());
    }

    @Test
    void createRejectsNonPositiveIndexMaintenanceThreads() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> newRegistry(1, 0, 1));
        assertEquals(
                "Property 'indexMaintenanceThreads' must be greater than 0",
                ex.getMessage());
    }

    @Test
    void createRejectsNonPositiveSplitMaintenanceThreads() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> RuntimeExecutorPools.create("hestia-test", 1, 0,
                        30_000));
        assertEquals(
                "Property 'splitMaintenanceThreads' must be greater than 0",
                ex.getMessage());
    }

    @Test
    void createRejectsNonPositiveRegistryMaintenanceThreads() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> newRegistry(1, 1, 0));
        assertEquals(
                "Property 'registryMaintenanceThreads' must be greater than 0",
                ex.getMessage());
    }

    @Test
    void createRejectsBlankIndexNameWhenContextLoggingEnabled() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> {
                    runtimeExecutorPools = RuntimeExecutorPools.create(
                            "hestia-test", 1, 1, 30_000);
                    ExecutorRegistry.create("  ", true, 1,
                            runtimeExecutorPools, 1, 30_000);
                });
        assertEquals("Property 'indexName' must not be blank.",
                ex.getMessage());
    }

    @Test
    void closeShutsDownIndexOwnedExecutors() {
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
        assertFalse(splitMaintenance.isShutdown());
        assertTrue(splitPolicyScheduler.isShutdown());
        assertFalse(stableSegmentMaintenance.isShutdown());
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
        assertThrows(IllegalStateException.class,
                registry::getWalAppendThreadFactory);
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
            assertSame(registry.getWalAppendThreadFactory(),
                    registry.getWalAppendThreadFactory());
        } finally {
            registry.close();
        }
    }

    @Test
    void closeDoesNotNeedEveryExecutorGetterToBeCalledFirst() {
        final ExecutorRegistry registry = newRegistry(1, 1, 1);

        final ExecutorService indexMaintenance = registry
                .getIndexMaintenanceExecutor();
        final ExecutorService splitMaintenance = registry
                .getSplitMaintenanceExecutor();
        final ExecutorService stableSegmentMaintenance = registry
                .getStableSegmentMaintenanceExecutor();

        registry.close();

        assertTrue(indexMaintenance.isShutdown());
        assertFalse(splitMaintenance.isShutdown());
        assertFalse(stableSegmentMaintenance.isShutdown());
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
            final Thread walAppendThread = registry
                    .getWalAppendThreadFactory().newThread(() -> {
                    });

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

            assertTrue(indexMaintenanceName.startsWith(
                    "hestia-test-executor-registry-test-index-maintenance-"));
            assertTrue(splitMaintenanceName
                    .startsWith("hestia-test-split-maintenance-"));
            assertTrue(splitPolicyName.startsWith(
                    "hestia-test-executor-registry-test-split-policy-"));
            assertTrue(stableSegmentMaintenanceName
                    .startsWith("hestia-test-segment-maintenance-"));
            assertTrue(registryMaintenanceName.startsWith(
                    "hestia-test-executor-registry-test-registry-maintenance-"));
            assertTrue(walAppendThread.getName().startsWith(
                    "hestia-test-executor-registry-test-wal-append-"));
            assertTrue(indexMaintenanceDaemon);
            assertTrue(splitMaintenanceDaemon);
            assertTrue(splitPolicyDaemon);
            assertTrue(stableSegmentMaintenanceDaemon);
            assertTrue(registryMaintenanceDaemon);
            assertTrue(walAppendThread.isDaemon());
        } finally {
            registry.close();
        }
    }

    @Test
    void stableSegmentMaintenanceExecutorUsesConfiguredThreadCount()
            throws InterruptedException {
        final int segmentMaintenanceThreads = 4;
        final ExecutorRegistry registry = newRegistry(segmentMaintenanceThreads,
                1, 1);
        final CountDownLatch started = new CountDownLatch(
                segmentMaintenanceThreads);
        final CountDownLatch release = new CountDownLatch(1);
        try {
            final List<java.util.concurrent.Future<String>> futures = java.util.stream.IntStream
                    .range(0, segmentMaintenanceThreads)
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
            assertEquals(segmentMaintenanceThreads,
                    threadNames.size());
        } finally {
            release.countDown();
            registry.close();
        }
    }

    @Test
    void statsSnapshotTracksIndexMaintenanceQueuePressureAndRejections()
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

            final ExecutorStats snapshot =
                    registry.statsSnapshot().getIndexMaintenance();
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
    void statsSnapshotTracksCompletedTasksAndCallerRuns()
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

            final ExecutorRegistryStats statsSnapshot =
                    registry.statsSnapshot();
            assertEquals(1L, statsSnapshot.getSplitMaintenance()
                    .getCompletedTaskCount());
            assertEquals(1L, statsSnapshot.getStableSegmentMaintenance()
                    .getCompletedTaskCount());
            assertEquals(1L, statsSnapshot.getStableSegmentMaintenance()
                    .getCallerRunsCount());
            assertEquals(1, statsSnapshot.getStableSegmentMaintenance()
                    .getActiveThreadCount());
            assertEquals(64, statsSnapshot.getStableSegmentMaintenance()
                    .getQueueSize());
            assertEquals(64, statsSnapshot.getStableSegmentMaintenance()
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

    private ExecutorRegistry newRegistry(
            final int segmentMaintenanceThreads,
            final int indexMaintenanceThreads,
            final int registryMaintenanceThreads) {
        runtimeExecutorPools = RuntimeExecutorPools.create("hestia-test",
                segmentMaintenanceThreads, 1, 30_000);
        return newRegistry(indexMaintenanceThreads, registryMaintenanceThreads,
                runtimeExecutorPools);
    }

    private ExecutorRegistry newRegistry(
            final int indexMaintenanceThreads,
            final int registryMaintenanceThreads,
            final RuntimeExecutorPools runtimeExecutorPools) {
        return ExecutorRegistry.create("executor-registry-test", false,
                indexMaintenanceThreads, runtimeExecutorPools,
                registryMaintenanceThreads,
                30_000);
    }
}
