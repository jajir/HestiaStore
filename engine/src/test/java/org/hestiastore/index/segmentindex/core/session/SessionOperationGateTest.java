package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

class SessionOperationGateTest {

    @Test
    void awaitOperationDrain_waitsForTrackedSyncTaskToFinish() throws Exception {
        final SessionOperationGate gate = SessionOperationGate
                .create();
        final CountDownLatch taskStarted = new CountDownLatch(1);
        final CountDownLatch releaseTask = new CountDownLatch(1);
        final ExecutorService trackedExecutor = Executors
                .newSingleThreadExecutor();
        final ExecutorService waiterExecutor = Executors
                .newSingleThreadExecutor();
        try {
            final Future<?> trackedTask = trackedExecutor.submit(
                    () -> {
                        gate.beginOperation();
                        try {
                            taskStarted.countDown();
                            await(releaseTask);
                        } finally {
                            gate.endOperation();
                        }
                    });
            assertTrue(taskStarted.await(1, TimeUnit.SECONDS));

            final Future<?> awaitTask = waiterExecutor
                    .submit(gate::awaitOperationDrain);
            assertThrows(TimeoutException.class,
                    () -> awaitTask.get(100, TimeUnit.MILLISECONDS));

            releaseTask.countDown();
            trackedTask.get(1, TimeUnit.SECONDS);
            awaitTask.get(1, TimeUnit.SECONDS);
        } finally {
            trackedExecutor.shutdownNow();
            waiterExecutor.shutdownNow();
        }
    }

    @Test
    void awaitOperationDrain_waitsForAllTrackedSyncTasksToFinish()
            throws Exception {
        final SessionOperationGate gate = SessionOperationGate.create();
        final CountDownLatch tasksStarted = new CountDownLatch(2);
        final CountDownLatch releaseTasks = new CountDownLatch(1);
        final ExecutorService trackedExecutor = Executors.newFixedThreadPool(2);
        final ExecutorService waiterExecutor = Executors
                .newSingleThreadExecutor();
        try {
            final Future<?> firstTask = submitTrackedTask(gate,
                    trackedExecutor, tasksStarted, releaseTasks);
            final Future<?> secondTask = submitTrackedTask(gate,
                    trackedExecutor, tasksStarted, releaseTasks);
            assertTrue(tasksStarted.await(1, TimeUnit.SECONDS));

            final Future<?> awaitTask = waiterExecutor
                    .submit(gate::awaitOperationDrain);
            assertThrows(TimeoutException.class,
                    () -> awaitTask.get(100, TimeUnit.MILLISECONDS));

            releaseTasks.countDown();
            firstTask.get(1, TimeUnit.SECONDS);
            secondTask.get(1, TimeUnit.SECONDS);
            awaitTask.get(1, TimeUnit.SECONDS);
        } finally {
            trackedExecutor.shutdownNow();
            waiterExecutor.shutdownNow();
        }
    }

    @Test
    void awaitOperationDrain_waitsForOutermostNestedOperation()
            throws Exception {
        final SessionOperationGate gate = SessionOperationGate.create();
        final CountDownLatch nestedOperationStarted = new CountDownLatch(1);
        final CountDownLatch releaseInnerOperation = new CountDownLatch(1);
        final CountDownLatch innerOperationFinished = new CountDownLatch(1);
        final CountDownLatch releaseOuterOperation = new CountDownLatch(1);
        final ExecutorService trackedExecutor = Executors
                .newSingleThreadExecutor();
        final ExecutorService waiterExecutor = Executors
                .newSingleThreadExecutor();
        try {
            final Future<?> trackedTask = trackedExecutor.submit(() -> {
                gate.beginOperation();
                try {
                    gate.beginOperation();
                    try {
                        nestedOperationStarted.countDown();
                        await(releaseInnerOperation);
                    } finally {
                        gate.endOperation();
                    }
                    innerOperationFinished.countDown();
                    await(releaseOuterOperation);
                } finally {
                    gate.endOperation();
                }
            });
            assertTrue(nestedOperationStarted.await(1, TimeUnit.SECONDS));

            final Future<?> awaitTask = waiterExecutor
                    .submit(gate::awaitOperationDrain);
            releaseInnerOperation.countDown();
            assertTrue(innerOperationFinished.await(1, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class,
                    () -> awaitTask.get(100, TimeUnit.MILLISECONDS));

            releaseOuterOperation.countDown();
            trackedTask.get(1, TimeUnit.SECONDS);
            awaitTask.get(1, TimeUnit.SECONDS);
        } finally {
            trackedExecutor.shutdownNow();
            waiterExecutor.shutdownNow();
        }
    }

    @Test
    void endOperation_throwsWhenNoTrackedOperationIsActive() {
        final SessionOperationGate gate = SessionOperationGate.create();

        final IllegalStateException thrown = assertThrows(
                IllegalStateException.class, gate::endOperation);

        assertEquals("No tracked index operation is active.",
                thrown.getMessage());
    }

    @Test
    void awaitOperationDrain_restoresInterruptAndThrows() throws Exception {
        final SessionOperationGate gate = SessionOperationGate.create();
        final CountDownLatch waiterStarted = new CountDownLatch(1);
        final AtomicReference<Thread> waiterThread = new AtomicReference<>();
        final AtomicBoolean interruptedAfterFailure = new AtomicBoolean();
        final ExecutorService waiterExecutor = Executors
                .newSingleThreadExecutor();
        gate.beginOperation();
        try {
            final Future<IllegalStateException> awaitTask = waiterExecutor
                    .submit(() -> {
                        waiterThread.set(Thread.currentThread());
                        waiterStarted.countDown();
                        try {
                            gate.awaitOperationDrain();
                            return null;
                        } catch (final IllegalStateException e) {
                            interruptedAfterFailure
                                    .set(Thread.currentThread().isInterrupted());
                            return e;
                        }
                    });
            assertTrue(waiterStarted.await(1, TimeUnit.SECONDS));

            waiterThread.get().interrupt();
            final IllegalStateException thrown = assertInstanceOf(
                    IllegalStateException.class,
                    awaitTask.get(1, TimeUnit.SECONDS));

            assertEquals(
                    "Interrupted while waiting for tracked operations to finish.",
                    thrown.getMessage());
            assertInstanceOf(InterruptedException.class, thrown.getCause());
            assertTrue(interruptedAfterFailure.get());
        } finally {
            gate.endOperation();
            waiterExecutor.shutdownNow();
        }
    }

    @Test
    void awaitOperationDrain_throwsWhenCalledFromTrackedSyncOperation() {
        final SessionOperationGate gate = SessionOperationGate
                .create();

        final IllegalStateException thrown = assertThrows(
                IllegalStateException.class,
                () -> {
                    gate.beginOperation();
                    try {
                        gate.awaitOperationDrain();
                    } finally {
                        gate.endOperation();
                    }
                });

        assertEquals("close() must not be called from an index operation.",
                thrown.getMessage());
    }

    private static Future<?> submitTrackedTask(
            final SessionOperationGate gate,
            final ExecutorService trackedExecutor,
            final CountDownLatch tasksStarted,
            final CountDownLatch releaseTasks) {
        return trackedExecutor.submit(() -> {
            gate.beginOperation();
            try {
                tasksStarted.countDown();
                await(releaseTasks);
            } finally {
                gate.endOperation();
            }
        });
    }

    private static void await(final CountDownLatch latch) {
        try {
            latch.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while waiting for tracked task release.", e);
        }
    }
}
