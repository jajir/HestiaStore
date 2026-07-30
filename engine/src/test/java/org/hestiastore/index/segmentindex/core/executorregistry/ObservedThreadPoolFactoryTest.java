package org.hestiastore.index.segmentindex.core.executorregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.jupiter.api.Test;

class ObservedThreadPoolFactoryTest {

    @Test
    void createAbortingPoolTracksRejectedTasks() {
        final ObservedThreadPool pool = new ObservedThreadPoolFactory()
                .createAbortingPool(1, "indexMaintenanceThreads",
                        "reject-test-");
        final ExecutorService executor = pool.executor();
        final CountDownLatch blocker = new CountDownLatch(1);
        try {
            executor.execute(() -> await(blocker));
            for (int i = 0; i < 64; i++) {
                executor.execute(() -> await(blocker));
            }

            assertThrows(RejectedExecutionException.class,
                    () -> executor.execute(() -> {
                    }));
            assertEquals(1L, pool.statsSnapshot().getRejectedTaskCount());
        } finally {
            blocker.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void createCallerRunsPoolTracksCallerRunsAndWarnsOnce() {
        final String warningPrefix = "Executor queue saturated for worker "
                + "prefix 'caller-runs-test-'";
        final List<LogEvent> logEvents = Collections
                .synchronizedList(new ArrayList<>());
        final LoggerContext context = (LoggerContext) LogManager
                .getContext(false);
        final Configuration configuration = context.getConfiguration();
        final LoggerConfig loggerConfig = configuration.getLoggerConfig(
                ObservedThreadPoolFactory.class.getName());
        final AbstractAppender appender = new AbstractAppender(
                "caller-runs-test-appender", null, null, false, null) {

            @Override
            public void append(final LogEvent event) {
                logEvents.add(event.toImmutable());
            }
        };
        appender.start();
        loggerConfig.addAppender(appender, Level.WARN, null);
        context.updateLoggers();
        final ObservedThreadPool pool = new ObservedThreadPoolFactory()
                .createCallerRunsPool(1, "segmentMaintenanceThreads",
                        "caller-runs-test-");
        final ExecutorService executor = pool.executor();
        final CountDownLatch blocker = new CountDownLatch(1);
        try {
            executor.execute(() -> await(blocker));
            for (int i = 0; i < 64; i++) {
                executor.execute(() -> await(blocker));
            }

            executor.execute(() -> {
            });
            executor.execute(() -> {
            });

            assertEquals(2L, pool.statsSnapshot().getCallerRunsCount());
            final List<LogEvent> saturationWarnings;
            synchronized (logEvents) {
                saturationWarnings = logEvents.stream()
                        .filter(event -> event.getLevel() == Level.WARN)
                        .filter(event -> event.getMessage()
                                .getFormattedMessage()
                                .startsWith(warningPrefix))
                        .toList();
            }
            assertEquals(1, saturationWarnings.size());
            final String warning = saturationWarnings.get(0).getMessage()
                    .getFormattedMessage();
            assertTrue(warning.contains("caller thread '"
                    + Thread.currentThread().getName() + "'"));
            assertTrue(warning.contains("queueRemainingCapacity=0"));
        } finally {
            blocker.countDown();
            executor.shutdownNow();
            loggerConfig.removeAppender(appender.getName());
            appender.stop();
            context.updateLoggers();
        }
    }

    @Test
    void createCallerRunsPoolDoesNotCountDiscardAfterShutdown() {
        final ObservedThreadPool pool = new ObservedThreadPoolFactory()
                .createCallerRunsPool(1, "segmentMaintenanceThreads",
                        "caller-runs-shutdown-test-");
        final ExecutorService executor = pool.executor();

        executor.shutdown();
        executor.execute(() -> {
        });

        assertEquals(0L, pool.statsSnapshot().getCallerRunsCount());
    }

    @Test
    void daemonThreadFactoryCreatesNamedDaemonThreads() {
        final Thread thread = new ObservedThreadPoolFactory()
                .daemonThreadFactory("observed-thread-").newThread(() -> {
                });

        assertTrue(thread.isDaemon());
        assertTrue(thread.getName().startsWith("observed-thread-"));
    }

    private static void await(final CountDownLatch blocker) {
        try {
            blocker.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
