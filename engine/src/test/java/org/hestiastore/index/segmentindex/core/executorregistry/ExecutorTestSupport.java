package org.hestiastore.index.segmentindex.core.executorregistry;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

final class ExecutorTestSupport {

    private ExecutorTestSupport() {
    }

    static ObservedThreadPool observedThreadPool(final int queueCapacity,
            final long rejectedTaskCount, final long callerRunsCount) {
        final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0L,
                TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(queueCapacity));
        final LongAdder rejectedTaskCounter = new LongAdder();
        final LongAdder callerRunsCounter = new LongAdder();
        rejectedTaskCounter.add(rejectedTaskCount);
        callerRunsCounter.add(callerRunsCount);
        return new ObservedThreadPool(executor, queueCapacity,
                rejectedTaskCounter, callerRunsCounter);
    }

    static class RecordingExecutorService extends AbstractExecutorService {

        private final String name;
        private final List<String> shutdownOrder;
        private boolean shutdown;

        RecordingExecutorService(final String name,
                final List<String> shutdownOrder) {
            this.name = name;
            this.shutdownOrder = shutdownOrder;
        }

        @Override
        public void shutdown() {
            shutdown = true;
            shutdownOrder.add(name);
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown();
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(final long timeout,
                final TimeUnit unit) {
            return shutdown;
        }

        @Override
        public void execute(final Runnable command) {
            command.run();
        }
    }

    static final class RecordingScheduledExecutorService
            extends RecordingExecutorService
            implements ScheduledExecutorService {

        RecordingScheduledExecutorService(final String name,
                final List<String> shutdownOrder) {
            super(name, shutdownOrder);
        }

        @Override
        public java.util.concurrent.ScheduledFuture<?> schedule(
                final Runnable command, final long delay,
                final TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <V> java.util.concurrent.ScheduledFuture<V> schedule(
                final java.util.concurrent.Callable<V> callable,
                final long delay, final TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.util.concurrent.ScheduledFuture<?> scheduleAtFixedRate(
                final Runnable command, final long initialDelay,
                final long period, final TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.util.concurrent.ScheduledFuture<?> scheduleWithFixedDelay(
                final Runnable command, final long initialDelay,
                final long delay, final TimeUnit unit) {
            throw new UnsupportedOperationException();
        }
    }
}
