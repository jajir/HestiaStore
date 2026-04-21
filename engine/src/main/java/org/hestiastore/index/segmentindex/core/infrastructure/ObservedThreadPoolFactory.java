package org.hestiastore.index.segmentindex.core.infrastructure;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.IndexConfiguration;

/**
 * Creates observed thread pools with consistent sizing, naming, and rejection
 * tracking.
 */
final class ObservedThreadPoolFactory {

    private static final int MIN_QUEUE_CAPACITY = 64;
    private static final int QUEUE_CAPACITY_MULTIPLIER = 64;

    private final IndexConfiguration<?, ?> configuration;

    ObservedThreadPoolFactory(final IndexConfiguration<?, ?> configuration) {
        this.configuration = Vldtn.requireNonNull(configuration,
                "configuration");
    }

    ObservedThreadPool createAbortingPool(
            final Function<IndexConfiguration<?, ?>, Integer> threadCountSupplier,
            final String threadCountArgumentName,
            final String threadNamePrefix) {
        final LongAdder rejectedTaskCount = new LongAdder();
        return createObservedThreadPool(threadCountSupplier,
                threadCountArgumentName, threadNamePrefix,
                new CountingAbortPolicy(rejectedTaskCount), rejectedTaskCount,
                new LongAdder());
    }

    ObservedThreadPool createCallerRunsPool(
            final Function<IndexConfiguration<?, ?>, Integer> threadCountSupplier,
            final String threadCountArgumentName,
            final String threadNamePrefix) {
        final LongAdder callerRunsCount = new LongAdder();
        return createObservedThreadPool(threadCountSupplier,
                threadCountArgumentName, threadNamePrefix,
                new CountingCallerRunsPolicy(callerRunsCount),
                new LongAdder(), callerRunsCount);
    }

    ThreadFactory daemonThreadFactory(final String threadNamePrefix) {
        final AtomicInteger threadCounter = new AtomicInteger(1);
        return runnable -> {
            final Thread thread = new Thread(runnable,
                    threadNamePrefix + threadCounter.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        };
    }

    int configuredThreadCount(
            final Function<IndexConfiguration<?, ?>, Integer> threadCountSupplier,
            final String threadCountArgumentName) {
        return Vldtn.requireGreaterThanZero(Vldtn.requireNonNull(
                Vldtn.requireNonNull(threadCountSupplier, "threadCountSupplier")
                        .apply(configuration),
                threadCountArgumentName), threadCountArgumentName);
    }

    private ObservedThreadPool createObservedThreadPool(
            final Function<IndexConfiguration<?, ?>, Integer> threadCountSupplier,
            final String threadCountArgumentName,
            final String threadNamePrefix,
            final RejectedExecutionHandler rejectedExecutionHandler,
            final LongAdder rejectedTaskCount,
            final LongAdder callerRunsCount) {
        final int threadCount = configuredThreadCount(threadCountSupplier,
                threadCountArgumentName);
        final int queueCapacity = queueCapacity(threadCount);
        return new ObservedThreadPool(new ThreadPoolExecutor(threadCount,
                threadCount, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                daemonThreadFactory(threadNamePrefix),
                rejectedExecutionHandler), queueCapacity, rejectedTaskCount,
                callerRunsCount);
    }

    private int queueCapacity(final int threadCount) {
        return Math.max(MIN_QUEUE_CAPACITY,
                threadCount * QUEUE_CAPACITY_MULTIPLIER);
    }

    private static final class CountingAbortPolicy
            implements RejectedExecutionHandler {

        private final LongAdder rejectedTaskCount;

        private CountingAbortPolicy(final LongAdder rejectedTaskCount) {
            this.rejectedTaskCount = Vldtn.requireNonNull(rejectedTaskCount,
                    "rejectedTaskCount");
        }

        @Override
        public void rejectedExecution(final Runnable runnable,
                final ThreadPoolExecutor executor) {
            rejectedTaskCount.increment();
            throw new RejectedExecutionException(String.format(
                    "Task %s rejected from %s", runnable, executor));
        }
    }

    private static final class CountingCallerRunsPolicy
            implements RejectedExecutionHandler {

        private final LongAdder callerRunsCount;
        private final ThreadPoolExecutor.CallerRunsPolicy delegate = new ThreadPoolExecutor.CallerRunsPolicy();

        private CountingCallerRunsPolicy(final LongAdder callerRunsCount) {
            this.callerRunsCount = Vldtn.requireNonNull(callerRunsCount,
                    "callerRunsCount");
        }

        @Override
        public void rejectedExecution(final Runnable runnable,
                final ThreadPoolExecutor executor) {
            callerRunsCount.increment();
            delegate.rejectedExecution(runnable, executor);
        }
    }
}
