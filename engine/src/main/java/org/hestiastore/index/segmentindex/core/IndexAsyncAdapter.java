package org.hestiastore.index.segmentindex.core;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.SegmentWindow;

/**
 * Adapter that provides async operations by running synchronous calls on a
 * background thread.
 *
 * @param <K> type of keys stored in the index
 * @param <V> type of values stored in the index
 */
class IndexAsyncAdapter<K, V> extends AbstractCloseableResource
        implements SegmentIndex<K, V> {

    private static final int MIN_QUEUE_CAPACITY = 64;
    private static final int QUEUE_CAPACITY_MULTIPLIER = 64;
    private static final String ARG_EXECUTOR = "executor";
    private static final String ARG_INDEX_CONFIGURATION = "indexConfiguration";
    private static final String ARG_INDEX_NAME = "indexName";
    private static final String ARG_INDEX_WORKER_THREAD_COUNT = "indexWorkerThreadCount";
    private static final String THREAD_NAME_PREFIX_INDEX_WORKER = "index-worker-";

    private final SegmentIndex<K, V> index;
    private final ExecutorService executor;
    private final boolean shutdownExecutorOnClose;

    IndexAsyncAdapter(final SegmentIndex<K, V> index) {
        this(index,
                createExecutor(Vldtn.requireNonNull(index, "index")
                        .getConfiguration()),
                true);
    }

    IndexAsyncAdapter(final SegmentIndex<K, V> index,
            final ExecutorService executor) {
        this(index, executor, false);
    }

    private IndexAsyncAdapter(final SegmentIndex<K, V> index,
            final ExecutorService executor,
            final boolean shutdownExecutorOnClose) {
        this.index = Vldtn.requireNonNull(index, "index");
        this.executor = Vldtn.requireNonNull(executor, ARG_EXECUTOR);
        this.shutdownExecutorOnClose = shutdownExecutorOnClose;
    }

    /** {@inheritDoc} */
    @Override
    public void put(final K key, final V value) {
        index.put(key, value);
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<Void> putAsync(final K key, final V value) {
        return runAsyncTracked(() -> {
            put(key, value);
            return null;
        });
    }

    /** {@inheritDoc} */
    @Override
    public V get(final K key) {
        return index.get(key);
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<V> getAsync(final K key) {
        return runAsyncTracked(() -> get(key));
    }

    /** {@inheritDoc} */
    @Override
    public void delete(final K key) {
        index.delete(key);
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<Void> deleteAsync(final K key) {
        return runAsyncTracked(() -> {
            delete(key);
            return null;
        });
    }

    /** {@inheritDoc} */
    @Override
    public void compact() {
        index.compact();
    }

    /** {@inheritDoc} */
    @Override
    public void compactAndWait() {
        index.compactAndWait();
    }

    /** {@inheritDoc} */
    @Override
    public void flush() {
        index.flush();
    }

    /** {@inheritDoc} */
    @Override
    public void flushAndWait() {
        index.flushAndWait();
    }

    /** {@inheritDoc} */
    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindows) {
        return index.getStream(segmentWindows);
    }

    /** {@inheritDoc} */
    @Override
    public Stream<Entry<K, V>> getStream(final SegmentWindow segmentWindows,
            final SegmentIteratorIsolation isolation) {
        return index.getStream(segmentWindows, isolation);
    }

    /** {@inheritDoc} */
    @Override
    public void checkAndRepairConsistency() {
        index.checkAndRepairConsistency();
    }

    /** {@inheritDoc} */
    @Override
    public IndexConfiguration<K, V> getConfiguration() {
        return index.getConfiguration();
    }

    /** {@inheritDoc} */
    @Override
    public SegmentIndexState getState() {
        return index.getState();
    }

    /** {@inheritDoc} */
    @Override
    public SegmentIndexMetricsSnapshot metricsSnapshot() {
        return index.metricsSnapshot();
    }

    /** {@inheritDoc} */
    @Override
    public IndexControlPlane controlPlane() {
        return index.controlPlane();
    }

    /** {@inheritDoc} */
    @Override
    protected void doClose() {
        if (IndexAsyncExecutionContext.isAsyncOperationActive()) {
            throw new IllegalStateException(
                    "close() must not be called from an async index operation.");
        }
        if (shutdownExecutorOnClose) {
            executor.shutdown();
        }
        index.close();
    }

    private <T> CompletionStage<T> runAsyncTracked(final Supplier<T> task) {
        try {
            return CompletableFuture.supplyAsync(
                    () -> IndexAsyncExecutionContext.runInAsyncContext(task),
                    executor);
        } catch (final RejectedExecutionException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private static ExecutorService createExecutor(
            final IndexConfiguration<?, ?> conf) {
        final IndexConfiguration<?, ?> configuration = Vldtn.requireNonNull(conf,
                ARG_INDEX_CONFIGURATION);
        final int workerThreadCount = Vldtn.requireGreaterThanZero(
                Vldtn.requireNonNull(configuration.getIndexWorkerThreadCount(),
                        ARG_INDEX_WORKER_THREAD_COUNT),
                ARG_INDEX_WORKER_THREAD_COUNT);
        final int queueCapacity = Math.max(MIN_QUEUE_CAPACITY,
                workerThreadCount * QUEUE_CAPACITY_MULTIPLIER);
        final AtomicInteger threadCounter = new AtomicInteger(1);
        final ExecutorService delegate = new ThreadPoolExecutor(
                workerThreadCount, workerThreadCount, 0L,
                TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(queueCapacity),
                runnable -> {
                    final Thread thread = new Thread(runnable,
                            THREAD_NAME_PREFIX_INDEX_WORKER
                                    + threadCounter.getAndIncrement());
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
        if (!Boolean.TRUE.equals(configuration.isContextLoggingEnabled())) {
            return delegate;
        }
        return new IndexNameMdcExecutorService(
                Vldtn.requireNotBlank(configuration.getIndexName(),
                        ARG_INDEX_NAME),
                delegate);
    }
}
