package org.hestiastore.index.senku.internal;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sorts independent Senku shard ranges on a process-wide bounded executor.
 *
 * <p>
 * A shared pool caps aggregate flush sorting at four workers even when several
 * indexes flush concurrently. Its daemon workers do not extend application
 * lifecycle, and tasks hold only references to their already bounded flush
 * storage.
 * </p>
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SenkuFlushSortTask<K, V> extends RecursiveAction {

    private static final long serialVersionUID = 1L;
    private static final int MAX_PARALLELISM = 4;
    private static final int PARALLELISM = Math.max(1, Math.min(
            MAX_PARALLELISM, Runtime.getRuntime().availableProcessors()));
    private static final AtomicInteger NEXT_WORKER_ID = new AtomicInteger(1);
    private static final ForkJoinPool SORT_POOL = new ForkJoinPool(PARALLELISM,
            SenkuFlushSortTask::newWorker, null, false);

    private final SenkuFlushOrder<K, V> order;
    private final int[] starts;
    private final int[] counts;
    private final int fromShard;
    private final int toShard;

    private SenkuFlushSortTask(final SenkuFlushOrder<K, V> order,
            final int[] starts, final int[] counts, final int fromShard,
            final int toShard) {
        this.order = order;
        this.starts = starts;
        this.counts = counts;
        this.fromShard = fromShard;
        this.toShard = toShard;
    }

    /**
     * Sorts all shard ranges, using bounded parallelism for large flushes.
     *
     * @param order partitioned order
     * @param starts shard start offsets
     * @param counts shard entry counts
     * @param parallel whether the caller selected parallel execution
     * @param <K> key type
     * @param <V> value type
     */
    static <K, V> void sortAll(final SenkuFlushOrder<K, V> order,
            final int[] starts, final int[] counts, final boolean parallel) {
        if (parallel && PARALLELISM > 1 && counts.length > 1) {
            SORT_POOL.invoke(new SenkuFlushSortTask<>(order, starts, counts, 0,
                    counts.length));
            return;
        }
        for (int shardId = 0; shardId < counts.length; shardId++) {
            order.sort(starts[shardId], starts[shardId] + counts[shardId]);
        }
    }

    /**
     * Returns the hard-bounded worker count for diagnostics and tests.
     *
     * @return configured parallelism in the range 1..4
     */
    static int parallelism() {
        return PARALLELISM;
    }

    @Override
    protected void compute() {
        if (toShard - fromShard <= 1) {
            final int from = starts[fromShard];
            order.sort(from, from + counts[fromShard]);
            return;
        }
        final int middle = (fromShard + toShard) >>> 1;
        invokeAll(new SenkuFlushSortTask<>(order, starts, counts, fromShard,
                middle), new SenkuFlushSortTask<>(order, starts, counts, middle,
                        toShard));
    }

    private static ForkJoinWorkerThread newWorker(final ForkJoinPool pool) {
        final ForkJoinWorkerThread worker = ForkJoinPool
                .defaultForkJoinWorkerThreadFactory.newThread(pool);
        worker.setName("senku-flush-sort-" + NEXT_WORKER_ID.getAndIncrement());
        worker.setDaemon(true);
        return worker;
    }
}
