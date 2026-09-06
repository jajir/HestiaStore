package org.hestiastore.index.senku.internal;

import java.util.concurrent.RecursiveAction;

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
     * @param order    partitioned order
     * @param starts   shard start offsets
     * @param counts   shard entry counts
     * @param parallel whether the caller selected parallel execution
     * @param <K>      key type
     * @param <V>      value type
     */
    static <K, V> void sortAll(final SenkuFlushOrder<K, V> order,
            final int[] starts, final int[] counts, final boolean parallel) {
        if (parallel && parallelism() > 1 && counts.length > 1) {
            SenkuFlushExecutor.submit(new SenkuFlushSortTask<>(order, starts,
                    counts, 0, counts.length)).join();
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
        return SenkuFlushExecutor.parallelism();
    }

    @Override
    protected void compute() {
        if (toShard - fromShard <= 1) {
            final int from = starts[fromShard];
            order.sort(from, from + counts[fromShard]);
            return;
        }
        final int middle = (fromShard + toShard) >>> 1;
        invokeAll(
                new SenkuFlushSortTask<>(order, starts, counts, fromShard,
                        middle),
                new SenkuFlushSortTask<>(order, starts, counts, middle,
                        toShard));
    }

}
