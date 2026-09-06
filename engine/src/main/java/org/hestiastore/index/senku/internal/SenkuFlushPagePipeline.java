package org.hestiastore.index.senku.internal;

import java.util.ArrayDeque;
import java.util.Deque;

import org.hestiastore.index.chunkstore.ChunkData;
import org.hestiastore.index.datatype.TypeDescriptor;

/**
 * Bounded parallel page preparation followed by single-owner ordered appends.
 * On failure every outstanding task is joined before its byte reservation is
 * released, so no worker can outlive the immutable detached flush batch.
 */
final class SenkuFlushPagePipeline<K, V> {
    private final SenkuFlushOrder<K, V> order;
    private final TypeDescriptor<K> keys;
    private final TypeDescriptor<V> values;
    private final SenkuStorageFormat format;
    private final int recordBytes;
    private final int pageLimit;
    private final Deque<SenkuFlushPageTask<K, V>> pending = new ArrayDeque<>();

    SenkuFlushPagePipeline(final SenkuFlushOrder<K, V> order,
            final TypeDescriptor<K> keys, final TypeDescriptor<V> values,
            final SenkuStorageFormat format, final int recordBytes,
            final int maxKeysPerPage) {
        this.order = order;
        this.keys = keys;
        this.values = values;
        this.format = format;
        this.recordBytes = recordBytes;
        pageLimit = Math.min(maxKeysPerPage,
                (SenkuFlushExecutor.PAGE_BYTES_BUDGET - 4096)
                        / (4 * recordBytes));
    }

    /**
     * Appends every nonempty shard and returns its first packed position. This
     * method does not commit the data or publish any metadata.
     */
    long[] write(final LargeFileWriterTx writer, final int[] starts,
            final int[] counts) {
        final long[] positions = new long[counts.length];
        final boolean[] started = new boolean[counts.length];
        try {
            for (int shard = 0; shard < counts.length; shard++) {
                int from = starts[shard];
                final int end = from + counts[shard];
                while (from < end) {
                    final int count = Math.min(pageLimit, end - from);
                    enqueue(new SenkuFlushPageTask<>(order, keys, values,
                            format, shard, from, count, recordBytes), writer,
                            positions, started);
                    from += count;
                }
            }
            while (!pending.isEmpty()) {
                appendFirst(writer, positions, started);
            }
            return positions;
        } catch (Exception failure) {
            joinAfterFailure(failure);
            throw failure;
        }
    }

    private void enqueue(final SenkuFlushPageTask<K, V> task,
            final LargeFileWriterTx writer, final long[] positions,
            final boolean[] started) {
        if (pending.size() >= SenkuFlushExecutor.parallelism()) {
            appendFirst(writer, positions, started);
        }
        final int bytes = task.reservedBytes();
        while (!pending.isEmpty() && !SenkuFlushExecutor.tryReserve(bytes)) {
            appendFirst(writer, positions, started);
        }
        if (pending.isEmpty()) {
            SenkuFlushExecutor.reserve(bytes);
        }
        try {
            SenkuFlushExecutor.submit(task);
            pending.addLast(task);
        } catch (Exception failure) {
            SenkuFlushExecutor.release(bytes);
            throw failure;
        }
    }

    private void appendFirst(final LargeFileWriterTx writer,
            final long[] positions, final boolean[] started) {
        final SenkuFlushPageTask<K, V> task = pending.removeFirst();
        try {
            final ChunkData page = task.join();
            final long position = writer.appendPreparedPage(page, task.count())
                    .getPacked();
            if (!started[task.shard()]) {
                positions[task.shard()] = position;
                started[task.shard()] = true;
            }
        } finally {
            SenkuFlushExecutor.release(task.reservedBytes());
        }
    }

    private void joinAfterFailure(final Exception primary) {
        while (!pending.isEmpty()) {
            final SenkuFlushPageTask<K, V> task = pending.removeFirst();
            try {
                task.join();
            } catch (Exception failure) {
                if (failure != primary) {
                    primary.addSuppressed(failure);
                }
            } finally {
                SenkuFlushExecutor.release(task.reservedBytes());
            }
        }
    }
}
