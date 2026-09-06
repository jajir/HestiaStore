package org.hestiastore.index.senku.internal;

import java.util.concurrent.RecursiveTask;

import org.hestiastore.index.chunkentryfile.SingleChunkEntryWriterImpl;
import org.hestiastore.index.chunkstore.ChunkData;
import org.hestiastore.index.chunkstore.ChunkFilterZstdCompress;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.chunkstore.ChunkHeader;
import org.hestiastore.index.datatype.TypeDescriptor;

/**
 * Encodes and compresses one independent sorted long-key page. It owns no file
 * resource; the flush owner joins tasks and appends their results in shard
 * order. Only fixed-size built-in key/value descriptors use this bounded path.
 */
final class SenkuFlushPageTask<K, V> extends RecursiveTask<ChunkData> {
    private static final long serialVersionUID = 1L;
    private final SenkuFlushOrder<K, V> order;
    private final TypeDescriptor<K> keys;
    private final TypeDescriptor<V> values;
    private final SenkuStorageFormat format;
    private final int shard;
    private final int from;
    private final int count;
    private final int recordBytes;

    SenkuFlushPageTask(final SenkuFlushOrder<K, V> order,
            final TypeDescriptor<K> keys, final TypeDescriptor<V> values,
            final SenkuStorageFormat format, final int shard, final int from,
            final int count, final int recordBytes) {
        this.order = order;
        this.keys = keys;
        this.values = values;
        this.format = format;
        this.shard = shard;
        this.from = from;
        this.count = count;
        this.recordBytes = recordBytes;
    }

    int shard() {
        return shard;
    }

    int count() {
        return count;
    }

    /**
     * Bounds encoded growth, materialized input and compression output with
     * ample slack. Native Zstd context overhead is separately bounded by the
     * shared worker count; this reservation bounds Java page working bytes.
     */
    int reservedBytes() {
        return estimatedBytes(count, recordBytes);
    }

    static int estimatedBytes(final int count, final int recordBytes) {
        return Math.toIntExact(4096L + 4L * count * recordBytes);
    }

    @Override
    protected ChunkData compute() {
        final SingleChunkEntryWriterImpl<K, V> writer = new SingleChunkEntryWriterImpl<>(
                keys, values, Math.multiplyExact(count, recordBytes),
                format.keyCodec());
        for (int index = from; index < from + count; index++) {
            writer.putLongKey(order.longKey(index), order.value(index));
        }
        final ChunkData page = ChunkData.ofSequence(0L, 0L,
                ChunkHeader.MAGIC_NUMBER, format.keyCodec().getId(),
                writer.closeSequence());
        final ChunkData encoded = format.compression().getLevel() == 0 ? page
                : new ChunkFilterZstdCompress(format.compression().getLevel())
                        .apply(page);
        return new ChunkFilterMagicNumberWriting().apply(encoded);
    }
}
