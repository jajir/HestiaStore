package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.datatype.NullValue.NULL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.chunkentryfile.KeyPageCodecs;
import org.hestiastore.index.chunkentryfile.LongKeyPageReader;
import org.hestiastore.index.chunkstore.ChunkData;
import org.hestiastore.index.chunkstore.ChunkFilterZstdDecompress;
import org.hestiastore.index.chunkstore.Compression;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.directory.MemFileReader;
import org.junit.jupiter.api.Test;

class SenkuFlushPageTaskTest {
    @Test
    void independentRawAndCompressedPagesRoundTrip() {
        for (final Compression compression : new Compression[] {
                Compression.none(), Compression.zstd(3) }) {
            final SenkuFlushOrder<Long, NullValue> order = order();
            final SenkuStorageFormat format = new SenkuStorageFormat(
                    KeyPageCodecs.longDeltaVarint(), compression);
            final SenkuFlushPageTask<Long, NullValue> task = new SenkuFlushPageTask<>(
                    order, new TypeDescriptorLong(), new TypeDescriptorNull(),
                    format, 2, 0, 100, 10);
            final ChunkData page = new ChunkFilterZstdDecompress()
                    .apply(task.invoke());
            try (MemFileReader input = new MemFileReader(
                    page.getPayloadSequence().toByteArray())) {
                final LongKeyPageReader reader = new LongKeyPageReader(
                        format.keyCodec());
                for (long key = 0; key < 100; key++) {
                    assertEquals(Long.valueOf(key), reader.read(input));
                }
                assertEquals(null, reader.read(input));
            }
            assertEquals(2, task.shard());
            assertEquals(100, task.count());
            assertTrue(
                    task.reservedBytes() >= page.getPayloadSequence().length());
        }
    }

    @Test
    void pageEncodingFailurePropagatesThroughTask() {
        final SenkuFlushOrder<Long, NullValue> order = order();
        order.set(1, 0L, NULL);
        final SenkuFlushPageTask<Long, NullValue> task = new SenkuFlushPageTask<>(
                order, new TypeDescriptorLong(), new TypeDescriptorNull(),
                new SenkuStorageFormat(KeyPageCodecs.longDeltaVarint(),
                        Compression.zstd(3)),
                0, 0, 100, 10);
        assertThrows(IllegalArgumentException.class, task::invoke);
    }

    private SenkuFlushOrder<Long, NullValue> order() {
        final SenkuFlushOrder<Long, NullValue> order = new SenkuFlushOrder<>(
                new TypeDescriptorLong(), new TypeDescriptorNull(), 100);
        for (int key = 0; key < 100; key++) {
            order.set(key, (long) key, NULL);
        }
        return order;
    }
}
