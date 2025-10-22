package org.hestiastore.index.chunkpairfile;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Pair;
import org.hestiastore.index.PairWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.CellPosition;
import org.hestiastore.index.chunkstore.ChunkPayload;
import org.hestiastore.index.chunkstore.ChunkStoreWriter;
import org.hestiastore.index.datatype.TypeDescriptor;

/**
 * A writer for chunk pair files. It writes key-value pairs to chunks and stores
 * them in a chunk store.
 *
 * @param <K> The type of keys.
 * @param <V> The type of values.
 */
public class ChunkPairFileWriter<K, V> extends AbstractCloseableResource
        implements PairWriter<K, V> {

    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final ChunkStoreWriter chunkStoreWriter;

    private SingleChunkPairWriter<K, V> chunkPairWriter;

    /**
     * Constructs a new ChunkPairFileWriter.
     *
     * @param chunkStoreWriter    required chunk store writer to write chunks
     *                            to.
     * @param keyTypeDescriptor   required type descriptor for keys.
     * @param valueTypeDescriptor required type descriptor for values.
     */
    ChunkPairFileWriter(final ChunkStoreWriter chunkStoreWriter,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.chunkStoreWriter = Vldtn.requireNonNull(chunkStoreWriter,
                "chunkStoreWriter");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        openNewChunkPairWriter();
    }

    @Override
    public void write(final Pair<K, V> pair) {
        chunkPairWriter.put(pair);
    }

    /**
     * Flushes the current chunk pair writer.
     *
     * @return The position of the written chunk.
     */
    public CellPosition flush() {
        final ChunkPayload payload = chunkPairWriter.close();
        chunkPairWriter = null; // reset for next write
        openNewChunkPairWriter();
        return chunkStoreWriter.write(payload, 1);
    }

    private void openNewChunkPairWriter() {
        if (this.chunkPairWriter != null) {
            throw new IllegalStateException(
                    "ChunkPairWriter is already set, cannot open new one.");
        }
        this.chunkPairWriter = new SingleChunkPairWriterImpl<>(
                keyTypeDescriptor, valueTypeDescriptor);
    }

    @Override
    protected void doClose() {
        if (chunkPairWriter != null) {
            chunkPairWriter.close();
            chunkPairWriter = null;
        }
        chunkStoreWriter.close();
    }

}
