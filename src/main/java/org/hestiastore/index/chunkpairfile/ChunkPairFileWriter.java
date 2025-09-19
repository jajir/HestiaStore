package org.hestiastore.index.chunkpairfile;

import org.hestiastore.index.Pair;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkPayload;
import org.hestiastore.index.chunkstore.ChunkStoreWriter;
import org.hestiastore.index.datablockfile.CellPosition;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.sorteddatafile.SortedDataFileWriter;

public class ChunkPairFileWriter<K, V> implements SortedDataFileWriter<K, V> {

    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final ChunkStoreWriter chunkStoreWriter;

    private SingleChunkPairWriter<K, V> chunkPairWriter;

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

    @Override
    public long writeFull(final Pair<K, V> pair) {
        chunkPairWriter.put(pair);
        return flush();
    }

    /**
     * Flushes the current chunk pair writer.
     *
     * @return The position of the written chunk.
     */
    public long flush() {
        ChunkPayload payload = chunkPairWriter.close();
        chunkPairWriter = null; // reset for next write
        openNewChunkPairWriter();
        CellPosition chunkPosition = chunkStoreWriter.write(payload, 1);
        return chunkPosition.getValue();
    }

    @Override
    public void close() {
        chunkStoreWriter.close();
    }

    private void openNewChunkPairWriter() {
        if (this.chunkPairWriter != null) {
            throw new IllegalStateException(
                    "ChunkPairWriter is already set, cannot open new one.");
        }
        this.chunkPairWriter = new SingleChunkPairWriterImpl<>(
                keyTypeDescriptor, valueTypeDescriptor);
    }

}
