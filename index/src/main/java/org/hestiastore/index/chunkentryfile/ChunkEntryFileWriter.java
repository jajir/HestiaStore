package org.hestiastore.index.chunkentryfile;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.CellPosition;
import org.hestiastore.index.chunkstore.ChunkPayload;
import org.hestiastore.index.chunkstore.ChunkStoreWriter;
import org.hestiastore.index.datatype.TypeDescriptor;

/**
 * A writer for chunk entry files. It writes key-value entries to chunks and stores
 * them in a chunk store.
 *
 * @param <K> The type of keys.
 * @param <V> The type of values.
 */
public class ChunkEntryFileWriter<K, V> extends AbstractCloseableResource
        implements EntryWriter<K, V> {

    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final ChunkStoreWriter chunkStoreWriter;

    private SingleChunkEntryWriter<K, V> chunkEntryWriter;

    /**
     * Constructs a new ChunkEntryFileWriter.
     *
     * @param chunkStoreWriter    required chunk store writer to write chunks
     *                            to.
     * @param keyTypeDescriptor   required type descriptor for keys.
     * @param valueTypeDescriptor required type descriptor for values.
     */
    ChunkEntryFileWriter(final ChunkStoreWriter chunkStoreWriter,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.chunkStoreWriter = Vldtn.requireNonNull(chunkStoreWriter,
                "chunkStoreWriter");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        openNewChunkEntryWriter();
    }

    @Override
    public void write(final Entry<K, V> entry) {
        chunkEntryWriter.put(entry);
    }

    /**
     * Flushes the current chunk entry writer.
     *
     * @return The position of the written chunk.
     */
    public CellPosition flush() {
        final ChunkPayload payload = chunkEntryWriter.close();
        chunkEntryWriter = null; // reset for next write
        openNewChunkEntryWriter();
        return chunkStoreWriter.write(payload, 1);
    }

    private void openNewChunkEntryWriter() {
        if (this.chunkEntryWriter != null) {
            throw new IllegalStateException(
                    "ChunkEntryWriter is already set, cannot open new one.");
        }
        this.chunkEntryWriter = new SingleChunkEntryWriterImpl<>(
                keyTypeDescriptor, valueTypeDescriptor);
    }

    @Override
    protected void doClose() {
        if (chunkEntryWriter != null) {
            chunkEntryWriter.close();
            chunkEntryWriter = null;
        }
        chunkStoreWriter.close();
    }

}
