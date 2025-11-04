package org.hestiastore.index.chunkentryfile;

import org.hestiastore.index.Commitable;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkStoreWriterTx;
import org.hestiastore.index.datatype.TypeDescriptor;

/**
 * A transaction for writing chunk entry files.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class ChunkEntryFileWriterTx<K, V> implements Commitable {

    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final ChunkStoreWriterTx chunkStoreWriterTx;

    /**
     * Constructs a new {@code ChunkEntryFileWriterTx}.
     *
     * @param chunkStoreWriterTx  required chunk store writer transaction
     * @param keyTypeDescriptor   required type descriptor for keys
     * @param valueTypeDescriptor required type descriptor for values
     */
    public ChunkEntryFileWriterTx(final ChunkStoreWriterTx chunkStoreWriterTx,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.chunkStoreWriterTx = Vldtn.requireNonNull(chunkStoreWriterTx,
                "chunkStoreWriterTx");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
    }

    /**
     * Opens a new chunk entry file writer.
     *
     * @return the opened chunk entry file writer
     */
    public ChunkEntryFileWriter<K, V> openWriter() {
        return new ChunkEntryFileWriter<>(chunkStoreWriterTx.open(),
                keyTypeDescriptor, valueTypeDescriptor);
    }

    @Override
    public void commit() {
        chunkStoreWriterTx.commit();
    }

}
