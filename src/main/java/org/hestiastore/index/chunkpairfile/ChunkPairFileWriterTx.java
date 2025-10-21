package org.hestiastore.index.chunkpairfile;

import org.hestiastore.index.Commitable;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkStoreWriterTx;
import org.hestiastore.index.datatype.TypeDescriptor;

/**
 * A transaction for writing chunk pair files.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class ChunkPairFileWriterTx<K, V> implements Commitable {

    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final ChunkStoreWriterTx chunkStoreWriterTx;

    /**
     * Constructs a new {@code ChunkPairFileWriterTx}.
     *
     * @param chunkStoreWriterTx  required chunk store writer transaction
     * @param keyTypeDescriptor   required type descriptor for keys
     * @param valueTypeDescriptor required type descriptor for values
     */
    public ChunkPairFileWriterTx(final ChunkStoreWriterTx chunkStoreWriterTx,
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
     * Opens a new chunk pair file writer.
     *
     * @return the opened chunk pair file writer
     */
    public ChunkPairFileWriter<K, V> openWriter() {
        return new ChunkPairFileWriter<>(chunkStoreWriterTx.open(),
                keyTypeDescriptor, valueTypeDescriptor);
    }

    @Override
    public void commit() {
        chunkStoreWriterTx.commit();
    }

}
