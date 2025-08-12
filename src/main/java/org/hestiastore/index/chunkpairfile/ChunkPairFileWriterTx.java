package org.hestiastore.index.chunkpairfile;

import org.hestiastore.index.Commitable;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkStoreWriterTx;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.sorteddatafile.SortedDataFileWriter;

public class ChunkPairFileWriterTx<K, V> implements Commitable {

    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final ChunkStoreWriterTx chunkStoreWriterTx;

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

    public SortedDataFileWriter<K, V> openWriter() {
        return new ChunkPairFileWriter<>(chunkStoreWriterTx.openWriter(),
                keyTypeDescriptor, valueTypeDescriptor);
    }

    @Override
    public void commit() {
        chunkStoreWriterTx.commit();
    }

}
