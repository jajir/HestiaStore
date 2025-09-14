package org.hestiastore.index.chunkpairfile;

import org.hestiastore.index.PairIterator;
import org.hestiastore.index.PairIteratorWithCurrent;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkStoreFile;
import org.hestiastore.index.chunkstore.ChunkStorePosition;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.sorteddatafile.SortedDataFileSearcher;

/**
 * Object providing search functionality for a chunk store.
 */
public class ChunkStoreSearcher<K, V> implements SortedDataFileSearcher<K, V> {

    private final ChunkStoreFile chunkStoreFile;
    private final int dataBlockSize;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;

    public ChunkStoreSearcher(final ChunkStoreFile chunkStoreFile,
            int dataBlockSize, final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.chunkStoreFile = Vldtn.requireNonNull(chunkStoreFile,
                "chunkStoreFile");
        this.dataBlockSize = dataBlockSize;
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
    }

    @Override
    public PairIterator<K, V> search(final long position) {
        return new ChunkPairFileIterator<>(
                chunkStoreFile.openReader(
                        ChunkStorePosition.of(dataBlockSize, (int) position)),
                chunk -> new SingleChunkPairIterator<>(chunk, keyTypeDescriptor,
                        valueTypeDescriptor));
    }

    @Override
    public PairIteratorWithCurrent<K, V> search() {
        return new ChunkPairFileIterator<>(
                chunkStoreFile
                        .openReader(ChunkStorePosition.of(dataBlockSize, 0)),
                chunk -> new SingleChunkPairIterator<>(chunk, keyTypeDescriptor,
                        valueTypeDescriptor));
    }

}
