package org.hestiastore.index.chunkentryfile;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryIteratorWithCurrent;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.CellPosition;
import org.hestiastore.index.chunkstore.ChunkStoreFile;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.TypeDescriptor;

/**
 * Object providing search functionality for a chunk store.
 */
public class ChunkStoreSearcher<K, V> {

    private final ChunkStoreFile chunkStoreFile;
    private final DataBlockSize dataBlockSize;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;

    /**
     * Constructor.
     *
     * @param chunkStoreFile      required chunk store file to search.
     * @param dataBlockSize       required data block size used in the chunk
     *                            store file.
     * @param keyTypeDescriptor   required type descriptor for keys.
     * @param valueTypeDescriptor required type descriptor for values.
     */
    public ChunkStoreSearcher(final ChunkStoreFile chunkStoreFile,
            final DataBlockSize dataBlockSize,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.chunkStoreFile = Vldtn.requireNonNull(chunkStoreFile,
                "chunkStoreFile");
        this.dataBlockSize = Vldtn.requireNonNull(dataBlockSize,
                "dataBlockSize");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
    }

    public EntryIterator<K, V> openIteratorAtPosition(final long position) {
        return new ChunkEntryFileIterator<>(
                chunkStoreFile.openReader(
                        CellPosition.of(dataBlockSize, (int) position)),
                chunk -> new SingleChunkEntryIterator<>(chunk, keyTypeDescriptor,
                        valueTypeDescriptor));
    }

    public EntryIteratorWithCurrent<K, V> openIterator() {
        return new ChunkEntryFileIterator<>(
                chunkStoreFile.openReader(CellPosition.of(dataBlockSize, 0)),
                chunk -> new SingleChunkEntryIterator<>(chunk, keyTypeDescriptor,
                        valueTypeDescriptor));
    }

}
