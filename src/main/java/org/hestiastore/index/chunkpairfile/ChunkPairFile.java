package org.hestiastore.index.chunkpairfile;

import org.hestiastore.index.PairIterator;
import org.hestiastore.index.PairIteratorWithCurrent;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkStoreFile;
import org.hestiastore.index.datablockfile.CellPosition;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segment.SegmentIndexSearcher;
import org.hestiastore.index.sorteddatafile.SortedDataFileSearcher;

/**
 * Class allows read and write pairs from and to chunks.
 */
public class ChunkPairFile<K, V> implements SortedDataFileSearcher<K, V> {

    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final ChunkStoreFile chunkStoreFile;
    private final DataBlockSize dataBlockSize;

    public ChunkPairFile(final ChunkStoreFile chunkStoreFile,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final DataBlockSize dataBlockSize) {
        this.chunkStoreFile = Vldtn.requireNonNull(chunkStoreFile,
                "chunkStoreFile");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.dataBlockSize = Vldtn.requireNonNull(dataBlockSize,
                "dataBlockSize");
    }

    public ChunkPairFileWriterTx<K, V> openWriterTx() {
        return new ChunkPairFileWriterTx<>(chunkStoreFile.openWriteTx(),
                keyTypeDescriptor, valueTypeDescriptor);
    }

    public PairIteratorWithCurrent<K, V> openIterator() {
        return search();
    }

    // TODO rename it to openIteratorAtPosition
    @Override
    public PairIterator<K, V> search(final long position) {
        return new ChunkPairFileIterator<>(
                chunkStoreFile.openReader(
                        CellPosition.of(dataBlockSize, (int) position)),
                chunk -> new SingleChunkPairIterator<>(chunk, keyTypeDescriptor,
                        valueTypeDescriptor));
    }

    @Override
    public PairIteratorWithCurrent<K, V> search() {
        return new ChunkPairFileIterator<>(
                chunkStoreFile.openReader(CellPosition.of(dataBlockSize, 0)),
                chunk -> new SingleChunkPairIterator<>(chunk, keyTypeDescriptor,
                        valueTypeDescriptor));
    }

    public SegmentIndexSearcher<K, V> getSegmentIndexSearcher(
            final int maxNumberOfKeysInIndexChunk) {
        return new ChunkPairFileSegmentIndexSearcher<>(getChunkStoreSearcher(),
                maxNumberOfKeysInIndexChunk, keyTypeDescriptor.getComparator());
    }

    private ChunkStoreSearcher<K, V> getChunkStoreSearcher() {
        return new ChunkStoreSearcher<>(chunkStoreFile, dataBlockSize,
                keyTypeDescriptor, valueTypeDescriptor);
    }

}
