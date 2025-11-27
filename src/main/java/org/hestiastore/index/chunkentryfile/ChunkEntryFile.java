package org.hestiastore.index.chunkentryfile;

import org.hestiastore.index.EntryIteratorWithCurrent;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.CellPosition;
import org.hestiastore.index.chunkstore.ChunkStoreFile;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.FileReaderSeekable;

/**
 * Class allows read and write entries from and to chunks.
 */
public class ChunkEntryFile<K, V> {

    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final ChunkStoreFile chunkStoreFile;
    private final DataBlockSize dataBlockSize;

    public ChunkEntryFile(final ChunkStoreFile chunkStoreFile,
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

    public ChunkEntryFileWriterTx<K, V> openWriterTx() {
        return new ChunkEntryFileWriterTx<>(chunkStoreFile.openWriteTx(),
                keyTypeDescriptor, valueTypeDescriptor);
    }

    public EntryIteratorWithCurrent<K, V> openIteratorAtPosition(
            final long position) {
        return new ChunkEntryFileIterator<>(
                chunkStoreFile.openReader(
                        CellPosition.of(dataBlockSize, (int) position)),
                chunk -> new SingleChunkEntryIterator<>(chunk, keyTypeDescriptor,
                        valueTypeDescriptor));
    }

    public EntryIteratorWithCurrent<K, V> openIteratorAtPosition(
            final long position, final FileReaderSeekable seekableReader) {
        return new ChunkEntryFileIterator<>(
                chunkStoreFile.openReader(
                        CellPosition.of(dataBlockSize, (int) position),
                        seekableReader),
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
