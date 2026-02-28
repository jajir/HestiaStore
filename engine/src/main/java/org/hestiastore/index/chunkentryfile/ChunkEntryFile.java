package org.hestiastore.index.chunkentryfile;

import java.util.Comparator;

import org.hestiastore.index.EntryIteratorWithCurrent;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.chunkstore.CellPosition;
import org.hestiastore.index.chunkstore.ChunkStoreFile;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datablockfile.Reader;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.directory.FileReaderSeekable;
import org.hestiastore.index.directory.MemFileReader;
import org.hestiastore.index.sorteddatafile.DiffKeyReader;

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
        return ChunkEntryFileIterator.fromPayloads(
                chunkStoreFile.openReader(
                        CellPosition.of(dataBlockSize, (int) position)),
                payload -> new SingleChunkEntryIterator<>(payload, keyTypeDescriptor,
                        valueTypeDescriptor));
    }

    public EntryIteratorWithCurrent<K, V> openIteratorAtPosition(
            final long position, final FileReaderSeekable seekableReader) {
        return ChunkEntryFileIterator.fromPayloads(
                chunkStoreFile.openReader(
                        CellPosition.of(dataBlockSize, (int) position),
                        seekableReader),
                payload -> new SingleChunkEntryIterator<>(payload, keyTypeDescriptor,
                        valueTypeDescriptor));
    }

    public EntryIteratorWithCurrent<K, V> openIterator() {
        return ChunkEntryFileIterator.fromPayloads(
                chunkStoreFile.openReader(CellPosition.of(dataBlockSize, 0)),
                payload -> new SingleChunkEntryIterator<>(payload, keyTypeDescriptor,
                        valueTypeDescriptor));
    }

    /**
     * Searches sorted entries starting at a specific on-disk position.
     *
     * <p>
     * This method is optimized for point lookups and avoids the close cascade of
     * iterator wrappers. The lifecycle of {@code seekableReader} is owned by the
     * caller.
     * </p>
     *
     * @param key target key to find
     * @param position start position in cells
     * @param maxEntries maximum number of entries to scan
     * @param keyComparator comparator used for key ordering
     * @param seekableReader externally managed seekable reader
     * @return value when key is found; otherwise {@code null}
     */
    public V searchAtPosition(final K key, final long position,
            final int maxEntries, final Comparator<K> keyComparator,
            final FileReaderSeekable seekableReader) {
        final K resolvedKey = Vldtn.requireNonNull(key, "key");
        final int resolvedMaxEntries = Vldtn.requireGreaterThanZero(maxEntries,
                "maxEntries");
        final Comparator<K> resolvedKeyComparator = Vldtn
                .requireNonNull(keyComparator, "keyComparator");
        final long resolvedPosition = Vldtn.requireGreaterThanOrEqualToZero(
                position, "position");
        final FileReaderSeekable resolvedSeekableReader = Vldtn
                .requireNonNull(seekableReader, "seekableReader");

        final Reader<ByteSequence> payloadReader = chunkStoreFile
                .openPayloadReader(
                        CellPosition.of(dataBlockSize, (int) resolvedPosition),
                        resolvedSeekableReader);
        int scannedEntries = 0;

        while (scannedEntries < resolvedMaxEntries) {
            final ByteSequence payload = payloadReader.read();
            if (payload == null) {
                return null;
            }
            final MemFileReader payloadReaderCursor = new MemFileReader(payload);
            final DiffKeyReader<K> keyReader = new DiffKeyReader<>(
                    keyTypeDescriptor.getTypeDecoder());
            final TypeReader<V> valueReader = valueTypeDescriptor
                    .getTypeReader();
            while (scannedEntries < resolvedMaxEntries) {
                final K currentKey = keyReader.read(payloadReaderCursor);
                if (currentKey == null) {
                    break;
                }
                final int cmp = resolvedKeyComparator.compare(currentKey,
                        resolvedKey);
                if (cmp > 0) {
                    return null;
                }
                final V currentValue = valueReader.read(payloadReaderCursor);
                if (cmp == 0) {
                    return currentValue;
                }
                scannedEntries++;
            }
        }
        return null;
    }

}
