package org.hestiastore.index.chunkpairfile;

import java.util.Optional;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIteratorWithCurrent;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.Chunk;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.sorteddatafile.SortedDataFile;

/**
 * It allows to iterate over all pairs stored in one chunk.
 */
public class SingleChunkPairIterator<K, V>
        implements PairIteratorWithCurrent<K, V> {

    private static final String CHUNK_FILE_NAME = "chunk";

    private final MemDirectory directory = new MemDirectory();

    private final PairIteratorWithCurrent<K, V> iterator;

    /**
     * It creates an iterator over all pairs stored in the given chunk.
     *
     * @param chunk               required chunk to iterate over
     * @param keyTypeDescriptor   required type descriptor of keys
     * @param valueTypeDescriptor required type descriptor of values
     */
    public SingleChunkPairIterator(final Chunk chunk,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        Vldtn.requireNonNull(valueTypeDescriptor, "valueTypeDescriptor");
        Vldtn.requireNonNull(chunk, CHUNK_FILE_NAME);
        directory.setFileBytes(CHUNK_FILE_NAME, chunk.getPayload().getBytes());
        final SortedDataFile<K, V> sortedDataFile = SortedDataFile
                .<K, V>builder() //
                .withDirectory(directory) //
                .withFileName(CHUNK_FILE_NAME)//
                .withKeyTypeDescriptor(keyTypeDescriptor) //
                .withValueTypeDescriptor(valueTypeDescriptor) //
                .withDiskIoBufferSize(1024)//
                .build();
        this.iterator = sortedDataFile.openIterator();
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Pair<K, V> next() {
        return iterator.next();
    }

    @Override
    public void close() {
        iterator.close();
    }

    @Override
    public Optional<Pair<K, V>> getCurrent() {
        return iterator.getCurrent();
    }

}
