package org.hestiastore.index.chunkentryfile;

import java.util.Optional;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorWithCurrent;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.Chunk;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.sorteddatafile.SortedDataFile;

/**
 * It allows to iterate over all entries stored in one chunk.
 */
public class SingleChunkEntryIterator<K, V>
        extends AbstractCloseableResource implements EntryIteratorWithCurrent<K, V> {

    private static final String CHUNK_FILE_NAME = "chunk";

    private final MemDirectory directory = new MemDirectory();

    private final EntryIteratorWithCurrent<K, V> iterator;

    /**
     * It creates an iterator over all entries stored in the given chunk.
     *
     * @param chunk               required chunk to iterate over
     * @param keyTypeDescriptor   required type descriptor of keys
     * @param valueTypeDescriptor required type descriptor of values
     */
    public SingleChunkEntryIterator(final Chunk chunk,
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
    public Entry<K, V> next() {
        return iterator.next();
    }

    @Override
    public Optional<Entry<K, V>> getCurrent() {
        return iterator.getCurrent();
    }

    @Override
    protected void doClose() {
        iterator.close();
    }

}
