package org.hestiastore.index.unsorteddatafile;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeWriter;
import org.hestiastore.index.directory.Directory.Access;
import org.hestiastore.index.directory.DirectoryFacade;
import org.hestiastore.index.directory.FileWriter;
import org.hestiastore.index.directory.async.AsyncFileWriterBlockingAdapter;

/**
 * Streaming writer that appends unsorted key/value entries to a file using the
 * provided serializers.
 */
public class UnsortedDataFileWriter<K, V> extends AbstractCloseableResource
        implements EntryWriter<K, V> {

    private final TypeWriter<K> keyWriter;
    private final TypeWriter<V> valueWriter;
    private final FileWriter fileWriter;

    /**
     * Creates a writer bound to the supplied directory and file.
     *
     * @param directoryFacade  target directory facade
     * @param fileName         file name to write to
     * @param keyWriter        serializer for keys
     * @param valueWriter      serializer for values
     * @param access           file access mode
     * @param diskIoBufferSize buffer size in bytes used when writing
     */
    public UnsortedDataFileWriter(final DirectoryFacade directoryFacade,
            final String fileName, final TypeWriter<K> keyWriter,
            final TypeWriter<V> valueWriter, final Access access,
            final int diskIoBufferSize) {
        this.keyWriter = Vldtn.requireNonNull(keyWriter, "keyWriter");
        this.valueWriter = Vldtn.requireNonNull(valueWriter, "valueWriter");
        Vldtn.requireNonNull(directoryFacade, "directoryFacade");
        Vldtn.requireNonNull(fileName, "fileName");
        fileWriter = new AsyncFileWriterBlockingAdapter(
                directoryFacade.getFileWriterAsync(fileName, access,
                        diskIoBufferSize).toCompletableFuture().join());
    }

    /**
     * Serialises the supplied entry to the underlying file.
     *
     * @param entry key/value entry to write
     * @throws IllegalArgumentException if the entry or any component is null
     */
    @Override
    public void write(final Entry<K, V> entry) {
        Vldtn.requireNonNull(entry, "entry");
        Vldtn.requireNonNull(entry.getKey(), "key");
        Vldtn.requireNonNull(entry.getValue(), "value");
        keyWriter.write(fileWriter, entry.getKey());
        valueWriter.write(fileWriter, entry.getValue());
    }

    /**
     * Flushes and closes the underlying {@link FileWriter}.
     */
    @Override
    protected void doClose() {
        fileWriter.close();
    }
}
