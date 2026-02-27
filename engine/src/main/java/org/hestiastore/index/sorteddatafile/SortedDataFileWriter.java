package org.hestiastore.index.sorteddatafile;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeWriter;
import org.hestiastore.index.directory.FileWriter;

/**
 * Writes key-value entries into sorted data file. Keys must be in ascending
 * order.
 * 
 * @param <K> key type
 * @param <V> value type
 */
public class SortedDataFileWriter<K, V> extends AbstractCloseableResource
        implements EntryWriter<K, V> {

    private final TypeWriter<V> valueWriter;

    private final FileWriter fileWriter;

    private long position;

    private DiffKeyWriter<K> diffKeyWriter;

    /**
     * Creates writer for sorted data file.
     * 
     * @param valueWriter       required value writer
     * @param fileWriter        required file writer
     * @param keyTypeDescriptor required key type descriptor
     */
    public SortedDataFileWriter(final TypeWriter<V> valueWriter,
            final FileWriter fileWriter,
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.valueWriter = Vldtn.requireNonNull(valueWriter, "valueWriter");
        this.fileWriter = Vldtn.requireNonNull(fileWriter, "fileWriter");
        final TypeDescriptor<K> validatedTypeDescriptor = Vldtn
                .requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        this.diffKeyWriter = new DiffKeyWriter<>(
                Vldtn.requireNonNull(validatedTypeDescriptor.getTypeEncoder(),
                        "keyTypeEncoder"),
                Vldtn.requireNonNull(validatedTypeDescriptor.getComparator(),
                        "keyComparator"));
        position = 0;
    }

    /**
     * Writes the given key-value entry.
     *
     * @param entry required key-value entry
     */
    @Override
    public void write(final Entry<K, V> entry) {
        Vldtn.requireNonNull(entry, "entry");
        Vldtn.requireNonNull(entry.getKey(), "key");
        Vldtn.requireNonNull(entry.getValue(), "value");

        final int writtenBytesInKey = diffKeyWriter.writeTo(fileWriter,
                entry.getKey());
        final int writtenBytesInValue = valueWriter.write(fileWriter,
                entry.getValue());
        position = position + writtenBytesInKey + writtenBytesInValue;
    }

    /**
     * Closes the underlying file writer.
     */
    @Override
    protected void doClose() {
        fileWriter.close();
    }

}
