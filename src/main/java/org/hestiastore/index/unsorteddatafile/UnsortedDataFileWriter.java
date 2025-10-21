package org.hestiastore.index.unsorteddatafile;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeWriter;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.Directory.Access;
import org.hestiastore.index.directory.FileWriter;

/**
 * Streaming writer that appends unsorted key/value pairs to a file using the
 * provided serializers.
 */
public class UnsortedDataFileWriter<K, V> implements PairWriter<K, V> {

    private final TypeWriter<K> keyWriter;
    private final TypeWriter<V> valueWriter;
    private final FileWriter fileWriter;

    /**
     * Creates a writer bound to the supplied directory and file.
     *
     * @param directory        target directory
     * @param fileName         file name to write to
     * @param keyWriter        serializer for keys
     * @param valueWriter      serializer for values
     * @param access           file access mode
     * @param diskIoBufferSize buffer size in bytes used when writing
     */
    public UnsortedDataFileWriter(final Directory directory,
            final String fileName, final TypeWriter<K> keyWriter,
            final TypeWriter<V> valueWriter, final Access access,
            final int diskIoBufferSize) {
        this.keyWriter = Vldtn.requireNonNull(keyWriter, "keyWriter");
        this.valueWriter = Vldtn.requireNonNull(valueWriter, "valueWriter");
        Vldtn.requireNonNull(directory, "directory");
        Vldtn.requireNonNull(fileName, "fileName");
        fileWriter = directory.getFileWriter(fileName, access,
                diskIoBufferSize);
    }

    /**
     * Serialises the supplied pair to the underlying file.
     *
     * @param pair key/value pair to write
     * @throws IllegalArgumentException if the pair or any component is null
     */
    @Override
    public void write(final Pair<K, V> pair) {
        Vldtn.requireNonNull(pair, "pair");
        Vldtn.requireNonNull(pair.getKey(), "key");
        Vldtn.requireNonNull(pair.getValue(), "value");
        keyWriter.write(fileWriter, pair.getKey());
        valueWriter.write(fileWriter, pair.getValue());
    }

    /**
     * Flushes and closes the underlying {@link FileWriter}.
     */
    @Override
    public void close() {
        fileWriter.close();
    }
}
