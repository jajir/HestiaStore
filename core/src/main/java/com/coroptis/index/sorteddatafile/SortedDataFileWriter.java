package com.coroptis.index.sorteddatafile;

import java.util.Objects;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.Pair;
import com.coroptis.index.datatype.TypeWriter;
import com.coroptis.index.directory.FileWriter;

public class SortedDataFileWriter<K, V> implements CloseableResource {

    private final TypeWriter<V> valueWriter;

    private final FileWriter fileWriter;

    private final DiffKeyWriter<K> diffKeyWriter;

    private long position;

    public SortedDataFileWriter(final TypeWriter<V> valueWriter,
            final FileWriter fileWriter, final DiffKeyWriter<K> diffKeyWriter) {
        this.valueWriter = Objects.requireNonNull(valueWriter, "valueWriter is required");
        this.fileWriter = Objects.requireNonNull(fileWriter, "fileWriter is required");
        this.diffKeyWriter = Objects.requireNonNull(diffKeyWriter, "diffKeyWriter is required");
        position = 0;
    }

    /**
     * Allows to put new key value pair into index.
     *
     * @param pair      required key value pair
     * @param fullWrite when it's <code>true</code> than key is written whole
     *                  without shared part with previous key.
     * @return position of end of record.
     */
    private long put(final Pair<K, V> pair, final boolean fullWrite) {
        final int diffKeyLength = diffKeyWriter.write(pair.getKey(), fileWriter,
                fullWrite);

        final int writenBytesInValue = valueWriter.write(fileWriter,
                pair.getValue());

        long lastPosition = position;
        position = position + diffKeyLength + writenBytesInValue;
        return lastPosition;
    }

    /**
     * Writes the given key-value pair.
     *
     * @param pair required key-value pair
     */
    public void write(final Pair<K, V> pair) {
        put(pair, false);
    }

    /**
     * Writes the given key-value pair, forcing all data to be written.
     *
     * @param pair required key-value pair
     * @return position where will next data starts
     */
    public long writeFull(final Pair<K, V> pair) {
        return put(pair, true);
    }

    @Override
    public void close() {
        fileWriter.close();
    }

}
