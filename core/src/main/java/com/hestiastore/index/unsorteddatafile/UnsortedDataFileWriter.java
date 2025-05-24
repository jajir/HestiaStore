package com.hestiastore.index.unsorteddatafile;

import java.util.Objects;

import com.hestiastore.index.Pair;
import com.hestiastore.index.PairWriter;
import com.hestiastore.index.datatype.TypeWriter;
import com.hestiastore.index.directory.Directory;
import com.hestiastore.index.directory.FileWriter;
import com.hestiastore.index.directory.Directory.Access;

public class UnsortedDataFileWriter<K, V> implements PairWriter<K, V> {

    private final TypeWriter<K> keyWriter;
    private final TypeWriter<V> valueWriter;
    private final FileWriter fileWriter;

    public UnsortedDataFileWriter(final Directory directory,
            final String fileName, final TypeWriter<K> keyWriter,
            final TypeWriter<V> valueWriter, final Access access,
            final int diskIoBufferSize) {
        this.keyWriter = Objects.requireNonNull(keyWriter);
        this.valueWriter = Objects.requireNonNull(valueWriter);
        Objects.requireNonNull(directory);
        Objects.requireNonNull(fileName);
        fileWriter = directory.getFileWriter(fileName, access,
                diskIoBufferSize);
    }

    @Override
    public void put(final Pair<K, V> pair) {
        Objects.requireNonNull(pair.getKey());
        Objects.requireNonNull(pair.getValue());
        keyWriter.write(fileWriter, pair.getKey());
        valueWriter.write(fileWriter, pair.getValue());
    }

    @Override
    public void close() {
        fileWriter.close();
    }
}
