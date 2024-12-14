package com.coroptis.index.unsorteddatafile;

import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.datatype.TypeWriter;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.Directory.Access;
import com.coroptis.index.directory.FileWriter;

public class UnsortedDataFileWriter<K, V> implements PairWriter<K, V> {

    private final TypeWriter<K> keyWriter;
    private final TypeWriter<V> valueWriter;
    private final FileWriter fileWriter;

    public UnsortedDataFileWriter(final Directory directory,
            final String fileName, final TypeWriter<K> keyWriter,
            final TypeWriter<V> valueWriter, final Access access) {
        this.keyWriter = Objects.requireNonNull(keyWriter);
        this.valueWriter = Objects.requireNonNull(valueWriter);
        Objects.requireNonNull(directory);
        Objects.requireNonNull(fileName);
        fileWriter = directory.getFileWriter(fileName, access);
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
