package com.hestiastore.index.unsorteddatafile;

import java.util.Objects;

import com.hestiastore.index.CloseablePairReader;
import com.hestiastore.index.Pair;
import com.hestiastore.index.datatype.TypeReader;
import com.hestiastore.index.directory.FileReader;

public class UnsortedDataFileReader<K, V> implements CloseablePairReader<K, V> {

    private final TypeReader<K> keyTypeReader;
    private final TypeReader<V> valueTypeReader;
    private final FileReader reader;

    UnsortedDataFileReader(final TypeReader<K> keyTypeReader,
            final TypeReader<V> valueTypeReader, final FileReader reader) {
        this.keyTypeReader = Objects.requireNonNull(keyTypeReader,
                "Key type reader can't be null.");
        this.valueTypeReader = Objects.requireNonNull(valueTypeReader,
                "Value type reader can't be null.");
        this.reader = Objects.requireNonNull(reader,
                "File reader can't be null.");
    }

    @Override
    public Pair<K, V> read() {
        final K key = keyTypeReader.read(reader);
        if (key == null) {
            return null;
        } else {
            final V value = valueTypeReader.read(reader);
            return new Pair<K, V>(key, value);
        }
    }

    @Override
    public void close() {
        reader.close();
    }

}
