package com.hestiastore.index.sorteddatafile;

import java.util.Objects;

import com.hestiastore.index.CloseablePairReader;
import com.hestiastore.index.Pair;
import com.hestiastore.index.datatype.TypeReader;
import com.hestiastore.index.directory.FileReader;

public class SortedDataFileReader<K, V> implements CloseablePairReader<K, V> {

    private final TypeReader<K> keyTypeReader;
    private final TypeReader<V> valueTypeReader;
    private final FileReader reader;

    SortedDataFileReader(final TypeReader<K> keyReader,
            final TypeReader<V> valueReader, final FileReader reader) {
        this.keyTypeReader = Objects.requireNonNull(keyReader);
        this.valueTypeReader = Objects.requireNonNull(valueReader);
        this.reader = Objects.requireNonNull(reader);
    }

    public void skip(final long position) {
        reader.skip(position);
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
