package com.coroptis.index.sorteddatafile;

import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairSeekableReader;
import com.coroptis.index.datatype.TypeReader;
import com.coroptis.index.directory.FileReaderSeekable;

public class PairSeekableReaderImpl<K, V> implements PairSeekableReader<K, V> {

    private final TypeReader<K> keyTypeReader;
    private final TypeReader<V> valueTypeReader;
    private final FileReaderSeekable reader;

    public PairSeekableReaderImpl(final TypeReader<K> keyReader,
            final TypeReader<V> valueReader,
            final FileReaderSeekable fileReader) {
        this.keyTypeReader = Objects.requireNonNull(keyReader);
        this.valueTypeReader = Objects.requireNonNull(valueReader);
        this.reader = Objects.requireNonNull(fileReader,
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
    public void seek(long position) {
        reader.seek(position);
    }

    @Override
    public void close() {
        reader.close();
    }

}
