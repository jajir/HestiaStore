package org.hestiastore.index.sorteddatafile;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairSeekableReader;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.directory.FileReaderSeekable;

public class PairSeekableReaderImpl<K, V> implements PairSeekableReader<K, V> {

    private final TypeReader<K> keyTypeReader;
    private final TypeReader<V> valueTypeReader;
    private final FileReaderSeekable reader;

    public PairSeekableReaderImpl(final TypeReader<K> keyReader,
            final TypeReader<V> valueReader,
            final FileReaderSeekable fileReader) {
        this.keyTypeReader = Vldtn.requireNonNull(keyReader, "keyReader");
        this.valueTypeReader = Vldtn.requireNonNull(valueReader, "valueReader");
        this.reader = Vldtn.requireNonNull(fileReader, "valueReader");
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
