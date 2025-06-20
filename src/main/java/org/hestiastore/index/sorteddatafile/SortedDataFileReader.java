package org.hestiastore.index.sorteddatafile;

import org.hestiastore.index.CloseablePairReader;
import org.hestiastore.index.Pair;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.directory.FileReader;

public class SortedDataFileReader<K, V> implements CloseablePairReader<K, V> {

    private final TypeReader<K> keyTypeReader;
    private final TypeReader<V> valueTypeReader;
    private final FileReader reader;

    SortedDataFileReader(final TypeReader<K> keyReader,
            final TypeReader<V> valueReader, final FileReader reader) {
        this.keyTypeReader = Vldtn.requireNonNull(keyReader, "keyReader");
        this.valueTypeReader = Vldtn.requireNonNull(valueReader, "valueReader");
        this.reader = Vldtn.requireNonNull(reader, "reader");
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
